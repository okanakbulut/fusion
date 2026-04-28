"""Targeted tests to cover missed branches across ORM modules.

These tests exercise specific edge-case paths that weren't triggered by the
primary feature tests: error paths, empty conditions, less-common branches.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from fusion.orm.column import Column, Condition
from fusion.orm.conditions import Q
from fusion.orm.constraints import ForeignKey
from fusion.orm.expressions import Exp, cte, union
from fusion.orm.fields import field
from fusion.orm.model import Model

# ---------------------------------------------------------------------------
# Shared models
# ---------------------------------------------------------------------------


class User(Model):
    id: int | None = None
    email: str


class Post(Model):
    id: int | None = None
    user_id: int
    title: str

    __constraints__ = [ForeignKey("user_id", User)]


class Category(Model):
    id: int | None = None
    name: str
    parent_id: int | None = None


# ---------------------------------------------------------------------------
# model.py — _pluralize "y" branch → categories
# ---------------------------------------------------------------------------


def test_pluralize_y_suffix():
    assert Category.__table_name__ == "categories"


def test_pluralize_es_suffix():
    class Box(Model):
        id: int | None = None

    assert Box.__table_name__ == "boxes"


# ---------------------------------------------------------------------------
# model.py — class-level access for attribute NOT in __fields__
# ---------------------------------------------------------------------------


def test_class_level_non_field_access_returns_normally():
    # __table_name__ is not a field — __getattribute__ should pass through
    assert Post.__table_name__ == "posts"


# ---------------------------------------------------------------------------
# column.py — hash method
# ---------------------------------------------------------------------------


def test_column_is_hashable():
    col = Column("email", table="users")
    assert hash(col) == hash(("email", "users"))


def test_column_can_be_used_in_set():
    cols = {Column("a"), Column("b"), Column("a")}
    assert len(cols) == 2


# ---------------------------------------------------------------------------
# conditions.py — Q with a Q as positional arg
# ---------------------------------------------------------------------------


def test_q_with_q_positional_arg():
    inner = Q(user_id=1)
    outer = Q(inner)
    # inner Q is added as child
    assert len(outer.children) == 1
    assert outer.children[0] is inner


# ---------------------------------------------------------------------------
# query.py — unknown join alias raises ValueError
# ---------------------------------------------------------------------------


def test_unknown_alias_raises():
    with pytest.raises(ValueError, match="Unknown join alias"):
        Post.select().where(ghost__col=5).build()


# ---------------------------------------------------------------------------
# query.py — _build_criterion with empty conditions returns None
# (exercised via Q with no conditions and no children)
# ---------------------------------------------------------------------------


def test_q_with_no_conditions_produces_no_where():
    empty_q = Q()
    sql, _ = Post.select().where(empty_q).build()
    assert "WHERE" not in sql


def test_update_with_empty_q_produces_no_where():
    empty_q = Q()
    sql, _ = Post.update().set(title="x").where(empty_q).build()
    assert "WHERE" not in sql


def test_delete_with_empty_q_produces_no_where():
    empty_q = Q()
    sql, _ = Post.delete().where(empty_q).build()
    assert "WHERE" not in sql


# ---------------------------------------------------------------------------
# query.py — _where_arg_to_criterion with raw Condition
# ---------------------------------------------------------------------------


def test_where_with_condition_object_directly():
    cond = Condition(column="user_id", lookup="eq", value=1)
    sql, params = Post.select().where(cond).build()
    assert "user_id" in sql
    assert params == [1]


# ---------------------------------------------------------------------------
# query.py — join without FK → no ON clause
# ---------------------------------------------------------------------------


def test_join_without_fk_produces_join_without_on():
    class Tag(Model):
        id: int | None = None
        name: str

    sql, _ = Post.select().join(Tag).build()
    assert "JOIN" in sql
    # No ForeignKey declared, so no ON clause — pypika emits cross join style
    assert "tags" in sql


# ---------------------------------------------------------------------------
# query.py — .exists() and ~exists()
# ---------------------------------------------------------------------------


def test_exists_returns_exists_expression():
    from fusion.orm.query import ExistsExpression

    e = Post.select().where(user_id=1).exists()
    assert isinstance(e, ExistsExpression)


def test_exists_invert():
    from fusion.orm.query import ExistsExpression

    e = ~Post.select().where(user_id=1).exists()
    assert isinstance(e, ExistsExpression)
    assert e._negated is True


# ---------------------------------------------------------------------------
# query.py — UpdateQuery.where() with Q positional arg
# ---------------------------------------------------------------------------


def test_update_where_with_q_positional():
    sql, _ = Post.update().set(title="x").where(Q(user_id=1)).build()
    assert "user_id" in sql


# ---------------------------------------------------------------------------
# query.py — DeleteQuery.where() with Q positional arg
# ---------------------------------------------------------------------------


def test_delete_where_with_q_positional():
    sql, _ = Post.delete().where(Q(user_id=1)).build()
    assert "user_id" in sql


# ---------------------------------------------------------------------------
# query.py — _infer_join_on returns None when no FK matches
# ---------------------------------------------------------------------------


def test_infer_join_on_returns_none_for_no_fk():
    from pypika import Table

    from fusion.orm.query import _infer_join_on

    source = Table("posts")
    target = Table("users")

    class NoFK(Model):
        id: int | None = None
        name: str

    result = _infer_join_on(NoFK, User, source, target)
    assert result is None


# ---------------------------------------------------------------------------
# expressions.py — UnionQuery.fetch() and CTEQuery.fetch()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_union_fetch_calls_conn():
    conn = AsyncMock()
    record = MagicMock()
    record.items = MagicMock(return_value=[("id", 1), ("title", "hi")])
    conn.fetch = AsyncMock(return_value=[record])

    q = union(
        Post.select("id", "title").where(user_id=1),
        Post.select("id", "title").where(user_id=2),
    )
    result = await q.fetch(conn)
    conn.fetch.assert_called_once()
    assert isinstance(result, list)


@pytest.mark.asyncio
async def test_cte_fetch_calls_conn():
    conn = AsyncMock()
    record = MagicMock()
    record.items = MagicMock(return_value=[("id", 1), ("title", "hi")])
    conn.fetch = AsyncMock(return_value=[record])

    q = cte(
        main=Post.select("id").where(user_id=1),
        recent=Post.select().where(user_id=1),
    )
    result = await q.fetch(conn)
    conn.fetch.assert_called_once()
    assert isinstance(result, list)


# ---------------------------------------------------------------------------
# query.py — SelectQuery.fetch() returns model instances (not raw)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_select_fetch_returns_model_instances():
    conn = AsyncMock()
    row = {"id": 1, "user_id": 1, "title": "hello"}
    record = MagicMock()
    record.items = MagicMock(return_value=list(row.items()))
    conn.fetch = AsyncMock(return_value=[record])

    results = await Post.select().where(user_id=1).fetch(conn)
    assert len(results) == 1
    assert isinstance(results[0], Post)
    assert results[0].title == "hello"


@pytest.mark.asyncio
async def test_select_fetch_one_returns_model_instance():
    conn = AsyncMock()
    row = {"id": 1, "user_id": 1, "title": "hello"}
    record = MagicMock()
    record.items = MagicMock(return_value=list(row.items()))
    conn.fetchrow = AsyncMock(return_value=record)

    result = await Post.select().where(id=1).fetch_one(conn)
    assert isinstance(result, Post)
    assert result.id == 1
