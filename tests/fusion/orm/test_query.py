"""Tests for fusion.orm.query — SQL generation for all four query builders.

SQL strings are verified via .build() → (sql, params).
.fetch() / .fetch_one() are tested with a mock async connection.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from pypika import Table

from fusion.orm.column import Condition
from fusion.orm.conditions import Q
from fusion.orm.constraints import ForeignKey
from fusion.orm.fields import db_now, db_uuid, field
from fusion.orm.functions import Coalesce
from fusion.orm.model import Model
from fusion.orm.query import (
    DeleteQuery,
    ExistsExpression,
    InsertQuery,
    Query,
    SelectQuery,
    UpdateQuery,
    _infer_join_on,
    _where_arg_to_criterion,
)

# ---------------------------------------------------------------------------
# Shared models
# ---------------------------------------------------------------------------


class User(Model):
    id: int | None = None
    email: str
    username: str


class Post(Model):
    id: int | None = None
    user_id: int
    title: str = field(min_length=1, max_length=255)
    body: str | None = None
    created_at: str | None = field(db_type="TIMESTAMPTZ", default=db_now())


class Author(Model):
    id: int | None = None
    email: str


class Article(Model):
    id: int | None = None
    author_id: int
    title: str

    __constraints__ = [ForeignKey("author_id", Author)]


class Metric(Model):
    __schema__ = "analytics"
    id: int | None = None
    name: str


# ---------------------------------------------------------------------------
# SELECT — basic
# ---------------------------------------------------------------------------


def test_select_all_builds_correct_sql():
    sql, params = Post.select().build()
    assert sql == 'SELECT * FROM "posts"'
    assert params == []


def test_select_specific_columns():
    sql, params = Post.select("id", "title").build()
    assert sql == 'SELECT "id","title" FROM "posts"'
    assert params == []


def test_select_where_equality():
    sql, params = Post.select().where(user_id=1).build()
    assert sql == 'SELECT * FROM "posts" WHERE "user_id"=$1'
    assert params == [1]


def test_select_where_multiple_conditions():
    sql, params = Post.select().where(user_id=1, body=None).build()
    assert sql == 'SELECT * FROM "posts" WHERE "user_id"=$1 AND "body"=$2'
    assert params == [1, None]


def test_select_where_gt_lookup():
    sql, params = Post.select().where(id__gt=5).build()
    assert sql == 'SELECT * FROM "posts" WHERE "id">$1'
    assert params == [5]


def test_select_where_gte_lookup():
    sql, params = Post.select().where(id__gte=5).build()
    assert sql == 'SELECT * FROM "posts" WHERE "id">=$1'
    assert params == [5]


def test_select_where_lt_lookup():
    sql, params = Post.select().where(id__lt=100).build()
    assert sql == 'SELECT * FROM "posts" WHERE "id"<$1'
    assert params == [100]


def test_select_where_lte_lookup():
    sql, params = Post.select().where(id__lte=100).build()
    assert sql == 'SELECT * FROM "posts" WHERE "id"<=$1'
    assert params == [100]


def test_select_where_ne_lookup():
    sql, params = Post.select().where(user_id__ne=0).build()
    assert sql == 'SELECT * FROM "posts" WHERE "user_id"<>$1'
    assert params == [0]


def test_select_where_like_lookup():
    sql, params = Post.select().where(title__like="%python%").build()
    assert sql == 'SELECT * FROM "posts" WHERE "title" LIKE $1'
    assert params == ["%python%"]


def test_select_where_ilike_lookup():
    sql, params = Post.select().where(title__ilike="%python%").build()
    assert sql == 'SELECT * FROM "posts" WHERE "title" ILIKE $1'
    assert params == ["%python%"]


def test_select_where_is_null():
    sql, params = Post.select().where(body__is_null=True).build()
    assert sql == 'SELECT * FROM "posts" WHERE "body" IS NULL'
    assert params == []


def test_select_where_is_not_null():
    sql, params = Post.select().where(body__is_not_null=True).build()
    assert sql == 'SELECT * FROM "posts" WHERE "body" IS NOT NULL'
    assert params == []


def test_select_where_in_lookup():
    sql, params = Post.select().where(id__in=[1, 2, 3]).build()
    assert sql == 'SELECT * FROM "posts" WHERE "id" IN $1'
    assert params == [[1, 2, 3]]


def test_select_where_startswith_lookup():
    sql, params = Post.select().where(title__startswith="Hello").build()
    assert sql == 'SELECT * FROM "posts" WHERE "title" LIKE $1'
    assert params == ["Hello%"]


def test_select_order_by_asc():
    sql, params = Post.select().order_by("created_at").build()
    assert sql == 'SELECT * FROM "posts" ORDER BY "created_at" ASC'
    assert params == []


def test_select_order_by_desc():
    sql, params = Post.select().order_by("created_at", desc=True).build()
    assert sql == 'SELECT * FROM "posts" ORDER BY "created_at" DESC'
    assert params == []


def test_select_limit():
    sql, params = Post.select().limit(10).build()
    assert sql == 'SELECT * FROM "posts" LIMIT 10'
    assert params == []


def test_select_offset():
    sql, params = Post.select().offset(20).build()
    assert sql == 'SELECT * FROM "posts" OFFSET 20'
    assert params == []


def test_select_pagination_chain():
    sql, params = (
        Post.select()
        .where(user_id=1)
        .order_by("created_at", desc=True)
        .limit(10)
        .offset(20)
        .build()
    )
    assert (
        sql
        == 'SELECT * FROM "posts" WHERE "user_id"=$1 ORDER BY "created_at" DESC LIMIT 10 OFFSET 20'
    )
    assert params == [1]


# ---------------------------------------------------------------------------
# SELECT — WHERE with Q objects
# ---------------------------------------------------------------------------


def test_select_where_q_or():
    sql, params = Post.select().where(Q(user_id=1) | Q(user_id=2)).build()
    assert sql == 'SELECT * FROM "posts" WHERE "user_id"=$1 OR "user_id"=$2'
    assert params == [1, 2]


def test_select_where_q_and():
    sql, params = Post.select().where(Q(user_id=1) & Q(body__is_null=True)).build()
    assert sql == 'SELECT * FROM "posts" WHERE "user_id"=$1 AND "body" IS NULL'
    assert params == [1]


def test_select_where_q_not():
    sql, params = Post.select().where(~Q(user_id=1)).build()
    assert sql == 'SELECT * FROM "posts" WHERE NOT "user_id"=$1'
    assert params == [1]


# ---------------------------------------------------------------------------
# SELECT — JOIN (inner, left, right, outer)
# ---------------------------------------------------------------------------


def test_select_inner_join_infers_on_clause():
    sql, params = Article.select().join(Author).build()
    assert sql == 'SELECT * FROM "articles" JOIN "authors" ON "articles"."author_id"="authors"."id"'
    assert params == []


def test_select_left_join():
    sql, params = Article.select().join(Author, how="left").build()
    assert (
        sql
        == 'SELECT * FROM "articles" LEFT JOIN "authors" ON "articles"."author_id"="authors"."id"'
    )
    assert params == []


def test_select_right_join():
    sql, params = Article.select().join(Author, how="right").build()
    assert (
        sql
        == 'SELECT * FROM "articles" RIGHT JOIN "authors" ON "articles"."author_id"="authors"."id"'
    )
    assert params == []


def test_select_outer_join():
    sql, params = Article.select().join(Author, how="outer").build()
    assert (
        sql
        == 'SELECT * FROM "articles" FULL OUTER JOIN "authors" ON "articles"."author_id"="authors"."id"'
    )
    assert params == []


def test_select_join_with_where():
    sql, params = Article.select().join(Author).where(author_id=1).build()
    assert (
        sql
        == 'SELECT * FROM "articles" JOIN "authors" ON "articles"."author_id"="authors"."id" WHERE "articles"."author_id"=$1'
    )
    assert params == [1]


# ---------------------------------------------------------------------------
# SELECT — JOIN with alias-based WHERE filtering
# ---------------------------------------------------------------------------


def test_select_join_auto_alias_where():
    sql, params = Article.select().join(Author).where(author__email="a@b.com").build()
    assert sql == (
        'SELECT * FROM "articles" JOIN "authors" ON "articles"."author_id"="authors"."id"'
        ' WHERE "authors"."email"=$1'
    )
    assert params == ["a@b.com"]


def test_select_join_explicit_alias_where():
    sql, params = Article.select().join(a=Author).where(a__email="a@b.com").build()
    assert sql == (
        'SELECT * FROM "articles" JOIN "authors" ON "articles"."author_id"="authors"."id"'
        ' WHERE "authors"."email"=$1'
    )
    assert params == ["a@b.com"]


def test_select_join_alias_where_with_lookup():
    sql, params = Article.select().join(Author).where(author__email__startswith="john").build()
    assert sql == (
        'SELECT * FROM "articles" JOIN "authors" ON "articles"."author_id"="authors"."id"'
        ' WHERE "authors"."email" LIKE $1'
    )
    assert params == ["john%"]


def test_select_join_q_alias():
    sql, params = Article.select().join(Author).where(Q(author__email="a@b.com")).build()
    assert sql == (
        'SELECT * FROM "articles" JOIN "authors" ON "articles"."author_id"="authors"."id"'
        ' WHERE "authors"."email"=$1'
    )
    assert params == ["a@b.com"]


def test_select_join_q_alias_combined():
    sql, params = (
        Article.select().join(Author).where(Q(author__email="a@b.com") | Q(author_id=5)).build()
    )
    assert sql == (
        'SELECT * FROM "articles" JOIN "authors" ON "articles"."author_id"="authors"."id"'
        ' WHERE "authors"."email"=$1 OR "articles"."author_id"=$2'
    )
    assert params == ["a@b.com", 5]


def test_select_join_explicit_alias_with_how():
    sql, params = Article.select().join(a=Author, how="left").where(a__email="a@b.com").build()
    assert sql == (
        'SELECT * FROM "articles" LEFT JOIN "authors" ON "articles"."author_id"="authors"."id"'
        ' WHERE "authors"."email"=$1'
    )
    assert params == ["a@b.com"]


def test_select_join_alias_where_source_and_joined():
    sql, params = (
        Article.select().join(Author).where(title="hello").where(author__email="a@b.com").build()
    )
    assert sql == (
        'SELECT * FROM "articles" JOIN "authors" ON "articles"."author_id"="authors"."id"'
        ' WHERE "articles"."title"=$1 AND "authors"."email"=$2'
    )
    assert params == ["hello", "a@b.com"]


def test_select_join_explicit_on_single_pair_no_fk():
    sql, params = Post.select().join(User, on=(Post.user_id, User.id)).build()
    assert sql == 'SELECT * FROM "posts" JOIN "users" ON "posts"."user_id"="users"."id"'
    assert params == []


def test_select_join_explicit_on_overrides_fk():
    sql, params = Article.select().join(Author, on=(Article.title, Author.email)).build()
    assert sql == 'SELECT * FROM "articles" JOIN "authors" ON "articles"."title"="authors"."email"'
    assert params == []


def test_select_join_explicit_on_multi_pair():
    sql, params = (
        Post.select().join(User, on=[(Post.user_id, User.id), (Post.title, User.username)]).build()
    )
    assert (
        sql
        == 'SELECT * FROM "posts" JOIN "users" ON "posts"."user_id"="users"."id" AND "posts"."title"="users"."username"'
    )
    assert params == []


def test_select_join_explicit_on_left_join():
    sql, params = Post.select().join(User, on=(Post.user_id, User.id), how="left").build()
    assert sql == 'SELECT * FROM "posts" LEFT JOIN "users" ON "posts"."user_id"="users"."id"'
    assert params == []


# ---------------------------------------------------------------------------
# SELECT — schema-qualified table
# ---------------------------------------------------------------------------


def test_select_schema_qualified_table():
    sql, params = Metric.select().build()
    assert sql == 'SELECT * FROM "analytics"."metrics"'
    assert params == []


def test_select_where_with_schema():
    sql, params = Metric.select().where(name="cpu").build()
    assert sql == 'SELECT * FROM "analytics"."metrics" WHERE "name"=$1'
    assert params == ["cpu"]


def test_insert_with_schema():
    sql, params = Metric.insert().values(name="cpu").build()
    assert sql == 'INSERT INTO "analytics"."metrics" ("name") VALUES ($1) RETURNING *'
    assert params == ["cpu"]


def test_update_with_schema():
    sql, params = Metric.update().set(name="mem").where(id=1).build()
    assert sql == 'UPDATE "analytics"."metrics" SET "name"=$1 WHERE "id"=$2 RETURNING *'
    assert params == ["mem", 1]


def test_delete_with_schema():
    sql, params = Metric.delete().where(id=1).build()
    assert sql == 'DELETE FROM "analytics"."metrics" WHERE "id"=$1 RETURNING *'
    assert params == [1]


# ---------------------------------------------------------------------------
# INSERT — SQL generation
# ---------------------------------------------------------------------------


def test_insert_single_row_kwargs():
    sql, params = Post.insert().values(user_id=1, title="hello").build()
    assert sql == 'INSERT INTO "posts" ("user_id","title","body") VALUES ($1,$2,$3) RETURNING *'
    assert params == [1, "hello", None]


def test_insert_bulk_list_of_dicts():
    sql, params = (
        Post.insert()
        .values(
            [
                {"user_id": 1, "title": "first"},
                {"user_id": 2, "title": "second"},
            ]
        )
        .build()
    )
    assert sql == (
        'INSERT INTO "posts" ("user_id","title","body") VALUES ($1,$2,$3),($4,$5,$6) RETURNING *'
    )
    assert params == [1, "first", None, 2, "second", None]


def test_insert_subquery():
    sql, params = Post.insert().values(User.select("id", "username").where(id=5)).build()
    assert sql == (
        'INSERT INTO "posts" ("user_id","title","body")'
        ' SELECT "id","username" FROM "users" WHERE "id"=$1'
        " RETURNING *"
    )
    assert params == [5]


def test_insert_values_both_positional_and_kwargs_raises():
    with pytest.raises(ValueError, match="cannot pass both positional rows and kwargs"):
        Post.insert().values([{"user_id": 1, "title": "a"}], title="b")


def test_insert_values_empty_list_raises():
    with pytest.raises(ValueError, match="at least one row"):
        Post.insert().values([])


def test_insert_values_list_of_non_dict_raises():
    with pytest.raises(TypeError, match="list of dicts"):
        Post.insert().values(["not a dict"])


def test_insert_values_wrong_type_raises():
    with pytest.raises(TypeError, match="list of dicts"):
        Post.insert().values(42)


def test_insert_values_function_in_dict():
    """A Coalesce (sql function) as a dict value renders inline in INSERT."""
    sql, params = (
        Post.insert()
        .values(
            user_id=1,
            title=Coalesce("default_title", None),
        )
        .build()
    )
    assert sql == (
        'INSERT INTO "posts" ("user_id","title","body") VALUES ($1,COALESCE($2,$3),$4) RETURNING *'
    )
    assert params == [1, "default_title", None, None]


def test_insert_values_subquery_in_dict():
    """A SelectQuery as a dict value wraps in parens in INSERT VALUES."""
    sql, params = (
        Post.insert()
        .values(
            user_id=User.select("id").where(id=99),
            title="hello",
        )
        .build()
    )
    assert sql == (
        'INSERT INTO "posts" ("user_id","title","body")'
        ' VALUES ((SELECT "id" FROM "users" WHERE "id"=$1),$2,$3)'
        " RETURNING *"
    )
    assert params == [99, "hello", None]


# ---------------------------------------------------------------------------
# UPDATE — SQL generation
# ---------------------------------------------------------------------------


def test_update_set_single_field():
    sql, params = Post.update().set(title="new title").where(user_id=1).build()
    assert sql == 'UPDATE "posts" SET "title"=$1 WHERE "user_id"=$2 RETURNING *'
    assert params == ["new title", 1]


def test_update_set_multiple_fields():
    sql, params = Post.update().set(title="t", body="b").where(id=1).build()
    assert sql == 'UPDATE "posts" SET "title"=$1,"body"=$2 WHERE "id"=$3 RETURNING *'
    assert params == ["t", "b", 1]


def test_update_params_ordered_correctly():
    sql, params = Post.update().set(title="new title").where(id=5).build()
    assert sql == 'UPDATE "posts" SET "title"=$1 WHERE "id"=$2 RETURNING *'
    assert params == ["new title", 5]


# ---------------------------------------------------------------------------
# DELETE — SQL generation
# ---------------------------------------------------------------------------


def test_delete_with_where():
    sql, params = Post.delete().where(user_id=1).build()
    assert sql == 'DELETE FROM "posts" WHERE "user_id"=$1 RETURNING *'
    assert params == [1]


def test_delete_params():
    _, params = Post.delete().where(id=42).build()
    assert params == [42]


# ---------------------------------------------------------------------------
# .fetch() and .fetch_one() — mock connection
# ---------------------------------------------------------------------------


@pytest.fixture
def record_conn():
    rows = [{"id": 1, "user_id": 1, "title": "hello", "body": None, "created_at": "2024-01-01"}]
    conn = AsyncMock()
    record = MagicMock()
    record.__iter__ = MagicMock(return_value=iter(rows[0].items()))
    record.keys = MagicMock(return_value=list(rows[0].keys()))
    record.items = MagicMock(return_value=list(rows[0].items()))
    conn.fetch = AsyncMock(return_value=[record])
    conn.fetchrow = AsyncMock(return_value=record)
    return conn


@pytest.mark.asyncio
async def test_fetch_calls_conn_fetch(record_conn):
    await Post.select().where(user_id=1).fetch(record_conn)
    record_conn.fetch.assert_called_once()
    call_args = record_conn.fetch.call_args
    sql = call_args[0][0]
    assert 'SELECT * FROM "posts" WHERE "user_id"=$1' == sql


@pytest.mark.asyncio
async def test_fetch_passes_params_as_positional_args(record_conn):
    await Post.select().where(user_id=1).fetch(record_conn)
    call_args = record_conn.fetch.call_args
    assert 1 in call_args[0][1:]


@pytest.mark.asyncio
async def test_fetch_one_calls_conn_fetchrow(record_conn):
    await Post.select().where(id=1).fetch_one(record_conn)
    record_conn.fetchrow.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_one_returns_none_when_no_row():
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=None)
    result = await Post.select().where(id=999).fetch_one(conn)
    assert result is None


@pytest.mark.asyncio
async def test_insert_fetch_calls_conn_fetch(record_conn):
    await Post.insert().values(user_id=1, title="hello").fetch(record_conn)
    record_conn.fetch.assert_called_once()
    sql = record_conn.fetch.call_args[0][0]
    assert sql == 'INSERT INTO "posts" ("user_id","title","body") VALUES ($1,$2,$3) RETURNING *'


@pytest.mark.asyncio
async def test_update_fetch_calls_conn_fetch(record_conn):
    await Post.update().set(title="t").where(id=1).fetch(record_conn)
    record_conn.fetch.assert_called_once()
    sql = record_conn.fetch.call_args[0][0]
    assert sql == 'UPDATE "posts" SET "title"=$1 WHERE "id"=$2 RETURNING *'


@pytest.mark.asyncio
async def test_delete_fetch_calls_conn_fetch(record_conn):
    await Post.delete().where(user_id=1).fetch(record_conn)
    record_conn.fetch.assert_called_once()
    sql = record_conn.fetch.call_args[0][0]
    assert sql == 'DELETE FROM "posts" WHERE "user_id"=$1 RETURNING *'


# ---------------------------------------------------------------------------
# .fetch(conn, raw=True) returns list[dict]
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_raw_true_returns_dicts(record_conn):
    result = await Post.select().where(user_id=1).fetch(record_conn, raw=True)
    assert isinstance(result, list)
    if result:
        assert isinstance(result[0], dict)


# ---------------------------------------------------------------------------
# SELECT — .exists()
# ---------------------------------------------------------------------------


def test_exists_returns_exists_expression():
    e = Post.select().where(user_id=1).exists()
    assert isinstance(e, ExistsExpression)


def test_exists_invert():
    e = ~Post.select().where(user_id=1).exists()
    assert isinstance(e, ExistsExpression)
    assert e._negated is True


# ---------------------------------------------------------------------------
# SELECT / UPDATE / DELETE — empty Q produces no WHERE
# ---------------------------------------------------------------------------


def test_select_empty_q_produces_no_where():
    sql, _ = Post.select().where(Q()).build()
    assert sql == 'SELECT * FROM "posts"'


def test_select_nested_empty_q_produces_no_where():
    sql, _ = Post.select().where(Q(Q())).build()
    assert sql == 'SELECT * FROM "posts"'


def test_update_empty_q_produces_no_where():
    sql, params = Post.update().set(title="x").where(Q()).build()
    assert sql == 'UPDATE "posts" SET "title"=$1 RETURNING *'
    assert params == ["x"]


def test_delete_empty_q_produces_no_where():
    sql, _ = Post.delete().where(Q()).build()
    assert sql == 'DELETE FROM "posts" RETURNING *'


# ---------------------------------------------------------------------------
# UPDATE / DELETE — Q as positional arg
# ---------------------------------------------------------------------------


def test_update_where_with_q_positional():
    sql, params = Post.update().set(title="x").where(Q(user_id=1)).build()
    assert sql == 'UPDATE "posts" SET "title"=$1 WHERE "user_id"=$2 RETURNING *'
    assert params == ["x", 1]


def test_delete_where_with_q_positional():
    sql, params = Post.delete().where(Q(user_id=1)).build()
    assert sql == 'DELETE FROM "posts" WHERE "user_id"=$1 RETURNING *'
    assert params == [1]


# ---------------------------------------------------------------------------
# WHERE — Condition object, where_raw no-op, unknown type
# ---------------------------------------------------------------------------


def test_where_with_condition_object_directly():
    cond = Condition(column="user_id", lookup="eq", value=1)
    sql, params = Post.select().where(cond).build()
    assert sql == 'SELECT * FROM "posts" WHERE "user_id"=$1'
    assert params == [1]


def test_where_raw_with_non_exp_is_noop():
    sql, _ = Post.select().where_raw("ignored_string").build()  # type: ignore[arg-type]
    assert sql == 'SELECT * FROM "posts"'


def test_where_arg_to_criterion_unknown_type():
    result = _where_arg_to_criterion("not_a_q_or_condition", Table("posts"), [])  # type: ignore[arg-type]
    assert result is None


# ---------------------------------------------------------------------------
# JOIN — no FK and multiple alias kwargs error
# ---------------------------------------------------------------------------


def test_join_without_fk_produces_join_without_on():
    class Tag(Model):
        id: int | None = None
        name: str

    sql, _ = Post.select().join(Tag).build()
    assert sql == 'SELECT * FROM "posts" JOIN "tags" ON true'


def test_join_multiple_alias_kwargs_raises():
    with pytest.raises(ValueError, match="positional model or exactly one alias kwarg"):
        Post.select().join(a=User, b=Post)


def test_infer_join_on_returns_none_for_no_fk():
    class NoFK(Model):
        id: int | None = None
        name: str

    source = Table("posts")
    target = Table("users")
    result = _infer_join_on(NoFK, User, source, target)
    assert result is None


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


def test_unknown_alias_raises():
    with pytest.raises(ValueError, match="Unknown join alias"):
        Post.select().where(ghost__col=5).build()


def test_unknown_lookup_raises():
    cond = Condition(column="title", lookup="BOGUS", value="x")
    with pytest.raises(ValueError, match="Unknown lookup"):
        Post.select().where(cond).build()


# ---------------------------------------------------------------------------
# .fetch() — returns model instances
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_select_fetch_returns_model_instances():
    conn = AsyncMock()
    row = {"id": 1, "user_id": 1, "title": "hello", "body": None, "created_at": None}
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
    row = {"id": 1, "user_id": 1, "title": "hello", "body": None, "created_at": None}
    record = MagicMock()
    record.items = MagicMock(return_value=list(row.items()))
    conn.fetchrow = AsyncMock(return_value=record)

    result = await Post.select().where(id=1).fetch_one(conn)
    assert isinstance(result, Post)
    assert result.id == 1


# ---------------------------------------------------------------------------
# params accumulator — shared list passed to .build()
# ---------------------------------------------------------------------------


def test_select_build_with_shared_params():
    shared: list = []
    sql, params = Post.select().where(user_id=1).build(shared)
    assert sql == 'SELECT * FROM "posts" WHERE "user_id"=$1'
    assert params is shared
    assert shared == [1]


def test_insert_build_with_shared_params():
    shared: list = []
    _, params = Post.insert().values(user_id=1, title="hi").build(shared)
    assert params is shared
    assert 1 in shared
    assert "hi" in shared


def test_update_build_with_shared_params():
    shared: list = []
    _, params = Post.update().set(title="new").where(user_id=1).build(shared)
    assert params is shared
    assert "new" in shared
    assert 1 in shared


def test_delete_build_with_shared_params():
    shared: list = []
    _, params = Post.delete().where(user_id=1).build(shared)
    assert params is shared
    assert 1 in shared


def test_select_shared_params_continues_numbering():
    """When params already has items, new params continue the sequence."""
    shared: list = ["existing"]  # offset of 1
    sql, params = Post.select().where(user_id=2).build(shared)
    assert "$2" in sql  # not $1 — continues from offset
    assert params is shared
    assert shared == ["existing", 2]


# ---------------------------------------------------------------------------
# Cycle 2: SelectQuery extends Query base class
# ---------------------------------------------------------------------------

# --- Tracer bullet ---


def test_select_query_inherits_from_query():
    """SelectQuery must be a subclass of Query."""
    assert issubclass(SelectQuery, Query)


def test_select_query_with_columns_builds_correct_sql():
    """Tracer bullet: positional columns → SELECT col1,col2 FROM table."""
    sql, params = SelectQuery(User, "id", "name").build()
    assert sql == 'SELECT "id","name" FROM "users"'
    assert params == []


# --- Expression projections ---


def test_select_query_expression_projection():
    """SelectQuery(Model, alias=expr) → SELECT expr AS alias FROM table."""
    sql, params = SelectQuery(User, total=Coalesce("x", None)).build()
    assert sql == 'SELECT COALESCE($1,$2) AS "total" FROM "users"'
    assert params == ["x", None]


# --- Mixed columns + expressions ---


def test_select_query_mixed_columns_and_expressions():
    """Both named columns and expr projections together."""
    sql, params = SelectQuery(User, "id", total=Coalesce("x", None)).build()
    assert sql == 'SELECT "id",COALESCE($1,$2) AS "total" FROM "users"'
    assert params == ["x", None]


# --- No model (free projection) ---


def test_select_query_no_model_free_projection():
    """SelectQuery with no model and only exprs produces a free SELECT (no FROM)."""
    sql, params = SelectQuery(total=Coalesce("x", None)).build()
    assert sql == 'SELECT COALESCE($1,$2) AS "total"'
    assert params == ["x", None]


# --- Inherited methods return SelectQuery type ---


def test_select_query_where_inherited_returns_select_query():
    """.where() is inherited from Query and returns SelectQuery (not plain Query)."""
    result = SelectQuery(User, "id").where(id=1)
    assert type(result) is SelectQuery


def test_select_query_order_by_inherited_returns_select_query():
    """.order_by() is inherited from Query and returns SelectQuery."""
    result = SelectQuery(User, "id").order_by("id")
    assert type(result) is SelectQuery


# --- group_by in SelectQuery.build() ---


def test_select_query_group_by_builds_group_by_clause():
    """group_by() is inherited and SelectQuery.build() applies GROUP BY."""
    sql, params = SelectQuery(User, total=Coalesce("x", None)).where(id=1).group_by("email").build()
    assert sql == 'SELECT COALESCE($1,$2) AS "total" FROM "users" WHERE "id"=$3 GROUP BY "email"'
    assert params == ["x", None, 1]


# --- prefetch still works ---


def test_select_query_prefetch_still_works():
    """.prefetch() still returns a SelectQuery instance."""
    result = Post.select().prefetch(User)
    assert isinstance(result, SelectQuery)


# --- where_raw still works ---


def test_select_query_where_raw_still_works():
    """where_raw() with Exp object still filters rows."""
    from fusion.orm.expressions import Exp

    sql, params = Post.select().where_raw(Exp("id IS NOT NULL")).build()
    assert sql == 'SELECT * FROM "posts" WHERE id IS NOT NULL'
    assert params == []


# ---------------------------------------------------------------------------
# Cycle 3: Model.select() passes *columns AND **exprs to SelectQuery
# ---------------------------------------------------------------------------


# --- Tracer bullet ---


def test_model_select_with_single_column_tracer():
    """Model.select('id') returns SelectQuery with correct SQL."""
    q = User.select("id")
    assert isinstance(q, SelectQuery)
    sql, params = q.build()
    assert sql == 'SELECT "id" FROM "users"'
    assert params == []


# --- SELECT * ---


def test_model_select_no_args_returns_star():
    """User.select() → SELECT * FROM users."""
    sql, params = User.select().build()
    assert sql == 'SELECT * FROM "users"'
    assert params == []


# --- Specific columns ---


def test_model_select_multiple_columns():
    """User.select('id', 'email') → SELECT id,email FROM users."""
    sql, params = User.select("id", "email").build()
    assert sql == 'SELECT "id","email" FROM "users"'
    assert params == []


# --- Expression projection via Model.select() ---


def test_model_select_expression_kwarg():
    """User.select(total=Coalesce(...)) passes **exprs through to SelectQuery."""
    sql, params = User.select(total=Coalesce("x", None)).build()
    assert sql == 'SELECT COALESCE($1,$2) AS "total" FROM "users"'
    assert params == ["x", None]


# --- Mixed columns + expressions via Model.select() ---


def test_model_select_mixed_columns_and_expressions():
    """User.select('id', total=Coalesce(...)) passes both through."""
    sql, params = User.select("id", total=Coalesce("x", None)).build()
    assert sql == 'SELECT "id",COALESCE($1,$2) AS "total" FROM "users"'
    assert params == ["x", None]


# ---------------------------------------------------------------------------
# Cycle 4: UpdateQuery and DeleteQuery extend Query base class
# ---------------------------------------------------------------------------


# --- Tracer bullet ---


def test_update_query_inherits_from_query():
    """UpdateQuery must be a subclass of Query."""
    assert issubclass(UpdateQuery, Query)


# --- UpdateQuery: where() returns UpdateQuery ---


def test_update_query_where_returns_update_query():
    """.where() on UpdateQuery returns UpdateQuery, not plain Query."""
    result = Post.update().where(id=1)
    assert type(result) is UpdateQuery


# --- UpdateQuery: set() returns UpdateQuery ---


def test_update_query_set_returns_update_query():
    """.set() returns UpdateQuery, not plain object."""
    result = Post.update().set(title="x")
    assert type(result) is UpdateQuery


# --- UpdateQuery: chained where + set preserves type ---


def test_update_query_chained_where_and_set_preserves_type():
    """.set().where() chain returns UpdateQuery at every step."""
    q = Post.update().set(title="x").where(id=1)
    assert type(q) is UpdateQuery


# --- UpdateQuery: where() still produces correct SQL ---


def test_update_query_where_sql_unchanged():
    """Inheriting where() from Query must not change the UPDATE SQL output."""
    sql, params = Post.update().set(title="new title").where(user_id=1).build()
    assert sql == 'UPDATE "posts" SET "title"=$1 WHERE "user_id"=$2 RETURNING *'
    assert params == ["new title", 1]


# --- UpdateQuery: multiple where() calls accumulate conditions ---


def test_update_query_multiple_where_calls_accumulate():
    sql, params = Post.update().set(body="b").where(user_id=1).where(id=5).build()
    assert sql == 'UPDATE "posts" SET "body"=$1 WHERE "user_id"=$2 AND "id"=$3 RETURNING *'
    assert params == ["b", 1, 5]


# --- DeleteQuery: subclass tracer ---


def test_delete_query_inherits_from_query():
    """DeleteQuery must be a subclass of Query."""
    assert issubclass(DeleteQuery, Query)


# --- DeleteQuery: where() returns DeleteQuery ---


def test_delete_query_where_returns_delete_query():
    """.where() on DeleteQuery returns DeleteQuery, not plain Query."""
    result = Post.delete().where(id=1)
    assert type(result) is DeleteQuery


# --- DeleteQuery: where() still produces correct SQL ---


def test_delete_query_where_sql_unchanged():
    """Inheriting where() from Query must not change the DELETE SQL output."""
    sql, params = Post.delete().where(user_id=1).build()
    assert sql == 'DELETE FROM "posts" WHERE "user_id"=$1 RETURNING *'
    assert params == [1]


# --- DeleteQuery: multiple where() calls accumulate conditions ---


def test_delete_query_multiple_where_calls_accumulate():
    sql, params = Post.delete().where(user_id=1).where(id=5).build()
    assert sql == 'DELETE FROM "posts" WHERE "user_id"=$1 AND "id"=$2 RETURNING *'
    assert params == [1, 5]


# ---------------------------------------------------------------------------
# Cycle 5: InsertQuery extends Query base class
# ---------------------------------------------------------------------------


# --- Tracer bullet ---


def test_insert_query_inherits_from_query():
    """InsertQuery must be a subclass of Query."""
    assert issubclass(InsertQuery, Query)


# --- InsertQuery: values() returns InsertQuery (not plain Query) ---


def test_insert_query_values_returns_insert_query():
    """.values() must return InsertQuery, not plain Query."""
    result = Post.insert().values(user_id=1, title="hello")
    assert type(result) is InsertQuery


# --- InsertQuery: existing INSERT SQL still correct (build() behavior unchanged) ---


def test_insert_query_single_row_sql_unchanged():
    """Inheriting from Query must not change the INSERT SQL output."""
    sql, params = Post.insert().values(user_id=1, title="hello").build()
    assert sql == 'INSERT INTO "posts" ("user_id","title","body") VALUES ($1,$2,$3) RETURNING *'
    assert params == [1, "hello", None]


def test_insert_query_bulk_values_sql_unchanged():
    """Bulk insert SQL must remain correct after InsertQuery extends Query."""
    sql, params = (
        Post.insert()
        .values(
            [
                {"user_id": 1, "title": "first"},
                {"user_id": 2, "title": "second"},
            ]
        )
        .build()
    )
    assert sql == (
        'INSERT INTO "posts" ("user_id","title","body") VALUES ($1,$2,$3),($4,$5,$6) RETURNING *'
    )
    assert params == [1, "first", None, 2, "second", None]
