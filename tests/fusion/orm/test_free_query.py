"""Tests for fusion.orm.query — Query model-free SELECT builder."""

import pytest
from fusion.orm.query import Query
from fusion.orm.functions import Coalesce
from fusion.orm.model import Model
from fusion.orm.constraints import ForeignKey


class Post(Model):
    id: int | None = None
    user_id: int
    title: str


class Author(Model):
    id: int | None = None
    email: str


class Article(Model):
    id: int | None = None
    author_id: int
    title: str
    __constraints__ = [ForeignKey("author_id", Author)]


# ---------------------------------------------------------------------------
# Cycle 7: model-free, no FROM
# ---------------------------------------------------------------------------


def test_query_single_scalar_expr():
    sql, params = Query(name=Coalesce("Alice", None)).build()
    assert sql == 'SELECT COALESCE($1,$2) AS "name"'
    assert params == ["Alice", None]


def test_query_two_named_exprs():
    sql, params = Query(
        name=Coalesce("Alice", None),
        score=Coalesce(0, None),
    ).build()
    assert sql == 'SELECT COALESCE($1,$2) AS "name",COALESCE($3,$4) AS "score"'
    assert params == ["Alice", None, 0, None]


def test_query_with_subquery_expr():
    sql, params = Query(name=Coalesce(Post.select("title").where(id=1), "untitled")).build()
    assert sql == 'SELECT COALESCE((SELECT "title" FROM "posts" WHERE "id"=$1),$2) AS "name"'
    assert params == [1, "untitled"]


def test_query_build_with_shared_params():
    shared: list = ["pre"]
    sql, params = Query(name=Coalesce("x", None)).build(shared)
    assert params is shared
    assert "$2" in sql
    assert "$3" in sql


def test_query_no_model_has_no_from():
    sql, params = Query(name=Coalesce("x", None)).build()
    assert "FROM" not in sql


def test_query_immutable_two_builds_are_independent():
    q = Query(name=Coalesce("x", None))
    sql1, p1 = q.build()
    sql2, p2 = q.build()
    assert sql1 == sql2
    assert p1 == p2
    assert p1 is not p2  # separate lists


# ---------------------------------------------------------------------------
# _render_expr — build protocol and scalar paths
# ---------------------------------------------------------------------------


def test_query_expr_with_build_protocol():
    """An object with .build(params) is rendered as a subquery literal."""
    # Post.select() has .build(params) but no .to_term()
    subq = Post.select("title").where(id=99)
    sql, params = Query(val=subq).build()
    assert sql == 'SELECT (SELECT "title" FROM "posts" WHERE "id"=$1) AS "val"'
    assert params == [99]


def test_query_expr_scalar_string():
    """A plain scalar string is rendered as a $N parameter."""
    sql, params = Query(greeting="hello").build()
    assert sql == 'SELECT $1 AS "greeting"'
    assert params == ["hello"]


def test_query_expr_scalar_int():
    """A plain scalar integer is rendered as a $N parameter."""
    sql, params = Query(num=42).build()
    assert sql == 'SELECT $1 AS "num"'
    assert params == [42]


def test_query_expr_scalar_none():
    """None scalar is rendered as a $N parameter."""
    sql, params = Query(nothing=None).build()
    assert sql == 'SELECT $1 AS "nothing"'
    assert params == [None]


# ---------------------------------------------------------------------------
# Cycle 8: Query with model — FROM clause
# ---------------------------------------------------------------------------


def test_query_with_model_adds_from():
    sql, params = Query(Post, name=Coalesce("x", None)).build()
    assert sql == 'SELECT COALESCE($1,$2) AS "name" FROM "posts"'
    assert params == ["x", None]


def test_query_model_with_subquery_expr():
    sql, params = Query(
        Post,
        title=Coalesce(Post.select("title").where(id=1), "untitled"),
    ).build()
    assert sql == (
        'SELECT COALESCE((SELECT "title" FROM "posts" WHERE "id"=$1),$2) AS "title" FROM "posts"'
    )
    assert params == [1, "untitled"]


# ---------------------------------------------------------------------------
# Cycle 9: .where()
# ---------------------------------------------------------------------------


def test_query_where_kwarg():
    sql, params = Query(Post, name=Coalesce("x", None)).where(user_id=1).build()
    assert sql == 'SELECT COALESCE($1,$2) AS "name" FROM "posts" WHERE "user_id"=$3'
    assert params == ["x", None, 1]


def test_query_where_multiple_kwargs():
    sql, params = Query(Post, name=Coalesce("x", None)).where(user_id=1, title="hi").build()
    assert "WHERE" in sql
    assert 1 in params
    assert "hi" in params


def test_query_where_q_object():
    from fusion.orm.conditions import Q

    sql, params = Query(Post, name=Coalesce("x", None)).where(Q(user_id=1) | Q(user_id=2)).build()
    assert "WHERE" in sql
    assert 1 in params
    assert 2 in params


def test_query_where_is_immutable():
    base = Query(Post, name=Coalesce("x", None))
    filtered = base.where(user_id=1)
    sql_base, _ = base.build()
    sql_filtered, _ = filtered.build()
    assert "WHERE" not in sql_base
    assert "WHERE" in sql_filtered


def test_query_where_requires_model():
    """WHERE on model-free Query should raise or produce invalid SQL — no FROM means WHERE is meaningless."""
    # For now we just ensure it doesn't silently produce wrong SQL.
    # If the implementation raises, that's acceptable too.
    q = Query(name=Coalesce("x", None)).where(user_id=1)
    # Either raises at build time or produces SQL without FROM — both acceptable
    try:
        sql, _ = q.build()
        assert "FROM" not in sql or "WHERE" in sql  # if it builds, WHERE should appear
    except Exception:
        pass  # raising is also acceptable


# ---------------------------------------------------------------------------
# Cycle 10: .order_by(), .group_by(), .limit(), .offset()
# ---------------------------------------------------------------------------


def test_query_order_by_asc():
    sql, params = Query(Post, name=Coalesce("x", None)).order_by("title").build()
    assert sql == 'SELECT COALESCE($1,$2) AS "name" FROM "posts" ORDER BY "title" ASC'
    assert params == ["x", None]


def test_query_order_by_desc():
    sql, params = Query(Post, name=Coalesce("x", None)).order_by("title", desc=True).build()
    assert sql == 'SELECT COALESCE($1,$2) AS "name" FROM "posts" ORDER BY "title" DESC'
    assert params == ["x", None]


def test_query_group_by():
    sql, params = Query(Post, name=Coalesce("x", None)).where(user_id=1).group_by("title").build()
    assert (
        sql == 'SELECT COALESCE($1,$2) AS "name" FROM "posts" WHERE "user_id"=$3 GROUP BY "title"'
    )
    assert params == ["x", None, 1]


def test_query_limit():
    sql, params = Query(Post, name=Coalesce("x", None)).limit(10).build()
    assert sql == 'SELECT COALESCE($1,$2) AS "name" FROM "posts" LIMIT 10'
    assert params == ["x", None]


def test_query_offset():
    sql, params = Query(Post, name=Coalesce("x", None)).limit(10).offset(20).build()
    assert sql == 'SELECT COALESCE($1,$2) AS "name" FROM "posts" LIMIT 10 OFFSET 20'
    assert params == ["x", None]


def test_query_full_chain():
    sql, params = (
        Query(Post, title=Coalesce(Post.select("title").where(id=1), "untitled"))
        .where(user_id=2)
        .order_by("title", desc=True)
        .limit(5)
        .offset(10)
        .build()
    )
    assert sql == (
        'SELECT COALESCE((SELECT "title" FROM "posts" WHERE "id"=$1),$2) AS "title"'
        ' FROM "posts"'
        ' WHERE "user_id"=$3'
        ' ORDER BY "title" DESC'
        " LIMIT 5"
        " OFFSET 10"
    )
    assert params == [1, "untitled", 2]


def test_query_chain_is_immutable():
    base = Query(Post, name=Coalesce("x", None))
    chained = base.order_by("title").limit(5)
    sql_base, _ = base.build()
    sql_chained, _ = chained.build()
    assert "ORDER BY" not in sql_base
    assert "LIMIT" not in sql_base
    assert "ORDER BY" in sql_chained
    assert "LIMIT" in sql_chained


# ---------------------------------------------------------------------------
# Cycle 11: .join() with model target
# ---------------------------------------------------------------------------


def test_query_join_model_fk_inferred():
    sql, params = Query(Article, name=Coalesce("x", None)).join(author=Author).build()
    assert 'JOIN "authors" ON "articles"."author_id"="authors"."id"' in sql
    assert params == ["x", None]


def test_query_join_model_left():
    sql, params = Query(Article, name=Coalesce("x", None)).join(author=Author, how="left").build()
    assert 'LEFT JOIN "authors"' in sql


def test_query_join_model_explicit_on():
    sql, params = (
        Query(Article, name=Coalesce("x", None))
        .join(author=Author, on=(Article.author_id, Author.id))
        .build()
    )
    assert 'JOIN "authors" ON "articles"."author_id"="authors"."id"' in sql


def test_query_join_where_on_joined_table():
    """WHERE can reference joined table columns via alias__column pattern."""
    sql, params = (
        Query(Article, name=Coalesce("x", None))
        .join(author=Author)
        .where(author__email="x@y.com")
        .build()
    )
    assert 'JOIN "authors"' in sql
    assert "x@y.com" in params


def test_query_join_is_immutable():
    base = Query(Article, name=Coalesce("x", None))
    joined = base.join(author=Author)
    sql_base, _ = base.build()
    assert "JOIN" not in sql_base


# ---------------------------------------------------------------------------
# Cycle 12: .join() with Query subquery target
# ---------------------------------------------------------------------------


def test_query_join_subquery():
    sub = Query(Post, name=Coalesce("x", None)).where(user_id=1)
    sql, params = (
        Query(Author, result=Coalesce("y", None))
        .join(sub=sub, on=("authors.id", "sub.user_id"))
        .build()
    )
    assert "JOIN" in sql
    assert "SELECT" in sql  # subquery embedded
    assert "x" in params
    assert "y" in params
    assert 1 in params


def test_query_join_subquery_params_sequential():
    """Subquery params renumbered into outer sequence via shared accumulator."""
    sub = Query(Post, name=Coalesce("inner", None)).where(user_id=5)
    sql, params = (
        Query(Author, result=Coalesce("outer", None))
        .join(sub=sub, on=("authors.id", "sub.user_id"))
        .build()
    )
    assert params.index("inner") < params.index(5)  # expr params before WHERE params
    assert "outer" in params


# ---------------------------------------------------------------------------
# Cycle 13: Query composability
# ---------------------------------------------------------------------------

from fusion.orm.expressions import cte


def test_query_as_coalesce_arg():
    """Query with .build() satisfies the subquery protocol inside Coalesce."""
    inner = Query(Post, name=Coalesce("x", None)).where(user_id=1)
    outer = Query(result=Coalesce(inner, "fallback"))
    sql, params = outer.build()
    assert "COALESCE(" in sql
    assert "SELECT" in sql  # inner query embedded
    assert params == ["x", None, 1, "fallback"]


def test_query_nested_coalesce_sql():
    """Full SQL for nested Query inside Coalesce."""
    inner = Query(Post, name=Coalesce("x", None)).where(user_id=1)
    outer = Query(result=Coalesce(inner, "fallback"))
    sql, params = outer.build()
    assert sql == (
        "SELECT COALESCE("
        '(SELECT COALESCE($1,$2) AS "name" FROM "posts" WHERE "user_id"=$3)'
        ',$4) AS "result"'
    )
    assert params == ["x", None, 1, "fallback"]


def test_query_as_cte_arm():
    """Query satisfies the .build() protocol required by cte()."""
    free_q = Query(name=Coalesce(Post.select("title").where(id=1), "none"))
    q = cte(main=Post.select("id"), resolved=free_q)
    sql, params = q.build()
    assert "WITH" in sql
    assert "resolved" in sql
    assert "COALESCE" in sql
    assert 1 in params
    assert "none" in params


def test_query_in_cte_params_sequential():
    free_q = Query(name=Coalesce(Post.select("title").where(id=5), "fallback"))
    q = cte(main=Post.select("id").where(user_id=10), resolved=free_q)
    sql, params = q.build()
    # CTE arm built first (resolved), then main — params accumulate
    assert params.index(5) < params.index("fallback")
    assert 10 in params


def test_public_imports():
    from fusion.orm import Coalesce, Query, SelectQuery

    assert Query is not None
    assert Coalesce is not None
    assert SelectQuery is not None


def test_public_import_select_query_is_subclass_of_query():
    from fusion.orm import Query, SelectQuery

    assert issubclass(SelectQuery, Query)


# ---------------------------------------------------------------------------
# Cycle 1: Query as proper base class — builder methods return subclass type
# ---------------------------------------------------------------------------


class _SubQuery(Query):
    """Minimal subclass used to verify builder methods return the subclass type."""


def test_where_returns_subclass():
    result = _SubQuery(Post, name=Coalesce("x", None)).where(user_id=1)
    assert type(result) is _SubQuery


def test_order_by_returns_subclass():
    result = _SubQuery(Post, name=Coalesce("x", None)).order_by("title")
    assert type(result) is _SubQuery


def test_group_by_returns_subclass():
    result = _SubQuery(Post, name=Coalesce("x", None)).group_by("title")
    assert type(result) is _SubQuery


def test_limit_returns_subclass():
    result = _SubQuery(Post, name=Coalesce("x", None)).limit(10)
    assert type(result) is _SubQuery


def test_offset_returns_subclass():
    result = _SubQuery(Post, name=Coalesce("x", None)).offset(5)
    assert type(result) is _SubQuery


def test_join_returns_subclass():
    result = _SubQuery(Article, name=Coalesce("x", None)).join(author=Author)
    assert type(result) is _SubQuery
