"""Tests for fusion.orm.functions — Coalesce with unified build() interface."""

import pytest
from pypika import Parameter

from fusion.orm.functions import Coalesce


# ---------------------------------------------------------------------------
# Coalesce — build() interface
# ---------------------------------------------------------------------------


def test_coalesce_has_build_method():
    """Coalesce.build(params) returns (sql_str, params) like queries do."""
    params: list = []
    sql, out_params = Coalesce("Alice", None).build(params)
    assert out_params is params
    assert out_params == ["Alice", None]
    assert sql == "COALESCE($1,$2)"


# ---------------------------------------------------------------------------
# Coalesce — scalar args
# ---------------------------------------------------------------------------


def test_coalesce_single_string_scalar():
    params: list = []
    sql, _ = Coalesce("Alice").build(params)
    assert params == ["Alice"]
    assert sql == "COALESCE($1)"


def test_coalesce_two_scalars():
    params: list = []
    sql, _ = Coalesce("Alice", None).build(params)
    assert params == ["Alice", None]
    assert sql == "COALESCE($1,$2)"


def test_coalesce_int_scalar():
    params: list = []
    sql, _ = Coalesce(42, 0).build(params)
    assert params == [42, 0]
    assert sql == "COALESCE($1,$2)"


def test_coalesce_continues_shared_params():
    """When params already has items, Coalesce continues the numbering."""
    params: list = ["pre"]
    sql, _ = Coalesce("x", None).build(params)
    assert sql == "COALESCE($2,$3)"
    assert params == ["pre", "x", None]


def test_coalesce_bool_scalar():
    params: list = []
    sql, _ = Coalesce(True, False).build(params)
    assert params == [True, False]


def test_coalesce_unsupported_type_raises():
    params: list = []
    with pytest.raises(TypeError):
        Coalesce(object()).build(params)


# ---------------------------------------------------------------------------
# Coalesce — subquery args (cycles 4 & 5)
# ---------------------------------------------------------------------------

from fusion.orm.fields import field
from fusion.orm.model import Model


class Post(Model):
    id: int | None = None
    user_id: int
    title: str


class Draft(Model):
    id: int | None = None
    user_id: int
    title: str


def test_coalesce_single_select_subquery():
    params: list = []
    sql, _ = Coalesce(Post.select("title").where(id=1)).build(params)
    assert params == [1]
    assert sql == 'COALESCE((SELECT "title" FROM "posts" WHERE "id"=$1))'


def test_coalesce_subquery_with_scalar_fallback():
    params: list = []
    sql, _ = Coalesce(Post.select("title").where(id=1), "untitled").build(params)
    assert params == [1, "untitled"]
    assert sql == 'COALESCE((SELECT "title" FROM "posts" WHERE "id"=$1),$2)'


def test_coalesce_two_subqueries_sequential_params():
    """Cycle 5: two subqueries — both use $1 independently but must become $1, $2."""
    params: list = []
    sql, _ = Coalesce(
        Post.select("title").where(id=1),
        Draft.select("title").where(id=1),
    ).build(params)
    assert params == [1, 1]
    assert sql == (
        "COALESCE("
        '(SELECT "title" FROM "posts" WHERE "id"=$1),'
        '(SELECT "title" FROM "drafts" WHERE "id"=$2)'
        ")"
    )


def test_coalesce_subquery_continues_shared_params():
    """Subquery params appended after pre-existing shared params."""
    params: list = ["pre"]
    sql, _ = Coalesce(Post.select("title").where(id=5)).build(params)
    assert sql == 'COALESCE((SELECT "title" FROM "posts" WHERE "id"=$2))'
    assert params == ["pre", 5]


def test_coalesce_insert_subquery():
    """InsertQuery also has .build(params) — works as Coalesce arg."""
    params: list = []
    sql, _ = Coalesce(Post.insert().values(user_id=1, title="hi"), "failed").build(params)
    assert "INSERT" in sql
    assert 1 in params
    assert "hi" in params


# ---------------------------------------------------------------------------
# Coalesce nested inside Coalesce (cycle 6)
# ---------------------------------------------------------------------------


def test_coalesce_nested_function_arg():
    """Inner Coalesce is a Function; its params are added before outer continues."""
    params: list = []
    inner = Coalesce(Post.select("title").where(id=1), "inner_fallback")
    outer = Coalesce(inner, "outer_fallback")
    sql, _ = outer.build(params)
    assert params == [1, "inner_fallback", "outer_fallback"]
    assert sql == ('COALESCE(COALESCE((SELECT "title" FROM "posts" WHERE "id"=$1),$2),$3)')


def test_coalesce_deeply_nested():
    """Three levels of nesting — params accumulate left to right."""
    params: list = []
    level1 = Coalesce(Post.select("title").where(id=1), "l1")
    level2 = Coalesce(level1, Post.select("title").where(id=2), "l2")
    level3 = Coalesce(level2, "l3")
    sql, _ = level3.build(params)
    assert params == [1, "l1", 2, "l2", "l3"]


def test_coalesce_function_and_scalar_mixed():
    """Function arg followed by scalar — params continue correctly."""
    params: list = []
    inner = Coalesce("x", "y")
    outer = Coalesce(inner, "z")
    outer.build(params)
    assert params == ["x", "y", "z"]
