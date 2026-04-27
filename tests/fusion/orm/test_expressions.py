"""Tests for fusion.orm.expressions — Exp, union(), cte(), recursive_cte()."""

from fusion.orm.expressions import Exp, cte, recursive_cte, union
from fusion.orm.fields import field
from fusion.orm.model import Model


class Post(Model):
    id: int | None = None
    user_id: int
    title: str
    status: str | None = None
    created_at: str | None = None


class Category(Model):
    id: int | None = None
    name: str
    parent_id: int | None = None


# ---------------------------------------------------------------------------
# Exp — raw SQL passthrough
# ---------------------------------------------------------------------------


def test_exp_stores_sql():
    e = Exp("score + 1")
    assert e.sql == "score + 1"


def test_exp_appears_in_update_set():
    sql, params = Post.update().set(user_id=Exp("user_id + 1")).where(id=1).build()
    assert "user_id + 1" in sql


def test_exp_in_where_clause():
    sql, params = Post.select().where_raw(Exp('"meta" @> \'{"active": true}\'')).build()
    assert "@>" in sql


# ---------------------------------------------------------------------------
# union()
# ---------------------------------------------------------------------------


def test_union_builds_union_sql():
    q = union(
        Post.select("id", "title").where(status="published"),
        Post.select("id", "title").where(status="featured"),
    )
    sql, params = q.build()
    assert "UNION" in sql
    assert "published" in params
    assert "featured" in params


def test_union_all_includes_all_keyword():
    q = union(
        Post.select("id", "title").where(user_id=1),
        Post.select("id", "title").where(user_id=2),
        all=True,
    )
    sql, params = q.build()
    assert "UNION ALL" in sql


def test_union_deduplicates_by_default():
    q = union(
        Post.select("id").where(status="a"),
        Post.select("id").where(status="b"),
    )
    sql, params = q.build()
    assert "UNION ALL" not in sql
    assert "UNION" in sql


# ---------------------------------------------------------------------------
# cte()
# ---------------------------------------------------------------------------


def test_cte_builds_with_clause():
    q = cte(
        main=Post.select("id", "title").where(status="recent"),
        recent=Post.select().where(created_at__is_not_null=True),
    )
    sql, params = q.build()
    assert "WITH" in sql
    assert "recent" in sql


def test_cte_multiple_named_ctes():
    q = cte(
        main=Post.select("id").where(status="ok"),
        cte1=Post.select().where(user_id=1),
        cte2=Post.select().where(user_id=2),
    )
    sql, params = q.build()
    assert "cte1" in sql
    assert "cte2" in sql


# ---------------------------------------------------------------------------
# recursive_cte()
# ---------------------------------------------------------------------------


def test_recursive_cte_includes_recursive_keyword():
    q = recursive_cte(
        main=Category.select("id", "name").where(parent_id__is_null=True),
        tree=union(
            Category.select("id", "name", "parent_id").where(parent_id__is_null=True),
            Category.select("id", "name", "parent_id").where(parent_id__is_not_null=True),
            all=True,
        ),
    )
    sql, params = q.build()
    assert "RECURSIVE" in sql
    assert "tree" in sql
