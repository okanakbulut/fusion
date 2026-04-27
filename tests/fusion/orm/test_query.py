"""Tests for fusion.orm.query — SQL generation for all four query builders.

SQL strings are verified via .build() → (sql, params).
.fetch() / .fetch_one() are tested with a mock async connection.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from fusion.orm.conditions import Q
from fusion.orm.constraints import ForeignKey
from fusion.orm.fields import db_now, db_uuid, field
from fusion.orm.model import Model


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
    assert sql == 'SELECT * FROM "posts" WHERE "user_id"=$1 ORDER BY "created_at" DESC LIMIT 10 OFFSET 20'
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
    assert sql == 'SELECT * FROM "articles" LEFT JOIN "authors" ON "articles"."author_id"="authors"."id"'
    assert params == []


def test_select_right_join():
    sql, params = Article.select().join(Author, how="right").build()
    assert sql == 'SELECT * FROM "articles" RIGHT JOIN "authors" ON "articles"."author_id"="authors"."id"'
    assert params == []


def test_select_outer_join():
    sql, params = Article.select().join(Author, how="outer").build()
    assert sql == 'SELECT * FROM "articles" FULL OUTER JOIN "authors" ON "articles"."author_id"="authors"."id"'
    assert params == []


def test_select_join_with_where():
    sql, params = Article.select().join(Author).where(author_id=1).build()
    assert sql == 'SELECT * FROM "articles" JOIN "authors" ON "articles"."author_id"="authors"."id" WHERE "articles"."author_id"=$1'
    assert params == [1]


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
    metric = Metric(name="cpu")
    sql, params = Metric.insert().values(metric).build()
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


def test_insert_single_row_sql():
    post = Post(user_id=1, title="hello")
    sql, params = Post.insert().values(post).build()
    assert sql == 'INSERT INTO "posts" ("user_id","title","body") VALUES ($1,$2,$3) RETURNING *'
    assert params == [1, "hello", None]


def test_insert_bulk_values():
    p1 = Post(user_id=1, title="first")
    p2 = Post(user_id=2, title="second")
    sql, params = Post.insert().values([p1, p2]).build()
    assert sql == (
        'INSERT INTO "posts" ("user_id","title","body") '
        'VALUES ($1,$2,$3),($4,$5,$6) RETURNING *'
    )
    assert params == [1, "first", None, 2, "second", None]


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
    sql, params = Post.delete().where(id=42).build()
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
    post = Post(user_id=1, title="hello")
    await Post.insert().values(post).fetch(record_conn)
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
