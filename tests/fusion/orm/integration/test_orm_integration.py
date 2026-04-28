"""Integration tests: migration → insert → join → prefetch against a real Postgres instance."""

import asyncpg
import pytest

from fusion.orm import Model
from fusion.orm.migration.apply import to_ddl
from fusion.orm.migration.diff import diff
from fusion.orm.migration.snapshot import serialize

from .conftest import apply_schema, drop_tables

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Models used across all integration tests
# ---------------------------------------------------------------------------


class Org(Model):
    id: int | None = None
    name: str


class User(Model):
    id: int | None = None
    email: str
    org: Org | None = None


_MODELS = [Org, User]


# ---------------------------------------------------------------------------
# Bullet 1 — migration creates the expected tables
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_migration_creates_tables(pg_conn: asyncpg.Connection):
    await drop_tables(pg_conn, _MODELS)
    await apply_schema(pg_conn, _MODELS)

    tables = await pg_conn.fetch(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
    )
    table_names = {r["tablename"] for r in tables}
    assert "orgs" in table_names
    assert "users" in table_names


# ---------------------------------------------------------------------------
# Bullet 2 — insert returns the persisted row with server-assigned id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_insert_returns_row(pg_conn: asyncpg.Connection):
    await drop_tables(pg_conn, _MODELS)
    await apply_schema(pg_conn, _MODELS)

    results = await Org.insert().values(Org(name="Acme")).fetch(pg_conn)

    assert len(results) == 1
    assert results[0].name == "Acme"
    assert results[0].id is not None


# ---------------------------------------------------------------------------
# Bullet 3 — join filters rows via the joined table
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_join_filters_by_joined_table(pg_conn: asyncpg.Connection):
    await drop_tables(pg_conn, _MODELS)
    await apply_schema(pg_conn, _MODELS)

    (acme,) = await Org.insert().values(Org(name="Acme")).fetch(pg_conn)
    (other,) = await Org.insert().values(Org(name="Other")).fetch(pg_conn)
    await User.insert().values(User(email="alice@acme.com", org_id=acme.id)).fetch(pg_conn)
    await User.insert().values(User(email="bob@other.com", org_id=other.id)).fetch(pg_conn)

    results = await User.select().join(Org).where(org__name="Acme").fetch(pg_conn)

    assert len(results) == 1
    assert results[0].email == "alice@acme.com"


# ---------------------------------------------------------------------------
# Bullet 4 — prefetch hydrates the relationship on each result
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_prefetch_hydrates_org_on_user(pg_conn: asyncpg.Connection):
    await drop_tables(pg_conn, _MODELS)
    await apply_schema(pg_conn, _MODELS)

    (acme,) = await Org.insert().values(Org(name="Acme")).fetch(pg_conn)
    await User.insert().values(User(email="alice@acme.com", org_id=acme.id)).fetch(pg_conn)

    results = await User.select().prefetch(Org).fetch(pg_conn)

    assert len(results) == 1
    assert isinstance(results[0].org, Org)
    assert results[0].org.name == "Acme"


# ---------------------------------------------------------------------------
# Bullet 5 — join + prefetch on the same model: no DuplicateAliasError,
#             relationship hydrated, join filter applied
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_join_and_prefetch_same_model(pg_conn: asyncpg.Connection):
    await drop_tables(pg_conn, _MODELS)
    await apply_schema(pg_conn, _MODELS)

    (acme,) = await Org.insert().values(Org(name="Acme")).fetch(pg_conn)
    (other,) = await Org.insert().values(Org(name="Other")).fetch(pg_conn)
    await User.insert().values(User(email="alice@acme.com", org_id=acme.id)).fetch(pg_conn)
    await User.insert().values(User(email="bob@other.com", org_id=other.id)).fetch(pg_conn)

    results = await User.select().join(Org).prefetch(Org).where(org__name="Acme").fetch(pg_conn)

    assert len(results) == 1
    assert results[0].email == "alice@acme.com"
    assert isinstance(results[0].org, Org)
    assert results[0].org.name == "Acme"
