"""Integration tests: fusion CLI migrate command against a real Postgres instance."""

import argparse
import os
import sys
import types

import asyncpg
import pytest
import pytest_asyncio

from fusion.orm.model import Model

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def models_module(monkeypatch):
    module_name = "_fusion_migration_cli_models"

    class Org(Model):
        id: int | None = None
        name: str

    class Member(Model):
        id: int | None = None
        org_id: int
        email: str

    Org.__module__ = module_name
    Member.__module__ = module_name

    mod = types.ModuleType(module_name)
    mod.Org = Org
    mod.Member = Member

    monkeypatch.setitem(sys.modules, module_name, mod)
    return module_name


@pytest_asyncio.fixture
async def pg_conn():
    conn = await asyncpg.connect(os.environ["POSTGRES_DSN"])
    yield conn
    await conn.close()


async def _drop_tables(conn: asyncpg.Connection, names: list[str]) -> None:
    for name in reversed(names):
        await conn.execute(f'DROP TABLE IF EXISTS "{name}" CASCADE')


async def _drop_schema_tables(conn: asyncpg.Connection, schema: str, names: list[str]) -> None:
    for name in reversed(names):
        await conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{name}" CASCADE')


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_migrate_creates_tables_in_postgres(tmp_path, models_module, pg_conn):
    from fusion.cli import cmd_migrate

    await _drop_tables(pg_conn, ["members", "orgs"])

    cmd_migrate(
        argparse.Namespace(
            module=[models_module],
            dsn=os.environ["POSTGRES_DSN"],
            snapshot=str(tmp_path / "snapshot.yaml"),
            drop=False,
        )
    )

    rows = await pg_conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    names = {r["tablename"] for r in rows}
    assert "orgs" in names
    assert "members" in names


@pytest.mark.asyncio
async def test_migrate_writes_snapshot_after_apply(tmp_path, models_module, pg_conn):
    from fusion.cli import cmd_migrate

    await _drop_tables(pg_conn, ["members", "orgs"])
    snap = tmp_path / "snapshot.yaml"

    cmd_migrate(
        argparse.Namespace(
            module=[models_module],
            dsn=os.environ["POSTGRES_DSN"],
            snapshot=str(snap),
            drop=False,
        )
    )

    assert snap.exists()


@pytest.mark.asyncio
async def test_migrate_is_idempotent(tmp_path, models_module, pg_conn, capsys):
    from fusion.cli import cmd_migrate

    await _drop_tables(pg_conn, ["members", "orgs"])
    snap = str(tmp_path / "snapshot.yaml")
    dsn = os.environ["POSTGRES_DSN"]
    args = argparse.Namespace(module=[models_module], dsn=dsn, snapshot=snap, drop=False)

    cmd_migrate(args)
    capsys.readouterr()

    cmd_migrate(args)
    assert "Nothing to migrate" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_migrate_applies_additive_schema_change(
    tmp_path, models_module, pg_conn, monkeypatch
):
    from fusion.cli import cmd_migrate

    await _drop_tables(pg_conn, ["members", "orgs"])
    snap = str(tmp_path / "snapshot.yaml")
    dsn = os.environ["POSTGRES_DSN"]

    cmd_migrate(argparse.Namespace(module=[models_module], dsn=dsn, snapshot=snap, drop=False))

    class Tag(Model):
        id: int | None = None
        label: str

    Tag.__module__ = models_module
    sys.modules[models_module].Tag = Tag

    cmd_migrate(argparse.Namespace(module=[models_module], dsn=dsn, snapshot=snap, drop=False))

    rows = await pg_conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    assert "tags" in {r["tablename"] for r in rows}


@pytest.mark.asyncio
async def test_migrate_multiple_modules(tmp_path, monkeypatch, pg_conn):
    from fusion.cli import cmd_migrate

    mod_a = "_fusion_integ_mod_a"
    mod_b = "_fusion_integ_mod_b"

    class Widget(Model):
        id: int | None = None
        label: str

    class Gadget(Model):
        id: int | None = None
        code: str

    Widget.__module__ = mod_a
    Gadget.__module__ = mod_b

    a = types.ModuleType(mod_a)
    a.Widget = Widget
    b = types.ModuleType(mod_b)
    b.Gadget = Gadget

    monkeypatch.setitem(sys.modules, mod_a, a)
    monkeypatch.setitem(sys.modules, mod_b, b)

    await _drop_tables(pg_conn, ["gadgets", "widgets"])

    cmd_migrate(
        argparse.Namespace(
            module=[mod_a, mod_b],
            dsn=os.environ["POSTGRES_DSN"],
            snapshot=str(tmp_path / "snapshot.yaml"),
            drop=False,
        )
    )

    rows = await pg_conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    names = {r["tablename"] for r in rows}
    assert "widgets" in names
    assert "gadgets" in names


@pytest.mark.asyncio
async def test_migrate_schema_qualified_table(tmp_path, monkeypatch, pg_conn):
    from fusion.cli import cmd_migrate

    module_name = "_fusion_integ_schema_mod"

    class Report(Model):
        __schema__ = "reporting"
        id: int | None = None
        title: str

    Report.__module__ = module_name
    mod = types.ModuleType(module_name)
    mod.Report = Report
    monkeypatch.setitem(sys.modules, module_name, mod)

    await pg_conn.execute("CREATE SCHEMA IF NOT EXISTS reporting")
    await _drop_schema_tables(pg_conn, "reporting", ["reports"])

    cmd_migrate(
        argparse.Namespace(
            module=[module_name],
            dsn=os.environ["POSTGRES_DSN"],
            snapshot=str(tmp_path / "snapshot.yaml"),
            drop=False,
        )
    )

    rows = await pg_conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'reporting'")
    assert "reports" in {r["tablename"] for r in rows}
