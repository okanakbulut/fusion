"""Tests for fusion shift apply logic and CLI wiring."""

import pytest

# ---------------------------------------------------------------------------
# Slice 1: CREATE_TRACKING_TABLE_SQL constant
# ---------------------------------------------------------------------------


def test_create_tracking_table_sql_constant():
    from fusion.orm.shift.apply import CREATE_TRACKING_TABLE_SQL

    assert CREATE_TRACKING_TABLE_SQL == (
        "CREATE TABLE IF NOT EXISTS fusion_shifts (\n"
        "    id          SERIAL PRIMARY KEY,\n"
        "    name        TEXT NOT NULL UNIQUE,\n"
        "    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()\n"
        ")"
    )


# ---------------------------------------------------------------------------
# Slice 2: apply_shifts() with no shift files prints "Nothing to apply."
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_shifts_no_files_prints_nothing_to_apply(capsys):
    from unittest.mock import AsyncMock

    from fusion.orm.shift.apply import _apply_shifts_on_conn

    mock_conn = AsyncMock()
    mock_conn.fetch.return_value = []

    await _apply_shifts_on_conn(mock_conn, [])

    out = capsys.readouterr().out
    assert out == "Nothing to apply.\n"


# ---------------------------------------------------------------------------
# Slice 3: already-applied shift → "Nothing to apply."
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_shifts_already_applied_prints_nothing_to_apply(tmp_path, capsys):
    from unittest.mock import AsyncMock, MagicMock

    from fusion.orm.shift.apply import _apply_shifts_on_conn

    # Write a shift file
    shift_file = tmp_path / "20260101_000000_initial.py"
    shift_file.write_text(
        "from fusion.orm.shift import Shift, RunSQL\n\n"
        "class Initial(Shift):\n"
        "    operations = [RunSQL('SELECT 1')]\n"
    )

    mock_conn = AsyncMock()
    # DB says it's already applied
    row = MagicMock()
    row.__getitem__ = lambda self, key: "20260101_000000_initial"
    mock_conn.fetch.return_value = [row]

    await _apply_shifts_on_conn(mock_conn, [shift_file])

    out = capsys.readouterr().out
    assert out == "Nothing to apply.\n"


# ---------------------------------------------------------------------------
# Slice 4: one unapplied shift with RunSQL operation
# ---------------------------------------------------------------------------


def _make_mock_conn():
    """Build an AsyncMock connection with a working transaction() context manager."""
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    mock_conn = AsyncMock()

    @asynccontextmanager
    async def _transaction():
        yield

    mock_conn.transaction = _transaction
    return mock_conn


@pytest.mark.asyncio
async def test_apply_shifts_one_unapplied_run_sql(tmp_path, capsys):
    from unittest.mock import call

    from fusion.orm.shift.apply import CREATE_TRACKING_TABLE_SQL, _apply_shifts_on_conn

    shift_file = tmp_path / "20260101_000000_initial.py"
    shift_file.write_text(
        "from fusion.orm.shift import Shift, RunSQL\n\n"
        "class Initial(Shift):\n"
        "    operations = [RunSQL('SELECT 1')]\n"
    )

    mock_conn = _make_mock_conn()
    mock_conn.fetch.return_value = []  # nothing applied yet

    await _apply_shifts_on_conn(mock_conn, [shift_file])

    # Should have called execute with the tracking table creation first
    assert mock_conn.execute.call_args_list[0] == call(CREATE_TRACKING_TABLE_SQL)

    # Inside the transaction: RunSQL DDL + INSERT
    assert mock_conn.execute.call_args_list[1] == call("SELECT 1")
    assert mock_conn.execute.call_args_list[2] == call(
        "INSERT INTO fusion_shifts (name) VALUES ($1)",
        "20260101_000000_initial",
    )

    out = capsys.readouterr().out
    assert out == "→ Applied: 20260101_000000_initial\n1 shift(s) applied.\n"


# ---------------------------------------------------------------------------
# Slice 5: Multiple shifts — applied in sorted filename order
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_shifts_multiple_unapplied_applied_in_order(tmp_path, capsys):
    from unittest.mock import call

    from fusion.orm.shift.apply import CREATE_TRACKING_TABLE_SQL, _apply_shifts_on_conn

    # Create two shift files — intentionally pass them in reverse order to test sorting
    shift_b = tmp_path / "20260102_000000_second.py"
    shift_a = tmp_path / "20260101_000000_first.py"

    shift_a.write_text(
        "from fusion.orm.shift import Shift, RunSQL\n\n"
        "class First(Shift):\n"
        "    operations = [RunSQL('SELECT 1')]\n"
    )
    shift_b.write_text(
        "from fusion.orm.shift import Shift, RunSQL\n\n"
        "class Second(Shift):\n"
        "    operations = [RunSQL('SELECT 2')]\n"
    )

    mock_conn = _make_mock_conn()
    mock_conn.fetch.return_value = []

    await _apply_shifts_on_conn(mock_conn, [shift_b, shift_a])  # reversed order

    execute_calls = mock_conn.execute.call_args_list
    assert execute_calls[0] == call(CREATE_TRACKING_TABLE_SQL)
    # First shift operations
    assert execute_calls[1] == call("SELECT 1")
    assert execute_calls[2] == call(
        "INSERT INTO fusion_shifts (name) VALUES ($1)",
        "20260101_000000_first",
    )
    # Second shift operations
    assert execute_calls[3] == call("SELECT 2")
    assert execute_calls[4] == call(
        "INSERT INTO fusion_shifts (name) VALUES ($1)",
        "20260102_000000_second",
    )

    out = capsys.readouterr().out
    assert out == (
        "→ Applied: 20260101_000000_first\n→ Applied: 20260102_000000_second\n2 shift(s) applied.\n"
    )


# ---------------------------------------------------------------------------
# Slice 6: RunPython operation receives the live connection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_shifts_run_python_receives_connection(tmp_path, capsys):
    from fusion.orm.shift.apply import _apply_shifts_on_conn

    received_conn = []

    async def my_fn(conn):
        received_conn.append(conn)

    shift_file = tmp_path / "20260101_000000_py_op.py"
    # We can't easily embed a closure in a file, so we use a module-level approach.
    # Instead, directly pass a shift with RunPython via patching _load_shift.
    from unittest.mock import patch

    from fusion.orm.shift.operations import RunPython, Shift

    class MyShift(Shift):
        operations = [RunPython(my_fn)]

    mock_conn = _make_mock_conn()
    mock_conn.fetch.return_value = []

    # Create a dummy file so the path exists for stem extraction
    shift_file.write_text("# placeholder")

    with patch("fusion.orm.shift.replay._load_shift", return_value=MyShift):
        await _apply_shifts_on_conn(mock_conn, [shift_file])

    assert len(received_conn) == 1
    assert received_conn[0] is mock_conn

    out = capsys.readouterr().out
    assert out == "→ Applied: 20260101_000000_py_op\n1 shift(s) applied.\n"


# ---------------------------------------------------------------------------
# Slice 7: failed shift — transaction rolls back, error message includes name
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_shifts_failed_shift_raises_with_name(tmp_path, capsys):
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock, call, patch

    from fusion.orm.shift.apply import CREATE_TRACKING_TABLE_SQL, _apply_shifts_on_conn
    from fusion.orm.shift.operations import RunSQL, Shift

    shift_file = tmp_path / "20260101_000000_bad.py"
    shift_file.write_text("# placeholder")

    class BadShift(Shift):
        operations = [RunSQL("INVALID SQL")]

    # Track transaction rollback — asynccontextmanager raises on exit if body raises
    rolled_back = []

    @asynccontextmanager
    async def _transaction():
        try:
            yield
        except Exception:
            rolled_back.append(True)
            raise

    mock_conn = AsyncMock()
    mock_conn.fetch.return_value = []
    mock_conn.transaction = _transaction
    mock_conn.execute.side_effect = [
        None,  # CREATE_TRACKING_TABLE_SQL
        RuntimeError("syntax error"),  # INVALID SQL
    ]

    with patch("fusion.orm.shift.replay._load_shift", return_value=BadShift):
        with pytest.raises(RuntimeError, match="syntax error"):
            await _apply_shifts_on_conn(mock_conn, [shift_file])

    assert rolled_back == [True]
    # Only the tracking table creation and the failing operation were called
    assert mock_conn.execute.call_args_list[0] == call(CREATE_TRACKING_TABLE_SQL)
    assert mock_conn.execute.call_args_list[1] == call("INVALID SQL")
    # INSERT should NOT have been called
    assert len(mock_conn.execute.call_args_list) == 2


# ---------------------------------------------------------------------------
# Slice 8: cmd_shift CLI function — reads DSN, calls apply_shifts, exits 1 on error
# ---------------------------------------------------------------------------


def test_cmd_shift_calls_apply_shifts(tmp_path, monkeypatch, capsys):
    import argparse
    from unittest.mock import AsyncMock, patch

    from fusion.cli import cmd_shift

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    args = argparse.Namespace(
        dsn="postgresql://localhost/test",
        migrations_dir=str(migrations_dir),
    )

    with patch("fusion.orm.shift.apply.apply_shifts", new=AsyncMock()) as mock_apply:
        cmd_shift(args)

    mock_apply.assert_called_once_with("postgresql://localhost/test", [])


def test_cmd_shift_reads_dsn_from_env(tmp_path, monkeypatch, capsys):
    import argparse
    from unittest.mock import AsyncMock, patch

    from fusion.cli import cmd_shift

    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/envdb")
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    args = argparse.Namespace(
        dsn=None,
        migrations_dir=str(migrations_dir),
    )

    with patch("fusion.orm.shift.apply.apply_shifts", new=AsyncMock()) as mock_apply:
        cmd_shift(args)

    mock_apply.assert_called_once_with("postgresql://localhost/envdb", [])


def test_cmd_shift_reads_postgres_dsn_env(tmp_path, monkeypatch):
    import argparse
    from unittest.mock import AsyncMock, patch

    from fusion.cli import cmd_shift

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://localhost/pgdb")
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    args = argparse.Namespace(
        dsn=None,
        migrations_dir=str(migrations_dir),
    )

    with patch("fusion.orm.shift.apply.apply_shifts", new=AsyncMock()) as mock_apply:
        cmd_shift(args)

    mock_apply.assert_called_once_with("postgresql://localhost/pgdb", [])


def test_cmd_shift_exits_1_on_error(tmp_path, monkeypatch, capsys):
    import argparse
    from unittest.mock import AsyncMock, patch

    from fusion.cli import cmd_shift

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    shift_file = migrations_dir / "20260101_000000_bad.py"
    shift_file.write_text("# placeholder")

    args = argparse.Namespace(
        dsn="postgresql://localhost/test",
        migrations_dir=str(migrations_dir),
    )

    failing_apply = AsyncMock(side_effect=RuntimeError("connection refused"))

    with patch("fusion.orm.shift.apply.apply_shifts", new=failing_apply):
        with pytest.raises(SystemExit) as exc_info:
            cmd_shift(args)

    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert "20260101_000000_bad" in err
    assert "connection refused" in err


# ---------------------------------------------------------------------------
# Slice 9: Missing DSN — prints clear error, exits 1
# ---------------------------------------------------------------------------


def test_cmd_shift_missing_dsn_exits_1(tmp_path, monkeypatch, capsys):
    import argparse

    from fusion.cli import cmd_shift

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)

    args = argparse.Namespace(
        dsn=None,
        migrations_dir=str(tmp_path / "migrations"),
    )

    with pytest.raises(SystemExit) as exc_info:
        cmd_shift(args)

    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert "DSN required" in err


# ---------------------------------------------------------------------------
# apply_shifts wrapper — connects and delegates to _apply_shifts_on_conn
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_shifts_connects_and_delegates(capsys):
    from unittest.mock import AsyncMock, patch

    from fusion.orm.shift.apply import apply_shifts

    mock_conn = _make_mock_conn()
    mock_conn.fetch.return_value = []
    mock_conn.close = AsyncMock()

    with patch("asyncpg.connect", new=AsyncMock(return_value=mock_conn)):
        await apply_shifts("postgresql://localhost/test", [])

    mock_conn.close.assert_called_once()
    out = capsys.readouterr().out
    assert out == "Nothing to apply.\n"


@pytest.mark.asyncio
async def test_apply_shifts_closes_conn_on_error(capsys):
    from unittest.mock import AsyncMock, patch

    from fusion.orm.shift.apply import apply_shifts

    mock_conn = _make_mock_conn()
    mock_conn.fetch.side_effect = RuntimeError("db error")
    mock_conn.close = AsyncMock()

    with patch("asyncpg.connect", new=AsyncMock(return_value=mock_conn)):
        with pytest.raises(RuntimeError, match="db error"):
            await apply_shifts("postgresql://localhost/test", [])

    mock_conn.close.assert_called_once()
