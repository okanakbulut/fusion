"""Tests for fusion history CLI subcommand."""

# ---------------------------------------------------------------------------
# Slice 1: SQL constants
# ---------------------------------------------------------------------------


def test_query_shifts_sql_constant():
    from fusion.orm.shift.history import QUERY_SHIFTS_SQL

    assert QUERY_SHIFTS_SQL == ("SELECT id, name, applied_at FROM fusion_shifts ORDER BY id ASC")


def test_check_table_sql_constant():
    from fusion.orm.shift.history import CHECK_TABLE_SQL

    assert CHECK_TABLE_SQL == (
        "SELECT 1 FROM information_schema.tables"
        " WHERE table_name = 'fusion_shifts' AND table_schema = 'public'"
    )


# ---------------------------------------------------------------------------
# Slice 2: _history_on_conn — table doesn't exist
# ---------------------------------------------------------------------------


import pytest


@pytest.mark.asyncio
async def test_history_on_conn_table_missing():
    from unittest.mock import AsyncMock

    from fusion.orm.shift.history import CHECK_TABLE_SQL, _history_on_conn

    mock_conn = AsyncMock()
    mock_conn.fetchrow.return_value = None  # table doesn't exist

    result = await _history_on_conn(mock_conn)

    assert result == "No shifts have been applied yet.\n"
    mock_conn.fetchrow.assert_called_once_with(CHECK_TABLE_SQL)


# ---------------------------------------------------------------------------
# Slice 3: _history_on_conn — table exists but empty
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_history_on_conn_table_empty():
    from unittest.mock import AsyncMock, MagicMock

    from fusion.orm.shift.history import CHECK_TABLE_SQL, QUERY_SHIFTS_SQL, _history_on_conn

    mock_conn = AsyncMock()
    mock_conn.fetchrow.return_value = MagicMock()  # table exists (truthy)
    mock_conn.fetch.return_value = []  # no rows

    result = await _history_on_conn(mock_conn)

    assert result == "No shifts have been applied yet.\n"
    mock_conn.fetchrow.assert_called_once_with(CHECK_TABLE_SQL)
    mock_conn.fetch.assert_called_once_with(QUERY_SHIFTS_SQL)


# ---------------------------------------------------------------------------
# Slice 4: _history_on_conn — rows → formatted table
# ---------------------------------------------------------------------------


def _make_row(id, name, applied_at_str):
    from datetime import datetime, timezone
    from unittest.mock import MagicMock

    dt = datetime.fromisoformat(applied_at_str).replace(tzinfo=timezone.utc)
    row = MagicMock()
    row.__getitem__ = lambda self, key: {"id": id, "name": name, "applied_at": dt}[key]
    return row


@pytest.mark.asyncio
async def test_history_on_conn_formatted_table():
    from unittest.mock import AsyncMock, MagicMock

    from fusion.orm.shift.history import _history_on_conn

    mock_conn = AsyncMock()
    mock_conn.fetchrow.return_value = MagicMock()  # table exists
    mock_conn.fetch.return_value = [
        _make_row(1, "20260430_000000_initial", "2026-04-30T00:01:23"),
        _make_row(2, "20260501_143012_add_status_to_users", "2026-05-01T14:30:15"),
    ]

    result = await _history_on_conn(mock_conn)

    expected = (
        "  #   Name                                      Applied At\n"
        "  1   20260430_000000_initial                   2026-04-30 00:01:23 UTC\n"
        "  2   20260501_143012_add_status_to_users       2026-05-01 14:30:15 UTC\n"
        "\n"
        "2 shift(s) applied.\n"
    )
    assert result == expected


# ---------------------------------------------------------------------------
# Slice 6: --json flag with rows
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_history_on_conn_json_with_rows():
    import json
    from unittest.mock import AsyncMock, MagicMock

    from fusion.orm.shift.history import _history_on_conn

    mock_conn = AsyncMock()
    mock_conn.fetchrow.return_value = MagicMock()  # table exists
    mock_conn.fetch.return_value = [
        _make_row(1, "20260430_000000_initial", "2026-04-30T00:01:23"),
    ]

    result = await _history_on_conn(mock_conn, as_json=True)

    parsed = json.loads(result)
    assert (
        result
        == '[{"id": 1, "name": "20260430_000000_initial", "applied_at": "2026-04-30T00:01:23+00:00"}]\n'
    )
    assert parsed == [
        {"id": 1, "name": "20260430_000000_initial", "applied_at": "2026-04-30T00:01:23+00:00"}
    ]


# ---------------------------------------------------------------------------
# Slice 7: --json with zero rows returns "[]\n"
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_history_on_conn_json_empty():
    from unittest.mock import AsyncMock, MagicMock

    from fusion.orm.shift.history import _history_on_conn

    mock_conn = AsyncMock()
    mock_conn.fetchrow.return_value = MagicMock()  # table exists
    mock_conn.fetch.return_value = []

    result = await _history_on_conn(mock_conn, as_json=True)

    assert result == "[]\n"


# ---------------------------------------------------------------------------
# Slice 8: get_history wrapper — connects, delegates, closes conn
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_history_connects_and_delegates():
    from unittest.mock import AsyncMock, MagicMock, patch

    from fusion.orm.shift.history import get_history

    mock_conn = AsyncMock()
    mock_conn.fetchrow.return_value = None  # table doesn't exist
    mock_conn.close = AsyncMock()

    with patch("asyncpg.connect", new=AsyncMock(return_value=mock_conn)) as mock_connect:
        result = await get_history("postgresql://localhost/test")

    mock_connect.assert_called_once_with("postgresql://localhost/test")
    mock_conn.close.assert_called_once()
    assert result == "No shifts have been applied yet.\n"


@pytest.mark.asyncio
async def test_get_history_closes_conn_on_error():
    from unittest.mock import AsyncMock, patch

    from fusion.orm.shift.history import get_history

    mock_conn = AsyncMock()
    mock_conn.fetchrow.side_effect = RuntimeError("db error")
    mock_conn.close = AsyncMock()

    with patch("asyncpg.connect", new=AsyncMock(return_value=mock_conn)):
        with pytest.raises(RuntimeError, match="db error"):
            await get_history("postgresql://localhost/test")

    mock_conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# Slice 9: cmd_history CLI function
# ---------------------------------------------------------------------------


def test_cmd_history_calls_get_history_and_prints(capsys):
    import argparse
    from unittest.mock import AsyncMock, patch

    from fusion.cli import cmd_history

    args = argparse.Namespace(
        dsn="postgresql://localhost/test",
        json=False,
    )

    with patch(
        "fusion.orm.shift.history.get_history",
        new=AsyncMock(return_value="No shifts have been applied yet.\n"),
    ) as mock_get:
        cmd_history(args)

    mock_get.assert_called_once_with("postgresql://localhost/test", as_json=False)
    out = capsys.readouterr().out
    assert out == "No shifts have been applied yet.\n"


def test_cmd_history_reads_dsn_from_env(monkeypatch, capsys):
    import argparse
    from unittest.mock import AsyncMock, patch

    from fusion.cli import cmd_history

    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/envdb")

    args = argparse.Namespace(
        dsn=None,
        json=False,
    )

    with patch(
        "fusion.orm.shift.history.get_history",
        new=AsyncMock(return_value="No shifts have been applied yet.\n"),
    ) as mock_get:
        cmd_history(args)

    mock_get.assert_called_once_with("postgresql://localhost/envdb", as_json=False)


def test_cmd_history_exits_1_on_error(capsys):
    import argparse
    from unittest.mock import AsyncMock, patch

    from fusion.cli import cmd_history

    args = argparse.Namespace(
        dsn="postgresql://localhost/test",
        json=False,
    )

    with patch(
        "fusion.orm.shift.history.get_history",
        new=AsyncMock(side_effect=RuntimeError("connection refused")),
    ):
        with pytest.raises(SystemExit) as exc_info:
            cmd_history(args)

    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert err == "Error: connection refused\n"


# ---------------------------------------------------------------------------
# Slice 10: missing DSN — prints error to stderr, exits 1
# ---------------------------------------------------------------------------


def test_cmd_history_missing_dsn_exits_1(monkeypatch, capsys):
    import argparse

    from fusion.cli import cmd_history

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)

    args = argparse.Namespace(
        dsn=None,
        json=False,
    )

    with pytest.raises(SystemExit) as exc_info:
        cmd_history(args)

    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert err == "Error: DSN required. Pass --dsn or set POSTGRES_DSN / DATABASE_URL.\n"


# ---------------------------------------------------------------------------
# Slice 5: singular grammar for 1 shift
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_history_on_conn_single_shift_singular():
    from unittest.mock import AsyncMock, MagicMock

    from fusion.orm.shift.history import _history_on_conn

    mock_conn = AsyncMock()
    mock_conn.fetchrow.return_value = MagicMock()  # table exists
    mock_conn.fetch.return_value = [
        _make_row(1, "20260430_000000_initial", "2026-04-30T00:01:23"),
    ]

    result = await _history_on_conn(mock_conn)

    expected = (
        "  #   Name                                      Applied At\n"
        "  1   20260430_000000_initial                   2026-04-30 00:01:23 UTC\n"
        "\n"
        "1 shift applied.\n"
    )
    assert result == expected
