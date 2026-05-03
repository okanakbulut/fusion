"""Fusion shift history — queries applied shifts from the database."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    import asyncpg

QUERY_SHIFTS_SQL = "SELECT id, name, applied_at FROM fusion_shifts ORDER BY id ASC"

CHECK_TABLE_SQL = (
    "SELECT 1 FROM information_schema.tables"
    " WHERE table_name = 'fusion_shifts' AND table_schema = 'public'"
)


def _format_json(rows: list) -> str:
    """Format rows as a JSON array string."""
    data = [
        {
            "id": row["id"],
            "name": row["name"],
            "applied_at": row["applied_at"].isoformat(),
        }
        for row in rows
    ]
    return json.dumps(data) + "\n"


def _format_table(rows: list) -> str:
    """Format a list of asyncpg rows into the history table string."""
    lines = [f"{'#':>3}   {'Name':<42}Applied At"]
    for row in rows:
        id_ = row["id"]
        name = row["name"]
        applied_at = row["applied_at"].strftime("%Y-%m-%d %H:%M:%S UTC")
        lines.append(f"{id_:>3}   {name:<42}{applied_at}")
    count = len(rows)
    noun = "shift" if count == 1 else "shift(s)"
    lines.append("")
    lines.append(f"{count} {noun} applied.")
    return "\n".join(lines) + "\n"


async def _history_on_conn(conn: asyncpg.Connection, as_json: bool = False) -> str:
    """Return formatted history string from an already-open connection."""
    table_row = await conn.fetchrow(CHECK_TABLE_SQL)
    if table_row is None:
        return "No shifts have been applied yet.\n"

    rows = await conn.fetch(QUERY_SHIFTS_SQL)
    if not rows:
        if as_json:
            return "[]\n"
        return "No shifts have been applied yet.\n"

    if as_json:
        return _format_json(rows)
    return _format_table(rows)


async def get_history(dsn: str, as_json: bool = False) -> str:
    """Connect to the database and return the formatted history string."""
    import asyncpg

    conn = await asyncpg.connect(dsn)
    try:
        return await _history_on_conn(conn, as_json=as_json)
    finally:
        await conn.close()
