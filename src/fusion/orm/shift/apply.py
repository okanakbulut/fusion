"""Fusion shift apply logic — applies unapplied shift files to the database."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    import asyncpg

CREATE_TRACKING_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS fusion_shifts (\n"
    "    id          SERIAL PRIMARY KEY,\n"
    "    name        TEXT NOT NULL UNIQUE,\n"
    "    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()\n"
    ")"
)


async def _apply_shifts_on_conn(conn: asyncpg.Connection, shift_files: list[Path]) -> None:
    """Apply unapplied shifts to an already-open connection."""
    from fusion.orm.shift.operations import RunPython
    from fusion.orm.shift.replay import _load_shift

    await conn.execute(CREATE_TRACKING_TABLE_SQL)

    rows = await conn.fetch("SELECT name FROM fusion_shifts")
    applied = {row["name"] for row in rows}

    pending = [f for f in sorted(shift_files, key=lambda p: p.name) if f.stem not in applied]

    if not pending:
        print("Nothing to apply.")
        return

    for path in pending:
        shift_cls = _load_shift(path)
        name = path.stem
        async with conn.transaction():
            for op in shift_cls.operations:
                if isinstance(op, RunPython):
                    await op.fn(conn)
                else:
                    await conn.execute(op.to_ddl())
            await conn.execute(
                "INSERT INTO fusion_shifts (name) VALUES ($1)",
                name,
            )
        print(f"→ Applied: {name}")

    print(f"{len(pending)} shift(s) applied.")


async def apply_shifts(dsn: str, shift_files: list[Path]) -> None:
    """Connect to the database and apply unapplied shifts."""
    import asyncpg

    conn = await asyncpg.connect(dsn)
    try:
        await _apply_shifts_on_conn(conn, shift_files)
    finally:
        await conn.close()
