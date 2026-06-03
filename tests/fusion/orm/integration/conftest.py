import os
import types
import typing

import asyncpg
import pytest_asyncio

from fusion.orm.constraints import ForeignKey
from fusion.orm.model import Model

_PY_TO_SQL: dict[type, str] = {int: "INT", str: "TEXT", float: "FLOAT", bool: "BOOLEAN"}


def _unwrap_optional(annotation: typing.Any) -> typing.Any | None:
    if typing.get_origin(annotation) is types.UnionType:
        args = typing.get_args(annotation)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return non_none[0]
    return None


@pytest_asyncio.fixture
async def pg_conn() -> typing.AsyncGenerator[asyncpg.Connection]:
    conn = await asyncpg.connect(os.environ["POSTGRES_DSN"])
    yield conn
    await conn.close()


async def apply_schema(conn: asyncpg.Connection, models: list[type[Model]]) -> None:
    for model in models:
        hints = typing.get_type_hints(model)
        rel_fields = model.__relationship_fields__
        fk_by_col = {c.column: c for c in model.__db_constraints__ if isinstance(c, ForeignKey)}

        col_defs: list[str] = []
        for field, annotation in hints.items():
            if field in rel_fields or field in fk_by_col:
                continue
            inner = _unwrap_optional(annotation)
            nullable = inner is not None
            actual = inner if nullable else annotation

            if field == "id":
                col_defs.append('"id" SERIAL PRIMARY KEY')
                continue

            sql_type = _PY_TO_SQL.get(actual, "TEXT")
            col_defs.append(f'"{field}" {sql_type}{"" if nullable else " NOT NULL"}')

        for col, c in fk_by_col.items():
            ref_table = c.target.__table_name__
            on_delete = f" ON DELETE {c.on_delete}" if c.on_delete else ""
            col_defs.append(f'"{col}" INT REFERENCES "{ref_table}"("{c.target_column}"){on_delete}')

        table = model.__table_name__
        await conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(col_defs)})'
        )


async def drop_tables(conn: asyncpg.Connection, models: list[type[Model]]) -> None:
    for model in reversed(models):
        await conn.execute(f'DROP TABLE IF EXISTS "{model.__table_name__}" CASCADE')
