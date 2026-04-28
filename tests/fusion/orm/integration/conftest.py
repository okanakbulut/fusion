import os
import typing

import asyncpg
import pytest_asyncio

from fusion.orm.migration.apply import to_ddl
from fusion.orm.migration.diff import diff
from fusion.orm.migration.snapshot import serialize
from fusion.orm.model import Model


@pytest_asyncio.fixture
async def pg_conn() -> typing.AsyncGenerator[asyncpg.Connection]:
    conn = await asyncpg.connect(os.environ["POSTGRES_DSN"])
    yield conn
    await conn.close()


async def apply_schema(conn: asyncpg.Connection, models: list[type[Model]]) -> None:
    snapshot = serialize(models)
    statements = to_ddl(diff({}, snapshot))
    for stmt in statements:
        await conn.execute(stmt)


async def drop_tables(conn: asyncpg.Connection, models: list[type[Model]]) -> None:
    for model in reversed(models):
        await conn.execute(f'DROP TABLE IF EXISTS "{model.__table_name__}" CASCADE')
