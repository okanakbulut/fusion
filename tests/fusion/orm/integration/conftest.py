import os
import typing

import asyncpg
import pytest_asyncio

from fusion.orm.model import Model
from fusion.orm.shift.draft import diff_states, models_to_schema_state
from fusion.orm.shift.state import SchemaState


@pytest_asyncio.fixture
async def pg_conn() -> typing.AsyncGenerator[asyncpg.Connection]:
    conn = await asyncpg.connect(os.environ["POSTGRES_DSN"])
    yield conn
    await conn.close()


async def apply_schema(conn: asyncpg.Connection, models: list[type[Model]]) -> None:
    target = models_to_schema_state(models)
    ops = diff_states(SchemaState(), target)
    for op in ops:
        await conn.execute(op.to_ddl())


async def drop_tables(conn: asyncpg.Connection, models: list[type[Model]]) -> None:
    for model in reversed(models):
        await conn.execute(f'DROP TABLE IF EXISTS "{model.__table_name__}" CASCADE')
