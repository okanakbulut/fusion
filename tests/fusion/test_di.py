import typing
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from fusion import Fusion, Get, Http, Inject, Injectable, Object, Response, factory
from fusion.resolvers import __factories__, has_factory

from .conftest import client_for


class Out(Object):
    value: typing.Any


class Database:
    url = "postgres://test"


@pytest.mark.asyncio
async def test_factory_backed_dependency_is_injected():
    @factory
    async def db_factory() -> Database:
        return Database()

    async def handler(db: Inject[Database]) -> Response[Out]:
        return Response(Out(value=db.url))

    app = Fusion(routes=[Get("/db", handler)])
    async with client_for(app) as client:
        assert (await client.get("/db")).json() == {"value": "postgres://test"}


@pytest.mark.asyncio
async def test_injectable_subclass_is_built_from_its_own_markers():
    @factory
    async def db_factory() -> Database:
        return Database()

    class Deps(Injectable):
        db: Inject[Database]

    async def handler(deps: Inject[Deps]) -> Response[Out]:
        return Response(Out(value=deps.db.url))

    app = Fusion(routes=[Get("/deps", handler)])
    async with client_for(app) as client:
        assert (await client.get("/deps")).json() == {"value": "postgres://test"}


@pytest.mark.asyncio
async def test_dependency_is_constructed_once_per_request():
    calls = []

    @factory
    async def db_factory() -> Database:
        calls.append(1)
        return Database()

    async def handler(a: Inject[Database], b: Inject[Database]) -> Response[Out]:
        return Response(Out(value=a is b))

    app = Fusion(routes=[Get("/twice", handler)])
    async with client_for(app) as client:
        assert (await client.get("/twice")).json() == {"value": True}

    assert calls == [1]


@pytest.mark.asyncio
async def test_each_request_gets_a_fresh_dependency():
    calls = []

    @factory
    async def db_factory() -> Database:
        calls.append(1)
        return Database()

    async def handler(db: Inject[Database]) -> Response[Out]:
        return Response(Out(value=id(db)))

    app = Fusion(routes=[Get("/db", handler)])
    async with client_for(app) as client:
        await client.get("/db")
        await client.get("/db")

    assert calls == [1, 1]


@pytest.mark.asyncio
async def test_async_context_manager_factory_tears_down_after_the_response():
    events: list[str] = []

    class Session:
        pass

    @factory
    @asynccontextmanager
    async def session_factory() -> AsyncIterator[Session]:
        events.append("open")
        try:
            yield Session()
        finally:
            events.append("close")

    async def handler(session: Inject[Session]) -> Response[Out]:
        events.append("handle")
        return Response(Out(value="ok"))

    app = Fusion(routes=[Get("/s", handler)])
    async with client_for(app) as client:
        await client.get("/s")

    assert events == ["open", "handle", "close"]


@pytest.mark.asyncio
async def test_teardown_runs_even_when_the_handler_raises():
    events: list[str] = []

    class Session:
        pass

    @factory
    @asynccontextmanager
    async def session_factory() -> AsyncIterator[Session]:
        try:
            yield Session()
        finally:
            events.append("close")

    async def handler(session: Inject[Session]) -> Response[Out]:
        raise RuntimeError("boom")

    app = Fusion(routes=[Get("/s", handler)])
    async with client_for(app) as client:
        assert (await client.get("/s")).status_code == 500

    assert events == ["close"]


@pytest.mark.asyncio
async def test_injecting_an_unprovided_type_is_an_error():
    class Orphan:
        pass

    async def handler(x: Inject[Orphan]) -> Response[Out]:
        return Response(Out(value=None))

    app = Fusion(routes=[Get("/x", handler)])
    async with client_for(app) as client:
        assert (await client.get("/x")).status_code == 500


def test_factory_requires_a_return_annotation():
    with pytest.raises(ValueError, match="return type annotation"):

        @factory
        async def bad():  # type: ignore[no-untyped-def]
            return 1


def test_factory_unwraps_async_iterator_annotations():
    class Thing:
        pass

    @factory
    @asynccontextmanager
    async def thing_factory() -> AsyncIterator[Thing]:
        yield Thing()

    assert has_factory(Thing)
    assert __factories__[Thing] is thing_factory


def test_injectable_rejects_an_unmarked_field():
    with pytest.raises(TypeError, match="carries no Fusion marker"):

        class Bad(Injectable):
            db: Database
