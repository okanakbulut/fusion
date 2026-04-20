"""Tests for fusion's dependency injection system.

These tests document how @factory, Injectable, and Handler collaborate
to wire dependencies into request handlers automatically.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from fusion import Fusion, Handler, Injectable, Object, Request, Response, Route, factory
from fusion.injectable import Injectable as _Injectable
from fusion.resolvers import __factories__
from fusion.testing import TestClient

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _app(*routes):
    return Fusion(routes=list(routes))


# ---------------------------------------------------------------------------
# @factory basics
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_factory_injection():
    class DB:
        url = "postgres://test"

    @factory
    async def db_factory() -> DB:
        return DB()

    class Deps(Injectable):
        db: DB

    class Output(Object):
        url: str

    class Handler1(Handler):
        deps: Deps

        async def handle(self, request: Request) -> Response[Output]:
            return Response(Output(url=self.deps.db.url))

    app = _app(Route("/db", methods=["GET"], handler=Handler1))

    async with TestClient(app) as c:
        r = await c.get("/db")
    assert r.status_code == 200
    assert r.json()["url"] == "postgres://test"


@pytest.mark.asyncio
async def test_factory_context_manager_cleanup_runs_after_response():
    events: list[str] = []

    class Conn:
        pass

    @factory
    @asynccontextmanager
    async def conn_factory() -> AsyncIterator[Conn]:
        events.append("open")
        try:
            yield Conn()
        finally:
            events.append("close")

    class Deps(Injectable):
        conn: Conn

    class Output(Object):
        ok: bool

    class Handler2(Handler):
        deps: Deps

        async def handle(self, request: Request) -> Response[Output]:
            events.append("handle")
            return Response(Output(ok=True))

    app = _app(Route("/conn", methods=["GET"], handler=Handler2))

    async with TestClient(app) as c:
        r = await c.get("/conn")

    assert r.status_code == 200
    assert events == ["open", "handle", "close"]


@pytest.mark.asyncio
async def test_factory_missing_return_annotation_raises():
    with pytest.raises(ValueError, match="return type"):

        @factory
        async def bad():  # type: ignore[return]
            return object()


@pytest.mark.asyncio
async def test_factory_no_registered_factory_raises_runtime_error():
    class Ghost:
        pass

    from fusion.resolvers import FactoryResolver

    resolver = FactoryResolver(name="ghost", typ=Ghost)

    from fusion.context import Context

    sent: list = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(msg):
        sent.append(msg)

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "query_string": b"",
        "headers": [],
        "state": {},
    }
    async with Context(scope, receive, send) as _ctx:
        with pytest.raises(RuntimeError, match="No factory found"):
            await resolver.resolve()


# ---------------------------------------------------------------------------
# Injectable with multiple deps
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_injectable_with_multiple_deps():
    class A(Injectable):
        pass

    class B(Injectable):
        pass

    class Composite(Injectable):
        a: A
        b: B

    inst = await Composite.instance()
    assert isinstance(inst.a, A)
    assert isinstance(inst.b, B)


# ---------------------------------------------------------------------------
# Invalid annotation raises TypeError at class-definition time
# ---------------------------------------------------------------------------


def test_invalid_annotation_on_injectable_raises():
    with pytest.raises(TypeError, match="not a valid type"):

        class Bad(Injectable):
            x: int  # int is not Injectable, not a factory, not Annotated


def test_handler_request_scoped_annotation_raises():
    from fusion import QueryParam

    with pytest.raises(TypeError, match="request-scoped"):

        class BadHandler(Handler):
            message: QueryParam[str]


def test_list_annotation_raises_on_injectable():
    with pytest.raises(TypeError, match="not a valid type"):

        class BadInjectable(Injectable):
            items: list[int]  # list[int] has non-None origin but no TypeAlias __value__


def test_handler_get_request_class_no_hint_raises():
    from fusion.handler import Handler as _Handler

    class NoHintHandler(_Handler):
        async def handle(self, request) -> None:  # type: ignore[override]
            pass

    instance = object.__new__(NoHintHandler)
    with pytest.raises(TypeError, match="'request' parameter"):
        instance.get_request_class()


def test_resolver_raises_when_no_context():
    from fusion.resolvers import QueryParamResolver

    resolver = QueryParamResolver(name="x", typ=str)
    import asyncio

    with pytest.raises(RuntimeError, match="No context available"):
        asyncio.run(resolver.resolve())


def test_allowed_annotation_violation_on_request_raises():
    import typing

    from fusion import Request
    from fusion.resolvers import QueryParamResolver

    type _Custom[T] = typing.Annotated[T, {"resolver": QueryParamResolver}]

    with pytest.raises(TypeError, match="not allowed"):

        class BadRequest(Request):
            dep: _Custom[str]  # _Custom is not in Request.__allowed_annotations__
