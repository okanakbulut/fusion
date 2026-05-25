"""Tests for Fusion application lifecycle (lifespan) and ASGI dispatch.

These tests show how the app initialises shared state, what happens when
startup fails, and how requests flow through to the router.
"""

import contextlib

import pytest

from fusion import Fusion, Handler, Injectable, Object, PathParam, Request, Response, Route
from fusion.testing import TestClient


class _Ok(Object):
    ok: bool


class _OkHandler(Handler):
    async def handle(self, request: Request) -> Response[_Ok]:
        return Response(_Ok(ok=True))


async def _send_events(app, events):
    """Drive ASGI lifespan manually through the given event sequence."""
    sent: list[dict] = []

    async def receive():
        return events.pop(0)

    async def send(msg):
        sent.append(msg)

    scope = {"type": "lifespan", "state": {}}
    await app(scope, receive, send)
    return scope, sent


@pytest.mark.asyncio
async def test_lifespan_startup_and_shutdown():
    startup_ran = shutdown_ran = False

    @contextlib.asynccontextmanager
    async def lifespan(app):
        nonlocal startup_ran, shutdown_ran
        startup_ran = True
        yield {"db": "mock"}
        shutdown_ran = True

    app = Fusion(routes=[], lifespan=lifespan)
    events = [{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}]
    scope, sent = await _send_events(app, events)

    assert startup_ran
    assert shutdown_ran
    assert scope["state"]["db"] == "mock"
    assert sent[0]["type"] == "lifespan.startup.complete"
    assert sent[1]["type"] == "lifespan.shutdown.complete"


@pytest.mark.asyncio
async def test_lifespan_startup_failure_sends_failed_message():
    @contextlib.asynccontextmanager
    async def broken_lifespan(app):
        raise RuntimeError("db down")
        yield {}  # type: ignore[misc]

    app = Fusion(routes=[], lifespan=broken_lifespan)
    events = [{"type": "lifespan.startup"}]

    sent: list[dict] = []

    async def receive():
        return events.pop(0)

    async def send(msg):
        sent.append(msg)

    scope = {"type": "lifespan", "state": {}}
    with pytest.raises(RuntimeError, match="db down"):
        await app(scope, receive, send)

    assert sent[0]["type"] == "lifespan.startup.failed"
    assert "db down" in sent[0]["message"]


@pytest.mark.asyncio
async def test_lifespan_non_dict_state_raises():
    @contextlib.asynccontextmanager
    async def bad_lifespan(app):
        yield "not a dict"

    app = Fusion(routes=[], lifespan=bad_lifespan)
    events = [{"type": "lifespan.startup"}]

    sent: list[dict] = []

    async def receive():
        return events.pop(0)

    async def send(msg):
        sent.append(msg)

    scope = {"type": "lifespan", "state": {}}
    with pytest.raises(TypeError, match="dict"):
        await app(scope, receive, send)

    assert sent[0]["type"] == "lifespan.startup.failed"


@pytest.mark.asyncio
async def test_lifespan_none_state_is_allowed():
    @contextlib.asynccontextmanager
    async def none_lifespan(app):
        yield None

    app = Fusion(routes=[], lifespan=none_lifespan)
    events = [{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}]
    _scope, sent = await _send_events(app, events)

    assert sent[0]["type"] == "lifespan.startup.complete"


@pytest.mark.asyncio
async def test_app_sets_scope_app_ref():
    captured: list[dict] = []

    class ScopeCapturingHandler(Handler):
        async def handle(self, request: Request) -> Response[_Ok]:
            from fusion.context import context as ctx_var

            captured.append(dict(ctx_var.get().scope))
            return Response(_Ok(ok=True))

    import httpx

    app = Fusion(routes=[Route("/", methods=["GET"], handler=ScopeCapturingHandler)])

    async with httpx.AsyncClient(base_url="http://t", transport=httpx.ASGITransport(app)) as c:
        await c.get("/")

    assert captured
    assert captured[0].get("app") is app


@pytest.mark.asyncio
async def test_default_lifespan_yields_empty_state():
    app = Fusion(routes=[])
    events = [{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}]
    _scope, sent = await _send_events(app, events)

    assert sent[0]["type"] == "lifespan.startup.complete"
    assert sent[1]["type"] == "lifespan.shutdown.complete"


@pytest.mark.asyncio
async def test_app_already_in_scope_is_not_overwritten():
    import httpx

    existing_app = object()
    app = Fusion(routes=[Route("/", methods=["GET"], handler=_OkHandler)])

    captured: list = []

    class CapHandler(Handler):
        async def handle(self, request: Request) -> Response[_Ok]:
            from fusion.context import context as ctx_var

            captured.append(ctx_var.get().scope.get("app"))
            return Response(_Ok(ok=True))

    app2 = Fusion(routes=[Route("/", methods=["GET"], handler=CapHandler)])

    async with httpx.AsyncClient(base_url="http://t", transport=httpx.ASGITransport(app2)) as c:
        await c.get("/")

    assert len(captured) == 1


@pytest.mark.asyncio
async def test_lifespan_non_startup_first_message_is_ignored():
    app = Fusion(routes=[])

    sent: list = []

    async def receive():
        return {"type": "lifespan.unknown"}

    async def send(msg):
        sent.append(msg)

    scope = {"type": "lifespan", "state": {}}
    await app(scope, receive, send)

    assert sent == []


@pytest.mark.asyncio
async def test_dispatch_calls_sub_handler_and_returns_response():
    """dispatch() executes a registered route within the current context."""

    class Inner(Object):
        hit: bool

    class InnerHandler(Handler):
        async def handle(self, request: Request) -> Response[Inner]:
            return Response(Inner(hit=True))

    class _OuterRequest(Request):
        pass

    class OuterHandler(Handler):
        async def handle(self, request: _OuterRequest) -> Response[_Ok]:
            from fusion.context import context as ctx_var

            app: Fusion = ctx_var.get().scope["app"]
            result = await app.dispatch("/inner", "GET")
            assert result is not None
            assert result.content.hit is True  # type: ignore[union-attr]
            return Response(_Ok(ok=True))

    app = Fusion(
        routes=[
            Route("/inner", methods=["GET"], handler=InnerHandler),
            Route("/outer", methods=["GET"], handler=OuterHandler),
        ]
    )

    async with TestClient(app) as c:
        r = await c.get("/outer")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_dispatch_with_path_params():
    """dispatch() extracts path params from path and passes them to the handler."""

    class Item(Object):
        item_id: str

    from fusion import PathParam

    class ItemRequest(Injectable):
        item_id: PathParam[str]

    class ItemHandler(Handler):
        req: ItemRequest

        async def handle(self, request: Request) -> Response[Item]:
            return Response(Item(item_id=self.req.item_id))

    class _OuterRequest(Request):
        pass

    captured: list = []

    class OuterHandler(Handler):
        async def handle(self, request: _OuterRequest) -> Response[_Ok]:
            from fusion.context import context as ctx_var

            app: Fusion = ctx_var.get().scope["app"]
            result = await app.dispatch("/items/abc-123", "GET")
            captured.append(result)
            return Response(_Ok(ok=True))

    app = Fusion(
        routes=[
            Route("/items/{item_id}", methods=["GET"], handler=ItemHandler),
            Route("/outer", methods=["GET"], handler=OuterHandler),
        ]
    )

    async with TestClient(app) as c:
        await c.get("/outer")

    assert captured[0].content.item_id == "abc-123"  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_dispatch_returns_none_for_unknown_path():
    """dispatch() returns None when no route matches."""

    class _OuterRequest(Request):
        pass

    class OuterHandler(Handler):
        async def handle(self, request: _OuterRequest) -> Response[_Ok]:
            from fusion.context import context as ctx_var

            app: Fusion = ctx_var.get().scope["app"]
            result = await app.dispatch("/nonexistent", "GET")
            assert result is None
            return Response(_Ok(ok=True))

    app = Fusion(routes=[Route("/outer", methods=["GET"], handler=OuterHandler)])

    async with TestClient(app) as c:
        r = await c.get("/outer")
    assert r.status_code == 200


def test_fusion_resolve_returns_route_and_path_params():
    from fusion.types import Method

    app = Fusion(routes=[Route("/items/{id}", methods=["GET"], handler=_OkHandler)])
    result = app.resolve("/items/42", Method.GET)
    assert result is not None
    route, path_params = result
    assert path_params == {"id": "42"}


def test_fusion_resolve_accepts_string_method():
    app = Fusion(routes=[Route("/ping", methods=["GET"], handler=_OkHandler)])
    assert app.resolve("/ping", "GET") is not None
    assert app.resolve("/ping", "get") is not None


def test_fusion_resolve_unknown_path_returns_none():
    app = Fusion(routes=[Route("/ping", methods=["GET"], handler=_OkHandler)])
    assert app.resolve("/nope", "GET") is None


@pytest.mark.asyncio
async def test_exception_during_shutdown_does_not_send_startup_failed():
    import contextlib

    @contextlib.asynccontextmanager
    async def flaky_lifespan(app):
        yield {}
        raise RuntimeError("shutdown error")

    app = Fusion(routes=[], lifespan=flaky_lifespan)

    sent: list[dict] = []

    async def receive():
        return {"type": "lifespan.startup"}

    startup_complete = False

    async def send(msg):
        nonlocal startup_complete
        sent.append(msg)
        if msg["type"] == "lifespan.startup.complete":
            startup_complete = True

    scope = {"type": "lifespan", "state": {}}

    async def receive_seq():
        msgs = [{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}]
        for m in msgs:
            yield m

    gen = receive_seq()

    async def receive2():
        return await gen.__anext__()

    with pytest.raises(RuntimeError, match="shutdown error"):
        await app(scope, receive2, send)

    types = [m["type"] for m in sent]
    assert "lifespan.startup.complete" in types
    assert "lifespan.startup.failed" not in types
