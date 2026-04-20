"""Tests for fusion.testing — the LifespanManager and TestClient helpers."""

import contextlib

import pytest

from fusion import Fusion, Handler, Object, Request, Response, Route
from fusion.testing import LifespanManager, TestClient


class _Ok(Object):
    ok: bool


class _OkHandler(Handler):
    async def handle(self, request: Request) -> Response[_Ok]:
        return Response(_Ok(ok=True))


@pytest.mark.asyncio
async def test_test_client_runs_request_through_lifespan():
    startup_done = False

    @contextlib.asynccontextmanager
    async def lifespan(app):
        nonlocal startup_done
        startup_done = True
        yield {"key": "val"}

    app = Fusion(routes=[Route("/", methods=["GET"], handler=_OkHandler)], lifespan=lifespan)

    async with TestClient(app) as client:
        r = await client.get("/")

    assert r.status_code == 200
    assert startup_done


@pytest.mark.asyncio
async def test_lifespan_manager_provides_state():
    @contextlib.asynccontextmanager
    async def lifespan(app):
        yield {"db": "connected"}

    app = Fusion(routes=[], lifespan=lifespan)

    async with LifespanManager(app) as manager:
        assert manager.state["db"] == "connected"


@pytest.mark.asyncio
async def test_lifespan_manager_startup_failed_raises():
    async def bad_app(scope, receive, send):
        await receive()  # lifespan.startup
        await send({"type": "lifespan.startup.failed", "message": "db down"})

    with pytest.raises(RuntimeError, match="Lifespan startup failed"):
        async with LifespanManager(bad_app):
            pass


@pytest.mark.asyncio
async def test_lifespan_manager_shutdown_failed_raises():
    async def flaky_app(scope, receive, send):
        msg = await receive()
        if msg["type"] == "lifespan.startup":
            await send({"type": "lifespan.startup.complete"})
        msg = await receive()
        if msg["type"] == "lifespan.shutdown":
            await send({"type": "lifespan.shutdown.failed", "message": "error"})

    with pytest.raises(RuntimeError, match="Lifespan shutdown failed"):
        async with LifespanManager(flaky_app):
            pass
