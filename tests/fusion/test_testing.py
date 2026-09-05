import contextlib

import pytest

from fusion import Fusion, Get, Object, Response
from fusion.testing import LifespanManager, TestClient


class Out(Object):
    value: str


async def handler() -> Response[Out]:
    return Response(Out(value="ok"))


@pytest.mark.asyncio
async def test_test_client_drives_the_app():
    app = Fusion(routes=[Get("/x", handler)])
    async with TestClient(app) as client:
        response = await client.get("/x")

    assert response.status_code == 200
    assert response.json() == {"value": "ok"}


@pytest.mark.asyncio
async def test_test_client_runs_the_lifespan():
    events: list[str] = []

    @contextlib.asynccontextmanager
    async def lifespan(app):
        events.append("up")
        yield {"k": "v"}
        events.append("down")

    app = Fusion(routes=[Get("/x", handler)], lifespan=lifespan)
    async with TestClient(app) as client:
        await client.get("/x")

    assert events == ["up", "down"]


@pytest.mark.asyncio
async def test_lifespan_state_reaches_the_scope():
    @contextlib.asynccontextmanager
    async def lifespan(app):
        yield {"answer": 42}

    seen = {}

    async def peek(request: FromContext[Request]) -> Response[Out]:
        seen.update(request.scope.get("state", {}))
        return Response(Out(value="ok"))

    from fusion import Request
    from fusion.annotations import FromContext

    peek.__annotations__["request"] = FromContext[Request]

    app = Fusion(routes=[Get("/x", peek)], lifespan=lifespan)
    async with TestClient(app) as client:
        await client.get("/x")

    assert seen == {"answer": 42}


@pytest.mark.asyncio
async def test_lifespan_manager_exposes_state():
    @contextlib.asynccontextmanager
    async def lifespan(app):
        yield {"ready": True}

    app = Fusion(routes=[], lifespan=lifespan)
    async with LifespanManager(app) as manager:
        assert manager.state == {"ready": True}


@pytest.mark.asyncio
async def test_test_client_accepts_a_base_url():
    app = Fusion(routes=[Get("/x", handler)])
    async with TestClient(app, base_url="http://example.test") as client:
        assert str(client.base_url) == "http://example.test"


@pytest.mark.asyncio
async def test_lifespan_manager_reports_a_failed_shutdown():
    from fusion.types import Message

    class Broken:
        async def __call__(self, scope, receive, send):
            await receive()
            await send({"type": "lifespan.startup.complete"})
            await receive()
            await send({"type": "lifespan.shutdown.failed", "message": "nope"})

    with pytest.raises(RuntimeError, match="shutdown failed"):
        async with LifespanManager(Broken()):
            pass


@pytest.mark.asyncio
async def test_lifespan_manager_reports_a_failed_startup():
    class Broken:
        async def __call__(self, scope, receive, send):
            await receive()
            await send({"type": "lifespan.startup.failed", "message": "nope"})

    with pytest.raises(RuntimeError, match="startup failed"):
        async with LifespanManager(Broken()):
            pass  # pragma: no cover
