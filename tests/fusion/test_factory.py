from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import pytest

from fusion import Fusion, Handler, Injectable, Object, Request, Response, Route, factory


@pytest.mark.asyncio
async def test_factory_injection_for_third_party_type():
    class Connection:
        def __init__(self, dsn: str) -> None:
            self.dsn = dsn

    @factory
    async def connection_factory() -> Connection:
        return Connection("postgresql://example")

    class Input(Injectable):
        connection: Connection

    class Output(Object):
        dsn: str

    class ConnectionHandler(Handler):
        input: Input

        async def handle(self, request: Request) -> Response[Output]:
            return Response(Output(dsn=self.input.connection.dsn))

    app = Fusion(routes=[Route(path="/factory", methods=["GET"], handler=ConnectionHandler)])

    async with httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.ASGITransport(app=app),
    ) as client:
        response = await client.get("/factory")

    assert response.status_code == 200
    assert response.json() == {"dsn": "postgresql://example"}


@pytest.mark.asyncio
async def test_factory_async_context_manager_cleanup():
    events: list[str] = []

    class Session:
        pass

    @factory
    @asynccontextmanager
    async def session_factory() -> AsyncIterator[Session]:
        events.append("enter")
        try:
            yield Session()
        finally:
            events.append("exit")

    class Input(Injectable):
        session: Session

    class Output(Object):
        ok: bool

    class SessionHandler(Handler):
        input: Input

        async def handle(self, request: Request) -> Response[Output]:
            return Response(Output(ok=isinstance(self.input.session, Session)))

    app = Fusion(routes=[Route(path="/session", methods=["GET"], handler=SessionHandler)])

    async with httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.ASGITransport(app=app),
    ) as client:
        response = await client.get("/session")

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    assert events == ["enter", "exit"]
