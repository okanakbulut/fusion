"""Tests for request body parsing and the Context body reader.

These tests document what happens when a handler reads the request body:
normal JSON, malformed input, oversized payloads, and disconnect events.
"""

import pytest

from fusion import Fusion, Handler, Object, Request, RequestBody, Response, Route
from fusion.injectable import Injectable
from fusion.responses import BadRequest
from fusion.testing import TestClient


class _Echo(Object):
    msg: str


# ---------------------------------------------------------------------------
# Normal body parsing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_valid_json_body_is_parsed():
    class Input(Injectable):
        body: RequestBody[_Echo]

    class EchoHandler(Handler):
        inp: Input

        async def handle(self, request: Request) -> Response[_Echo]:
            return Response(self.inp.body)

    app = Fusion(routes=[Route("/echo", methods=["POST"], handler=EchoHandler)])

    async with TestClient(app) as c:
        r = await c.post("/echo", json={"msg": "hello"})
    assert r.status_code == 200
    assert r.json() == {"msg": "hello"}


@pytest.mark.asyncio
async def test_malformed_json_returns_400():
    class Input(Injectable):
        body: RequestBody[_Echo]

    class EchoHandler(Handler):
        inp: Input

        async def handle(self, request: Request) -> Response[_Echo]:
            return Response(self.inp.body)

    app = Fusion(routes=[Route("/echo", methods=["POST"], handler=EchoHandler)])

    async with TestClient(app) as c:
        r = await c.post("/echo", content=b"not-json", headers={"content-type": "application/json"})
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_body_exceeding_max_size_returns_400():
    from fusion.context import MAX_BODY_SIZE

    class Input(Injectable):
        body: RequestBody[_Echo]

    class EchoHandler(Handler):
        inp: Input

        async def handle(self, request: Request) -> Response[_Echo]:
            return Response(self.inp.body)

    app = Fusion(routes=[Route("/echo", methods=["POST"], handler=EchoHandler)])

    oversized = b"x" * (MAX_BODY_SIZE + 1)

    async with TestClient(app) as c:
        r = await c.post("/echo", content=oversized, headers={"content-type": "application/json"})
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# Client disconnect
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_client_disconnect_raises_runtime_error():
    from fusion.context import Context

    sent: list = []

    async def receive():
        return {"type": "http.disconnect"}

    async def send(msg):
        sent.append(msg)

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "query_string": b"",
        "headers": [],
    }
    async with Context(scope, receive, send) as ctx:
        with pytest.raises(RuntimeError, match="disconnected"):
            await ctx.body()


# ---------------------------------------------------------------------------
# Request properties
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_request_exposes_query_params():
    class Output(Object):
        q: str

    class QHandler(Handler):
        async def handle(self, request: Request) -> Response[Output]:
            return Response(Output(q=request.query_params.get("q", "")))

    app = Fusion(routes=[Route("/search", methods=["GET"], handler=QHandler)])

    async with TestClient(app) as c:
        r = await c.get("/search?q=fusion")
    assert r.status_code == 200
    assert r.json() == {"q": "fusion"}


@pytest.mark.asyncio
async def test_request_exposes_headers():
    class Output(Object):
        ua: str

    class UAHandler(Handler):
        async def handle(self, request: Request) -> Response[Output]:
            return Response(Output(ua=request.headers.get("user_agent", "")))

    app = Fusion(routes=[Route("/ua", methods=["GET"], handler=UAHandler)])

    async with TestClient(app) as c:
        r = await c.get("/ua", headers={"user-agent": "test/1"})
    assert r.status_code == 200
    assert r.json()["ua"] == "test/1"


@pytest.mark.asyncio
async def test_request_exposes_path_params():
    class Output(Object):
        id: str

    class IdHandler(Handler):
        async def handle(self, request: Request) -> Response[Output]:
            return Response(Output(id=str(request.path_params.get("id", ""))))

    app = Fusion(routes=[Route("/items/{id}", methods=["GET"], handler=IdHandler)])

    async with TestClient(app) as c:
        r = await c.get("/items/abc")
    assert r.status_code == 200
    assert r.json() == {"id": "abc"}


@pytest.mark.asyncio
async def test_request_body_method():
    class RawHandler(Handler):
        async def handle(self, request: Request) -> Response[Object]:
            raw = await request.body()
            assert raw == b'{"msg": "hi"}'
            return Response(None)

    app = Fusion(routes=[Route("/raw", methods=["POST"], handler=RawHandler)])

    async with TestClient(app) as c:
        r = await c.post(
            "/raw", content=b'{"msg": "hi"}', headers={"content-type": "application/json"}
        )
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_request_scope_and_receive_and_send_accessible():
    class InspectHandler(Handler):
        async def handle(self, request: Request) -> Response[Object]:
            _ = request.scope
            _ = request.receive
            _ = request.send
            return Response(None)

    app = Fusion(routes=[Route("/inspect", methods=["GET"], handler=InspectHandler)])

    async with TestClient(app) as c:
        r = await c.get("/inspect")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_request_cookies_accessible():
    class Output(Object):
        session: str

    class CookieHandler(Handler):
        async def handle(self, request: Request) -> Response[Output]:
            cookies = request.cookies
            return Response(Output(session=cookies.get("session", "")))

    app = Fusion(routes=[Route("/cookies", methods=["GET"], handler=CookieHandler)])

    async with TestClient(app) as c:
        r = await c.get("/cookies", headers={"cookie": "session=abc123"})
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_body_is_cached_across_multiple_calls():
    call_count = 0

    class CachedHandler(Handler):
        async def handle(self, request: Request) -> Response[Object]:
            nonlocal call_count
            b1 = await request.body()
            b2 = await request.body()
            call_count += 1
            assert b1 is b2
            return Response(None)

    app = Fusion(routes=[Route("/cached", methods=["POST"], handler=CachedHandler)])

    async with TestClient(app) as c:
        r = await c.post("/cached", content=b"hello", headers={"content-type": "application/json"})
    assert r.status_code == 200
    assert call_count == 1


@pytest.mark.asyncio
async def test_context_properties():
    from fusion.context import Context

    sent: list = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(msg):
        sent.append(msg)

    scope = {
        "type": "http",
        "method": "GET",
        "scheme": "https",
        "path": "/",
        "query_string": b"x=1",
        "headers": [],
    }
    async with Context(scope, receive, send) as ctx:
        assert ctx.type == "http"
        assert ctx.scheme == "https"
        assert ctx.query_string == "x=1"


@pytest.mark.asyncio
async def test_nested_context_raises():
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
    }
    async with Context(scope, receive, send):
        with pytest.raises(RuntimeError, match="Nested context"):
            async with Context(scope, receive, send):
                pass


@pytest.mark.asyncio
async def test_body_loop_ignores_unknown_message_type():
    from fusion.context import Context

    sent: list = []
    messages = [
        {"type": "http.unknown"},
        {"type": "http.request", "body": b"hello", "more_body": False},
    ]

    async def receive():
        return messages.pop(0)

    async def send(msg):
        sent.append(msg)

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "query_string": b"",
        "headers": [],
    }
    async with Context(scope, receive, send) as ctx:
        body = await ctx.body()
    assert body == b"hello"
