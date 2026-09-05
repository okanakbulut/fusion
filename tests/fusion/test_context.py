import pytest

from fusion import Fusion, Get, Object, Post, Request, Response
from fusion.annotations import FromContext
from fusion.context import MAX_BODY_SIZE, Context
from fusion.exceptions import ValidationException

from .conftest import client_for


class Out(Object):
    value: object


def _context(scope=None, messages=None):
    scope = {
        "type": "http",
        "scheme": "https",
        "method": "GET",
        "path": "/",
        "query_string": b"a=1",
        "headers": [],
        **(scope or {}),
    }
    queue = list(messages or [{"type": "http.request", "body": b"hi", "more_body": False}])

    async def receive():
        return queue.pop(0)

    async def send(message):  # pragma: no cover
        pass

    return Context(scope, receive, send)


@pytest.mark.asyncio
async def test_scope_backed_properties():
    ctx = _context()
    assert ctx.type == "http"
    assert ctx.scheme == "https"
    assert ctx.query_string == "a=1"
    assert ctx.method == "GET"
    assert ctx.path == "/"


@pytest.mark.asyncio
async def test_missing_scope_keys_default_to_empty():
    ctx = Context({}, None, None)
    assert ctx.type == ""
    assert ctx.scheme == ""
    assert ctx.query_string == ""


@pytest.mark.asyncio
async def test_body_is_read_once_and_memoised():
    reads = []

    async def receive():
        reads.append(1)
        return {"type": "http.request", "body": b"payload", "more_body": False}

    ctx = Context({"type": "http"}, receive, None)
    assert await ctx.body() == b"payload"
    assert await ctx.body() == b"payload"
    assert len(reads) == 1


@pytest.mark.asyncio
async def test_body_is_reassembled_from_chunks():
    ctx = _context(
        messages=[
            {"type": "http.request", "body": b"a", "more_body": True},
            {"type": "http.request", "body": b"b", "more_body": False},
        ]
    )
    assert await ctx.body() == b"ab"


@pytest.mark.asyncio
async def test_oversized_body_is_rejected():
    chunk = b"x" * (1024 * 1024)

    async def receive():
        return {"type": "http.request", "body": chunk, "more_body": True}

    ctx = Context({"type": "http"}, receive, None)
    with pytest.raises(ValidationException, match="maximum size"):
        await ctx.body()

    assert MAX_BODY_SIZE == 10 * 1024 * 1024


@pytest.mark.asyncio
async def test_disconnect_while_reading_the_body():
    ctx = _context(messages=[{"type": "http.disconnect"}])
    with pytest.raises(RuntimeError, match="Client disconnected"):
        await ctx.body()


@pytest.mark.asyncio
async def test_contexts_nest_and_unwind_in_order():
    """An MCP tool call runs inside the HTTP request that carried it."""
    from fusion.context import context

    outer = _context()
    inner = _context()

    async with outer:
        assert context.get() is outer
        async with inner:
            assert context.get() is inner
        assert context.get() is outer


@pytest.mark.asyncio
async def test_each_context_has_its_own_dependency_cache():
    outer, inner = _context(), _context()
    async with outer:
        outer.dependencies[int] = 1
        async with inner:
            assert inner.dependencies == {}


@pytest.mark.asyncio
async def test_request_exposes_the_asgi_triple():
    seen = {}

    async def handler(request: FromContext[Request]) -> Response[Out]:
        seen["receive"] = request.receive
        seen["send"] = request.send
        seen["scope"] = request.scope
        return Response(Out(value="ok"))

    app = Fusion(routes=[Get("/x", handler)])
    async with client_for(app) as client:
        await client.get("/x")

    assert callable(seen["receive"])
    assert callable(seen["send"])
    assert seen["scope"]["path"] == "/x"
