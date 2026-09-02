import asyncio
import contextlib
import json
import typing
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from fusion import (
    Auth,
    Fusion,
    Get,
    Http,
    Inject,
    Object,
    Post,
    Request,
    Response,
    factory,
    requires,
)
from fusion.annotations import FromContext
from fusion.application import default_lifespan
from fusion.testing import LifespanManager, TestClient
from fusion.types import Method

from .conftest import client_for


class Out(Object):
    value: typing.Any


async def _echo() -> Response[Out]:
    return Response(Out(value="ok"))


# --- lifespan ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_lifespan_startup_and_shutdown():
    events: list[str] = []

    @contextlib.asynccontextmanager
    async def lifespan(app):
        events.append("startup")
        yield {"ready": True}
        events.append("shutdown")

    app = Fusion(routes=[Get("/x", _echo)], lifespan=lifespan)
    async with LifespanManager(app) as manager:
        assert manager.state == {"ready": True}

    assert events == ["startup", "shutdown"]


@pytest.mark.asyncio
async def test_lifespan_startup_failure_is_reported():
    @contextlib.asynccontextmanager
    async def lifespan(app):
        raise RuntimeError("nope")
        yield {}  # pragma: no cover

    app = Fusion(routes=[], lifespan=lifespan)
    with pytest.raises(RuntimeError):
        async with LifespanManager(app):
            pass  # pragma: no cover


@pytest.mark.asyncio
async def test_lifespan_must_yield_a_dict():
    @contextlib.asynccontextmanager
    async def lifespan(app):
        yield "not a dict"

    app = Fusion(routes=[], lifespan=lifespan)
    with pytest.raises(RuntimeError):
        async with LifespanManager(app):
            pass  # pragma: no cover


@pytest.mark.asyncio
async def test_lifespan_may_yield_none():
    @contextlib.asynccontextmanager
    async def lifespan(app):
        yield None

    app = Fusion(routes=[], lifespan=lifespan)
    async with LifespanManager(app) as manager:
        assert manager.state == {}


@pytest.mark.asyncio
async def test_default_lifespan_yields_empty_state():
    async with default_lifespan(None) as state:
        assert state == {}


@pytest.mark.asyncio
async def test_shutdown_exception_does_not_send_startup_failed():
    @contextlib.asynccontextmanager
    async def lifespan(app):
        yield {}
        raise RuntimeError("late")

    app = Fusion(routes=[], lifespan=lifespan)
    messages: list[dict] = []
    receive_queue = [{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}]

    async def receive():
        return receive_queue.pop(0)

    async def send(message):
        messages.append(message)

    with pytest.raises(RuntimeError):
        await app({"type": "lifespan"}, receive, send)

    assert [m["type"] for m in messages] == ["lifespan.startup.complete"]


@pytest.mark.asyncio
async def test_non_startup_first_lifespan_message_is_ignored():
    app = Fusion(routes=[])

    async def receive():
        return {"type": "lifespan.shutdown"}

    sent: list[dict] = []

    async def send(message):
        sent.append(message)  # pragma: no cover

    await app({"type": "lifespan"}, receive, send)
    assert sent == []


# --- scope -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_app_is_placed_in_scope():
    seen = {}

    async def handler(request: FromContext[Request]) -> Response[Out]:
        seen["app"] = request.scope["app"]
        return Response(Out(value="ok"))

    app = Fusion(routes=[Get("/x", handler)])
    async with client_for(app) as client:
        await client.get("/x")

    assert seen["app"] is app


@pytest.mark.asyncio
async def test_existing_scope_app_is_not_overwritten():
    app = Fusion(routes=[Get("/x", _echo)])
    sentinel = object()
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/x",
        "query_string": b"",
        "headers": [],
        "app": sentinel,
    }

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        pass

    await app(scope, receive, send)
    assert scope["app"] is sentinel


# --- resolve / execute ------------------------------------------------------


def test_resolve_returns_route_and_params():
    route = Get("/users/{id:int}", _echo)
    app = Fusion(routes=[route])
    result = app.resolve("/users/7", Method.GET)
    assert result is not None
    assert result[0] is route
    assert result[1] == {"id": "7"}


def test_resolve_accepts_a_string_method():
    app = Fusion(routes=[Get("/x", _echo)])
    assert app.resolve("/x", "get") is not None


def test_resolve_unknown_path_is_none():
    app = Fusion(routes=[Get("/x", _echo)])
    assert app.resolve("/nope", Method.GET) is None


@pytest.mark.asyncio
async def test_execute_runs_another_route():
    async def inner(q: Http.Query[str] = "") -> Response[Out]:
        return Response(Out(value=f"inner:{q}"))

    async def outer(request: FromContext[Request]) -> Response[Out]:
        result = await request.scope["app"].execute("GET", "/inner", query={"q": "hi"})
        return Response(Out(value=f"{result.status}:{result.body.decode()}"))

    app = Fusion(routes=[Get("/inner", inner), Get("/outer", outer)])
    async with client_for(app) as client:
        assert (await client.get("/outer")).json() == {"value": '200:{"value":"inner:hi"}'}


@pytest.mark.asyncio
async def test_execute_accepts_a_query_string_in_the_path():
    async def inner(q: Http.Query[str] = "") -> Response[Out]:
        return Response(Out(value=q))

    async def outer(request: FromContext[Request]) -> Response[Out]:
        result = await request.scope["app"].execute("GET", "/inner?q=inline")
        return Response(Out(value=result.body.decode()))

    app = Fusion(routes=[Get("/inner", inner), Get("/outer", outer)])
    async with client_for(app) as client:
        assert (await client.get("/outer")).json() == {"value": '{"value":"inline"}'}


@pytest.mark.asyncio
async def test_execute_passes_path_params():
    async def inner(id: Http.Path[int]) -> Response[Out]:
        return Response(Out(value=id))

    async def outer(request: FromContext[Request]) -> Response[Out]:
        result = await request.scope["app"].execute("GET", "/inner/5")
        return Response(Out(value=result.body.decode()))

    app = Fusion(routes=[Get("/inner/{id:int}", inner), Get("/outer", outer)])
    async with client_for(app) as client:
        assert (await client.get("/outer")).json() == {"value": '{"value":5}'}


@pytest.mark.asyncio
async def test_execute_inherits_headers_and_honours_overrides():
    async def inner(agent: Http.Header[str] = "?", trace: Http.Header[str] = "?") -> Response[Out]:
        return Response(Out(value=f"{agent}/{trace}"))

    async def outer(request: FromContext[Request]) -> Response[Out]:
        result = await request.scope["app"].execute("GET", "/inner", headers={"trace": "sub"})
        return Response(Out(value=result.body.decode()))

    app = Fusion(routes=[Get("/inner", inner), Get("/outer", outer)])
    async with client_for(app) as client:
        response = await client.get("/outer", headers={"agent": "outer", "trace": "outer"})

    assert response.json() == {"value": '{"value":"outer/sub"}'}


@pytest.mark.asyncio
async def test_execute_sends_a_body():
    class NewThing(Object):
        name: str

    async def inner(body: Http.Body[NewThing]) -> Response[Out]:
        return Response(Out(value=body.name))

    async def outer(request: FromContext[Request]) -> Response[Out]:
        result = await request.scope["app"].execute("POST", "/inner", body=b'{"name": "ada"}')
        return Response(Out(value=result.body.decode()))

    app = Fusion(routes=[Post("/inner", inner), Get("/outer", outer)])
    async with client_for(app) as client:
        assert (await client.get("/outer")).json() == {"value": '{"value":"ada"}'}


@pytest.mark.asyncio
async def test_execute_captures_a_failure_instead_of_raising():
    """One bad sub-request must not take the whole batch with it."""

    async def inner() -> Response[Out]:
        raise RuntimeError("sub-request exploded")

    async def outer(request: FromContext[Request]) -> Response[Out]:
        result = await request.scope["app"].execute("GET", "/inner")
        return Response(Out(value=result.status))

    app = Fusion(routes=[Get("/inner", inner), Get("/outer", outer)])
    async with client_for(app) as client:
        assert (await client.get("/outer")).json() == {"value": 500}


@pytest.mark.asyncio
async def test_execute_captures_an_unknown_path_as_a_404():
    async def outer(request: FromContext[Request]) -> Response[Out]:
        result = await request.scope["app"].execute("GET", "/nope")
        return Response(Out(value=result.status))

    app = Fusion(routes=[Get("/outer", outer)])
    async with client_for(app) as client:
        assert (await client.get("/outer")).json() == {"value": 404}


@pytest.mark.asyncio
async def test_execute_refuses_a_route_that_streams():
    async def stream() -> AsyncIterator[Out]:
        yield Out(value="tick")  # pragma: no cover

    async def outer(request: FromContext[Request]) -> Response[Out]:
        result = await request.scope["app"].execute("GET", "/stream")
        detail = json.loads(result.body)["detail"]
        return Response(Out(value=f"{result.status}:{detail}"))

    app = Fusion(routes=[Get("/stream", stream), Get("/outer", outer)])
    async with client_for(app) as client:
        value = (await client.get("/outer")).json()["value"]

    assert value.startswith("500:")
    assert "cannot be captured" in value


@pytest.mark.asyncio
async def test_execute_refuses_to_recurse_forever():
    """A route executing itself is bounded, rather than exhausting the stack."""
    calls: list[int] = []

    async def looping(request: FromContext[Request]) -> Response[Out]:
        calls.append(1)
        result = await request.scope["app"].execute("GET", "/loop")
        return Response(Out(value=result.status))

    app = Fusion(routes=[Get("/loop", looping)])
    async with client_for(app) as client:
        response = await client.get("/loop")

    # The real request, plus one nested call per level until the guard refuses.
    assert response.status_code == 200
    assert len(calls) == Fusion.MAX_SUBREQUEST_DEPTH + 1


@pytest.mark.asyncio
async def test_execute_runs_the_target_middleware_and_authorization():
    seen: list[str] = []

    class Allow:
        async def authorize(self, roles: frozenset[str]) -> bool:
            seen.append("authorize")
            return True

    async def guard(token: Auth.Bearer) -> None:
        seen.append("middleware")

    @requires("inner:read")
    async def inner(token: Auth.Bearer) -> Response[Out]:
        return Response(Out(value="inner"))

    async def outer(request: FromContext[Request]) -> Response[Out]:
        result = await request.scope["app"].execute("GET", "/inner")
        return Response(Out(value=result.status))

    app = Fusion(
        routes=[Get("/inner", inner, middlewares=[guard]), Get("/outer", outer)],
        authorizer=Allow(),
    )
    async with client_for(app) as client:
        response = await client.get("/outer", headers={"authorization": "Bearer t"})

    assert response.json() == {"value": 200}
    assert seen == ["middleware", "authorize"]


@pytest.mark.asyncio
async def test_execute_tears_down_its_dependencies():
    """Regression: the sub-context's exit stack was never unwound."""
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

    async def inner(session: Inject[Session]) -> Response[Out]:
        return Response(Out(value="inner"))

    async def outer(request: FromContext[Request]) -> Response[Out]:
        await request.scope["app"].execute("GET", "/inner")
        events.append("after-execute")
        return Response(Out(value="ok"))

    app = Fusion(routes=[Get("/inner", inner), Get("/outer", outer)])
    async with client_for(app) as client:
        await client.get("/outer")

    assert events == ["open", "close", "after-execute"]


@pytest.mark.asyncio
async def test_routes_and_tools_stay_enumerable():
    from fusion import Tool

    async def a_tool(q: Tool.Arg[str]) -> Response[Out]:
        """A tool."""
        return Response(Out(value=q))

    route = Get("/x", _echo)
    app = Fusion(routes=[route], tools=[a_tool])

    assert app.routes == [route]
    assert list(app.tools) == ["a_tool"]


@pytest.mark.asyncio
async def test_a_failure_after_the_stream_starts_cannot_be_turned_into_an_error():
    """Once the status line is out, there is no error response left to send."""
    from fusion import Event
    from fusion.exceptions import ValidationException

    async def handler() -> AsyncIterator[Event[Out]]:
        yield Event(data=Out(value="first"))
        raise ValidationException(detail="too late")

    app = Fusion(routes=[Get("/x", handler)])
    sent: list[dict] = []

    delivered = False

    async def receive():
        # One body message, then block the way a real server does between
        # messages; an instantly-resolving receive() is not realistic here.
        nonlocal delivered
        if not delivered:
            delivered = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await asyncio.Event().wait()  # pragma: no cover

    async def send(message):
        sent.append(message)

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/x",
        "query_string": b"",
        "headers": [],
    }

    with pytest.raises(ValidationException):
        await app(scope, receive, send)

    # The 200 stream had already begun, so no problem response was appended.
    assert sent[0]["status"] == 200
    assert not any(m.get("status") == 400 for m in sent)


@pytest.mark.asyncio
async def test_execute_delivers_its_body_once_then_reports_a_disconnect():
    """Matching a real request: after the body is consumed there is nothing more."""

    async def inner(request: FromContext[Request]) -> Response[Out]:
        first = await request.receive()
        second = await request.receive()
        return Response(Out(value=f"{first['type']}/{second['type']}"))

    async def outer(request: FromContext[Request]) -> Response[Out]:
        result = await request.scope["app"].execute("POST", "/inner", body=b"{}")
        return Response(Out(value=result.body.decode()))

    app = Fusion(routes=[Post("/inner", inner), Get("/outer", outer)])
    async with client_for(app) as client:
        assert (await client.get("/outer")).json() == {
            "value": '{"value":"http.request/http.disconnect"}'
        }


@pytest.mark.asyncio
async def test_execute_works_outside_a_request():
    """No ambient context to inherit from - the sub-request stands on its own."""

    async def inner(agent: Http.Header[str] = "none") -> Response[Out]:
        return Response(Out(value=agent))

    app = Fusion(routes=[Get("/inner", inner)])

    bare = await app.execute("GET", "/inner")
    with_header = await app.execute("GET", "/inner", headers={"agent": "script"})

    assert bare.status == 200
    assert bare.body == b'{"value":"none"}'
    assert with_header.body == b'{"value":"script"}'
