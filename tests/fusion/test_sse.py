import asyncio
import typing
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from fusion import Event, Fusion, Get, Http, Inject, NotFound, Object, Response, factory
from fusion.responses import KEEPALIVE_FRAME, EventStream, encode_event

from .conftest import client_for


class Tick(Object):
    n: int


async def collect(stream: EventStream, receive=None) -> list[dict]:
    """Drive an EventStream directly, capturing the ASGI messages it sends."""
    sent: list[dict] = []

    async def send(message):
        sent.append(message)

    async def default_receive():
        await asyncio.sleep(3600)
        return {"type": "http.disconnect"}  # pragma: no cover

    await stream({}, receive or default_receive, send)
    return sent


# --- the four first-pull outcomes -------------------------------------------


@pytest.mark.asyncio
async def test_stream_of_events():
    async def handler(count: Http.Query[int] = 2) -> AsyncIterator[Event[Tick] | NotFound]:
        """Stream ticks."""
        for i in range(count):
            yield Event(data=Tick(n=i), id=str(i), event="tick")

    app = Fusion(routes=[Get("/ticks", handler)])
    async with client_for(app) as client:
        response = await client.get("/ticks?count=2")

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/event-stream"
    assert response.headers["cache-control"] == "no-cache"
    assert response.headers["x-accel-buffering"] == "no"
    assert response.text == (
        'event: tick\nid: 0\ndata: {"n":0}\n\nevent: tick\nid: 1\ndata: {"n":1}\n\n'
    )


@pytest.mark.asyncio
async def test_yielded_problem_becomes_a_normal_error_response():
    """A pre-flight failure must not become a 200 stream carrying an error event."""

    async def handler(count: Http.Query[int] = 1) -> AsyncIterator[Event[Tick] | NotFound]:
        if count > 5:
            yield NotFound(detail="too many")
            return
        yield Event(data=Tick(n=0))

    app = Fusion(routes=[Get("/ticks", handler)])
    async with client_for(app) as client:
        response = await client.get("/ticks?count=9")

    assert response.status_code == 404
    assert response.headers["content-type"] == "application/problem+json"
    assert response.json()["detail"] == "too many"


@pytest.mark.asyncio
async def test_empty_generator_is_an_empty_stream():
    async def handler() -> AsyncIterator[Event[Tick]]:
        return
        yield  # pragma: no cover

    app = Fusion(routes=[Get("/ticks", handler)])
    async with client_for(app) as client:
        response = await client.get("/ticks")

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/event-stream"
    assert response.text == ""


@pytest.mark.asyncio
async def test_exception_before_first_yield_is_a_500():
    async def handler() -> AsyncIterator[Event[Tick]]:
        raise RuntimeError("pre-flight blew up")
        yield  # pragma: no cover

    app = Fusion(routes=[Get("/ticks", handler)])
    async with client_for(app) as client:
        response = await client.get("/ticks")

    assert response.status_code == 500
    assert response.json()["title"] == "Internal Server Error"


# --- generator lifecycle -----------------------------------------------------


@pytest.mark.asyncio
async def test_generator_is_closed_when_a_problem_is_yielded():
    closed = []

    async def source() -> AsyncIterator[typing.Any]:
        try:
            yield NotFound(detail="nope")
        finally:
            closed.append(True)

    sent = await collect(EventStream(source()))

    assert closed == [True]
    assert sent[0]["status"] == 404


@pytest.mark.asyncio
async def test_dependency_teardown_runs_after_a_stream_completes():
    events: list[str] = []

    class Session:
        pass

    class Deps(Object):
        @factory
        @asynccontextmanager
        async def session(self) -> AsyncIterator[Session]:
            events.append("open")
            try:
                yield Session()
            finally:
                events.append("close")

    async def handler(session: Inject[Session]) -> AsyncIterator[Event[Tick]]:
        events.append("stream")
        yield Event(data=Tick(n=1))

    app = Fusion(routes=[Get("/s", handler)], factories=Deps())
    async with client_for(app) as client:
        await client.get("/s")

    assert events == ["open", "stream", "close"]


@pytest.mark.asyncio
async def test_client_disconnect_stops_the_stream_and_closes_the_generator():
    closed = []
    produced = []

    async def source() -> AsyncIterator[typing.Any]:
        try:
            n = 0
            while True:
                produced.append(n)
                yield Event(data=Tick(n=n))
                n += 1
                await asyncio.sleep(0.01)
        finally:
            closed.append(True)

    disconnect_after = asyncio.Event()

    async def receive():
        await disconnect_after.wait()
        return {"type": "http.disconnect"}

    stream = EventStream(source())

    async def trigger():
        await asyncio.sleep(0.03)
        disconnect_after.set()

    await asyncio.gather(collect(stream, receive), trigger())

    assert closed == [True]
    # It stopped rather than running forever.
    assert len(produced) < 100


@pytest.mark.asyncio
async def test_keepalive_frame_is_sent_while_idle():
    async def source() -> AsyncIterator[typing.Any]:
        yield Event(data=Tick(n=0))
        await asyncio.sleep(0.08)
        yield Event(data=Tick(n=1))

    sent = await collect(EventStream(source(), keepalive=0.02))
    bodies = [m["body"] for m in sent if m["type"] == "http.response.body"]

    assert KEEPALIVE_FRAME in bodies


# --- framing -----------------------------------------------------------------


def test_bare_object_becomes_a_data_only_event():
    assert encode_event(Tick(n=3)) == b'data: {"n":3}\n\n'


def test_event_fields_are_framed_in_order():
    frame = encode_event(Event(data=Tick(n=1), event="tick", id="7", retry=500))
    assert frame == b'event: tick\nid: 7\nretry: 500\ndata: {"n":1}\n\n'


def test_streaming_route_accepts_a_keepalive():
    async def handler() -> AsyncIterator[Event[Tick]]:
        yield Event(data=Tick(n=0))

    route = Get("/s", handler, keepalive=5.0)
    assert route.keepalive == 5.0
    assert route.signature.is_asyncgen


@pytest.mark.asyncio
async def test_a_broken_receive_channel_is_not_treated_as_a_disconnect():
    """A failure reading from the transport must surface, not end the stream quietly."""

    async def source() -> AsyncIterator[typing.Any]:
        yield Event(data=Tick(n=0))
        await asyncio.sleep(0.05)
        yield Event(data=Tick(n=1))  # pragma: no cover

    async def receive():
        raise OSError("transport gone")

    with pytest.raises(OSError, match="transport gone"):
        await collect(EventStream(source()), receive)
