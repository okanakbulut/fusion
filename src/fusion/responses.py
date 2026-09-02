import asyncio
import typing

import msgspec

from .object import Object
from .types import Receive, Scope, Send

_encoder = msgspec.json.Encoder()


def raw_headers(
    media_type: str,
    headers: typing.Mapping[str, str] | None,
    content_length: int | None,
) -> list[tuple[bytes, bytes]]:
    """Assemble ASGI headers.

    Shared by every response kind so buffered and streaming replies cannot drift
    apart on encoding or ordering.  ``content_length`` is omitted for streams,
    which do not know their length up front.
    """
    raw = [(b"content-type", media_type.encode("latin-1"))]
    if content_length is not None:
        raw.append((b"content-length", str(content_length).encode("latin-1")))
    if headers:
        raw.extend((k.encode("latin-1"), v.encode("latin-1")) for k, v in headers.items())
    return raw


class Response[T: Object](Object):
    encoder: typing.ClassVar[msgspec.json.Encoder] = _encoder
    status_code: typing.ClassVar[int] = 200
    content: T | str | None = None
    headers: typing.Mapping[str, str] | None = None
    media_type: str = "application/json"

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        body = self.encoder.encode(self.content)
        await send(
            {
                "type": "http.response.start",
                "status": self.status_code,
                "headers": raw_headers(self.media_type, self.headers, len(body)),
            }
        )
        await send({"type": "http.response.body", "body": body})


class Created[T: Object](Response[T]):
    status_code: typing.ClassVar[int] = 201


class NoContent[T: Object](Response[T]):
    status_code: typing.ClassVar[int] = 204

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": self.status_code,
                "headers": raw_headers(self.media_type, self.headers, 0)[1:],
            }
        )
        await send({"type": "http.response.body", "body": b""})


class Problem(Object, omit_defaults=True):
    """Base RFC-9457 ASGI error response.

    Subclass and set ``type`` / ``status_code`` as ClassVars, ``title`` as a
    field default.  Note the class attribute is ``status_code`` - matching every
    other response - while the RFC's wire member stays ``status``.
    """

    encoder: typing.ClassVar[msgspec.json.Encoder] = _encoder
    type: typing.ClassVar[str] = "about:blank"
    status_code: typing.ClassVar[int] = 500
    media_type: typing.ClassVar[str] = "application/problem+json"
    title: str
    detail: str | None = None
    instance: str | None = None

    @property
    def body(self) -> dict:
        return dict(
            type=self.type,
            status=self.status_code,
            title=self.title,
            detail=self.detail,
            instance=self.instance,
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        body = self.encoder.encode(self.body)
        await send(
            {
                "type": "http.response.start",
                "status": self.status_code,
                "headers": raw_headers(self.media_type, None, len(body)),
            }
        )
        await send({"type": "http.response.body", "body": body})


class NotFound(Problem):
    status_code: typing.ClassVar[int] = 404
    title: str = "Not Found"


class BadRequest(Problem):
    status_code: typing.ClassVar[int] = 400
    title: str = "Bad Request"


class Unauthorized(Problem):
    status_code: typing.ClassVar[int] = 401
    title: str = "Unauthorized"


class Forbidden(Problem):
    status_code: typing.ClassVar[int] = 403
    title: str = "Forbidden"


class MethodNotAllowed(Problem):
    status_code: typing.ClassVar[int] = 405
    title: str = "Method Not Allowed"


class InternalServerError(Problem):
    status_code: typing.ClassVar[int] = 500
    title: str = "Internal Server Error"


class FieldError(Object):
    field: str
    location: str
    message: str


class ValidationProblem(BadRequest):
    errors: list[FieldError] | None = None

    @property
    def body(self) -> dict:
        problem = super().body
        problem.update(errors=self.errors)
        return problem


class Event[T](Object):
    """One server-sent event.

    Yield a bare object instead to emit a data-only event.
    """

    data: T
    event: str | None = None
    id: str | None = None
    retry: int | None = None


def encode_event(item: typing.Any) -> bytes:
    """Frame one item as an SSE block."""
    if not isinstance(item, Event):
        item = Event(data=item)

    lines: list[bytes] = []
    if item.event is not None:
        lines.append(b"event: " + str(item.event).encode())
    if item.id is not None:
        lines.append(b"id: " + str(item.id).encode())
    if item.retry is not None:
        lines.append(b"retry: " + str(int(item.retry)).encode())
    # JSON escapes newlines, so the payload is always a single data: line.
    lines.append(b"data: " + _encoder.encode(item.data))
    return b"\n".join(lines) + b"\n\n"


KEEPALIVE_FRAME = b":\n\n"
"""An SSE comment.  Ignored by clients, but keeps proxies from reaping an idle
connection."""


class EventStream:
    """Streams an async generator of events as ``text/event-stream``.

    Applied by the framework to an async-generator handler; user code never
    constructs one.  The first item is pulled *before* any ASGI message is sent,
    so a handler can yield a ``Problem`` as a pre-flight error and get an
    ordinary buffered response instead of a stream.
    """

    __slots__ = ("keepalive", "source")

    def __init__(
        self,
        source: typing.AsyncGenerator[typing.Any],
        *,
        keepalive: float | None = None,
    ) -> None:
        self.source = source
        self.keepalive = keepalive

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        source = self.source
        try:
            first = await anext(source)
        except StopAsyncIteration:
            await self._start(send)
            await send({"type": "http.response.body", "body": b"", "more_body": False})
            return

        if isinstance(first, Problem):
            # Nothing has been sent yet, so this is still an ordinary response.
            await source.aclose()
            await first(scope, receive, send)
            return

        await self._start(send)
        await send({"type": "http.response.body", "body": encode_event(first), "more_body": True})
        await self._pump(source, receive, send)

    async def _start(self, send: Send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": raw_headers(
                    "text/event-stream",
                    {
                        "cache-control": "no-cache",
                        "connection": "keep-alive",
                        # nginx buffers text/event-stream into uselessness otherwise.
                        "x-accel-buffering": "no",
                    },
                    None,
                ),
            }
        )

    async def _pump(
        self,
        source: typing.AsyncGenerator[typing.Any],
        receive: Receive,
        send: Send,
    ) -> None:
        """Emit events until the generator ends or the client goes away."""
        disconnected = asyncio.ensure_future(_wait_for_disconnect(receive))
        pending: asyncio.Future[typing.Any] | None = None
        try:
            while True:
                if pending is None:
                    pending = asyncio.ensure_future(anext(source))

                done, _ = await asyncio.wait(
                    {pending, disconnected},
                    timeout=self.keepalive,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if disconnected in done:
                    # Only a real disconnect ends the stream quietly; a failure
                    # in the receive channel must not masquerade as one.
                    if not disconnected.cancelled():
                        if (failure := disconnected.exception()) is not None:
                            raise failure
                    return

                if pending in done:
                    try:
                        item = pending.result()
                    except StopAsyncIteration:
                        break
                    finally:
                        pending = None
                    await send(
                        {
                            "type": "http.response.body",
                            "body": encode_event(item),
                            "more_body": True,
                        }
                    )
                else:
                    await send(
                        {
                            "type": "http.response.body",
                            "body": KEEPALIVE_FRAME,
                            "more_body": True,
                        }
                    )
        finally:
            # Cancel *and* reap: a fire-and-forget cancel leaves the task
            # pending, which keeps the event loop from settling.
            outstanding = [t for t in (pending, disconnected) if t is not None]
            for task in outstanding:
                task.cancel()
            if outstanding:
                await asyncio.gather(*outstanding, return_exceptions=True)
            await source.aclose()

        await send({"type": "http.response.body", "body": b"", "more_body": False})


async def _wait_for_disconnect(receive: Receive) -> None:
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            return
        # A receive() that resolves without suspending would spin here and
        # starve the loop, so always hand control back between messages.
        await asyncio.sleep(0)
