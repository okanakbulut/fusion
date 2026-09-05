import contextlib
import contextvars
import typing
from functools import cached_property
from urllib.parse import parse_qsl

from .exceptions import ValidationException
from .types import Receive, Scope, Send

context: contextvars.ContextVar[Context] = contextvars.ContextVar("context")

MAX_BODY_SIZE = 10 * 1024 * 1024  # 10 MB


def current() -> Context:
    """The context bound to this task.

    ``Context.__aenter__`` sets it, so anything running inside a request or a
    tool call has one; reaching for it outside either is a programming error
    rather than something a client did wrong.
    """
    if ctx := context.get(None):
        return ctx
    raise RuntimeError("No context available")


class Context(contextlib.AsyncExitStack):
    _token: contextvars.Token[Context]
    _body: bytes | None
    scope: Scope
    receive: Receive
    send: Send

    dependencies: dict[type, typing.Any]
    """Per-context dependency cache, keyed by provided type, so two parameters
    asking for the same type share one instance instead of constructing two."""

    arguments: typing.Mapping[str, typing.Any]
    """Flat argument source for tool calls.  Empty for HTTP, where parameters
    come from the path, query, headers, cookies or body instead."""

    def __init__(self, scope: Scope, receive: Receive, send: Send) -> None:
        super().__init__()
        self._body = None
        self.scope = scope
        self.receive = receive
        self.send = send
        self.dependencies = {}
        self.arguments = {}

    async def __aenter__(self) -> typing.Self:
        # Contexts nest: an MCP tool call runs inside the HTTP request that
        # carried it, and each needs its own exit stack and dependency cache.
        # ContextVar tokens already form a stack, so entering just pushes.
        self._token = context.set(self)
        return await super().__aenter__()

    async def __aexit__(self, *exc_details) -> None:  # type: ignore
        try:
            await super().__aexit__(*exc_details)  # type: ignore
        finally:
            context.reset(self._token)

    @property
    def type(self) -> str:
        return self.scope.get("type", "")

    @property
    def scheme(self) -> str:
        return self.scope.get("scheme", "")

    @property
    def method(self) -> str:
        return self.scope.get("method", "")

    @property
    def path(self) -> str:
        return self.scope.get("path", "")

    @property
    def query_string(self) -> str:
        return self.scope.get("query_string", b"").decode()

    @cached_property
    def headers(self) -> dict[str, str]:
        """Get the headers from the request."""
        return {
            k.decode().lower().replace("-", "_").replace(" ", "_"): v.decode()
            for k, v in self.scope["headers"]
        }

    def header(self, name: str) -> str | None:
        """One header by its normalised name, or ``None`` if it was not sent.

        A request carries a dozen headers and a handler names one or two, so
        building the whole table to answer that decodes eleven keys nobody
        asked for.  Scanning the raw list instead compares lengths first, which
        rejects almost every candidate before touching its bytes.  Once
        ``headers`` has been materialised it is the cheaper source, so the
        lookup goes there.
        """
        if (table := self.__dict__.get("headers")) is not None:
            return table.get(name)

        target = name.encode()
        size = len(target)
        for key, value in self.scope["headers"]:
            if len(key) == size and key.replace(b"-", b"_").replace(b" ", b"_").lower() == target:
                return value.decode()
        return None

    async def body(self) -> bytes:
        """Get the body from the request."""
        if self._body is not None:
            return self._body

        chunks: list[bytes] = []
        total = 0
        while True:
            message = await self.receive()
            if message["type"] == "http.request":
                chunk = message.get("body", b"")
                if chunk:
                    total += len(chunk)
                    if total > MAX_BODY_SIZE:
                        raise ValidationException(
                            detail=f"Request body exceeds maximum size of {MAX_BODY_SIZE} bytes"
                        )
                    chunks.append(chunk)
                if not message.get("more_body", False):
                    break
            elif message["type"] == "http.disconnect":
                raise RuntimeError("Client disconnected")
        self._body = b"".join(chunks)
        return self._body

    @cached_property
    def query_params(self) -> dict[str, typing.Any | list[typing.Any]]:
        """Get the query parameters from the request.

        Percent- and plus-decoding is the only thing ``parse_qsl`` does that a
        split cannot, and most query strings need neither, so the common case
        splits by hand and the rest falls back to the stdlib.  Both drop a field
        with no ``=`` or an empty value, which is what ``parse_qsl`` does with
        ``keep_blank_values=False``.
        """
        query_string = self.scope["query_string"].decode()
        if not query_string:
            return {}

        if "%" in query_string or "+" in query_string:
            pairs = parse_qsl(query_string)
        else:
            pairs = [
                (name, value)
                for name, sep, value in (field.partition("=") for field in query_string.split("&"))
                if sep and value
            ]

        params: dict[str, typing.Any | list[typing.Any]] = {}
        for name, value in pairs:
            if name.endswith(":list"):
                params[name[:-5]] = value.split(",")
            else:
                params[name] = value
        return params

    @cached_property
    def path_params(self) -> dict[str, typing.Any]:
        """Get the path parameters from the request."""
        return self.scope.get("path_params", {})

    @cached_property
    def cookies(self) -> dict[str, str]:
        """Get the cookies from the request, parsed from the Cookie header."""
        cookie_header = self.header("cookie") or ""
        cookies: dict[str, str] = {}
        for part in cookie_header.split(";"):
            part = part.strip()
            if "=" in part:
                key, _, value = part.partition("=")
                normalized = key.strip().lower().replace("-", "_").replace(" ", "_")
                cookies[normalized] = value.strip()
        return cookies
