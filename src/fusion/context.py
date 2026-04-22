import contextlib
import contextvars
import typing
from urllib.parse import parse_qsl

from ._utils import cached_property
from .exceptions import ValidationException
from .types import Receive, Scope, Send

context: contextvars.ContextVar[Context] = contextvars.ContextVar("context")

MAX_BODY_SIZE = 10 * 1024 * 1024  # 10 MB


class Context(contextlib.AsyncExitStack):
    _token: contextvars.Token[Context]
    _body: bytes | None
    scope: Scope
    receive: Receive
    send: Send

    def __init__(self, scope: Scope, receive: Receive, send: Send) -> None:
        super().__init__()
        self._body = None
        self.scope = scope
        self.receive = receive
        self.send = send

    async def __aenter__(self) -> typing.Self:
        if context.get(None):
            raise RuntimeError("Nested context is not allowed")
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
        """Get the query parameters from the request."""
        params = {}
        query_string = self.scope["query_string"].decode()
        for name, value in parse_qsl(query_string):
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
        cookie_header = self.headers.get("cookie", "")
        cookies: dict[str, str] = {}
        for part in cookie_header.split(";"):
            part = part.strip()
            if "=" in part:
                key, _, value = part.partition("=")
                normalized = key.strip().lower().replace("-", "_").replace(" ", "_")
                cookies[normalized] = value.strip()
        return cookies
