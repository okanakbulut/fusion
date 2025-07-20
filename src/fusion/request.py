from contextlib import AsyncExitStack
from contextvars import ContextVar, Token
from functools import cached_property
from typing import Any, Self, Union
from urllib.parse import parse_qsl

from fusion.types import Receive, Scope, Send

request: ContextVar["Request"] = ContextVar("request")


class Request(AsyncExitStack):
    _token: Token
    scope: Scope
    receive: Receive
    send: Send

    def __init__(self, scope: Scope, receive: Receive, send: Send) -> None:
        super().__init__()
        self.scope = scope
        self.receive = receive
        self.send = send

    async def __aenter__(self) -> Self:
        if request.get(None):
            raise RuntimeError("Nested context is not allowed")
        self._token = request.set(self)
        return await super().__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # type:ignore
        try:
            await super().__aexit__(exc_type, exc_val, exc_tb)
        finally:
            request.reset(self._token)

    @cached_property
    def query_params(self) -> dict[str, Union[Any, list[Any]]]:
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
    def path_params(self) -> dict[str, Union[Any, list[Any]]]:
        """Get the path parameters from the request."""
        return {}

    @cached_property
    def headers(self) -> dict[str, str]:
        """Get the headers from the request."""
        return {
            k.decode().lower().replace("-", "_").replace(" ", "_"): v.decode()
            for k, v in self.scope["headers"]
        }

    @cached_property
    def cookies(self) -> dict[str, str]:
        """Get the cookies from the request."""
        return {
            key.lower().replace("-", "_").replace(" ", "_"): value
            for key, value in self.scope["cookies"].items()
        }
