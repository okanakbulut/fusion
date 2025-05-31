from contextlib import AsyncExitStack
from contextvars import ContextVar, Token
from typing import Self

from starlette.requests import Request

context: ContextVar["Context"] = ContextVar("context")


class Context(AsyncExitStack):
    _token: Token
    request: Request

    def __init__(self, request: Request):
        super().__init__()
        self.request = request

    async def __aenter__(self) -> Self:
        if context.get(None) is not None:
            raise RuntimeError("Nested context is not allowed")
        self._token = context.set(self)
        return await super().__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # type:ignore
        try:
            await super().__aexit__(exc_type, exc_val, exc_tb)
        finally:
            context.reset(self._token)
