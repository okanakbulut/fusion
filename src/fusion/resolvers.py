from collections.abc import AsyncIterator, Awaitable
from contextlib import AbstractAsyncContextManager
from typing import (
    Callable,
    Generic,
    Type,
    TypeVar,
    get_origin,
)

import msgspec

from .protocols import Injectable, Resolver
from .request import Request

T = TypeVar("T")
type Constructor[T] = Callable[[], Awaitable[T] | AbstractAsyncContextManager[T]]
__factories__: dict[Type, Constructor] = {}


class InjectableResolver(Resolver[Injectable]):
    """Resolver for injected dependencies."""

    async def resolve(self, request: Request) -> tuple[str, Injectable]:
        """Resolve the injected dependency."""
        if not request:
            raise RuntimeError("Request is not available")

        return self.name, await self.typ.instance()


def isasynccontextmanager(func: Callable) -> bool:
    # assert hasattr(func, "__annotations__")
    ret = func.__annotations__.get("return", None)
    return get_origin(ret) is AsyncIterator if ret else False


class FactoryResolver(Resolver[T]):
    """Resolver for factory functions."""

    async def resolve(self, request: Request) -> tuple[str, T]:
        """Resolve the factory function."""
        factory: Constructor | None = __factories__.get(self.typ)
        if factory is None:
            raise ValueError(f"No factory found for {self.typ}")

        if isasynccontextmanager(factory):
            return self.name, await request.enter_async_context(factory())  # type: ignore
        else:
            return self.name, await factory()  # type: ignore


class QueryParamResolver(Resolver[T]):
    """Resolver for query parameters."""

    async def resolve(self, request: Request) -> tuple[str, T | None]:
        """Resolve the query parameter from the request context."""
        value = request.query_params.get(self.name, None)
        return self.name, msgspec.convert(value, self.typ, strict=False)


class PathParamResolver(Resolver[T]):
    """Resolver for path parameters."""

    async def resolve(self, request: Request) -> tuple[str, T | None]:
        """Resolve the path parameter from the request context."""
        value = request.path_params.get(self.name, None)
        if value is not None:
            value = msgspec.convert(value, self.typ, strict=False)
        return self.name, value


class RequestBodyResolver(Resolver[T]):
    """Resolver for request body parameters."""

    async def resolve(self, request: Request) -> tuple[str, T]:
        """Resolve the request body from the request context."""
        body = await request.body()
        value = msgspec.json.decode(body, type=self.typ, strict=True)
        return self.name, value


class HeaderResolver(Resolver[T]):
    """Resolver for header."""

    async def resolve(self, request: Request) -> tuple[str, T | None]:
        """Resolve the header parameter from the request context."""
        value = request.headers.get(self.name, None)
        return self.name, msgspec.convert(value, self.typ, strict=False)


class CookieResolver(Resolver[T]):
    """Resolver for cookie."""

    async def resolve(self, request: Request) -> tuple[str, T | None]:
        """Resolve the cookie parameter from the request context."""
        cookies = {
            key.lower().replace("-", "_").replace(" ", "_"): value
            for key, value in request.cookies.items()
        }
        value = cookies.get(self.name, None)
        if value is not None:
            value = msgspec.convert(value, self.typ, strict=False)
        return self.name, value
