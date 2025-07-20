from abc import abstractmethod
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

from fusion.request import request
from fusion.responses import Object
from fusion.types import Injectable

T = TypeVar("T")
type Constructor[T] = Callable[[], Awaitable[T] | AbstractAsyncContextManager[T]]
__factories__: dict[Type, Constructor] = {}


class Resolver(Object, Generic[T]):
    """Base class for resolvers."""

    name: str
    typ: Type[T]

    @abstractmethod
    async def resolve(self) -> tuple[str, T | None]:
        """Resolve the dependency."""
        raise NotImplementedError("Subclasses must implement this method")


class InjectableResolver(Resolver[Injectable]):
    """Resolver for injected dependencies."""

    async def resolve(self) -> tuple[str, Injectable]:
        """Resolve the injected dependency."""
        req = request.get()
        if not req:
            raise RuntimeError("Request is not available")

        return self.name, await self.typ.instance()


def isasynccontextmanager(func: Callable) -> bool:
    # assert hasattr(func, "__annotations__")
    ret = func.__annotations__.get("return", None)
    return get_origin(ret) is AsyncIterator if ret else False


class FactoryResolver(Resolver[T]):
    """Resolver for factory functions."""

    async def resolve(self) -> tuple[str, T]:
        """Resolve the factory function."""
        factory: Constructor | None = __factories__.get(self.typ)
        if factory is None:
            raise ValueError(f"No factory found for {self.typ}")

        if isasynccontextmanager(factory):
            req = request.get()
            return self.name, await req.enter_async_context(factory())  # type: ignore
        else:
            return self.name, await factory()  # type: ignore


class QueryParamResolver(Resolver[T]):
    """Resolver for query parameters."""

    async def resolve(self) -> tuple[str, T | None]:
        """Resolve the query parameter from the request context."""
        value = request.get().query_params.get(self.name, None)
        return self.name, msgspec.convert(value, self.typ, strict=False)


class PathParamResolver(Resolver[T]):
    """Resolver for path parameters."""

    async def resolve(self) -> tuple[str, T | None]:
        """Resolve the path parameter from the request context."""
        req = request.get()
        value = req.path_params.get(self.name, None)
        if value is not None:
            value = msgspec.convert(value, self.typ, strict=False)
        return self.name, value


class RequestBodyResolver(Resolver[T]):
    """Resolver for request body parameters."""

    async def resolve(self) -> tuple[str, T]:
        """Resolve the request body from the request context."""
        # req = request.get()
        body = ""  # await req.request.json()
        value = msgspec.convert(body, self.typ, strict=True)
        return self.name, value


class HeaderResolver(Resolver[T]):
    """Resolver for header."""

    async def resolve(self) -> tuple[str, T | None]:
        """Resolve the header parameter from the request context."""
        value = request.get().headers.get(self.name, None)
        return self.name, msgspec.convert(value, self.typ, strict=False)


class CookieResolver(Resolver[T]):
    """Resolver for cookie."""

    async def resolve(self) -> tuple[str, T | None]:
        """Resolve the cookie parameter from the request context."""
        req = request.get()
        cookies = {
            key.lower().replace("-", "_").replace(" ", "_"): value
            for key, value in req.cookies.items()
        }
        value = cookies.get(self.name, None)
        if value is not None:
            value = msgspec.convert(value, self.typ, strict=False)
        return self.name, value
