from abc import abstractmethod
from collections.abc import AsyncIterator, Awaitable
from contextlib import AbstractAsyncContextManager
from typing import (
    Callable,
    Generic,
    Protocol,
    Self,
    Type,
    TypeVar,
    get_origin,
)

import msgspec
from msgspec import Struct as Object

from fusion.context import context

T = TypeVar("T")
type Constructor[T] = Callable[[], Awaitable[T] | AbstractAsyncContextManager[T]]
__factories__: dict[Type, Constructor] = {}


class InjectableObject(Protocol):
    @classmethod
    async def instance(cls) -> Self:
        ...


class Resolver(Object, Generic[T]):
    """Base class for resolvers."""

    name: str
    typ: Type[T]

    @abstractmethod
    async def resolve(self) -> tuple[str, T | None]:
        """Resolve the dependency."""
        raise NotImplementedError("Subclasses must implement this method")


class InjectableResolver(Resolver[InjectableObject]):
    """Resolver for injected dependencies."""

    async def resolve(self) -> tuple[str, InjectableObject]:
        """Resolve the injected dependency."""
        ctx = context.get()
        if not ctx:
            raise RuntimeError("Request context is not available")

        return self.name, await self.typ.instance()


def isasynccontextmanager(func: Callable) -> bool:
    assert hasattr(func, "__annotations__")
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
            ctx = context.get()
            return self.name, await ctx.enter_async_context(factory())  # type: ignore
        else:
            return self.name, await factory()  # type: ignore


class QueryParamResolver(Resolver[T]):
    """Resolver for query parameters."""

    async def resolve(self) -> tuple[str, T | None]:
        """Resolve the query parameter from the request context."""
        ctx = context.get()
        value = ctx.request.query_params.get(self.name, None)
        if value is not None:
            value = msgspec.convert(value, self.typ, strict=False)
        return self.name, value


class PathParamResolver(Resolver[T]):
    """Resolver for path parameters."""

    async def resolve(self) -> tuple[str, T | None]:
        """Resolve the path parameter from the request context."""
        ctx = context.get()
        value = ctx.request.path_params.get(self.name, None)
        if value is not None:
            value = msgspec.convert(value, self.typ, strict=False)
        return self.name, value


class RequestBodyResolver(Resolver[T]):
    """Resolver for request body parameters."""

    async def resolve(self) -> tuple[str, T]:
        """Resolve the request body from the request context."""
        ctx = context.get()
        body = await ctx.request.json()
        value = msgspec.convert(body, self.typ, strict=True)
        return self.name, value


class HeaderResolver(Resolver[T]):
    """Resolver for header."""

    async def resolve(self) -> tuple[str, T | None]:
        """Resolve the header parameter from the request context."""
        ctx = context.get()
        headers = {
            key.lower().replace("-", "_").replace(" ", "_"): value
            for key, value in ctx.request.headers.items()
        }
        value = headers.get(self.name, None)
        if value is not None:
            value = msgspec.convert(value, self.typ, strict=False)
        return self.name, value


class CookieResolver(Resolver[T]):
    """Resolver for cookie."""

    async def resolve(self) -> tuple[str, T | None]:
        """Resolve the cookie parameter from the request context."""
        ctx = context.get()
        cookies = {
            key.lower().replace("-", "_").replace(" ", "_"): value
            for key, value in ctx.request.cookies.items()
        }
        value = cookies.get(self.name, None)
        if value is not None:
            value = msgspec.convert(value, self.typ, strict=False)
        return self.name, value
