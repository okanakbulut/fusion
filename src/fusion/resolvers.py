import typing
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager

import msgspec

from .context import Context, context
from .object import Object

T = typing.TypeVar("T")
type Constructor[T] = typing.Callable[[], typing.Awaitable[T] | AbstractAsyncContextManager[T]]

__factories__: dict[type[typing.Any], Constructor[typing.Any]] = {}


def has_factory(typ: type[typing.Any]) -> bool:
    return typ in __factories__


class Resolver(Object):
    """Base class for all resolvers."""

    name: str
    typ: type

    @property
    def context(self) -> Context:
        if ctx := context.get(None):
            return ctx
        raise RuntimeError("No context available")

    async def resolve(self) -> tuple[str, typing.Any]:
        """Resolve the value from the request context."""
        raise NotImplementedError


class InjectableResolver(Resolver):
    """Resolver for injected dependencies."""

    async def resolve(self) -> tuple[str, typing.Any]:
        """Resolve the injected dependency."""
        return self.name, await self.typ.instance()


class FactoryResolver(Resolver):
    """Resolver for third-party dependencies backed by registered factories."""

    async def resolve(self) -> tuple[str, typing.Any]:
        factory = __factories__.get(self.typ)
        if factory is None:
            raise RuntimeError(f"No factory found for {self.typ}")

        value = factory()
        if isinstance(value, AbstractAsyncContextManager):
            value = await self.context.enter_async_context(value)
        else:
            value = await value

        return self.name, value


class QueryParamResolver(Resolver):
    """Resolver for query parameters."""

    async def resolve(self) -> tuple[str, typing.Any]:
        """Resolve the query parameter from the request context."""
        value = self.context.query_params.get(self.name, None)
        return self.name, msgspec.convert(value, self.typ, strict=False)


class PathParamResolver(Resolver):
    """Resolver for path parameters."""

    async def resolve(self) -> tuple[str, typing.Any]:
        """Resolve the path parameter from the request context."""
        value = self.context.path_params.get(self.name, None)
        if value is not None:
            value = msgspec.convert(value, self.typ, strict=False)
        return self.name, value


class RequestBodyResolver(Resolver):
    """Resolver for request body parameters."""

    async def resolve(self) -> tuple[str, typing.Any]:
        """Resolve the request body from the request context."""
        body = await self.context.body()
        value = msgspec.json.decode(body, type=self.typ, strict=True)
        return self.name, value


class HeaderResolver(Resolver):
    """Resolver for header."""

    async def resolve(self) -> tuple[str, typing.Any]:
        """Resolve the header parameter from the request context."""
        value = self.context.headers.get(self.name, None)
        if value is None:
            raise ValueError(f"Missing header '{self.name}'")
        return self.name, msgspec.convert(value, self.typ, strict=False)


class CookieResolver(Resolver):
    """Resolver for cookie."""

    async def resolve(self) -> tuple[str, typing.Any]:
        """Resolve the cookie parameter from the request context."""
        cookies = {
            key.lower().replace("-", "_").replace(" ", "_"): value
            for key, value in self.context.cookies.items()
        }
        value = cookies.get(self.name, None)
        if value is not None:
            value = msgspec.convert(value, self.typ, strict=False)
        return self.name, value
