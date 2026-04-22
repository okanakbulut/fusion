import typing
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager

import msgspec

from .context import Context, context
from .exceptions import ValidationException
from .object import Object
from .responses import FieldError

T = typing.TypeVar("T")
type Constructor[T] = typing.Callable[[], typing.Awaitable[T] | AbstractAsyncContextManager[T]]

__factories__: dict[type[typing.Any], Constructor[typing.Any]] = {}


def has_factory(typ: type[typing.Any]) -> bool:
    return typ in __factories__


class Resolver(Object):
    """Base class for all resolvers."""

    name: str
    typ: type[typing.Any]

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

    location: typing.ClassVar[str] = "query"

    async def resolve(self) -> tuple[str, typing.Any]:
        """Resolve the query parameter from the request context."""
        value = self.context.query_params.get(self.name, None)
        return self.name, msgspec.convert(value, self.typ, strict=False)


class PathParamResolver(Resolver):
    """Resolver for path parameters."""

    location: typing.ClassVar[str] = "path"

    async def resolve(self) -> tuple[str, typing.Any]:
        """Resolve the path parameter from the request context."""
        value = self.context.path_params.get(self.name, None)
        return self.name, msgspec.convert(value, self.typ, strict=False)


class RequestBodyResolver(Resolver):
    """Resolver for request body parameters."""

    location: typing.ClassVar[str] = "body"

    async def resolve(self) -> tuple[str, typing.Any]:
        """Resolve the request body from the request context."""
        body = await self.context.body()

        if not (isinstance(self.typ, type) and issubclass(self.typ, msgspec.Struct)):
            return self.name, msgspec.json.decode(body, type=self.typ, strict=True)

        try:
            return self.name, msgspec.json.decode(body, type=self.typ, strict=True)
        except msgspec.DecodeError as exc:
            raise ValidationException(detail=str(exc)) from exc
        except msgspec.ValidationError:
            pass

        try:
            raw = msgspec.json.decode(body)
        except msgspec.DecodeError as exc:
            raise ValidationException(detail=str(exc)) from exc

        if not isinstance(raw, dict):
            raise ValidationException(detail="Request body must be a JSON object")

        field_errors: list[FieldError] = []
        params: dict[str, typing.Any] = {}

        for field in msgspec.structs.fields(self.typ):
            if field.encode_name in raw:
                try:
                    params[field.name] = msgspec.convert(
                        raw[field.encode_name], field.type, strict=False
                    )
                except msgspec.ValidationError as exc:
                    field_errors.append(
                        FieldError(field=field.name, location="body", message=str(exc))
                    )
            elif field.default is not msgspec.NODEFAULT:
                params[field.name] = field.default
            elif field.default_factory is not msgspec.NODEFAULT:
                params[field.name] = field.default_factory()
            else:
                try:
                    msgspec.convert(None, field.type, strict=False)
                except msgspec.ValidationError as exc:
                    field_errors.append(
                        FieldError(field=field.name, location="body", message=str(exc))
                    )

        if field_errors:
            raise ValidationException(errors=field_errors)

        return self.name, self.typ(**params)


class HeaderResolver(Resolver):
    """Resolver for header."""

    location: typing.ClassVar[str] = "header"

    async def resolve(self) -> tuple[str, typing.Any]:
        """Resolve the header parameter from the request context."""
        value = self.context.headers.get(self.name, None)
        return self.name, msgspec.convert(value, self.typ, strict=False)


class CookieResolver(Resolver):
    """Resolver for cookie."""

    location: typing.ClassVar[str] = "cookie"

    async def resolve(self) -> tuple[str, typing.Any]:
        """Resolve the cookie parameter from the request context."""
        cookies = {
            key.lower().replace("-", "_").replace(" ", "_"): value
            for key, value in self.context.cookies.items()
        }
        value = cookies.get(self.name, None)
        return self.name, msgspec.convert(value, self.typ, strict=False)
