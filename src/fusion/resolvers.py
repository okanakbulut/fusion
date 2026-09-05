import enum
import functools
import inspect
import typing
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager

import msgspec

from .context import Context, current
from .exceptions import ValidationException
from .object import MetaObject, Object
from .responses import FieldError
from .types import Transport

T = typing.TypeVar("T")
type Constructor[T] = typing.Callable[[], typing.Awaitable[T] | AbstractAsyncContextManager[T]]

__factories__: dict[type[typing.Any], Constructor[typing.Any]] = {}


class _Missing(enum.Enum):
    MISSING = enum.auto()


MISSING = _Missing.MISSING
"""Sentinel returned by a resolver when its value is absent from the request,
so the binder can omit the argument entirely and let the parameter's own
default apply, instead of forcing a conversion of ``None``."""


def has_factory(typ: type[typing.Any]) -> bool:
    return typ in __factories__


class Resolver(Object):
    """Base class for all resolvers."""

    name: str
    typ: type[typing.Any]

    location: typing.ClassVar[str] = "unknown"
    """Where the value came from.  Doubles as OpenAPI's ``in:`` for the HTTP
    resolvers and as ``FieldError.location`` for every resolver."""

    def __init_subclass__(cls, **kwargs: typing.Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._adapt_resolve()

    @classmethod
    def _adapt_resolve(cls) -> None:
        """Keep a ``resolve(self)`` override working now that ``bind`` passes a context.

        Declaring the parameter optional only helps the caller: an override
        that never declared it still raises ``TypeError`` when handed one.
        Wrapping such an override once, as its class is created, means the hot
        path can pass the context unconditionally and never test for this.
        """
        override = cls.__dict__.get("resolve")
        if override is None or len(inspect.signature(override).parameters) > 1:
            return

        async def resolve(self: Resolver, ctx: Context | None = None) -> tuple[str, typing.Any]:
            return await override(self)

        functools.update_wrapper(resolve, override)
        cls.resolve = resolve  # type: ignore[method-assign]

    @property
    def context(self) -> Context:
        """The active context, looked up from the contextvar.

        ``resolve`` is handed the context outright, so this is the fallback for
        a resolver written before that argument existed, and for code that
        reaches for the context outside a resolve call.
        """
        return current()

    async def resolve(self, ctx: Context | None = None) -> tuple[str, typing.Any]:
        """Resolve the value from ``ctx``, or from the active context.

        ``bind`` looks the context up once and hands it to every resolver: read
        through ``self.context`` instead, a handler with five parameters pays
        for five contextvar lookups to reach the one object they all share.
        The argument stays optional so an existing resolver, and any caller
        invoking ``resolve()`` directly, keeps working untouched - which is why
        each override below opens by falling back to the property.
        """
        raise NotImplementedError


class Marker(Object, frozen=True):
    """Metadata attached to an annotation alias, naming the value's source.

    Both halves matter: ``resolver`` says how to obtain the value, ``transport``
    says which transports the parameter is meaningful under, which is what lets
    a wrong-transport registration be rejected at construction time.
    """

    resolver: type[Resolver]
    transport: Transport


def marker_of(annotation: typing.Any) -> Marker | None:
    """Return the ``Marker`` carried by an annotation alias, or None.

    Markers are declared as PEP 695 aliases over ``Annotated``, so the payload
    sits at ``get_origin(annotation).__value__.__metadata__[0]`` - or on the
    alias itself, for a credential marker whose type the scheme already fixes.
    """
    origin = typing.get_origin(annotation) or annotation
    value = getattr(origin, "__value__", None)
    metadata = getattr(value, "__metadata__", None)
    if not metadata:
        return None
    candidate = metadata[0]
    return candidate if isinstance(candidate, Marker) else None


def build_resolvers(
    hints: dict[str, typing.Any],
    *,
    allowed: frozenset[Transport],
    owner: str,
) -> dict[str, Resolver]:
    """Build the resolver table for a set of annotations.

    Shared by ``Injectable.__init_subclass__`` (class annotations) and
    ``Signature`` (function parameters) so the two can never drift apart.
    """
    resolvers: dict[str, Resolver] = {}

    for name, annotation in hints.items():
        if name == "return":
            continue

        origin = typing.get_origin(annotation)
        if origin in (typing.ClassVar, type):
            continue

        marker = marker_of(annotation)
        if marker is None:
            raise TypeError(
                f"Parameter {name!r} on {owner!r} is annotated {annotation!r}, which carries "
                f"no Fusion marker. Every parameter must name its source explicitly - wrap a "
                f"dependency as Inject[...], or use an Http.* / Tool.* marker."
            )

        if marker.transport is not Transport.ANY and marker.transport not in allowed:
            allowed_names = ", ".join(sorted(t.value for t in allowed))
            raise TypeError(
                f"Parameter {name!r} on {owner!r} uses a {marker.transport.value!r} marker, "
                f"which has no meaning here (this accepts: {allowed_names}). "
                f"Use a marker for one of those transports, or Inject[...] for a dependency."
            )

        args = typing.get_args(annotation)
        inner_type: type[typing.Any] = args[0] if args else typing.cast(type, typing.Any)
        resolvers[name] = marker.resolver(name=name, typ=inner_type)

    return resolvers


class DependencyResolver(Resolver):
    """Resolver for a bare ``Inject[T]``, dispatching on how ``T`` is provided.

    ``Inject`` is one marker over two mechanisms - an ``Injectable`` subclass
    builds itself, a factory-backed type is built by its factory.  Which one
    applies is decided on first use and remembered, so a request never repeats
    the check, yet a factory registered after the handler was defined is still
    picked up.
    """

    location: typing.ClassVar[str] = "dependency"

    from_factory: bool | None = None

    async def resolve(self, ctx: Context | None = None) -> tuple[str, typing.Any]:
        ctx = ctx or self.context
        cache = ctx.dependencies
        if self.typ in cache:
            return self.name, cache[self.typ]

        if self.from_factory is None:
            self.from_factory = self._resolve_kind()

        if self.from_factory:
            value = await self._from_factory(ctx)
        else:
            value = await self.typ.instance(ctx)

        cache[self.typ] = value
        return self.name, value

    def _resolve_kind(self) -> bool:
        from .injectable import Injectable

        if isinstance(self.typ, type) and issubclass(self.typ, Injectable):
            return False
        if has_factory(self.typ):
            return True
        raise RuntimeError(
            f"Cannot inject {self.typ!r} for {self.name!r}: it is neither an Injectable "
            f"subclass nor a type with a registered @factory."
        )

    async def _from_factory(self, ctx: Context) -> typing.Any:
        factory = __factories__.get(self.typ)
        if factory is None:  # pragma: no cover - registry emptied after first use
            raise RuntimeError(f"No factory found for {self.typ}")
        produced = factory()
        if isinstance(produced, AbstractAsyncContextManager):
            return await ctx.enter_async_context(produced)
        return await produced


class ContextResolver(Resolver):
    """Resolver for context-backed façades such as ``Request``."""

    location: typing.ClassVar[str] = "context"

    async def resolve(self, ctx: Context | None = None) -> tuple[str, typing.Any]:
        return self.name, self.typ()


class ToolArgResolver(Resolver):
    """Resolver for a tool-call argument."""

    location: typing.ClassVar[str] = "argument"

    async def resolve(self, ctx: Context | None = None) -> tuple[str, typing.Any]:
        value = (ctx or self.context).arguments.get(self.name, MISSING)
        if value is MISSING:
            return self.name, MISSING
        return self.name, msgspec.convert(value, self.typ, strict=False)


class QueryParamResolver(Resolver):
    """Resolver for query parameters."""

    location: typing.ClassVar[str] = "query"

    async def resolve(self, ctx: Context | None = None) -> tuple[str, typing.Any]:
        value = (ctx or self.context).query_params.get(self.name, MISSING)
        if value is MISSING:
            return self.name, MISSING
        return self.name, msgspec.convert(value, self.typ, strict=False)


class PathParamResolver(Resolver):
    """Resolver for path parameters."""

    location: typing.ClassVar[str] = "path"

    async def resolve(self, ctx: Context | None = None) -> tuple[str, typing.Any]:
        value = (ctx or self.context).path_params.get(self.name, MISSING)
        if value is MISSING:
            return self.name, MISSING
        return self.name, msgspec.convert(value, self.typ, strict=False)


class RequestBodyResolver(Resolver):
    """Resolver for request body parameters."""

    location: typing.ClassVar[str] = "body"

    async def resolve(self, ctx: Context | None = None) -> tuple[str, typing.Any]:
        body = await (ctx or self.context).body()

        is_struct = isinstance(self.typ, type) and (
            issubclass(self.typ, msgspec.Struct) or isinstance(self.typ, MetaObject)
        )
        if not is_struct:
            return self.name, msgspec.json.decode(body, type=self.typ, strict=True)

        try:
            return self.name, msgspec.json.decode(body, type=self.typ, strict=True)
        except msgspec.ValidationError:
            pass
        except msgspec.DecodeError as exc:
            raise ValidationException(detail=str(exc)) from exc

        try:
            raw = msgspec.json.decode(body)
        except msgspec.DecodeError as exc:  # pragma: no cover - unreachable
            # Typed decoding parses the JSON before checking types, so malformed
            # input already raised DecodeError above; this only guards the
            # invariant.
            raise ValidationException(detail=str(exc)) from exc

        if not isinstance(raw, dict):
            raise ValidationException(detail="Request body must be a JSON object")

        field_errors: list[FieldError] = []
        params: dict[str, typing.Any] = {}

        struct_type = typing.cast(type[msgspec.Struct], self.typ)
        for field in msgspec.structs.fields(struct_type):
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

    async def resolve(self, ctx: Context | None = None) -> tuple[str, typing.Any]:
        value = (ctx or self.context).header(self.name)
        if value is None:
            return self.name, MISSING
        return self.name, msgspec.convert(value, self.typ, strict=False)


class CookieResolver(Resolver):
    """Resolver for cookie."""

    location: typing.ClassVar[str] = "cookie"

    async def resolve(self, ctx: Context | None = None) -> tuple[str, typing.Any]:
        # Context.cookies has already normalised its keys; re-normalising them
        # here would be the same work twice.
        value = (ctx or self.context).cookies.get(self.name, MISSING)
        if value is MISSING:
            return self.name, MISSING
        return self.name, msgspec.convert(value, self.typ, strict=False)
