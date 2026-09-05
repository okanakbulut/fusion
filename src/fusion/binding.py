"""Turning a function signature into a resolver table, and running it.

The framework never inspects a handler at call time.  ``Signature.of`` runs once
when a route or tool is registered; ``bind`` is the only per-call work.
"""

import enum
import inspect
import types as _types
import typing

import msgspec

from .context import current
from .exceptions import ValidationException
from .resolvers import MISSING, Resolver, build_resolvers
from .responses import FieldError, Problem
from .security import ROLES_ATTRIBUTE
from .types import Transport


class Signature:
    """Everything the framework needs to know about a handler, computed once."""

    __slots__ = (
        "doc",
        "func",
        "is_asyncgen",
        "name",
        "required",
        "resolvers",
        "return_type",
        "roles",
        "transport",
    )

    func: typing.Callable[..., typing.Any]
    resolvers: dict[str, Resolver]
    return_type: typing.Any
    name: str
    doc: str | None
    transport: Transport
    is_asyncgen: bool
    roles: frozenset[str]
    """Roles ``@requires`` declared on this function.  Empty for anything that
    did not ask for authorization."""
    required: frozenset[str]
    """Parameters with no Python default.  A resolver returning MISSING for one
    of these is a validation error, not a fall-through to the default."""

    def __init__(
        self,
        func: typing.Callable[..., typing.Any],
        *,
        resolvers: dict[str, Resolver],
        return_type: typing.Any,
        transport: Transport,
        is_asyncgen: bool,
    ) -> None:
        self.func = func
        self.resolvers = resolvers
        self.return_type = return_type
        self.transport = transport
        self.is_asyncgen = is_asyncgen
        self.name = getattr(func, "__name__", repr(func))
        self.roles = frozenset(getattr(func, ROLES_ATTRIBUTE, ()))
        self.doc = inspect.getdoc(func)
        parameters = inspect.signature(func).parameters
        self.required = frozenset(
            name for name in resolvers if parameters[name].default is inspect.Parameter.empty
        )

    @classmethod
    def of(cls, func: typing.Callable[..., typing.Any], *, transport: Transport) -> Signature:
        """Inspect ``func`` for use under ``transport``.

        Raises ``TypeError`` for anything that cannot serve as a handler, so a
        mistake surfaces when the application is constructed rather than on the
        first request.
        """
        is_asyncgen = inspect.isasyncgenfunction(func)
        if not is_asyncgen and not inspect.iscoroutinefunction(func):
            raise TypeError(
                f"Handler {getattr(func, '__name__', func)!r} must be defined with 'async def'."
            )

        hints = typing.get_type_hints(func, include_extras=True)
        allowed = frozenset({transport, Transport.ANY})
        resolvers = build_resolvers(
            hints, allowed=allowed, owner=getattr(func, "__name__", repr(func))
        )

        return cls(
            func,
            resolvers=resolvers,
            return_type=hints.get("return", typing.Any),
            transport=transport,
            is_asyncgen=is_asyncgen,
        )

    @property
    def credentials(self) -> tuple[Resolver, ...]:
        """Resolvers carrying a credential, in declaration order."""
        return tuple(r for r in self.resolvers.values() if r.location == "security")

    @property
    def summary(self) -> str | None:
        """First line of the docstring - OpenAPI ``summary``, MCP tool description."""
        if not self.doc:
            return None
        return self.doc.split("\n\n", 1)[0].strip() or None

    @property
    def description(self) -> str | None:
        """Docstring beyond the first paragraph, if any."""
        if not self.doc:
            return None
        parts = self.doc.split("\n\n", 1)
        return parts[1].strip() if len(parts) > 1 and parts[1].strip() else None

    def __repr__(self) -> str:
        return f"<Signature {self.name} transport={self.transport.value}>"


async def bind(signature: Signature) -> dict[str, typing.Any]:
    """Resolve every parameter of ``signature`` from the active context.

    Failures are accumulated rather than raised at the first bad parameter, so
    one response can report every problem with the call at once.  A resolver
    returning ``MISSING`` contributes no argument, letting the parameter's own
    default apply.
    """
    params: dict[str, typing.Any] = {}
    errors: list[FieldError] = []
    ctx = current()

    for resolver in signature.resolvers.values():
        try:
            name, value = await resolver.resolve(ctx)
            if value is not MISSING:
                params[name] = value
            elif name in signature.required:
                errors.append(
                    FieldError(
                        field=name,
                        location=resolver.location,
                        message="Missing required value",
                    )
                )
        except ValidationException as exc:
            if exc.errors:
                errors.extend(exc.errors)
            elif exc.detail:
                errors.append(
                    FieldError(field=resolver.name, location=resolver.location, message=exc.detail)
                )
        except msgspec.ValidationError as exc:
            errors.append(
                FieldError(field=resolver.name, location=resolver.location, message=str(exc))
            )

    if errors:
        raise ValidationException(errors=errors)

    return params


def _is_union(annotation: typing.Any) -> bool:
    return typing.get_origin(annotation) in (_types.UnionType, typing.Union)


def union_arms(annotation: typing.Any) -> tuple[typing.Any, ...]:
    """Split a union into its arms, leaving a non-union annotation intact.

    Guarding on union-ness matters: ``get_args`` of a *non*-union generic such as
    ``Response[User]`` returns its type parameter ``(User,)``, not its arms, so a
    naive ``get_args(...) or (annotation,)`` silently misreads every handler with
    a single return type.
    """
    if _is_union(annotation):
        return typing.get_args(annotation)
    return (annotation,)


def yield_type(annotation: typing.Any) -> typing.Any:
    """The type an async generator yields, or ``Any`` when it never said."""
    args = typing.get_args(annotation)
    return args[0] if args else typing.Any


def status_of(arm: typing.Any) -> int | None:
    """The status code a return arm stands for, or None if it is not a response."""
    status = getattr(typing.get_origin(arm) or arm, "status_code", None)
    return status if isinstance(status, int) else None


def response_arms(annotation: typing.Any) -> list[tuple[int, typing.Any]]:
    """Map a return annotation onto ``(status, content type or None)`` pairs."""
    if annotation is typing.Any or annotation is None:
        return []

    found: list[tuple[int, typing.Any]] = []
    for arm in union_arms(annotation):
        status = status_of(arm)
        if status is None:
            continue
        origin = typing.get_origin(arm) or arm
        if isinstance(origin, type) and issubclass(origin, Problem):
            found.append((status, None))
            continue
        args = typing.get_args(arm)
        found.append((status, args[0] if args else None))
    return found


def stream_arms(annotation: typing.Any) -> list[tuple[int, typing.Any, bool]]:
    """Map an async generator's yield annotation onto response arms.

    Returns ``(status, content type, is_stream)``.
    """
    args = typing.get_args(annotation)
    if not args:
        return []

    found: list[tuple[int, typing.Any, bool]] = []
    for arm in union_arms(args[0]):
        origin = typing.get_origin(arm) or arm
        if isinstance(origin, type) and issubclass(origin, Problem):
            found.append((origin.status_code, None, False))
            continue
        # ``Event[Tick]`` carries its payload as a type argument; a bare object is
        # yielded as a data-only event and is its own payload.
        inner = typing.get_args(arm)
        found.append((200, inner[0] if inner else arm, True))
    return found


class Returns(enum.StrEnum):
    """What a function may hand back, by the role it plays.

    A handler and a tool must answer; a middleware may decline to, which is what
    ``None`` means there.
    """

    HANDLER = "handler"
    MIDDLEWARE = "middleware"
    TOOL = "tool"


_EXAMPLES: dict[tuple[Returns, bool], str] = {
    (Returns.HANDLER, False): "Response[User] | NotFound",
    (Returns.HANDLER, True): "AsyncIterator[Event[Tick] | NotFound]",
    (Returns.MIDDLEWARE, False): "Unauthorized | None",
    (Returns.MIDDLEWARE, True): "AsyncIterator[NotFound | None]",
    (Returns.TOOL, False): "Response[User]",
    (Returns.TOOL, True): "",
}


def check_returns(signature: Signature, kind: Returns) -> None:
    """Reject a signature whose responses could not be documented.

    The generated document has exactly one source for an operation's responses -
    this annotation - so anything unreadable here is a hole in the spec rather
    than a matter of style.  Checking at registration is what makes the document
    complete by construction, and keeps the annotation something a type checker
    can inspect rather than decoration.
    """
    streams = signature.is_asyncgen
    subject = f"{kind.value.title()} {signature.name!r}"
    example = _EXAMPLES[(kind, streams)]
    verb = "yield" if streams else "return"

    annotation = signature.return_type
    if streams:
        if not typing.get_args(annotation):
            raise TypeError(f"{subject} must declare what it yields, e.g. '-> {example}'.")
        annotation = yield_type(annotation)

    arms = union_arms(annotation)
    if any(arm is typing.Any for arm in arms):
        raise TypeError(
            f"{subject} is annotated to {verb} Any, which documents nothing. Name what it "
            f"can {verb}, e.g. '-> {example}'."
        )

    if streams and kind is Returns.HANDLER:
        # Every arm of a stream is documentable already: a problem answers before
        # the stream opens, anything else is the payload of a data event.
        return

    for arm in arms:
        # ``typing.AsyncIterator[None]`` normalises to NoneType at subscription;
        # ``collections.abc.AsyncIterator[None]`` keeps the literal None.
        if arm is None or arm is _types.NoneType:
            if kind is Returns.MIDDLEWARE:
                continue
            raise TypeError(
                f"{subject} may not {verb} None - a {kind.value} has to answer the request. "
                f"Annotate what it responds with, e.g. '-> {example}'."
            )
        if status_of(arm) is None:
            raise TypeError(
                f"{subject} is annotated to {verb} {arm!r}, which carries no status code, so "
                f"no response can be documented for it. Use a Response[...] or a Problem, "
                f"e.g. '-> {example}'."
            )
