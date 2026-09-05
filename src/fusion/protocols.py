import typing

from typedprotocol import TypedProtocol

from .types import Receive, Scope, Send


class Injectable(TypedProtocol):
    @classmethod
    async def instance(cls) -> typing.Self: ...  # pragma: no cover


class HttpConnection(TypedProtocol):
    scope: Scope
    receive: Receive
    send: Send


class HttpResponse(TypedProtocol):
    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None: ...


class AnnotationResolver[T](TypedProtocol):
    """Resolver protocol for dependency resolution."""

    name: str
    typ: type[T]

    async def resolve(self) -> tuple[str, T | None]: ...  # pragma: no cover


class HttpHandler(TypedProtocol):
    """Anything that can terminate or forward a request, including middleware.

    Nothing is passed along the chain: every link reads the active context, so
    the request is ambient rather than threaded through.
    """

    async def handle(self) -> typing.Any: ...  # pragma: no cover


class Authorizer(TypedProtocol):
    """Decides whether the current request may run an operation.

    Called with the roles the operation declared.  Everything else - who is
    calling, what they hold, what is worth caching - is the implementation's
    business, read from the active context.
    """

    async def authorize(self, roles: frozenset[str]) -> bool: ...  # pragma: no cover
