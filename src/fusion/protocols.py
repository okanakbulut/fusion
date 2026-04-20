import typing

from typedprotocol import TypedProtocol

from .types import Match, Method, Receive, Scope, Send


class Injectable(TypedProtocol):
    # def __new__(cls, *args: typing.Any, **kwargs: typing.Any) -> typing.Self:
    #     return cls.instance(*args, **kwargs)

    @classmethod
    async def instance(cls) -> typing.Self: ...  # pragma: no cover


class RenderResult(TypedProtocol):
    body: bytes | None
    headers: list[tuple[bytes, bytes]] | None
    cookies: list[tuple[bytes, bytes]] | None


class Renderer(TypedProtocol):
    attr_name: str
    attr_type: type

    def render(self, obj: typing.Any) -> RenderResult: ...  # pragma: no cover


class HttpConnection(TypedProtocol):
    scope: Scope
    receive: Receive
    send: Send


class HttpResponse(TypedProtocol):
    async def __call__(  # pragma: no cover
        self, scope: Scope, receive: Receive, send: Send
    ) -> None: ...


class HttpRequest(Injectable):
    scope: Scope
    receive: Receive
    send: Send


class AnnotationResolver[T](TypedProtocol):
    """Resolver protocol for dependency resolution."""

    name: str
    typ: type[T]

    async def resolve(self) -> tuple[str, T | None]: ...  # pragma: no cover


class HttpHandler[TRequest: HttpRequest, TResponse: HttpResponse](TypedProtocol):
    async def handle(self, request: TRequest) -> TResponse: ...  # pragma: no cover


class HttpMiddleware[TRequest: HttpRequest, TResponse: HttpResponse](
    HttpHandler[TRequest, TResponse]
):
    app: HttpHandler[TRequest, TResponse]


class HttpRoute[TRequest: HttpRequest, TResponse: HttpResponse](HttpHandler[TRequest, TResponse]):
    path: str
    method: Method
    handler: HttpHandler[TRequest, TResponse]

    def match(self, path: str, method: str) -> Match: ...  # pragma: no cover
