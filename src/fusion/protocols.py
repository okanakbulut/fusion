import typing

from typedprotocol import TypedProtocol

from .types import Match, Method, Receive, Scope, Send


class Injectable(TypedProtocol):
    # def __new__(cls, *args: typing.Any, **kwargs: typing.Any) -> typing.Self:
    #     return cls.instance(*args, **kwargs)

    @classmethod
    async def instance(cls) -> typing.Self: ...


class RenderResult(TypedProtocol):
    body: typing.Optional[bytes]
    headers: typing.Optional[list[tuple[bytes, bytes]]]
    cookies: typing.Optional[list[tuple[bytes, bytes]]]


class Renderer(TypedProtocol):
    attr_name: str
    attr_type: type

    def render(self, obj: typing.Any) -> RenderResult: ...


class HttpConnection(TypedProtocol):
    scope: Scope
    receive: Receive
    send: Send


class HttpResponse(TypedProtocol):
    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None: ...


class HttpRequest(Injectable):
    scope: Scope
    receive: Receive
    send: Send


class AnnotationResolver[T](TypedProtocol):
    """Resolver protocol for dependency resolution."""

    name: str
    typ: typing.Type[T]

    async def resolve(self) -> tuple[str, T | None]: ...


class HttpHandler[TRequest: HttpRequest, TResponse: HttpResponse](TypedProtocol):
    async def handle(self, request: TRequest) -> TResponse: ...


class HttpMiddleware[TRequest: HttpRequest, TResponse: HttpResponse](
    HttpHandler[TRequest, TResponse]
):
    app: HttpHandler[TRequest, TResponse]


class HttpRoute[TRequest: HttpRequest, TResponse: HttpResponse](HttpHandler[TRequest, TResponse]):
    path: str
    method: Method
    handler: HttpHandler[TRequest, TResponse]

    def match(self, path: str, method: str) -> Match: ...
