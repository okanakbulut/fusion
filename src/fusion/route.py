import typing

from .middleware import Middleware
from .object import Object
from .protocols import HttpHandler, HttpRequest, HttpResponse, Injectable, InjectableHandler
from .types import Method


class HandlerWrapper(Object):
    Handler: type[HttpHandler[typing.Any, typing.Any]]

    async def handle(self, request: HttpRequest) -> HttpResponse:
        """Handle ASGI requests."""
        handler: typing.Any = self.Handler()
        return await handler.handle(request)


class InjectableHandlerWrapper(HandlerWrapper):
    async def handle(self, request: HttpRequest) -> HttpResponse:
        """Handle ASGI requests."""
        inj = typing.cast(type[InjectableHandler[typing.Any, typing.Any]], self.Handler)
        handler = await inj.instance()
        return typing.cast(HttpResponse, await handler.handle(request))


class Route[TRequest: HttpRequest, TResponse: HttpResponse]:
    __slots__ = ("_request_class", "handler", "method", "path")

    path: str
    method: Method
    handler: HttpHandler[TRequest, TResponse]
    _request_class: type[TRequest]

    def __init__(
        self,
        path: str,
        handler: type[HttpHandler[TRequest, TResponse]],
        method: Method | str | None = None,
        methods: list[Method | str] | None = None,
        middlewares: list[Middleware] | None = None,
    ) -> None:
        if methods:
            selected_method = methods[0]
        elif method is not None:
            selected_method = method
        else:
            raise ValueError("Either 'method' or 'methods' must be provided")

        if isinstance(selected_method, str):
            selected_method = Method(selected_method.upper())

        self.path = path
        self.method = selected_method
        self._request_class = typing.get_type_hints(handler.handle).get("request", HttpRequest)
        # TypedProtocol supports runtime subclass checks, but it is not a typing.Protocol
        # that a type checker can treat as @runtime_checkable.
        injectable = typing.cast(type, Injectable)
        wrapper_cls = (
            InjectableHandlerWrapper if issubclass(handler, injectable) else HandlerWrapper
        )
        self.handler = typing.cast("HttpHandler[TRequest, TResponse]", wrapper_cls(Handler=handler))
        for middleware in reversed(middlewares or []):
            # middleware.cls is a TypedProtocol subclass, so its __init__ signature is opaque.
            middleware_cls = typing.cast(typing.Callable[..., typing.Any], middleware.cls)
            self.handler = middleware_cls(self.handler, *middleware.args, **middleware.kwargs)

    async def handle(self, request: TRequest) -> TResponse:
        """Handle ASGI requests."""
        return await self.handler.handle(request)

    def get_request_class(self) -> type[TRequest]:
        return self._request_class


# shorthand for route creation


def Get(
    path: str, handler: type[HttpHandler], middlewares: list[Middleware] | None = None
) -> Route:
    return Route(path=path, handler=handler, method=Method.GET, middlewares=middlewares)


def Post(
    path: str, handler: type[HttpHandler], middlewares: list[Middleware] | None = None
) -> Route:
    return Route(path=path, handler=handler, method=Method.POST, middlewares=middlewares)


def Put(
    path: str, handler: type[HttpHandler], middlewares: list[Middleware] | None = None
) -> Route:
    return Route(path=path, handler=handler, method=Method.PUT, middlewares=middlewares)


def Delete(
    path: str, handler: type[HttpHandler], middlewares: list[Middleware] | None = None
) -> Route:
    return Route(path=path, handler=handler, method=Method.DELETE, middlewares=middlewares)


def Patch(
    path: str, handler: type[HttpHandler], middlewares: list[Middleware] | None = None
) -> Route:
    return Route(path=path, handler=handler, method=Method.PATCH, middlewares=middlewares)


def Options(
    path: str, handler: type[HttpHandler], middlewares: list[Middleware] | None = None
) -> Route:
    return Route(path=path, handler=handler, method=Method.OPTIONS, middlewares=middlewares)


def Head(
    path: str, handler: type[HttpHandler], middlewares: list[Middleware] | None = None
) -> Route:
    return Route(path=path, handler=handler, method=Method.HEAD, middlewares=middlewares)
