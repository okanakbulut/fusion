import typing

from .protocols import HttpHandler, Request
from .responses import Object, Response

P = typing.ParamSpec("P")


class HttpMiddleware(HttpHandler, typing.Protocol[P]):
    def __init__(self, app: HttpHandler, *args: P.args, **kwargs: P.kwargs) -> None:
        ...


class Middleware:
    def __init__(self, cls: type[HttpMiddleware[P]], *args: P.args, **kwargs: P.kwargs) -> None:
        self.cls = cls
        self.args = args
        self.kwargs = kwargs


class BaseMiddleware(Object):
    app: HttpHandler

    async def handle(self, request: Request) -> Response:
        """Handle ASGI requests."""
        return await self.app.handle(request)
