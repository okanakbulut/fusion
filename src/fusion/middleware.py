import typing

from .object import Object
from .protocols import HttpHandler, HttpMiddleware, HttpRequest, HttpResponse


class BaseMiddleware(Object):
    app: HttpHandler

    async def handle(self, request: HttpRequest) -> HttpResponse:
        """Handle ASGI requests."""
        return await self.app.handle(request)


P = typing.ParamSpec("P")


class Middleware:
    __slots__ = ("args", "cls", "kwargs")

    def __init__(self, cls: type[HttpMiddleware], *args: P.args, **kwargs: P.kwargs) -> None:
        self.cls = cls
        self.args = args
        self.kwargs = kwargs
