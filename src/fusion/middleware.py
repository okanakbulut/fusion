import typing

from fusion.di import inject
from fusion.responses import Object, Response
from fusion.types import HttpHandler

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

    def __init_subclass__(cls, *args, **kwargs):
        cls.handle = inject(cls.handle)
        return super().__init_subclass__(*args, **kwargs)

    async def handle(self, *args, **kwargs) -> Response:
        """Handle ASGI requests."""
        return await self.app.handle()
