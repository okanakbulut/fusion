import re
import typing
from enum import Enum

import msgspec

from fusion.middleware import Middleware
from fusion.request import request
from fusion.responses import BadRequest, Error, InternalServerError, NotFound, Object
from fusion.types import HttpHandler, InjectableHandler, R, Scope


class Match(Enum):
    NONE = 0
    PARTIAL = 1
    FULL = 2


def get_route_path(scope: Scope) -> str:
    root_path = scope.get("root_path", "")
    route_path = re.sub(r"^" + root_path, "", scope["path"])
    return route_path


class HandlerWrapper(Object, typing.Generic[R]):
    app: type[InjectableHandler[R]]

    async def handle(self, *args, **kwargs) -> R:
        """Handle ASGI requests."""
        handler = await self.app.instance()
        return await handler.handle(*args, **kwargs)


class Route(typing.Generic[R]):
    def __init__(
        self,
        path: str,
        handler: type[InjectableHandler[R]],
        methods: list[str] | None = None,
        middlewares: list[Middleware] | None = None,
    ) -> None:
        self.path = path
        self.handler: HttpHandler = HandlerWrapper(app=handler)
        self.methods = methods or ["GET", "HEAD"]
        for middleware in middlewares or []:
            self.handler = middleware.cls(self.handler, *middleware.args, **middleware.kwargs)

    def matches(self, scope: Scope) -> Match:
        if scope["type"] == "http" and self.path == get_route_path(scope):
            if scope["method"] not in self.methods:
                return Match.PARTIAL
            else:
                return Match.FULL
        return Match.NONE

    async def handle(self) -> R:
        """Handle ASGI requests."""
        return await self.handler.handle()


class Router(Object, typing.Generic[R]):
    routes: list[Route[R]]

    async def handle(self) -> R | NotFound | InternalServerError | BadRequest:
        """Handle ASGI requests."""
        req = request.get()
        if req.scope["type"] != "http":
            raise RuntimeError("Fusion only supports HTTP requests for now.")

        for route in self.routes:
            match = route.matches(req.scope)
            if match == Match.FULL:
                try:
                    await route.handle()
                except msgspec.ValidationError as e:
                    return BadRequest(
                        Error(
                            code="validation_error",
                            message="Validation error occurred.",
                            details=[{"error": str(e)}],
                        )
                    )
                except Exception as e:
                    return InternalServerError(
                        Error(
                            code="internal_server_error",
                            message="An internal server error occurred.",
                            details=[{"error": str(e)}],
                        )
                    )
                return await route.handle()

        # If no route matched, return a 404 response
        return NotFound(
            Error(
                code="not_found",
                message="The requested resource was not found.",
                details=[{"path": req.scope["path"]}],
            )
        )
