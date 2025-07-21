import re
from enum import Enum

import msgspec

from .middleware import Middleware
from .protocols import HttpHandler, InjectableHandler
from .request import Request
from .responses import BadRequest, Error, InternalServerError, NotFound, Object
from .types import Response, Scope


class Match(Enum):
    NONE = 0
    PARTIAL = 1
    FULL = 2


def get_route_path(scope: Scope) -> str:
    root_path = scope.get("root_path", "")
    route_path = re.sub(r"^" + root_path, "", scope["path"])
    return route_path


class HandlerWrapper(Object):
    app: type[InjectableHandler]

    async def handle(self, request: Request) -> Response:
        """Handle ASGI requests."""
        handler = await self.app.instance()
        return await handler.handle(request)


class Route:
    def __init__(
        self,
        path: str,
        handler: type[InjectableHandler],
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

    async def handle(self, request: Request) -> Response:
        """Handle ASGI requests."""
        return await self.handler.handle(request)


class Router(Object):
    routes: list[Route]

    async def handle(self, request: Request) -> Response:
        """Handle ASGI requests."""
        if request.scope["type"] != "http":
            raise RuntimeError("Fusion only supports HTTP requests for now.")

        for route in self.routes:
            match = route.matches(request.scope)
            if match == Match.FULL:
                try:
                    return await route.handle(request)
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
        # If no route matched, return a 404 response
        return NotFound(
            Error(
                code="not_found",
                message="The requested resource was not found.",
                details=[{"path": request.scope["path"]}],
            )
        )
