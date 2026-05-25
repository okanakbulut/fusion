import contextlib
import logging
import typing

from .exceptions import ValidationException
from .protocols import HttpResponse
from .responses import InternalServerError, MethodNotAllowed, NotFound, ValidationProblem
from .route import Route
from .router import TreeRouter
from .types import Lifespan, Method, Receive, Scope, Send

_logger = logging.getLogger(__name__)


@contextlib.asynccontextmanager
async def default_lifespan(app: typing.Any) -> typing.AsyncIterator[dict[str, typing.Any]]:
    yield dict()


class Fusion:
    """Fusion is a lightweight ASGI framework for building web applications."""

    __slots__ = ("lifespan", "router")

    def __init__(
        self,
        *,
        routes: list[Route],
        lifespan: Lifespan = default_lifespan,
        # middlewares: list[Middleware] | None = None,
    ) -> None:
        self.router = TreeRouter(routes=routes)
        self.lifespan = lifespan
        # if middlewares is not None:
        #     for middleware in reversed(middlewares):
        #         self.router = middleware.cls(self.router, *middleware.args, **middleware.kwargs)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Handle ASGI requests."""
        if "app" not in scope:
            scope["app"] = self

        if scope["type"] == "lifespan":
            return await self.handle_lifespan(scope, receive, send)

        return await self.handle_http(scope, receive, send)

    async def handle_http(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Resolve the route and execute the handler."""
        from .context import Context

        async with Context(scope, receive, send) as ctx:
            result = self.router.resolve(ctx.path, Method(ctx.method))

            if result is None:
                if self.router._has_path(ctx.path):
                    return await MethodNotAllowed()(scope, receive, send)
                return await NotFound(detail="Route not found")(scope, receive, send)

            route, path_params = result
            scope["path_params"] = path_params
            try:
                request_class = route.get_request_class()
                request = await request_class.instance()
                response = await route.handle(request)
            except ValidationException as exc:
                response = ValidationProblem(errors=exc.errors, detail=exc.detail)
            except Exception:
                _logger.exception("Unhandled exception in route handler")
                response = InternalServerError()
            return await response(scope, receive, send)

    async def dispatch(
        self,
        path: str,
        method: Method | str,
        params: dict[str, str] | None = None,
    ) -> HttpResponse | None:
        """Execute a route handler programmatically within the current request context.

        Resolves path (extracting path params via the router), temporarily replaces
        the active context with a sub-context carrying those params, and calls the
        handler. Returns the handler's response, or None when no route matches.
        """
        from urllib.parse import urlencode

        from .context import Context, context

        result = self.resolve(path, method)
        if result is None:
            return None

        route, path_params = result
        current_ctx = context.get()

        sub_scope = dict(current_ctx.scope)
        sub_scope["path_params"] = path_params
        sub_scope["query_string"] = urlencode(params or {}).encode()

        async def _noop_receive() -> dict:  # type: ignore[type-arg]
            return {"type": "http.disconnect"}

        sub = Context(sub_scope, _noop_receive, current_ctx.send)
        token = context.set(sub)
        try:
            request_class = route.get_request_class()
            request = await request_class.instance()
            return typing.cast("HttpResponse", await route.handle(request))
        finally:
            context.reset(token)

    def resolve(
        self, path: str, method: Method | str
    ) -> tuple[Route, dict[str, typing.Any]] | None:
        """Return (route, path_params) for the given path and method, or None."""
        if isinstance(method, str):
            method = Method(method.upper())
        return self.router.resolve(path, method)

    async def handle_lifespan(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Handle lifespan events."""
        message = await receive()
        if message["type"] == "lifespan.startup":
            app = scope.get("app")
            startup_complete = False
            try:
                async with self.lifespan(app) as state:
                    if state is not None:
                        if not isinstance(state, dict):
                            raise TypeError(
                                f"Lifespan must yield a dict, got {type(state).__name__}"
                            )
                        scope.setdefault("state", {})
                        scope["state"].update(state)
                    startup_complete = True
                    await send({"type": "lifespan.startup.complete"})
                    while True:
                        message = await receive()
                        if message["type"] == "lifespan.shutdown":
                            break
                await send({"type": "lifespan.shutdown.complete"})
            except Exception as exc:
                if not startup_complete:
                    await send({"type": "lifespan.startup.failed", "message": str(exc)})
                raise
