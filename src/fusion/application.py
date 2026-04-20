import contextlib
import typing

from .route import Route
from .router import TreeRouter
from .types import Lifespan, Receive, Scope, Send


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

        return await self.router(scope, receive, send)

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
