import contextlib
import logging
import typing
from urllib.parse import parse_qsl, urlencode

from .di import collect_factories, settle
from .exceptions import ProblemException, ValidationException
from .object import Object
from .protocols import Authorizer
from .responses import InternalServerError, MethodNotAllowed, NotFound, ValidationProblem
from .route import Route
from .router import TreeRouter
from .types import Lifespan, Message, Method, Receive, Scope, Send

_logger = logging.getLogger(__name__)

DEPTH_KEY = "fusion.subrequest_depth"
"""Scope key counting how deep a chain of sub-requests has gone."""


class CapturedResponse(Object, frozen=True):
    """One route's response, rendered but not sent.

    ``Fusion.execute`` hands this back rather than a live response: a batch
    envelope needs the status and the bytes, and there is no transport for a
    response to be written to.
    """

    status: int
    headers: dict[str, str]
    body: bytes


class _Capture:
    """Stands in for a transport, collecting what a response sends."""

    __slots__ = ("body", "headers", "status")

    def __init__(self) -> None:
        self.status = 500
        self.headers: list[tuple[bytes, bytes]] = []
        self.body: list[bytes] = []

    async def send(self, message: Message) -> None:
        if message["type"] == "http.response.start":
            self.status = message["status"]
            self.headers.extend(message.get("headers", ()))
        else:
            self.body.append(message.get("body", b""))

    def result(self) -> CapturedResponse:
        return CapturedResponse(
            status=self.status,
            headers={k.decode("latin-1"): v.decode("latin-1") for k, v in self.headers},
            body=b"".join(self.body),
        )


def _inherited_headers(
    parent: typing.Any, overrides: typing.Mapping[str, str] | None
) -> list[tuple[bytes, bytes]]:
    """The outer request's headers, with ``overrides`` merged over them by wire name.

    A tool context carries no headers at all, so this is also what lets a
    sub-request run from somewhere that never had any.
    """
    merged: dict[bytes, bytes] = {}
    if parent is not None:
        for key, value in parent.scope.get("headers", ()):
            merged[bytes(key).lower()] = bytes(value)
    for key, value in (overrides or {}).items():
        merged[key.lower().encode("latin-1")] = value.encode("latin-1")
    return list(merged.items())


def _query_string(inline: str, query: typing.Mapping[str, str] | None) -> bytes:
    if not query:
        return inline.encode("latin-1")
    merged = dict(parse_qsl(inline))
    merged.update(query)
    return urlencode(merged).encode("latin-1")


async def _no_body() -> Message:  # pragma: no cover - a refusal reads no body
    return {"type": "http.disconnect"}


@contextlib.asynccontextmanager
async def default_lifespan(app: typing.Any) -> typing.AsyncIterator[dict[str, typing.Any]]:
    yield dict()


class Fusion:
    """Fusion is a lightweight ASGI framework for building web applications."""

    __slots__ = ("_openapi", "authorizer", "lifespan", "router", "routes", "tools")

    def __init__(
        self,
        *,
        routes: list[Route] | None = None,
        tools: list[typing.Any] | None = None,
        factories: typing.Any = None,
        lifespan: Lifespan = default_lifespan,
        authorizer: Authorizer | None = None,
    ) -> None:
        self.routes = list(routes or [])
        self.authorizer = authorizer
        self._check_authorizer()
        # Kept alongside the router, which indexes routes by path and discards
        # the list; tools/list and OpenAPI both need a flat registry to walk.
        self.router = TreeRouter(routes=self.routes)
        self.tools = self._register_tools(tools or [])
        self._wire(factories)
        self.lifespan = lifespan
        self._openapi: dict[str, typing.Any] | None = None

    def _check_authorizer(self) -> None:
        """A declared role with nothing to check it is a silent hole."""
        if self.authorizer is not None:
            return
        for route in self.routes:
            if any(signature.roles for signature in (route.signature, *route.middlewares)):
                raise ValueError(
                    f"Route {route.path!r} declares required roles, but this application was "
                    f"built without an authorizer. Pass Fusion(authorizer=...) with something "
                    f"that implements 'async def authorize(roles) -> bool'."
                )

    def _wire(self, factories: typing.Any) -> None:
        """Settle every Inject[T] these routes and tools declare, or refuse to build.

        An ``Inject[T]`` with nothing to build it is the same silent hole
        ``_check_authorizer`` refuses one field over, and it used to surface as a
        500 on whichever request reached the route first.

        Nothing is kept.  The resolvers reached here come away holding the
        factory that answers them, so the application carries no dependency
        state and a request pays nothing to reach it.  Tools are wired as well,
        which is why this runs after they are registered: a tool's parameters
        are dependencies exactly as a route's are.
        """
        collected = collect_factories(factories)
        for route in self.routes:
            for signature in (route.signature, *route.middlewares):
                settle(signature.resolvers, collected, f"Route {route.path!r}")
        for name, tool in self.tools.items():
            settle(tool.signature.resolvers, collected, f"Tool {name!r}")

    def openapi(self, **kwargs: typing.Any) -> dict[str, typing.Any]:
        """Generate (and cache) this application's OpenAPI document."""
        if self._openapi is None or kwargs:
            from .openapi import generate

            document = generate(self.routes, **kwargs)
            if kwargs:
                return document
            self._openapi = document
        return self._openapi

    @staticmethod
    def _register_tools(tools: list[typing.Any]) -> dict[str, typing.Any]:
        from .tools import ToolDef

        registry: dict[str, ToolDef] = {}
        for tool in tools:
            definition = tool if isinstance(tool, ToolDef) else ToolDef(tool)
            if definition.name in registry:
                raise ValueError(f"Duplicate tool name: {definition.name!r}")
            registry[definition.name] = definition
        return registry

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Handle ASGI requests."""
        if "app" not in scope:
            scope["app"] = self

        if scope["type"] == "lifespan":
            return await self.handle_lifespan(scope, receive, send)

        return await self.handle_http(scope, receive, send)

    async def handle_http(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Resolve the route and execute the handler."""
        from .context import Context, context

        async with Context(scope, receive, send) as ctx:
            result = self.router.resolve(ctx.path, Method(ctx.method))

            if result is None:
                if self.router._has_path(ctx.path):
                    return await MethodNotAllowed()(scope, receive, send)
                return await NotFound(detail="Route not found")(scope, receive, send)

            route, path_params = result
            scope["path_params"] = path_params

            # A streaming handler does not run its body until the response is
            # awaited, so failures can surface after dispatch returns.  Track
            # whether the status line went out: once it has, no error response
            # can replace it.
            started = False

            async def tracked(message: Message) -> None:
                nonlocal started
                if message["type"] == "http.response.start":
                    started = True
                await send(message)

            try:
                response = await route.handle()
                await response(scope, receive, tracked)
            except ProblemException as exc:
                if started:
                    raise
                await exc.problem(scope, receive, send)
            except ValidationException as exc:
                if started:
                    raise
                problem = ValidationProblem(errors=exc.errors, detail=exc.detail)
                await problem(scope, receive, send)
            except Exception:
                _logger.exception("Unhandled exception in route handler")
                if not started:
                    await InternalServerError()(scope, receive, send)

    MAX_SUBREQUEST_DEPTH: typing.ClassVar[int] = 8
    """How far ``execute`` may nest before it refuses, so a route that calls
    itself fails fast instead of exhausting the stack."""

    async def execute(
        self,
        method: Method | str,
        path: str,
        *,
        headers: typing.Mapping[str, str] | None = None,
        query: typing.Mapping[str, str] | None = None,
        body: bytes = b"",
    ) -> CapturedResponse:
        """Run one route in-process and capture what it answered.

        The request is synthesised and driven down the ordinary ASGI path, so
        routing, middleware, authorization and validation behave exactly as they
        would for a real call - including the error mapping, which is why this
        never raises.  A route that blows up comes back as a captured 500, an
        unknown path as a 404: a batch of sub-requests cannot let one bad item
        take the whole envelope with it.

        Headers are inherited from the request in progress, when there is one,
        with ``headers`` merged over them by wire name.
        """
        from .context import context

        parent = context.get(None)
        target, _, inline = path.partition("?")
        scope: Scope = {
            "type": "http",
            "method": method.value if isinstance(method, Method) else method.upper(),
            "path": target,
            "query_string": _query_string(inline, query),
            "headers": _inherited_headers(parent, headers),
            "app": self,
            DEPTH_KEY: (parent.scope.get(DEPTH_KEY, 0) if parent is not None else 0) + 1,
        }

        if refusal := self._refuse(scope, target):
            capture = _Capture()
            await refusal(scope, _no_body, capture.send)
            return capture.result()

        delivered = False

        async def receive() -> Message:
            nonlocal delivered
            if delivered:
                return {"type": "http.disconnect"}
            delivered = True
            return {"type": "http.request", "body": body, "more_body": False}

        capture = _Capture()
        await self(scope, receive, capture.send)
        return capture.result()

    def _refuse(self, scope: Scope, target: str) -> InternalServerError | None:
        """Why this sub-request cannot be run at all, if it cannot."""
        if scope[DEPTH_KEY] > self.MAX_SUBREQUEST_DEPTH:
            return InternalServerError(
                detail=f"Sub-request depth exceeded {self.MAX_SUBREQUEST_DEPTH}: a route is "
                f"executing itself, directly or through another."
            )
        resolved = self.resolve(target, scope["method"])
        if resolved is not None and resolved[0].signature.is_asyncgen:
            return InternalServerError(
                detail=f"Route {target!r} streams its response, which cannot be captured. "
                f"Call a streaming route over HTTP instead."
            )
        return None

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
