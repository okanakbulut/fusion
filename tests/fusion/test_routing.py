"""Tests for the TreeRouter — how fusion matches URLs to handlers.

Reading these tests gives a complete picture of how routing works:
path matching, parameter extraction, type conversion, and error responses.
"""

import pytest

from fusion import (
    Delete,
    Fusion,
    Get,
    Handler,
    Head,
    Injectable,
    Object,
    Options,
    Patch,
    PathParam,
    Post,
    Put,
    Request,
    Response,
    Route,
)
from fusion.router import MAX_PATH_DEPTH, TreeRouter
from fusion.testing import TestClient
from fusion.types import Method


class _Msg(Object):
    value: str


class _EchoHandler(Handler):
    async def handle(self, request: Request) -> Response[_Msg]:
        return Response(_Msg(value="ok"))


def _app(*routes):
    return Fusion(routes=list(routes))


@pytest.mark.asyncio
async def test_root_path_resolves():
    app = _app(Route("/", methods=["GET"], handler=_EchoHandler))

    async with TestClient(app) as c:
        r = await c.get("/")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_static_segment_matches():
    app = _app(Route("/hello", methods=["GET"], handler=_EchoHandler))

    async with TestClient(app) as c:
        assert (await c.get("/hello")).status_code == 200
        assert (await c.get("/world")).status_code == 404


@pytest.mark.asyncio
async def test_path_param_string():
    class Input(Injectable):
        slug: PathParam[str]

    class Output(Object):
        slug: str

    class SlugHandler(Handler):
        inp: Input

        async def handle(self, request: Request) -> Response[Output]:
            return Response(Output(slug=self.inp.slug))

    app = _app(Route("/posts/{slug}", methods=["GET"], handler=SlugHandler))

    async with TestClient(app) as c:
        r = await c.get("/posts/hello-world")
    assert r.status_code == 200
    assert r.json() == {"slug": "hello-world"}


@pytest.mark.asyncio
async def test_path_param_int():
    class Input(Injectable):
        id: PathParam[int]

    class Output(Object):
        id: int

    class IdHandler(Handler):
        inp: Input

        async def handle(self, request: Request) -> Response[Output]:
            return Response(Output(id=self.inp.id))

    app = _app(Route("/users/{id:int}", methods=["GET"], handler=IdHandler))

    async with TestClient(app) as c:
        r = await c.get("/users/42")
    assert r.status_code == 200
    assert r.json() == {"id": 42}


@pytest.mark.asyncio
async def test_path_param_int_rejects_non_integer():
    app = _app(Route("/users/{id:int}", methods=["GET"], handler=_EchoHandler))

    async with TestClient(app) as c:
        r = await c.get("/users/abc")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_path_param_uuid():
    class Input(Injectable):
        item_id: PathParam[str]

    class Output(Object):
        item_id: str

    class UuidHandler(Handler):
        inp: Input

        async def handle(self, request: Request) -> Response[Output]:
            return Response(Output(item_id=self.inp.item_id))

    app = _app(Route("/items/{item_id:uuid}", methods=["GET"], handler=UuidHandler))

    async with TestClient(app) as c:
        assert (await c.get("/items/550e8400-e29b-41d4-a716-446655440000")).status_code == 200
        assert (await c.get("/items/not-a-uuid")).status_code == 404


@pytest.mark.asyncio
async def test_multiple_path_params():
    class Input(Injectable):
        user_id: PathParam[int]
        post_id: PathParam[int]

    class Output(Object):
        user_id: int
        post_id: int

    class PostHandler(Handler):
        inp: Input

        async def handle(self, request: Request) -> Response[Output]:
            return Response(Output(user_id=self.inp.user_id, post_id=self.inp.post_id))

    app = _app(
        Route("/users/{user_id:int}/posts/{post_id:int}", methods=["GET"], handler=PostHandler)
    )

    async with TestClient(app) as c:
        r = await c.get("/users/1/posts/99")
    assert r.status_code == 200
    assert r.json() == {"user_id": 1, "post_id": 99}


@pytest.mark.asyncio
async def test_method_not_allowed():
    app = _app(Route("/resource", methods=["GET"], handler=_EchoHandler))

    async with TestClient(app) as c:
        r = await c.post("/resource")
    assert r.status_code == 405
    assert r.headers["content-type"] == "application/problem+json"


@pytest.mark.asyncio
async def test_path_exceeding_max_depth_returns_404():
    from fusion.router import MAX_PATH_DEPTH

    app = _app(Route("/a", methods=["GET"], handler=_EchoHandler))
    deep = "/".join(["x"] * (MAX_PATH_DEPTH + 1))

    async with TestClient(app) as c:
        r = await c.get(f"/{deep}")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_route_shorthand_functions():
    class Out(Object):
        method: str

    class MethodHandler(Handler):
        async def handle(self, request: Request) -> Response[Out]:
            return Response(Out(method=request.scope.get("method", "")))

    app = Fusion(
        routes=[
            Get("/get", handler=MethodHandler),
            Post("/post", handler=MethodHandler),
            Put("/put", handler=MethodHandler),
            Delete("/delete", handler=MethodHandler),
            Patch("/patch", handler=MethodHandler),
            Options("/options", handler=MethodHandler),
            Head("/head", handler=MethodHandler),
        ]
    )

    async with TestClient(app) as c:
        assert (await c.get("/get")).status_code == 200
        assert (await c.post("/post")).status_code == 200
        assert (await c.put("/put")).status_code == 200
        assert (await c.delete("/delete")).status_code == 200
        assert (await c.patch("/patch")).status_code == 200
        assert (await c.options("/options")).status_code == 200
        assert (await c.head("/head")).status_code == 200


def test_route_without_method_raises():
    with pytest.raises(ValueError, match="Either"):

        class EchoHandler(Handler):
            async def handle(self, request: Request) -> Response[Object]:
                return Response(None)

        Route("/echo", handler=EchoHandler)


def test_route_with_method_enum_directly():
    from fusion.types import Method

    class EchoHandler(Handler):
        async def handle(self, request: Request) -> Response[Object]:
            return Response(None)

    r = Route("/echo", handler=EchoHandler, method=Method.GET)
    assert r.method == Method.GET


def test_route_with_non_injectable_handler():
    from fusion.protocols import HttpRequest, HttpResponse

    class PlainHandler:
        async def handle(self, request: HttpRequest) -> HttpResponse:
            return None  # type: ignore[return-value]

    r = Route("/plain", handler=PlainHandler, method="GET")
    assert r.path == "/plain"


@pytest.mark.asyncio
async def test_non_injectable_handler_is_invoked_through_wrapper():
    """HandlerWrapper instantiates a plain handler per call and delegates to it."""
    from fusion.protocols import HttpRequest, HttpResponse

    class PlainHandler:
        async def handle(self, request: HttpRequest) -> HttpResponse:
            return Response(None)

    r = Route("/plain", handler=PlainHandler, method="GET")
    result = await r.handle(None)  # type: ignore[arg-type]
    assert isinstance(result, Response)


@pytest.mark.asyncio
async def test_deeply_nested_static_path():
    app = _app(Route("/a/b/c/d", methods=["GET"], handler=_EchoHandler))

    async with TestClient(app) as c:
        assert (await c.get("/a/b/c/d")).status_code == 200
        # /a/b/c node exists in the tree but has no routes registered → 405
        assert (await c.get("/a/b/c")).status_code == 405
        # /a/b/x is not in the tree at all → 404
        assert (await c.get("/a/b/x")).status_code == 404


# ---------------------------------------------------------------------------
# TreeRouter.resolve() — pure routing without ASGI
# ---------------------------------------------------------------------------


def test_resolve_static_path_returns_route():
    router = TreeRouter([Get("/hello", handler=_EchoHandler)])
    result = router.resolve("/hello", Method.GET)
    assert result is not None
    route, path_params = result
    assert path_params == {}
    assert route.path == "/hello"
    assert route.method == Method.GET


def test_resolve_returns_path_params():
    router = TreeRouter([Get("/users/{id:int}", handler=_EchoHandler)])
    result = router.resolve("/users/42", Method.GET)
    assert result is not None
    _, path_params = result
    assert path_params == {"id": "42"}


def test_resolve_multiple_path_params():
    router = TreeRouter([Get("/a/{x}/b/{y}", handler=_EchoHandler)])
    result = router.resolve("/a/foo/b/bar", Method.GET)
    assert result is not None
    _, path_params = result
    assert path_params == {"x": "foo", "y": "bar"}


def test_resolve_unknown_path_returns_none():
    router = TreeRouter([Get("/hello", handler=_EchoHandler)])
    assert router.resolve("/nope", Method.GET) is None


def test_resolve_wrong_method_returns_none():
    router = TreeRouter([Get("/hello", handler=_EchoHandler)])
    assert router.resolve("/hello", Method.POST) is None


def test_resolve_path_node_exists_but_no_method_returns_none():
    router = TreeRouter([Get("/a/b/c/d", handler=_EchoHandler)])
    # node /a/b/c exists in the tree but has no route for GET
    assert router.resolve("/a/b/c", Method.GET) is None


def test_resolve_exceeds_max_depth_returns_none():
    router = TreeRouter([Get("/a", handler=_EchoHandler)])
    deep = "/" + "/".join(["x"] * (MAX_PATH_DEPTH + 1))
    assert router.resolve(deep, Method.GET) is None


def test_resolve_root_path():
    router = TreeRouter([Get("/", handler=_EchoHandler)])
    result = router.resolve("/", Method.GET)
    assert result is not None


def test_resolve_uuid_param():
    router = TreeRouter([Get("/items/{item_id:uuid}", handler=_EchoHandler)])
    valid_uuid = "550e8400-e29b-41d4-a716-446655440000"
    assert router.resolve(f"/items/{valid_uuid}", Method.GET) is not None
    assert router.resolve("/items/not-a-uuid", Method.GET) is None


# ---------------------------------------------------------------------------
# pk constraint — regex validation + optional transform
# ---------------------------------------------------------------------------


def test_pk_pattern_accepts_valid_public_key():
    """A segment matching `prefix-XXXX` (4+ suffix chars) is accepted."""
    from fusion.router import type_patterns

    pk_pattern = type_patterns["pk"]
    assert pk_pattern.match("matter-AbCd")
    assert pk_pattern.match("2024-0001-XXXX")
    assert pk_pattern.match("org-abc123")


def test_pk_pattern_rejects_no_suffix():
    """A segment with no hyphen-separated suffix is rejected."""
    from fusion.router import type_patterns

    pk_pattern = type_patterns["pk"]
    assert pk_pattern.match("nomatch") is None
    assert pk_pattern.match("abc") is None


def test_pk_pattern_rejects_short_suffix():
    """A suffix shorter than 4 chars is rejected."""
    from fusion.router import type_patterns

    pk_pattern = type_patterns["pk"]
    assert pk_pattern.match("matter-XY") is None
    assert pk_pattern.match("matter-XYZ") is None


def test_resolve_pk_param_returns_raw_string_without_transform():
    """Without a registered transform, pk resolves to the raw string."""
    router = TreeRouter([Get("/matters/{key:pk}", handler=_EchoHandler)])
    result = router.resolve("/matters/2024-0001-ABCD", Method.GET)
    assert result is not None
    _, path_params = result
    assert path_params == {"key": "2024-0001-ABCD"}


def test_resolve_pk_param_rejects_invalid_format():
    """A segment that doesn't match the pk pattern returns 404."""
    router = TreeRouter([Get("/matters/{key:pk}", handler=_EchoHandler)])
    assert router.resolve("/matters/notakey", Method.GET) is None
    assert router.resolve("/matters/short-X", Method.GET) is None


def test_resolve_pk_with_registered_transform_applies_it():
    """A registered type_transforms['pk'] is applied to the matched segment."""
    from fusion import router as router_module

    original = router_module.type_transforms.copy()
    try:
        # Register a simple test transform: extract the suffix after the last '-'
        router_module.type_transforms["pk"] = lambda s: int(s.split("-")[-1])
        r = TreeRouter([Get("/matters/{key:pk}", handler=_EchoHandler)])
        result = r.resolve("/matters/matter-1234", Method.GET)
        assert result is not None
        _, path_params = result
        assert path_params == {"key": 1234}
    finally:
        router_module.type_transforms.clear()
        router_module.type_transforms.update(original)


def test_resolve_pk_transform_raises_returns_404():
    """If the registered transform raises, the segment is treated as no match."""
    from fusion import router as router_module

    original = router_module.type_transforms.copy()
    try:
        router_module.type_transforms["pk"] = lambda s: (_ for _ in ()).throw(ValueError("bad"))
        r = TreeRouter([Get("/matters/{key:pk}", handler=_EchoHandler)])
        result = r.resolve("/matters/matter-ABCD", Method.GET)
        assert result is None
    finally:
        router_module.type_transforms.clear()
        router_module.type_transforms.update(original)
