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
from fusion.testing import TestClient


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
async def test_deeply_nested_static_path():
    app = _app(Route("/a/b/c/d", methods=["GET"], handler=_EchoHandler))

    async with TestClient(app) as c:
        assert (await c.get("/a/b/c/d")).status_code == 200
        # /a/b/c node exists in the tree but has no routes registered → 405
        assert (await c.get("/a/b/c")).status_code == 405
        # /a/b/x is not in the tree at all → 404
        assert (await c.get("/a/b/x")).status_code == 404
