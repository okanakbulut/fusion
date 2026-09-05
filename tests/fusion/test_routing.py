"""Tests for the TreeRouter - how fusion matches URLs to handlers.

Reading these tests gives a complete picture of how routing works:
path matching, parameter extraction, type conversion, and error responses.
"""

import pytest

from fusion import (
    Delete,
    Fusion,
    Get,
    Head,
    Http,
    Object,
    Options,
    Patch,
    Post,
    Put,
    Request,
    Response,
    Route,
)
from fusion.annotations import FromContext
from fusion.router import MAX_PATH_DEPTH, TreeRouter
from fusion.testing import TestClient
from fusion.types import Method


class _Msg(Object):
    value: str


async def _echo() -> Response[_Msg]:
    return Response(_Msg(value="ok"))


def _app(*routes):
    return Fusion(routes=list(routes))


@pytest.mark.asyncio
async def test_root_path_resolves():
    app = _app(Route("/", methods=["GET"], handler=_echo))

    async with TestClient(app) as c:
        assert (await c.get("/")).status_code == 200


@pytest.mark.asyncio
async def test_static_segment_matches():
    app = _app(Route("/hello", methods=["GET"], handler=_echo))

    async with TestClient(app) as c:
        assert (await c.get("/hello")).status_code == 200
        assert (await c.get("/world")).status_code == 404


@pytest.mark.asyncio
async def test_path_param_string():
    class Output(Object):
        slug: str

    async def handler(slug: Http.Path[str]) -> Response[Output]:
        return Response(Output(slug=slug))

    app = _app(Route("/posts/{slug}", methods=["GET"], handler=handler))

    async with TestClient(app) as c:
        r = await c.get("/posts/hello-world")
    assert r.json() == {"slug": "hello-world"}


@pytest.mark.asyncio
async def test_path_param_int():
    class Output(Object):
        id: int

    async def handler(id: Http.Path[int]) -> Response[Output]:
        return Response(Output(id=id))

    app = _app(Route("/users/{id:int}", methods=["GET"], handler=handler))

    async with TestClient(app) as c:
        r = await c.get("/users/42")
    assert r.json() == {"id": 42}


@pytest.mark.asyncio
async def test_path_param_int_rejects_non_integer():
    app = _app(Route("/users/{id:int}", methods=["GET"], handler=_echo))

    async with TestClient(app) as c:
        assert (await c.get("/users/abc")).status_code == 404


@pytest.mark.asyncio
async def test_path_param_uuid():
    class Output(Object):
        item_id: str

    async def handler(item_id: Http.Path[str]) -> Response[Output]:
        return Response(Output(item_id=item_id))

    app = _app(Route("/items/{item_id:uuid}", methods=["GET"], handler=handler))

    async with TestClient(app) as c:
        assert (await c.get("/items/550e8400-e29b-41d4-a716-446655440000")).status_code == 200
        assert (await c.get("/items/not-a-uuid")).status_code == 404


@pytest.mark.asyncio
async def test_multiple_path_params():
    class Output(Object):
        user_id: int
        post_id: int

    async def handler(user_id: Http.Path[int], post_id: Http.Path[int]) -> Response[Output]:
        return Response(Output(user_id=user_id, post_id=post_id))

    app = _app(Route("/users/{user_id:int}/posts/{post_id:int}", methods=["GET"], handler=handler))

    async with TestClient(app) as c:
        r = await c.get("/users/1/posts/99")
    assert r.json() == {"user_id": 1, "post_id": 99}


@pytest.mark.asyncio
async def test_method_not_allowed():
    app = _app(Route("/resource", methods=["GET"], handler=_echo))

    async with TestClient(app) as c:
        r = await c.post("/resource")
    assert r.status_code == 405
    assert r.headers["content-type"] == "application/problem+json"


@pytest.mark.asyncio
async def test_path_exceeding_max_depth_returns_404():
    app = _app(Route("/a", methods=["GET"], handler=_echo))
    deep = "/".join(["x"] * (MAX_PATH_DEPTH + 1))

    async with TestClient(app) as c:
        assert (await c.get(f"/{deep}")).status_code == 404


@pytest.mark.asyncio
async def test_route_shorthand_functions():
    class Out(Object):
        method: str

    async def handler(request: FromContext[Request]) -> Response[Out]:
        return Response(Out(method=request.method))

    app = Fusion(
        routes=[
            Get("/get", handler),
            Post("/post", handler),
            Put("/put", handler),
            Delete("/delete", handler),
            Patch("/patch", handler),
            Options("/options", handler),
            Head("/head", handler),
        ]
    )

    async with TestClient(app) as c:
        assert (await c.get("/get")).json() == {"method": "GET"}
        assert (await c.post("/post")).json() == {"method": "POST"}
        assert (await c.put("/put")).json() == {"method": "PUT"}
        assert (await c.delete("/delete")).json() == {"method": "DELETE"}
        assert (await c.patch("/patch")).json() == {"method": "PATCH"}
        assert (await c.options("/options")).json() == {"method": "OPTIONS"}
        assert (await c.head("/head")).status_code == 200


def test_shorthands_are_named_for_their_verb():
    assert Get.__name__ == "Get"
    assert Delete.__name__ == "Delete"


def test_route_with_method_enum_directly():
    route = Route("/x", method=Method.PUT, handler=_echo)
    assert route.methods == (Method.PUT,)


def test_route_accepts_lowercase_method_strings():
    route = Route("/x", method="put", handler=_echo)
    assert route.methods == (Method.PUT,)


@pytest.mark.asyncio
async def test_deeply_nested_static_path():
    app = _app(Route("/a/b/c/d", methods=["GET"], handler=_echo))

    async with TestClient(app) as c:
        assert (await c.get("/a/b/c/d")).status_code == 200
        # /a/b/c is a node in the tree but carries no routes -> 405
        assert (await c.get("/a/b/c")).status_code == 405
        # /a/b/x is not in the tree at all -> 404
        assert (await c.get("/a/b/x")).status_code == 404


# ---------------------------------------------------------------------------
# TreeRouter.resolve, without going through ASGI
# ---------------------------------------------------------------------------


def test_resolve_static_path_returns_route():
    route = Get("/health", _echo)
    router = TreeRouter([route])
    result = router.resolve("/health", Method.GET)
    assert result is not None
    assert result[0] is route
    assert result[1] == {}


def test_resolve_returns_path_params():
    router = TreeRouter([Get("/users/{id:int}", _echo)])
    result = router.resolve("/users/42", Method.GET)
    assert result is not None
    # The router validates the segment; conversion happens in the resolver.
    assert result[1] == {"id": "42"}


def test_resolve_multiple_path_params():
    router = TreeRouter([Get("/users/{user_id:int}/posts/{post_id:int}", _echo)])
    result = router.resolve("/users/1/posts/99", Method.GET)
    assert result is not None
    assert result[1] == {"user_id": "1", "post_id": "99"}


def test_resolve_unknown_path_returns_none():
    router = TreeRouter([Get("/health", _echo)])
    assert router.resolve("/nope", Method.GET) is None


def test_resolve_wrong_method_returns_none():
    router = TreeRouter([Get("/health", _echo)])
    assert router.resolve("/health", Method.POST) is None


def test_resolve_path_node_exists_but_no_method_returns_none():
    router = TreeRouter([Get("/a/b", _echo)])
    assert router.resolve("/a", Method.GET) is None
    assert router._has_path("/a") is True
    assert router._has_path("/zzz") is False


def test_resolve_exceeds_max_depth_returns_none():
    router = TreeRouter([Get("/a", _echo)])
    assert router.resolve("/" + "/".join(["x"] * (MAX_PATH_DEPTH + 1)), Method.GET) is None


def test_resolve_root_path():
    router = TreeRouter([Get("/", _echo)])
    assert router.resolve("/", Method.GET) is not None


def test_resolve_uuid_param():
    router = TreeRouter([Get("/items/{item_id:uuid}", _echo)])
    assert router.resolve("/items/550e8400-e29b-41d4-a716-446655440000", Method.GET)
    assert router.resolve("/items/not-a-uuid", Method.GET) is None


def test_one_route_registers_under_every_method():
    route = Route("/x", _echo, methods=["GET", "DELETE"])
    router = TreeRouter([route])
    assert router.resolve("/x", Method.GET) is not None
    assert router.resolve("/x", Method.DELETE) is not None
    assert router.resolve("/x", Method.POST) is None


# ---------------------------------------------------------------------------
# pk constraint - regex validation + optional transform
# ---------------------------------------------------------------------------


def test_pk_pattern_accepts_valid_public_key():
    from fusion.router import type_patterns

    pk_pattern = type_patterns["pk"]
    assert pk_pattern.match("matter-AbCd")
    assert pk_pattern.match("2024-0001-XXXX")
    assert pk_pattern.match("org-abc123")


def test_pk_pattern_rejects_no_suffix():
    from fusion.router import type_patterns

    pk_pattern = type_patterns["pk"]
    assert pk_pattern.match("nomatch") is None
    assert pk_pattern.match("abc") is None


def test_pk_pattern_rejects_short_suffix():
    from fusion.router import type_patterns

    pk_pattern = type_patterns["pk"]
    assert pk_pattern.match("matter-XY") is None
    assert pk_pattern.match("matter-XYZ") is None


def test_resolve_pk_param_returns_raw_string_without_transform():
    router = TreeRouter([Get("/matters/{key:pk}", _echo)])
    result = router.resolve("/matters/2024-0001-ABCD", Method.GET)
    assert result is not None
    assert result[1] == {"key": "2024-0001-ABCD"}


def test_resolve_pk_param_rejects_invalid_format():
    router = TreeRouter([Get("/matters/{key:pk}", _echo)])
    assert router.resolve("/matters/notakey", Method.GET) is None
    assert router.resolve("/matters/short-X", Method.GET) is None


def test_resolve_pk_with_registered_transform_applies_it():
    from fusion import router as router_module

    original = router_module.type_transforms.copy()
    try:
        router_module.type_transforms["pk"] = lambda s: int(s.split("-")[-1])
        r = TreeRouter([Get("/matters/{key:pk}", _echo)])
        result = r.resolve("/matters/matter-1234", Method.GET)
        assert result is not None
        assert result[1] == {"key": 1234}
    finally:
        router_module.type_transforms.clear()
        router_module.type_transforms.update(original)


def test_resolve_pk_transform_raises_returns_404():
    from fusion import router as router_module

    original = router_module.type_transforms.copy()
    try:
        router_module.type_transforms["pk"] = lambda s: (_ for _ in ()).throw(ValueError("bad"))
        r = TreeRouter([Get("/matters/{key:pk}", _echo)])
        assert r.resolve("/matters/matter-ABCD", Method.GET) is None
    finally:
        router_module.type_transforms.clear()
        router_module.type_transforms.update(original)
