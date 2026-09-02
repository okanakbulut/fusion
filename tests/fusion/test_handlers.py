import typing

import pytest

from fusion import (
    BadRequest,
    Created,
    FieldError,
    Fusion,
    Get,
    Http,
    NoContent,
    NotFound,
    Object,
    Post,
    Problem,
    Response,
    Route,
    ValidationProblem,
)

from .conftest import client_for


class Output(Object):
    message: str


@pytest.mark.asyncio
async def test_simple_handler():
    async def hello() -> Response[Output]:
        return Response(Output(message="Hello, World!"))

    app = Fusion(routes=[Get("/hello", hello)])
    async with client_for(app) as client:
        response = await client.get("/hello")

    assert response.status_code == 200
    assert response.json() == {"message": "Hello, World!"}


@pytest.mark.asyncio
async def test_handler_returns_created():
    async def make() -> Created[Output]:
        return Created(Output(message="made"))

    app = Fusion(routes=[Post("/make", make)])
    async with client_for(app) as client:
        response = await client.post("/make")

    assert response.status_code == 201


@pytest.mark.asyncio
async def test_no_content_sends_empty_body():
    async def drop() -> NoContent[Output]:
        return NoContent()

    app = Fusion(routes=[Post("/drop", drop)])
    async with client_for(app) as client:
        response = await client.post("/drop")

    assert response.status_code == 204
    assert response.content == b""


@pytest.mark.asyncio
async def test_handler_can_return_explicit_error():
    async def bad() -> BadRequest:
        return BadRequest(detail="Handled by handler")

    app = Fusion(routes=[Get("/bad", bad)])
    async with client_for(app) as client:
        response = await client.get("/bad")

    assert response.status_code == 400
    assert response.headers["content-type"] == "application/problem+json"
    assert response.json() == {
        "type": "about:blank",
        "status": 400,
        "title": "Bad Request",
        "detail": "Handled by handler",
        "instance": None,
    }


@pytest.mark.asyncio
async def test_unhandled_exception_becomes_500():
    async def boom() -> Response[Output]:
        raise RuntimeError("boom")

    app = Fusion(routes=[Get("/boom", boom)])
    async with client_for(app) as client:
        response = await client.get("/boom")

    assert response.status_code == 500
    assert response.json()["title"] == "Internal Server Error"


@pytest.mark.asyncio
async def test_unknown_route_is_404():
    app = Fusion(routes=[])
    async with client_for(app) as client:
        response = await client.get("/nope")

    assert response.status_code == 404
    assert response.json()["title"] == "Not Found"


@pytest.mark.asyncio
async def test_known_path_wrong_method_is_405():
    async def only_get() -> Response[Output]:
        return Response(Output(message="ok"))

    app = Fusion(routes=[Get("/resource", only_get)])
    async with client_for(app) as client:
        response = await client.post("/resource")

    assert response.status_code == 405
    assert response.json()["title"] == "Method Not Allowed"


@pytest.mark.asyncio
async def test_validation_problem_carries_field_errors():
    async def create() -> ValidationProblem | Response[Object]:
        return ValidationProblem(
            detail="Validation failed",
            errors=[
                FieldError(field="email", location="body", message="invalid format"),
                FieldError(field="name", location="body", message="required"),
            ],
        )

    app = Fusion(routes=[Post("/create", create)])
    async with client_for(app) as client:
        response = await client.post("/create")

    body = response.json()
    assert response.status_code == 400
    assert body["errors"] == [
        {"field": "email", "location": "body", "message": "invalid format"},
        {"field": "name", "location": "body", "message": "required"},
    ]


@pytest.mark.asyncio
async def test_custom_problem_subclass():
    class OutOfStock(Problem):
        type: typing.ClassVar[str] = "https://example.com/problems/out-of-stock"
        status_code: typing.ClassVar[int] = 409
        title: str = "Out of Stock"

    async def stock() -> OutOfStock | Response[Object]:
        return OutOfStock(detail="Item #42 is out of stock")

    app = Fusion(routes=[Get("/stock", stock)])
    async with client_for(app) as client:
        response = await client.get("/stock")

    assert response.status_code == 409
    assert response.json()["type"] == "https://example.com/problems/out-of-stock"


@pytest.mark.asyncio
async def test_falsy_content_is_not_coerced_to_empty_string():
    """Regression: `self.content or ""` turned 0, [] and {} into a bare string."""

    async def zero() -> Response[typing.Any]:
        return Response(content=0)

    async def empty() -> Response[typing.Any]:
        return Response(content=[])

    app = Fusion(routes=[Get("/zero", zero), Get("/empty", empty)])
    async with client_for(app) as client:
        assert (await client.get("/zero")).json() == 0
        assert (await client.get("/empty")).json() == []


@pytest.mark.asyncio
async def test_response_carries_custom_headers():
    async def hdr() -> Response[Output]:
        return Response(Output(message="ok"), headers={"x-trace": "abc"})

    app = Fusion(routes=[Get("/hdr", hdr)])
    async with client_for(app) as client:
        response = await client.get("/hdr")

    assert response.headers["x-trace"] == "abc"


def test_handler_must_be_async():
    def sync_handler() -> Response[Output]: ...

    with pytest.raises(TypeError, match="async def"):
        Get("/sync", sync_handler)


def test_route_requires_a_method():
    async def handler() -> Response[Output]: ...

    with pytest.raises(ValueError, match="method"):
        Route("/x", handler)


@pytest.mark.asyncio
async def test_route_registers_every_listed_method():
    """Regression: only methods[0] was registered, silently dropping the rest."""

    async def handler() -> Response[Output]:
        return Response(Output(message="ok"))

    app = Fusion(routes=[Route("/item", handler, methods=["GET", "DELETE"])])
    async with client_for(app) as client:
        assert (await client.get("/item")).status_code == 200
        assert (await client.delete("/item")).status_code == 200
        assert (await client.post("/item")).status_code == 405


def test_route_exposes_first_method():
    async def handler() -> Response[Output]: ...

    route = Route("/item", handler, methods=["GET", "DELETE"])
    assert route.method.value == "GET"
