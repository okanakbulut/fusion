import typing

import httpx
import pytest

from fusion import Fusion, Handler, Object, Request, Response, Route
from fusion.protocols import HttpHandler
from fusion.responses import BadRequest, FieldError, NotFound, ValidationError
from fusion.types import Receive, Scope, Send


class MyResponse(Object):
    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"text/plain")],
        })
        await send({
            "type": "http.response.body",
            "body": b"Hello, World!",
        })


def test_handler_protocol_compliance():
    class BaseRequest(Object):
        @classmethod
        async def instance(cls) -> typing.Self:
            return cls()

    class MyRequest(BaseRequest):
        scope: Scope
        receive: Receive
        _send: Send

    class CompliantHandler:
        async def handle(self, request: MyRequest) -> MyResponse:
            return MyResponse()

    assert not issubclass(CompliantHandler, HttpHandler)

    class NewRequest(BaseRequest):
        scope: Scope
        receive: Receive
        send: Send

    class NewCompliantHandler:
        async def handle(self, request: NewRequest) -> MyResponse:
            return MyResponse()

    assert issubclass(NewCompliantHandler, HttpHandler)


@pytest.mark.asyncio
async def test_simple_handler():
    class Output(Object):
        message: str

    class SimpleHandler(Handler):
        async def handle(self, request: Request) -> Response[Output]:
            return Response(Output(message="Hello, World!"))

    app = Fusion(
        routes=[
            Route("/handler", methods=["GET"], handler=SimpleHandler),
        ],
    )

    async with httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.ASGITransport(app=app),
    ) as client:
        response = await client.get("/handler")
        assert response.status_code == 200
        assert response.json() == {"message": "Hello, World!"}


@pytest.mark.asyncio
async def test_handler_can_return_explicit_error_response():
    class ErrorHandler(Handler):
        async def handle(self, request: Request) -> BadRequest:
            return BadRequest(detail="Handled by handler")

    app = Fusion(
        routes=[
            Route("/handled-error", methods=["GET"], handler=ErrorHandler),
        ],
    )

    async with httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.ASGITransport(app=app),
    ) as client:
        response = await client.get("/handled-error")

    assert response.status_code == 400
    assert response.headers["content-type"] == "application/problem+json"
    assert response.json() == {
        "type": "about:blank",
        "title": "Bad Request",
        "status": 400,
        "detail": "Handled by handler",
        "instance": None,
    }


@pytest.mark.asyncio
async def test_unhandled_exception_returns_internal_server_error():
    class FailingHandler(Handler):
        async def handle(self, request: Request) -> Response[Object]:
            raise RuntimeError("boom")

    app = Fusion(
        routes=[
            Route("/error", methods=["GET"], handler=FailingHandler),
        ],
    )

    async with httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.ASGITransport(app=app),
    ) as client:
        response = await client.get("/error")

    assert response.status_code == 500
    assert response.headers["content-type"] == "application/problem+json"
    assert response.json() == {
        "type": "about:blank",
        "title": "Internal Server Error",
        "status": 500,
        "detail": None,
        "instance": None,
    }


@pytest.mark.asyncio
async def test_not_found_route_returns_problem_json():
    app = Fusion(routes=[])

    async with httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.ASGITransport(app=app),
    ) as client:
        response = await client.get("/nonexistent")

    assert response.status_code == 404
    assert response.headers["content-type"] == "application/problem+json"
    assert response.json()["type"] == "about:blank"
    assert response.json()["title"] == "Not Found"
    assert response.json()["status"] == 404


@pytest.mark.asyncio
async def test_method_not_allowed_returns_problem_json():
    class Output(Object):
        message: str

    class GetHandler(Handler):
        async def handle(self, request: Request) -> Response[Output]:
            return Response(Output(message="ok"))

    app = Fusion(
        routes=[Route("/resource", methods=["GET"], handler=GetHandler)],
    )

    async with httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.ASGITransport(app=app),
    ) as client:
        response = await client.post("/resource")

    assert response.status_code == 405
    assert response.headers["content-type"] == "application/problem+json"
    assert response.json()["type"] == "about:blank"
    assert response.json()["title"] == "Method Not Allowed"
    assert response.json()["status"] == 405


@pytest.mark.asyncio
async def test_validation_error_with_field_errors():
    class CreateHandler(Handler):
        async def handle(self, request: Request) -> ValidationError | Response[Object]:
            return ValidationError(
                detail="Validation failed",
                errors=[
                    FieldError(field="email", message="invalid format"),
                    FieldError(field="name", message="required"),
                ],
            )

    app = Fusion(
        routes=[Route("/create", methods=["POST"], handler=CreateHandler)],
    )

    async with httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.ASGITransport(app=app),
    ) as client:
        response = await client.post("/create")

    assert response.status_code == 400
    assert response.headers["content-type"] == "application/problem+json"
    body = response.json()
    assert body["type"] == "about:blank"
    assert body["title"] == "Bad Request"
    assert body["status"] == 400
    assert body["detail"] == "Validation failed"
    assert body["errors"] == [
        {"field": "email", "message": "invalid format"},
        {"field": "name", "message": "required"},
    ]


@pytest.mark.asyncio
async def test_custom_problem_subclass():
    from fusion.responses import Problem

    class OutOfStockProblem(Problem):
        type: typing.ClassVar[str] = "https://example.com/problems/out-of-stock"
        status: typing.ClassVar[int] = 409
        title: str = "Out of Stock"

    class StockHandler(Handler):
        async def handle(self, request: Request) -> OutOfStockProblem | Response[Object]:
            return OutOfStockProblem(detail="Item #42 is out of stock")

    app = Fusion(
        routes=[Route("/stock", methods=["GET"], handler=StockHandler)],
    )

    async with httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.ASGITransport(app=app),
    ) as client:
        response = await client.get("/stock")

    assert response.status_code == 409
    assert response.headers["content-type"] == "application/problem+json"
    body = response.json()
    assert body["type"] == "https://example.com/problems/out-of-stock"
    assert body["title"] == "Out of Stock"
    assert body["status"] == 409
    assert body["detail"] == "Item #42 is out of stock"
