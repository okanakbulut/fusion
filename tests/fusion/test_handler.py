import typing

import httpx
import pytest

from fusion import Fusion, Handler, Object, Request, Response, Route
from fusion.protocols import HttpHandler
from fusion.responses import BadRequest, Error
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
            return BadRequest(content=Error(code="ERR-BAD-REQUEST", message="Handled by handler"))

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
    assert response.json() == {
        "code": "ERR-BAD-REQUEST",
        "message": "Handled by handler",
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
    assert response.json() == {
        "code": "ERR-INTERNAL-SERVER-ERROR",
        "message": "Internal server error",
    }
