import pytest

from fusion import Fusion, Handler, Injectable, Object, QueryParam, Request, Response, Route
from fusion.context import Context
from fusion.testing import TestClient


@pytest.mark.asyncio
async def test_query_param_string():
    class Output(Object):
        message: str

    class QueryInput(Injectable):
        message: QueryParam[str]

    class QueryParamHandler(Handler):
        input: QueryInput

        async def handle(self, request: Request) -> Response[Output]:
            return Response(Output(message=self.input.message))

    app = Fusion(routes=[Route(path="/echo", methods=["GET"], handler=QueryParamHandler)])

    async with TestClient(app) as client:
        response = await client.get("/echo?message=hi")
        assert response.status_code == 200
        assert response.json() == {"message": "hi"}


@pytest.mark.asyncio
async def test_query_param_int():
    class Output(Object):
        number: int

    class QueryInput(Injectable):
        number: QueryParam[int]

    class QueryParamHandler(Handler):
        input: QueryInput

        async def handle(self, request: Request) -> Response[Output]:
            return Response(Output(number=self.input.number))

    app = Fusion(routes=[Route(path="/echo", methods=["GET"], handler=QueryParamHandler)])

    async with TestClient(app) as client:
        response = await client.get("/echo?number=42")
        assert response.status_code == 200
        assert response.json() == {"number": 42}


@pytest.mark.asyncio
async def test_query_param_float():
    class Output(Object):
        temperature: float

    class QueryInput(Injectable):
        temperature: QueryParam[float]

    class QueryParamHandler(Handler):
        input: QueryInput

        async def handle(self, request: Request) -> Response[Output]:
            return Response(Output(temperature=self.input.temperature))

    app = Fusion(routes=[Route(path="/echo", methods=["GET"], handler=QueryParamHandler)])

    async with TestClient(app) as client:
        response = await client.get("/echo?temperature=3.14")
        assert response.status_code == 200
        assert response.json() == {"temperature": 3.14}


@pytest.mark.asyncio
async def test_query_param_multiple():
    class Input(Injectable):
        numbers: QueryParam[list[int]]
        message: QueryParam[str]
        temperature: QueryParam[float]

    class Output(Object):
        numbers: list[int]
        message: str
        temperature: float

    class QueryParamHandler(Handler):
        input: Input

        async def handle(self, request: Request) -> Response[Output]:
            return Response(
                Output(
                    numbers=self.input.numbers,
                    message=self.input.message,
                    temperature=self.input.temperature,
                )
            )

    app = Fusion(routes=[Route(path="/echo", methods=["GET"], handler=QueryParamHandler)])

    async with TestClient(app) as client:
        response = await client.get("/echo?numbers:list=1,2,3&temperature=25.5&message=Hi")
        assert response.status_code == 200
        assert response.json() == {"numbers": [1, 2, 3], "message": "Hi", "temperature": 25.5}


@pytest.mark.asyncio
async def test_query_param_url_encoded_value():
    class Output(Object):
        message: str

    class QueryInput(Injectable):
        message: QueryParam[str]

    class QueryParamHandler(Handler):
        input: QueryInput

        async def handle(self, request: Request) -> Response[Output]:
            return Response(Output(message=self.input.message))

    app = Fusion(routes=[Route(path="/echo", methods=["GET"], handler=QueryParamHandler)])

    async with TestClient(app) as client:
        response = await client.get("/echo?message=hello%20world")
        assert response.status_code == 200
        assert response.json() == {"message": "hello world"}


@pytest.mark.asyncio
async def test_query_params_parsed_from_context():
    sent: list = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(msg):
        sent.append(msg)

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "query_string": b"a=1&b:list=x,y&c=",
        "headers": [],
    }
    async with Context(scope, receive, send) as ctx:
        params = ctx.query_params

    assert params["a"] == "1"
    assert params["b"] == ["x", "y"]
    assert "c" not in params  # blank values are not kept by parse_qsl by default
