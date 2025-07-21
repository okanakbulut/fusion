import httpx
import pytest

from fusion import Fusion, Handler, Injectable, Object, QueryParam, Request, Response, Route


@pytest.mark.asyncio
async def test_query_param_string():
    class Output(Object):
        message: str

    class QueryParamHandler(Handler):
        message: QueryParam[str]

        async def handle(self, request: Request) -> Response[Output]:
            return Response(Output(message=self.message))

    app = Fusion(routes=[Route(path="/echo", methods=["GET"], handler=QueryParamHandler)])

    async with httpx.AsyncClient(
        base_url="http://localhost", transport=httpx.ASGITransport(app)
    ) as client:
        response = await client.get("/echo?message=hi")
        assert response.status_code == 200
        assert response.json() == {"message": "hi"}


@pytest.mark.asyncio
async def test_query_param_int():
    class Output(Object):
        number: int

    class QueryParamHandler(Handler):
        number: QueryParam[int]

        async def handle(self, request: Request) -> Response[Output]:
            return Response(Output(number=self.number))

    app = Fusion(routes=[Route(path="/echo", methods=["GET"], handler=QueryParamHandler)])

    async with httpx.AsyncClient(
        base_url="http://localhost", transport=httpx.ASGITransport(app)
    ) as client:
        response = await client.get("/echo?number=42")
        assert response.status_code == 200
        assert response.json() == {"number": 42}


@pytest.mark.asyncio
async def test_query_param_float():
    class Output(Object):
        temperature: float

    class QueryParamHandler(Handler):
        temperature: QueryParam[float]

        async def handle(self, request: Request) -> Response[Output]:
            return Response(Output(temperature=self.temperature))

    app = Fusion(routes=[Route(path="/echo", methods=["GET"], handler=QueryParamHandler)])

    async with httpx.AsyncClient(
        base_url="http://localhost", transport=httpx.ASGITransport(app)
    ) as client:
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

    async with httpx.AsyncClient(
        base_url="http://localhost", transport=httpx.ASGITransport(app)
    ) as client:
        response = await client.get("/echo?numbers:list=1,2,3&temperature=25.5&message=Hi")
        assert response.status_code == 200
        assert response.json() == {"numbers": [1, 2, 3], "message": "Hi", "temperature": 25.5}
