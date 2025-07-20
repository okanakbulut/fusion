import httpx
import pytest

from fusion import Fusion, Handler, Injectable, Object, QueryParam, Response, Route


@pytest.mark.asyncio
async def test_query_param_string():
    class Output(Object):
        message: str

    class QueryParamHandler(Handler):
        async def handle(self, message: QueryParam[str]) -> Response[Output]:
            return Response(Output(message=message))

    app = Fusion(routes=[Route(path="/echo", handler=QueryParamHandler)])

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
        async def handle(self, number: QueryParam[int]) -> Response[Output]:
            return Response(Output(number=number))

    app = Fusion(routes=[Route(path="/echo", handler=QueryParamHandler)])

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
        async def handle(self, temperature: QueryParam[float]) -> Response[Output]:
            return Response(Output(temperature=temperature))

    app = Fusion(routes=[Route(path="/echo", handler=QueryParamHandler)])

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
        async def handle(self, input: Input) -> Response[Output]:
            return Response(
                Output(
                    numbers=input.numbers,
                    message=input.message,
                    temperature=input.temperature,
                )
            )

    app = Fusion(routes=[Route(path="/echo", handler=QueryParamHandler)])

    async with httpx.AsyncClient(
        base_url="http://localhost", transport=httpx.ASGITransport(app)
    ) as client:
        response = await client.get("/echo?numbers:list=1,2,3&temperature=25.5&message=Hi")
        assert response.status_code == 200
        assert response.json() == {"numbers": [1, 2, 3], "message": "Hi", "temperature": 25.5}
