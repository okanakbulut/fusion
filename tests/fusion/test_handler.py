import httpx
import pytest

from fusion import Fusion, Handler, Object, Response, Route


@pytest.mark.asyncio
async def test_simple_handler():
    class Output(Object):
        message: str

    class SimpleHandler(Handler):
        async def handle(self) -> Response[Output]:
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
