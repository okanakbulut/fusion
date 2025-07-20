import httpx
import pytest

from fusion import Fusion, Handler, Header, Injectable, Object, Response, Route


@pytest.mark.asyncio
async def test_headers():
    class Output(Object):
        authorization: str
        user_id: int

    class AuthorizationHandler(Handler):
        async def handle(
            self, authorization: Header[str], user_id: Header[int]
        ) -> Response[Output]:
            return Response(Output(authorization=authorization, user_id=user_id))

    app = Fusion(routes=[Route(path="/auth", handler=AuthorizationHandler)])

    async with httpx.AsyncClient(
        base_url="http://localhost", transport=httpx.ASGITransport(app=app)
    ) as client:
        response = await client.get(
            "/auth", headers={"Authorization": "Bearer token", "User-ID": "123"}
        )
        assert response.status_code == 200
        assert response.json() == {"authorization": "Bearer token", "user_id": 123}


@pytest.mark.asyncio
async def test_headers_with_missing_header():
    class Input(Injectable):
        authorization: Header[str]
        user_id: Header[int]

    class Output(Object):
        authorization: str
        user_id: int

    class AuthorizationHandler(Handler):
        async def handle(self, input: Input) -> Response[Output]:
            return Response(Output(authorization=input.authorization, user_id=input.user_id))

    app = Fusion(routes=[Route(path="/auth", handler=AuthorizationHandler)])

    async with httpx.AsyncClient(
        base_url="http://localhost",
        transport=httpx.ASGITransport(app=app),
    ) as client:
        response = await client.get("/auth")
        assert response.status_code == 400
        # assert response.json() == {"error": "Missing header 'Authorization'"}
        # TODO: Implement error handling for missing headers
