import httpx
import pytest

from fusion import Fusion, Handler, Header, Injectable, Object, Request, Response, Route


@pytest.mark.asyncio
async def test_headers():
    class Output(Object):
        authorization: str
        user_id: int

    class AuthInput(Injectable):
        authorization: Header[str]
        user_id: Header[int]

    class AuthorizationHandler(Handler):
        auth: AuthInput

        async def handle(
            self,
            request: Request,
        ) -> Response[Output]:
            return Response(
                Output(authorization=self.auth.authorization, user_id=self.auth.user_id)
            )

    app = Fusion(routes=[Route(path="/auth", methods=["GET"], handler=AuthorizationHandler)])

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
        input: Input

        async def handle(self, request: Request) -> Response[Output]:
            return Response(
                Output(authorization=self.input.authorization, user_id=self.input.user_id)
            )

    app = Fusion(routes=[Route(path="/auth", methods=["GET"], handler=AuthorizationHandler)])

    async with httpx.AsyncClient(
        base_url="http://localhost",
        transport=httpx.ASGITransport(app=app),
    ) as client:
        response = await client.get("/auth")
        assert response.status_code == 400
        # assert response.json() == {"error": "Missing header 'Authorization'"}
        # TODO: Implement error handling for missing headers
