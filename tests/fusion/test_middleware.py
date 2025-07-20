import httpx
import pytest

from fusion import (
    BaseMiddleware,
    Error,
    Fusion,
    Handler,
    Header,
    Injectable,
    Middleware,
    Object,
    Response,
    Route,
)
from fusion.responses import Unauthorized


@pytest.mark.asyncio
async def test_middleware():
    class AuthenticatedMiddleware(BaseMiddleware):
        async def handle(self, authorization: Header[str]):
            if not authorization.startswith("Bearer "):
                return Unauthorized(
                    Error(code="ERR-UNAUTHORIZED", message="Unknown authentication method")
                )
            return await self.app.handle()

    class Item(Object):
        id: int

    class GetItemHandler(Handler):
        async def handle(self) -> Response[Item]:
            return Response(Item(id=1))

    app = Fusion(
        routes=[Route("/items", GetItemHandler, middlewares=[Middleware(AuthenticatedMiddleware)])]
    )

    async with httpx.AsyncClient(
        base_url="http://localhost",
        transport=httpx.ASGITransport(app=app),
    ) as client:
        response = await client.get("/items")
        assert response.status_code == 400

        response = await client.get("/items", headers={"Authorization": "token"})
        assert response.status_code == 401
        assert response.json() == {
            "code": "ERR-UNAUTHORIZED",
            "message": "Unknown authentication method",
        }

        response = await client.get("/items", headers={"Authorization": "Bearer token"})
        assert response.status_code == 200
        assert response.json() == {"id": 1}
