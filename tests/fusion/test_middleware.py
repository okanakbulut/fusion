import httpx
import pytest

from fusion import (
    Fusion,
    Get,
    Handler,
    Middleware,
    Object,
    Request,
    Response,
    Route,
    Unauthorized,
)
from fusion.middleware import BaseMiddleware


@pytest.mark.asyncio
async def test_middleware():
    class AuthenticationMiddleware(BaseMiddleware):
        async def handle(self, request: Request) -> Unauthorized | Response:
            authorization = request.headers.get("authorization")
            if not authorization or not authorization.startswith("Bearer "):
                return Unauthorized(detail="Unknown authentication method")
            return await self.app.handle(request)

    class Item(Object):
        id: int

    class GetItemsHandler(Handler):
        async def handle(self, request: Request) -> Response[Item]:
            return Response(Item(id=1))

    app = Fusion(
        routes=[
            Get(
                "/items",
                handler=GetItemsHandler,
                middlewares=[Middleware(AuthenticationMiddleware)],
            )
        ]
    )

    async with httpx.AsyncClient(
        base_url="http://localhost",
        transport=httpx.ASGITransport(app=app),
    ) as client:
        response = await client.get("/items")
        assert response.status_code == 401

        response = await client.get("/items", headers={"Authorization": "token"})
        assert response.status_code == 401
        assert response.headers["content-type"] == "application/problem+json"
        assert response.json()["type"] == "about:blank"
        assert response.json()["title"] == "Unauthorized"
        assert response.json()["status"] == 401
        assert response.json()["detail"] == "Unknown authentication method"

        response = await client.get("/items", headers={"Authorization": "Bearer token"})
        assert response.status_code == 200
        assert response.json() == {"id": 1}


@pytest.mark.asyncio
async def test_base_middleware_passthrough():
    from fusion.middleware import BaseMiddleware

    class Item(Object):
        id: int

    class GetItemsHandler(Handler):
        async def handle(self, request: Request) -> Response[Item]:
            return Response(Item(id=99))

    app = Fusion(
        routes=[
            Get(
                "/items",
                handler=GetItemsHandler,
                middlewares=[Middleware(BaseMiddleware)],
            )
        ]
    )

    async with httpx.AsyncClient(
        base_url="http://localhost",
        transport=httpx.ASGITransport(app=app),
    ) as client:
        response = await client.get("/items")
        assert response.status_code == 200
        assert response.json() == {"id": 99}
