from typing import TypeVar

import httpx
import pytest

from fusion import Fusion, Handler, Injectable, Object, Request, RequestBody, Response, Route

T = TypeVar("T")


@pytest.mark.asyncio
async def test_request_body():
    class Data[T](Object):
        data: T

    class User(Object):
        id: int
        name: str
        email: str

    class UserRequest(Injectable):
        body: RequestBody[Data[User]]  # expect request body to be like this {"data": {...}}

    class UserHandler(Handler):
        request_data: UserRequest

        async def handle(self, request: Request) -> Response[User]:
            user: User = self.request_data.body.data
            print(f"user id: {user.id}, name: {user.name}, email: {user.email}")

            return Response(user)

    app = Fusion(routes=[Route("/users", methods=["POST"], handler=UserHandler)])

    async with httpx.AsyncClient(
        base_url="http://localhost",
        transport=httpx.ASGITransport(app=app),
    ) as client:
        response = await client.post(
            "/users", json={"data": {"id": 1, "name": "John Doe", "email": "john@example.com"}}
        )
        assert response.status_code == 200
        assert response.json() == {"id": 1, "name": "John Doe", "email": "john@example.com"}
