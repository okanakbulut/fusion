from typing import TypeVar

import httpx
import msgspec
import pytest

from fusion import Fusion, Handler, Injectable, Object, Request, RequestBody, Response, Route
from fusion.testing import TestClient

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


@pytest.mark.asyncio
async def test_struct_body_field_type_error_returns_per_field_errors():
    class Payload(msgspec.Struct):
        name: str
        age: int

    class CreateRequest(Request, kw_only=True):
        body: RequestBody[Payload]

    class CreateHandler(Handler):
        async def handle(self, request: CreateRequest) -> Response:
            return Response(content={"name": request.body.name, "age": request.body.age})

    app = Fusion(routes=[Route("/create", methods=["POST"], handler=CreateHandler)])

    async with TestClient(app) as c:
        r = await c.post("/create", json={"name": "Alice", "age": "not-a-number"})

    assert r.status_code == 400
    body = r.json()
    assert "errors" in body
    assert any(e["field"] == "age" and e["location"] == "body" for e in body["errors"])


@pytest.mark.asyncio
async def test_struct_body_missing_required_field_returns_field_error():
    class Payload(msgspec.Struct):
        name: str
        age: int

    class CreateRequest(Request, kw_only=True):
        body: RequestBody[Payload]

    class CreateHandler(Handler):
        async def handle(self, request: CreateRequest) -> Response:
            return Response(content={"name": request.body.name})

    app = Fusion(routes=[Route("/create", methods=["POST"], handler=CreateHandler)])

    async with TestClient(app) as c:
        r = await c.post("/create", json={})

    assert r.status_code == 400
    body = r.json()
    errors = body["errors"]
    assert any(e["field"] == "name" for e in errors)
    assert any(e["field"] == "age" for e in errors)


@pytest.mark.asyncio
async def test_struct_body_non_dict_json_returns_400():
    class Payload(msgspec.Struct):
        x: int

    class PayloadRequest(Request, kw_only=True):
        body: RequestBody[Payload]

    class PayloadHandler(Handler):
        async def handle(self, request: PayloadRequest) -> Response:
            return Response(content={"x": request.body.x})

    app = Fusion(routes=[Route("/payload", methods=["POST"], handler=PayloadHandler)])

    async with TestClient(app) as c:
        r = await c.post(
            "/payload",
            content=b"[1, 2, 3]",
            headers={"content-type": "application/json"},
        )

    assert r.status_code == 400
    body = r.json()
    messages = [e["message"] for e in body.get("errors", [])] + [body.get("detail", "") or ""]
    assert any("must be a JSON object" in m for m in messages)


@pytest.mark.asyncio
async def test_struct_body_field_with_default_uses_default_when_missing():
    class Payload(msgspec.Struct):
        name: str = "default"
        age: int = 0

    class PayloadRequest(Request, kw_only=True):
        body: RequestBody[Payload]

    class PayloadHandler(Handler):
        async def handle(self, request: PayloadRequest) -> Response:
            return Response(content={"name": request.body.name, "age": request.body.age})

    app = Fusion(routes=[Route("/payload", methods=["POST"], handler=PayloadHandler)])

    async with TestClient(app) as c:
        r = await c.post("/payload", json={"name": "Alice", "age": "not-a-number"})

    assert r.status_code == 400
    assert any(e["field"] == "age" for e in r.json()["errors"])


@pytest.mark.asyncio
async def test_struct_body_field_with_default_factory_uses_factory_when_missing():
    class Payload(msgspec.Struct):
        age: int
        tags: list[str] = msgspec.field(default_factory=list)

    class PayloadRequest(Request, kw_only=True):
        body: RequestBody[Payload]

    class PayloadHandler(Handler):
        async def handle(self, request: PayloadRequest) -> Response:
            return Response(content={"age": request.body.age, "tags": request.body.tags})

    app = Fusion(routes=[Route("/payload", methods=["POST"], handler=PayloadHandler)])

    async with TestClient(app) as c:
        r = await c.post("/payload", json={"age": "not-a-number"})

    assert r.status_code == 400
    assert any(e["field"] == "age" for e in r.json()["errors"])


@pytest.mark.asyncio
async def test_struct_body_coercion_succeeds_via_two_pass():
    class Payload(msgspec.Struct):
        count: int

    class PayloadRequest(Request, kw_only=True):
        body: RequestBody[Payload]

    class PayloadHandler(Handler):
        async def handle(self, request: PayloadRequest) -> Response:
            return Response(content={"count": request.body.count})

    app = Fusion(routes=[Route("/payload", methods=["POST"], handler=PayloadHandler)])

    async with TestClient(app) as c:
        r = await c.post(
            "/payload",
            content=b'{"count": "5"}',
            headers={"content-type": "application/json"},
        )

    assert r.status_code == 200
    assert r.json() == {"count": 5}


@pytest.mark.asyncio
async def test_request_body_size_exceeded_detail_collected_as_field_error():
    from fusion.context import MAX_BODY_SIZE

    class Payload(msgspec.Struct):
        data: str

    class BigRequest(Request, kw_only=True):
        body: RequestBody[Payload]

    class BigHandler(Handler):
        async def handle(self, request: BigRequest) -> Response:
            return Response(content={"data": request.body.data})

    app = Fusion(routes=[Route("/big", methods=["POST"], handler=BigHandler)])

    async with TestClient(app) as c:
        r = await c.post(
            "/big",
            content=b"x" * (MAX_BODY_SIZE + 1),
            headers={"content-type": "application/json"},
        )

    assert r.status_code == 400
