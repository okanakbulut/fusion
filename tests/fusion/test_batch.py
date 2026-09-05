"""A batch endpoint, written the way an application would write one.

The envelope is the endpoint's own contract - fusion ships only ``execute`` -
so this doubles as the worked example for that boundary.
"""

import asyncio
import typing

import msgspec
import pytest

from fusion import (
    Auth,
    Created,
    Fusion,
    Get,
    Http,
    Object,
    Post,
    Request,
    Response,
    Unauthorized,
    field,
    requires,
)
from fusion.annotations import FromContext
from fusion.types import Method

from .conftest import client_for

# --- the application's own wire contract ---------------------------------


class SubRequest(Object):
    method: Method
    path: str
    id: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    # Two msgspec facts shape this field.  It is not `Raw | None`, because Raw
    # matches any JSON and the union collapses to null; and its default comes
    # from a factory, because a Raw *default* cannot be encoded into a JSON
    # Schema and would break document generation.  Empty means "no body".
    body: msgspec.Raw = field(default_factory=lambda: msgspec.Raw(b""))


class SubResponse(Object):
    id: str | None
    status: int
    body: msgspec.Raw


class BatchIn(Object):
    requests: list[SubRequest]
    sequential: bool = False


class BatchOut(Object):
    responses: list[SubResponse]


# --- the routes being batched --------------------------------------------


class User(Object):
    id: int
    name: str


class NewUser(Object):
    name: str


_users: dict[int, User] = {1: User(id=1, name="ada")}


async def get_user(id: Http.Path[int]) -> Response[User] | Unauthorized:
    return Response(_users[id])


async def create_user(body: Http.Body[NewUser]) -> Created[User]:
    user = User(id=len(_users) + 1, name=body.name)
    _users[user.id] = user
    return Created(user)


async def slow() -> Response[User]:
    await asyncio.sleep(0.05)
    return Response(User(id=99, name="slow"))


@requires("admin")
async def admin_only(token: Auth.Bearer) -> Response[User]:
    return Response(User(id=0, name="admin"))


async def batch(body: Http.Body[BatchIn], request: FromContext[Request]) -> Response[BatchOut]:
    """Run several routes in one round trip."""
    app = request.scope["app"]

    async def run(item: SubRequest) -> SubResponse:
        result = await app.execute(
            item.method,
            item.path,
            headers=item.headers,
            body=bytes(item.body),
        )
        return SubResponse(id=item.id, status=result.status, body=msgspec.Raw(result.body))

    if body.sequential:
        responses = [await run(item) for item in body.requests]
    else:
        responses = list(await asyncio.gather(*(run(item) for item in body.requests)))
    return Response(BatchOut(responses=responses))


class Allow:
    def __init__(self, *granted: str) -> None:
        self.granted = frozenset(granted)

    async def authorize(self, roles: frozenset[str]) -> bool:
        return roles <= self.granted


def _app(authorizer: typing.Any = None) -> Fusion:
    return Fusion(
        routes=[
            Get("/users/{id:int}", get_user),
            Post("/users", create_user),
            Get("/slow", slow),
            Get("/admin", admin_only),
            Post("/batch", batch),
        ],
        authorizer=authorizer or Allow("admin"),
    )


@pytest.mark.asyncio
async def test_a_batch_runs_every_sub_request():
    payload = {
        "requests": [
            {"id": "a", "method": "GET", "path": "/users/1"},
            {"id": "b", "method": "POST", "path": "/users", "body": {"name": "grace"}},
        ]
    }

    async with client_for(_app()) as client:
        response = await client.post("/batch", json=payload)

    assert response.status_code == 200
    assert response.json() == {
        "responses": [
            {"id": "a", "status": 200, "body": {"id": 1, "name": "ada"}},
            {"id": "b", "status": 201, "body": {"id": 2, "name": "grace"}},
        ]
    }


@pytest.mark.asyncio
async def test_one_failing_item_does_not_sink_the_batch():
    payload = {
        "requests": [
            {"id": "ok", "method": "GET", "path": "/users/1"},
            {"id": "missing", "method": "GET", "path": "/users/404"},
            {"id": "nosuch", "method": "GET", "path": "/nope"},
        ]
    }

    async with client_for(_app()) as client:
        body = (await client.post("/batch", json=payload)).json()

    assert [(r["id"], r["status"]) for r in body["responses"]] == [
        ("ok", 200),
        ("missing", 500),  # the handler raised KeyError - captured, not propagated
        ("nosuch", 404),
    ]


@pytest.mark.asyncio
async def test_each_item_is_authorized_on_its_own_route():
    """Auth is per route: the batch envelope grants nothing by itself."""
    payload = {"requests": [{"id": "a", "method": "GET", "path": "/admin"}]}

    async with client_for(_app()) as client:
        without = await client.post("/batch", json=payload)
        with_token = await client.post(
            "/batch", json=payload, headers={"authorization": "Bearer t"}
        )

    assert without.json()["responses"][0]["status"] == 401
    assert with_token.json()["responses"][0]["status"] == 200


@pytest.mark.asyncio
async def test_an_item_denied_its_role_is_403():
    payload = {"requests": [{"id": "a", "method": "GET", "path": "/admin"}]}

    async with client_for(_app(authorizer=Allow())) as client:
        response = await client.post("/batch", json=payload, headers={"authorization": "Bearer t"})

    assert response.json()["responses"][0]["status"] == 403


@pytest.mark.asyncio
async def test_a_per_item_header_overrides_the_inherited_one():
    payload = {
        "requests": [
            {"id": "inherit", "method": "GET", "path": "/admin"},
            {
                "id": "override",
                "method": "GET",
                "path": "/admin",
                "headers": {"authorization": "not-a-bearer"},
            },
        ]
    }

    async with client_for(_app()) as client:
        body = (
            await client.post("/batch", json=payload, headers={"authorization": "Bearer t"})
        ).json()

    assert [(r["id"], r["status"]) for r in body["responses"]] == [
        ("inherit", 200),
        ("override", 401),
    ]


@pytest.mark.asyncio
async def test_items_run_concurrently_unless_asked_otherwise():
    items = [{"id": str(n), "method": "GET", "path": "/slow"} for n in range(4)]

    async with client_for(_app()) as client:
        loop = asyncio.get_running_loop()

        start = loop.time()
        await client.post("/batch", json={"requests": items})
        concurrent = loop.time() - start

        start = loop.time()
        await client.post("/batch", json={"requests": items, "sequential": True})
        sequential = loop.time() - start

    # Four 50ms routes: together they cost about one, in sequence about four.
    assert concurrent < 0.15
    assert sequential > concurrent


@pytest.mark.asyncio
async def test_the_batch_operation_documents_itself_completely():
    """The envelope is always a 200, so nothing about it is left undocumented."""
    operation = _app().openapi()["paths"]["/batch"]["post"]

    assert set(operation["responses"]) == {"200"}
    assert operation["requestBody"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/BatchIn"
    }
