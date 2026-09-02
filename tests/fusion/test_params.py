import typing

import pytest

from fusion import (
    Fusion,
    Get,
    Http,
    Object,
    Post,
    Request,
    Response,
    Route,
)
from fusion.annotations import FromContext

from .conftest import client_for


class Echo(Object):
    value: typing.Any


@pytest.mark.asyncio
async def test_path_param_is_converted():
    async def handler(id: Http.Path[int]) -> Response[Echo]:
        return Response(Echo(value=id))

    app = Fusion(routes=[Get("/items/{id:int}", handler)])
    async with client_for(app) as client:
        assert (await client.get("/items/42")).json() == {"value": 42}


@pytest.mark.asyncio
async def test_query_params_with_defaults_and_lists():
    async def handler(
        q: Http.Query[str],
        page: Http.Query[int] = 1,
        tags: Http.Query[list[str]] = (),
    ) -> Response[Echo]:
        return Response(Echo(value={"q": q, "page": page, "tags": list(tags)}))

    app = Fusion(routes=[Get("/search", handler)])
    async with client_for(app) as client:
        assert (await client.get("/search?q=x")).json()["value"] == {
            "q": "x",
            "page": 1,
            "tags": [],
        }
        got = (await client.get("/search?q=x&page=3&tags:list=a,b")).json()["value"]
        assert got == {"q": "x", "page": 3, "tags": ["a", "b"]}


@pytest.mark.asyncio
async def test_missing_required_query_param_is_a_validation_error():
    async def handler(q: Http.Query[str]) -> Response[Echo]:
        return Response(Echo(value=q))

    app = Fusion(routes=[Get("/search", handler)])
    async with client_for(app) as client:
        response = await client.get("/search")

    assert response.status_code == 400
    assert response.json()["errors"] == [
        {"field": "q", "location": "query", "message": "Missing required value"}
    ]


@pytest.mark.asyncio
async def test_bad_query_param_type_reports_location():
    async def handler(page: Http.Query[int]) -> Response[Echo]:
        return Response(Echo(value=page))

    app = Fusion(routes=[Get("/search", handler)])
    async with client_for(app) as client:
        response = await client.get("/search?page=nope")

    assert response.status_code == 400
    assert response.json()["errors"][0]["location"] == "query"


@pytest.mark.asyncio
async def test_headers_are_normalised_and_converted():
    async def handler(authorization: Http.Header[str], user_id: Http.Header[int]) -> Response[Echo]:
        return Response(Echo(value={"auth": authorization, "user": user_id}))

    app = Fusion(routes=[Get("/auth", handler)])
    async with client_for(app) as client:
        response = await client.get("/auth", headers={"Authorization": "Bearer x", "User-ID": "7"})

    assert response.json()["value"] == {"auth": "Bearer x", "user": 7}


@pytest.mark.asyncio
async def test_cookies_are_read():
    async def handler(session: Http.Cookie[str]) -> Response[Echo]:
        return Response(Echo(value=session))

    app = Fusion(routes=[Get("/me", handler)])
    async with client_for(app) as client:
        client.cookies.set("session", "abc")
        response = await client.get("/me")

    assert response.json() == {"value": "abc"}


class NewUser(Object):
    name: str
    email: str


@pytest.mark.asyncio
async def test_request_body_is_decoded():
    async def handler(body: Http.Body[NewUser]) -> Response[NewUser]:
        return Response(body)

    app = Fusion(routes=[Post("/users", handler)])
    async with client_for(app) as client:
        response = await client.post("/users", json={"name": "Ada", "email": "a@b.c"})

    assert response.json() == {"name": "Ada", "email": "a@b.c"}


@pytest.mark.asyncio
async def test_body_field_errors_are_per_field():
    async def handler(body: Http.Body[NewUser]) -> Response[NewUser]:
        return Response(body)

    app = Fusion(routes=[Post("/users", handler)])
    async with client_for(app) as client:
        response = await client.post("/users", json={"name": 1})

    assert response.status_code == 400
    fields = {e["field"] for e in response.json()["errors"]}
    assert fields == {"name", "email"}
    assert all(e["location"] == "body" for e in response.json()["errors"])


@pytest.mark.asyncio
async def test_malformed_body_is_a_validation_error():
    async def handler(body: Http.Body[NewUser]) -> Response[NewUser]:
        return Response(body)

    app = Fusion(routes=[Post("/users", handler)])
    async with client_for(app) as client:
        response = await client.post(
            "/users", content=b"{oops", headers={"content-type": "application/json"}
        )

    assert response.status_code == 400


@pytest.mark.asyncio
async def test_non_object_body_type():
    async def handler(body: Http.Body[list[int]]) -> Response[Echo]:
        return Response(Echo(value=body))

    app = Fusion(routes=[Post("/nums", handler)])
    async with client_for(app) as client:
        assert (await client.post("/nums", json=[1, 2, 3])).json() == {"value": [1, 2, 3]}


@pytest.mark.asyncio
async def test_body_must_be_a_json_object_for_struct_targets():
    async def handler(body: Http.Body[NewUser]) -> Response[NewUser]:
        return Response(body)

    app = Fusion(routes=[Post("/users", handler)])
    async with client_for(app) as client:
        response = await client.post("/users", json=[1, 2])

    assert response.status_code == 400
    # A body-level failure is reported against the body parameter itself.
    assert response.json()["errors"] == [
        {"field": "body", "location": "body", "message": "Request body must be a JSON object"}
    ]


@pytest.mark.asyncio
async def test_errors_from_several_locations_are_aggregated():
    """One response reports every bad parameter, not just the first."""

    async def handler(page: Http.Query[int], body: Http.Body[NewUser]) -> Response[Echo]:
        return Response(Echo(value=None))

    app = Fusion(routes=[Post("/mixed", handler)])
    async with client_for(app) as client:
        response = await client.post("/mixed?page=nope", json={"name": 1})

    assert response.status_code == 400
    locations = {e["location"] for e in response.json()["errors"]}
    assert locations == {"query", "body"}


@pytest.mark.asyncio
async def test_request_facade_gives_raw_access():
    async def handler(request: FromContext[Request]) -> Response[Echo]:
        return Response(
            Echo(
                value={
                    "method": request.method,
                    "path": request.path,
                    "query": dict(request.query_params),
                    "agent": request.headers.get("user_agent"),
                }
            )
        )

    app = Fusion(routes=[Get("/raw", handler)])
    async with client_for(app) as client:
        response = await client.get("/raw?a=1", headers={"user-agent": "pytest"})

    value = response.json()["value"]
    assert value["method"] == "GET"
    assert value["path"] == "/raw"
    assert value["query"] == {"a": "1"}
    assert value["agent"] == "pytest"


@pytest.mark.asyncio
async def test_request_facade_exposes_body_and_scope():
    async def handler(request: FromContext[Request]) -> Response[Echo]:
        raw = await request.body()
        return Response(Echo(value={"raw": raw.decode(), "type": request.scope["type"]}))

    app = Fusion(routes=[Post("/raw", handler)])
    async with client_for(app) as client:
        response = await client.post("/raw", content=b"hi")

    assert response.json()["value"] == {"raw": "hi", "type": "http"}


@pytest.mark.asyncio
async def test_request_facade_exposes_cookies_and_path_params():
    async def handler(request: FromContext[Request]) -> Response[Echo]:
        return Response(Echo(value={"cookies": request.cookies, "path": request.path_params}))

    app = Fusion(routes=[Get("/x/{id}", handler)])
    async with client_for(app) as client:
        client.cookies.set("a", "b")
        response = await client.get("/x/7")

    assert response.json()["value"] == {"cookies": {"a": "b"}, "path": {"id": "7"}}


def test_unmarked_parameter_is_rejected():
    async def handler(db: int) -> Response[Echo]: ...

    with pytest.raises(TypeError, match="carries no Fusion marker"):
        Get("/x", handler)


def test_tool_marker_rejected_on_an_http_route():
    from fusion import Tool

    async def handler(q: Tool.Arg[str]) -> Response[Echo]: ...

    with pytest.raises(TypeError, match="'tool' marker"):
        Get("/x", handler)


@pytest.mark.asyncio
async def test_an_absent_optional_header_falls_back_to_its_default():
    """Regression: an absent header was converted from None and failed the type."""

    async def handler(last_event_id: Http.Header[str] = "") -> Response[Echo]:
        return Response(Echo(value=last_event_id))

    app = Fusion(routes=[Get("/x", handler)])
    async with client_for(app) as client:
        assert (await client.get("/x")).json() == {"value": ""}
        assert (await client.get("/x", headers={"last-event-id": "7"})).json() == {"value": "7"}


@pytest.mark.asyncio
async def test_an_absent_optional_cookie_falls_back_to_its_default():
    async def handler(session: Http.Cookie[str] = "anon") -> Response[Echo]:
        return Response(Echo(value=session))

    app = Fusion(routes=[Get("/x", handler)])
    async with client_for(app) as client:
        assert (await client.get("/x")).json() == {"value": "anon"}


@pytest.mark.asyncio
async def test_a_missing_required_header_is_reported_as_missing():
    async def handler(authorization: Http.Header[str]) -> Response[Echo]:
        return Response(Echo(value=authorization))

    app = Fusion(routes=[Get("/x", handler)])
    async with client_for(app) as client:
        response = await client.get("/x")

    assert response.json()["errors"] == [
        {"field": "authorization", "location": "header", "message": "Missing required value"}
    ]


@pytest.mark.asyncio
async def test_a_missing_required_cookie_is_reported_as_missing():
    async def handler(session: Http.Cookie[str]) -> Response[Echo]:
        return Response(Echo(value=session))

    app = Fusion(routes=[Get("/x", handler)])
    async with client_for(app) as client:
        response = await client.get("/x")

    assert response.json()["errors"][0]["location"] == "cookie"


@pytest.mark.asyncio
async def test_a_path_param_absent_from_the_scope_is_reported_as_missing():
    from fusion.binding import Signature, bind
    from fusion.context import Context
    from fusion.exceptions import ValidationException
    from fusion.types import Transport

    async def handler(id: Http.Path[int]) -> Response[Echo]: ...

    signature = Signature.of(handler, transport=Transport.HTTP)
    scope = {"type": "http", "path_params": {}, "query_string": b"", "headers": []}

    async def receive():  # pragma: no cover
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):  # pragma: no cover
        pass

    async with Context(scope, receive, send):
        with pytest.raises(ValidationException) as excinfo:
            await bind(signature)

    assert excinfo.value.errors[0].message == "Missing required value"
