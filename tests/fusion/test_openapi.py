import typing
from collections.abc import AsyncIterator

import pytest

from fusion import (
    Created,
    Event,
    FromContext,
    Fusion,
    Get,
    Http,
    Inject,
    NotFound,
    Object,
    Post,
    Request,
    Response,
    Route,
    Unauthorized,
    ValidationProblem,
    factory,
    openapi_route,
)
from fusion.openapi import generate, response_arms, union_arms

from .conftest import client_for


class User(Object):
    id: int
    name: str


class NewUser(Object):
    name: str


class Database:
    pass


@factory
async def db_factory() -> Database:
    return Database()


async def get_user(id: Http.Path[int], db: Inject[Database]) -> Response[User] | NotFound:
    """Fetch one user.

    Looks the user up by primary key.
    """
    return Response(User(id=id, name="x"))


async def list_users(q: Http.Query[str] = "") -> Response[list[User]]:
    """List users."""
    return Response([])


async def create_user(body: Http.Body[NewUser]) -> Created[User] | ValidationProblem:
    """Create a user."""
    return Created(User(id=1, name=body.name))


async def only_ok(id: Http.Path[int] = 1) -> Response[User]:
    """A single return type, with no union."""
    return Response(User(id=id, name="x"))


async def user_events(id: Http.Path[int]) -> AsyncIterator[Event[User] | NotFound]:
    """Stream changes to one user."""
    yield Event(data=User(id=id, name="x"))


@pytest.fixture
def document():
    return generate(
        [
            Get("/users/{id:int}", get_user, tags=["users"]),
            Get("/users", list_users, tags=["users"]),
            Post("/users", create_user),
            Get("/only/{id:int}", only_ok),
            Get("/users/{id:int}/events", user_events),
        ],
        title="Demo",
        version="1.2.3",
        description="A demo API.",
    )


def test_document_metadata(document):
    assert document["openapi"] == "3.1.0"
    assert document["info"] == {
        "title": "Demo",
        "version": "1.2.3",
        "description": "A demo API.",
    }


def test_path_params_are_stripped_of_their_converter(document):
    assert "/users/{id}" in document["paths"]
    assert "/users/{id:int}" not in document["paths"]


def test_parameter_locations_match_resolvers(document):
    params = document["paths"]["/users/{id}"]["get"]["parameters"]
    assert [(p["name"], p["in"], p["required"]) for p in params] == [("id", "path", True)]

    query = document["paths"]["/users"]["get"]["parameters"]
    assert [(p["name"], p["in"], p["required"]) for p in query] == [("q", "query", False)]


def test_injected_parameters_are_not_documented(document):
    names = {p["name"] for p in document["paths"]["/users/{id}"]["get"]["parameters"]}
    assert "db" not in names


def test_single_return_type_is_not_misread(document):
    """Regression: get_args on a non-union Response[User] returns (User,), not arms."""
    responses = document["paths"]["/only/{id}"]["get"]["responses"]
    assert set(responses) == {"200"}
    assert responses["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/User"
    }


def test_union_return_becomes_one_response_per_status(document):
    responses = document["paths"]["/users/{id}"]["get"]["responses"]
    assert set(responses) == {"200", "404"}
    assert responses["404"]["content"]["application/problem+json"]["schema"] == {
        "$ref": "#/components/schemas/Problem"
    }


def test_created_uses_its_own_status(document):
    responses = document["paths"]["/users"]["post"]["responses"]
    assert set(responses) == {"201", "400"}


def test_request_body_is_documented(document):
    body = document["paths"]["/users"]["post"]["requestBody"]
    assert body["required"] is True
    assert body["content"]["application/json"]["schema"] == {"$ref": "#/components/schemas/NewUser"}


def test_streaming_route_documents_event_stream(document):
    responses = document["paths"]["/users/{id}/events"]["get"]["responses"]
    assert set(responses) == {"200", "404"}
    assert "text/event-stream" in responses["200"]["content"]


def test_shared_schemas_are_defined_once(document):
    schemas = document["components"]["schemas"]
    assert sorted(schemas) == ["NewUser", "Problem", "User"]


def test_summary_and_description_come_from_the_docstring(document):
    operation = document["paths"]["/users/{id}"]["get"]
    assert operation["summary"] == "Fetch one user."
    assert operation["description"] == "Looks the user up by primary key."
    assert operation["operationId"] == "get_user"
    assert operation["tags"] == ["users"]


def test_explicit_metadata_overrides_the_docstring():
    document = generate(
        [Get("/x", only_ok, summary="Custom", description="Other", operation_id="op")]
    )
    operation = document["paths"]["/x"]["get"]
    assert operation["summary"] == "Custom"
    assert operation["description"] == "Other"
    assert operation["operationId"] == "op"


def test_multi_method_route_documents_each_verb():
    async def handler(id: Http.Path[int]) -> Response[User]:
        return Response(User(id=id, name="x"))

    document = generate([Route("/x/{id:int}", handler, methods=["GET", "DELETE"])])
    assert set(document["paths"]["/x/{id}"]) == {"get", "delete"}


def test_a_handler_without_a_return_annotation_is_rejected():
    """An operation the generator cannot describe is a hole in the document."""

    async def bare(id: Http.Path[int]):
        return Response(content=None)  # pragma: no cover

    with pytest.raises(TypeError, match="annotated to return Any"):
        Get("/bare/{id:int}", bare)


def test_union_arms_leaves_a_non_union_intact():
    assert union_arms(Response[User]) == (Response[User],)
    assert set(union_arms(Response[User] | NotFound)) == {Response[User], NotFound}


def test_response_arms_of_an_unannotated_handler():
    assert response_arms(typing.Any) == []
    assert response_arms(None) == []


def test_response_arms_ignores_arms_without_a_status():
    assert response_arms(Response[User] | int) == [(200, User)]


@pytest.mark.asyncio
async def test_openapi_is_served_and_cached():
    app = Fusion(routes=[Get("/users/{id:int}", get_user), openapi_route()])
    async with client_for(app) as client:
        response = await client.get("/openapi.json")

    assert response.status_code == 200
    assert response.json()["openapi"] == "3.1.0"
    assert app.openapi() is app.openapi()


@pytest.mark.asyncio
async def test_openapi_with_overrides_is_not_cached():
    app = Fusion(routes=[Get("/users/{id:int}", get_user)])
    assert app.openapi(title="One")["info"]["title"] == "One"
    assert app.openapi()["info"]["title"] == "Fusion"


def test_mcp_and_openapi_schemas_come_from_the_same_generator():
    """Anti-drift: one struct, one description, whichever consumer asks."""
    from fusion import Tool
    from fusion.tools import ToolDef

    async def tool(body: Tool.Arg[NewUser]) -> Response[User]:
        """Make a user."""

    tool_schema = ToolDef(tool).schema["$defs"]["NewUser"]
    document = generate([Post("/users", create_user)])
    openapi_schema = document["components"]["schemas"]["NewUser"]

    assert tool_schema["properties"] == openapi_schema["properties"]
    assert tool_schema["required"] == openapi_schema["required"]


def test_generating_from_no_routes():
    document = generate([])
    assert document["paths"] == {}
    assert list(document["components"]["schemas"]) == ["Problem"]


def test_streaming_handler_without_a_yield_annotation():
    from fusion.openapi import stream_arms

    assert stream_arms(typing.Any) == []


def test_deprecated_and_described_fields_reach_the_schema():
    from fusion import field

    class Thing(Object):
        old: str = field(default="x", deprecated=True, description="do not use")

    async def handler(body: Http.Body[Thing]) -> Response[Thing]:
        """Handle."""

    document = generate([Post("/things", handler)])
    prop = document["components"]["schemas"]["Thing"]["properties"]["old"]

    assert prop["description"] == "do not use"
    assert prop["deprecated"] is True


async def _api_key_guard(
    x_api_key: Http.Header[str], tenant: Http.Query[str] = ""
) -> Unauthorized | None:
    if not x_api_key:
        return Unauthorized(detail="no key")


async def _tracing() -> typing.AsyncIterator[None]:
    yield


def _document_for(route: Route) -> dict:
    return Fusion(routes=[route]).openapi()


def test_a_middleware_header_is_documented_on_the_operation():
    """The route rejects a request without it, so the document has to say so."""

    async def handler(id: Http.Path[int]) -> Response[User]:
        return Response(User(id=id, name="x"))

    document = _document_for(
        Get("/users/{id:int}", handler, middlewares=[_api_key_guard, _tracing])
    )
    parameters = document["paths"]["/users/{id}"]["get"]["parameters"]
    by_name = {(p["name"], p["in"]): p for p in parameters}

    assert ("id", "path") in by_name
    assert by_name[("x_api_key", "header")]["required"] is True
    assert by_name[("tenant", "query")]["required"] is False
    assert by_name[("x_api_key", "header")]["schema"] == {"type": "string"}


def test_a_status_only_a_guard_can_return_is_documented():
    async def handler() -> Response[User]:
        return Response(User(id=1, name="x"))

    document = _document_for(Get("/users", handler, middlewares=[_api_key_guard]))
    responses = document["paths"]["/users"]["get"]["responses"]

    assert set(responses) == {"200", "401"}
    assert responses["401"]["content"]["application/problem+json"]


def test_a_handler_and_a_middleware_declaring_one_header_document_it_once():
    async def handler(x_api_key: Http.Header[str]) -> Response[User]:
        return Response(User(id=1, name="x"))

    document = _document_for(Get("/users", handler, middlewares=[_api_key_guard]))
    parameters = document["paths"]["/users"]["get"]["parameters"]

    assert [p["name"] for p in parameters if p["in"] == "header"] == ["x_api_key"]


def test_middleware_dependencies_stay_out_of_the_document():
    """Only what travels over HTTP is documented; Inject and FromContext are not."""

    async def auditing(request: FromContext[Request]) -> typing.AsyncIterator[None]:
        yield

    async def handler() -> Response[User]:
        return Response(User(id=1, name="x"))

    document = _document_for(Get("/users", handler, middlewares=[auditing]))

    assert "parameters" not in document["paths"]["/users"]["get"]


def test_a_wrapper_return_annotation_is_not_read_as_a_response():
    """`-> AsyncIterator[None]` describes the generator, not a 200 event stream."""

    async def handler() -> Response[User]:
        return Response(User(id=1, name="x"))

    document = _document_for(Get("/users", handler, middlewares=[_tracing]))
    responses = document["paths"]["/users"]["get"]["responses"]

    assert set(responses) == {"200"}
    assert "text/event-stream" not in responses["200"]["content"]


def test_a_wrapper_documents_the_response_it_can_yield():
    """A replacement yielded on the way out is a status a client really receives."""

    async def as_problem() -> AsyncIterator[NotFound | None]:
        try:
            yield
        except LookupError:
            yield NotFound(detail="gone")  # pragma: no cover

    document = _document_for(Get("/only", only_ok, middlewares=[as_problem]))
    responses = document["paths"]["/only"]["get"]["responses"]

    assert set(responses) == {"200", "404"}
    assert responses["404"]["content"]["application/problem+json"]


def test_a_pass_through_wrapper_adds_no_response():
    async def tracing() -> AsyncIterator[None]:
        yield

    document = _document_for(Get("/only", only_ok, middlewares=[tracing]))

    assert set(document["paths"]["/only"]["get"]["responses"]) == {"200"}


def test_a_data_only_event_stream_documents_its_payload():
    """A bare object is yielded as a data-only event, so it is the payload itself."""

    async def ticks() -> AsyncIterator[User]:
        yield User(id=1, name="x")  # pragma: no cover

    document = _document_for(Get("/ticks", ticks))
    content = document["paths"]["/ticks"]["get"]["responses"]["200"]["content"]

    assert content["text/event-stream"]["schema"] == {"$ref": "#/components/schemas/User"}
