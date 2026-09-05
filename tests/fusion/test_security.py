import base64
import typing

import pytest

from fusion import (
    Auth,
    Forbidden,
    Fusion,
    Get,
    Http,
    Object,
    Response,
    Unauthorized,
    requires,
)
from fusion.exceptions import ProblemException
from fusion.security import ROLES_ATTRIBUTE, authorize

from .conftest import client_for


class Item(Object):
    id: int


class Allow:
    """An authorizer that grants a fixed set of roles."""

    def __init__(self, *granted: str) -> None:
        self.granted = frozenset(granted)
        self.asked: list[frozenset[str]] = []

    async def authorize(self, roles: frozenset[str]) -> bool:
        self.asked.append(roles)
        return roles <= self.granted


def _basic(username: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()


# --- credentials ---------------------------------------------------------


@pytest.mark.asyncio
async def test_a_bearer_token_arrives_without_its_prefix():
    async def whoami(token: Auth.Bearer) -> Response[Item]:
        assert token == "abc123"
        return Response(Item(id=1))

    app = Fusion(routes=[Get("/me", whoami)])
    async with client_for(app) as client:
        response = await client.get("/me", headers={"authorization": "Bearer abc123"})

    assert response.status_code == 200


@pytest.mark.asyncio
@pytest.mark.parametrize("header", ["", "abc123", "Basic abc123", "Bearer", "Bearer   "], ids=repr)
async def test_a_missing_or_malformed_bearer_is_401_not_422(header: str):
    """A credential is not a validation failure - the honest answer is 401."""

    async def whoami(token: Auth.Bearer) -> Response[Item]:
        return Response(Item(id=1))  # pragma: no cover

    app = Fusion(routes=[Get("/me", whoami)])
    async with client_for(app) as client:
        response = await client.get("/me", headers={"authorization": header} if header else {})

    assert response.status_code == 401
    assert response.headers["content-type"] == "application/problem+json"


@pytest.mark.asyncio
async def test_basic_credentials_are_decoded():
    async def whoami(credentials: Auth.Basic) -> Response[Item]:
        assert (credentials.username, credentials.password) == ("ada", "s3cret")
        return Response(Item(id=1))

    app = Fusion(routes=[Get("/me", whoami)])
    async with client_for(app) as client:
        response = await client.get("/me", headers={"authorization": _basic("ada", "s3cret")})

    assert response.status_code == 200


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value",
    ["Basic not-base64!!", "Basic " + base64.b64encode(b"no-colon").decode()],
    ids=["undecodable", "no separator"],
)
async def test_a_malformed_basic_credential_is_401(value: str):
    async def whoami(credentials: Auth.Basic) -> Response[Item]:
        return Response(Item(id=1))  # pragma: no cover

    app = Fusion(routes=[Get("/me", whoami)])
    async with client_for(app) as client:
        response = await client.get("/me", headers={"authorization": value})

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_an_api_key_is_read_from_its_header():
    async def search(x_api_key: Auth.ApiKey) -> Response[Item]:
        assert x_api_key == "k-1"
        return Response(Item(id=1))

    app = Fusion(routes=[Get("/search", search)])
    async with client_for(app) as client:
        assert (await client.get("/search", headers={"x-api-key": "k-1"})).status_code == 200
        assert (await client.get("/search")).status_code == 401


@pytest.mark.asyncio
async def test_an_api_key_can_come_from_the_query_or_a_cookie():
    async def by_query(api_key: Auth.ApiKeyQuery) -> Response[Item]:
        return Response(Item(id=1))

    async def by_cookie(session: Auth.ApiKeyCookie) -> Response[Item]:
        return Response(Item(id=2))

    app = Fusion(routes=[Get("/q", by_query), Get("/c", by_cookie)])
    async with client_for(app) as client:
        assert (await client.get("/q?api_key=k")).status_code == 200
        assert (await client.get("/q")).status_code == 401
        client.cookies.set("session", "s")
        assert (await client.get("/c")).status_code == 200
        client.cookies.clear()
        assert (await client.get("/c")).status_code == 401


# --- requires ------------------------------------------------------------


def test_requires_records_its_roles_and_unions_when_stacked():
    @requires("b")
    @requires("a")
    async def handler() -> Response[Item]: ...

    assert getattr(handler, ROLES_ATTRIBUTE) == frozenset({"a", "b"})


@pytest.mark.parametrize("roles", [(), ("",), ("ok", "   ")], ids=["none", "empty", "blank"])
def test_an_empty_requirement_is_rejected(roles: tuple[str, ...]):
    with pytest.raises(ValueError, match="at least one non-empty role"):
        requires(*roles)


@pytest.mark.asyncio
async def test_a_granted_role_runs_the_handler_and_a_missing_one_is_403():
    @requires("items:write")
    async def update(token: Auth.Bearer) -> Response[Item] | Forbidden:
        return Response(Item(id=1))

    authorizer = Allow("items:write")
    app = Fusion(routes=[Get("/items", update)], authorizer=authorizer)
    async with client_for(app) as client:
        assert (
            await client.get("/items", headers={"authorization": "Bearer t"})
        ).status_code == 200

    denying = Allow()
    app = Fusion(routes=[Get("/items", update)], authorizer=denying)
    async with client_for(app) as client:
        response = await client.get("/items", headers={"authorization": "Bearer t"})

    assert response.status_code == 403
    assert response.json()["detail"] == "Requires items:write"
    assert authorizer.asked == [frozenset({"items:write"})]


@pytest.mark.asyncio
async def test_a_role_on_a_middleware_is_checked_before_the_handler_runs():
    reached: list[str] = []

    @requires("admin")
    async def admin_only(token: Auth.Bearer) -> None:
        reached.append("middleware")

    async def get_item() -> Response[Item]:
        reached.append("handler")  # pragma: no cover
        return Response(Item(id=1))  # pragma: no cover

    app = Fusion(routes=[Get("/items", get_item, middlewares=[admin_only])], authorizer=Allow())
    async with client_for(app) as client:
        response = await client.get("/items", headers={"authorization": "Bearer t"})

    assert response.status_code == 403
    assert reached == []


@pytest.mark.asyncio
async def test_authorize_asks_nothing_when_no_role_is_declared():
    assert await authorize(frozenset()) is None


# --- registration-time checks -------------------------------------------


def test_roles_without_a_credential_are_rejected_at_registration():
    @requires("items:write")
    async def update(id: Http.Path[int]) -> Response[Item]: ...

    with pytest.raises(TypeError, match="requires roles but declares no credential"):
        Get("/items/{id:int}", update)


def test_a_credential_anywhere_in_the_chain_satisfies_the_check():
    """The requirement and the credential need not sit on the same function."""

    async def authenticate(token: Auth.Bearer) -> None: ...

    @requires("items:write")
    async def update(id: Http.Path[int]) -> Response[Item]: ...

    route = Get("/items/{id:int}", update, middlewares=[authenticate])

    assert route.signature.roles == frozenset({"items:write"})


def test_an_application_with_roles_and_no_authorizer_is_rejected():
    @requires("items:write")
    async def update(token: Auth.Bearer) -> Response[Item]: ...

    with pytest.raises(ValueError, match="built without an authorizer"):
        Fusion(routes=[Get("/items", update)])


def test_an_application_without_roles_needs_no_authorizer():
    async def get_item() -> Response[Item]:
        return Response(Item(id=1))

    assert Fusion(routes=[Get("/items", get_item)]).authorizer is None


# --- generated document --------------------------------------------------


def _document(*routes: typing.Any, authorizer: typing.Any = None) -> dict:
    return Fusion(routes=list(routes), authorizer=authorizer).openapi()


def test_a_credential_is_a_scheme_not_a_parameter():
    """OpenAPI ignores a header parameter named Authorization; the scheme carries it."""

    async def whoami(token: Auth.Bearer) -> Response[Item]:
        return Response(Item(id=1))

    document = _document(Get("/me", whoami))
    operation = document["paths"]["/me"]["get"]

    assert "parameters" not in operation
    assert document["components"]["securitySchemes"] == {
        "bearerAuth": {"type": "http", "scheme": "bearer"}
    }
    assert operation["security"] == [{"bearerAuth": []}]


def test_roles_ride_on_the_security_requirement():
    @requires("items:write")
    async def update(token: Auth.Bearer) -> Response[Item]:
        return Response(Item(id=1))

    document = _document(Get("/items", update), authorizer=Allow())
    operation = document["paths"]["/items"]["get"]

    assert operation["security"] == [{"bearerAuth": ["items:write"]}]
    assert set(operation["responses"]) == {"200", "401", "403"}


def test_an_api_key_scheme_uses_the_name_a_client_sends():
    async def search(x_api_key: Auth.ApiKey) -> Response[Item]:
        return Response(Item(id=1))

    document = _document(Get("/search", search))

    assert document["components"]["securitySchemes"] == {
        "xApiKeyAuth": {"type": "apiKey", "in": "header", "name": "x-api-key"}
    }


def test_two_schemes_in_one_chain_are_both_required():
    async def authenticate(token: Auth.Bearer) -> None: ...

    async def search(x_api_key: Auth.ApiKey) -> Response[Item]:
        return Response(Item(id=1))

    document = _document(Get("/search", search, middlewares=[authenticate]))

    assert document["paths"]["/search"]["get"]["security"] == [
        {"bearerAuth": [], "xApiKeyAuth": []}
    ]


def test_a_route_without_credentials_has_no_security_key():
    async def get_item() -> Response[Item]:
        return Response(Item(id=1))

    operation = _document(Get("/items", get_item))["paths"]["/items"]["get"]

    assert "security" not in operation
    assert set(operation["responses"]) == {"200"}


def test_basic_auth_is_documented_as_its_own_scheme():
    async def whoami(credentials: Auth.Basic) -> Response[Item]:
        return Response(Item(id=1))  # pragma: no cover

    document = _document(Get("/me", whoami))

    assert document["components"]["securitySchemes"] == {
        "basicAuth": {"type": "http", "scheme": "basic"}
    }
    assert document["paths"]["/me"]["get"]["security"] == [{"basicAuth": []}]


@pytest.mark.asyncio
async def test_a_problem_raised_after_the_stream_started_is_not_swallowed():
    """Once the status line is out, no problem response can replace it."""

    async def events() -> typing.AsyncIterator[Item]:
        yield Item(id=1)
        raise ProblemException(Unauthorized(detail="too late"))

    app = Fusion(routes=[Get("/events", events)])
    with pytest.raises(ProblemException, match="too late"):
        async with client_for(app) as client:
            await client.get("/events")
