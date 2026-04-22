import msgspec
import pytest

from fusion import Cookie, Fusion, Handler, Header, Object, Request, Response, Route
from fusion.testing import TestClient


@pytest.mark.asyncio
async def test_headers():
    class Output(Object):
        authorization: str
        user_id: int

    class AuthRequest(Request, kw_only=True):
        authorization: Header[str]
        user_id: Header[int]

    class AuthorizationHandler(Handler):
        async def handle(self, request: AuthRequest) -> Response[Output]:
            return Response(Output(authorization=request.authorization, user_id=request.user_id))

    app = Fusion(routes=[Route(path="/auth", methods=["GET"], handler=AuthorizationHandler)])

    async with TestClient(app) as client:
        response = await client.get(
            "/auth", headers={"Authorization": "Bearer token", "User-ID": "123"}
        )
        assert response.status_code == 200
        assert response.json() == {"authorization": "Bearer token", "user_id": 123}


@pytest.mark.asyncio
async def test_headers_with_missing_header():
    class Output(Object):
        authorization: str
        user_id: int

    class AuthRequest(Request, kw_only=True):
        authorization: Header[str]
        user_id: Header[int]

    class AuthorizationHandler(Handler):
        async def handle(self, request: AuthRequest) -> Response[Output]:
            return Response(Output(authorization=request.authorization, user_id=request.user_id))

    app = Fusion(routes=[Route(path="/auth", methods=["GET"], handler=AuthorizationHandler)])

    async with TestClient(app) as client:
        response = await client.get("/auth")
        assert response.status_code == 400
        assert response.headers["content-type"] == "application/problem+json"
        body = response.json()
        assert body["status"] == 400
        errors = body["errors"]
        assert any(e["field"] == "authorization" for e in errors)
        assert any(e["field"] == "user_id" for e in errors)


@pytest.mark.asyncio
async def test_optional_header_resolves_to_none_when_missing():
    class AuthRequest(Request, kw_only=True):
        authorization: Header[str | None] = None

    class AuthorizationHandler(Handler):
        async def handle(self, request: AuthRequest) -> Response:
            return Response(content={"auth": request.authorization})

    app = Fusion(routes=[Route(path="/auth", methods=["GET"], handler=AuthorizationHandler)])

    async with TestClient(app) as client:
        response = await client.get("/auth")
        assert response.status_code == 200
        assert response.json() == {"auth": None}


@pytest.mark.asyncio
async def test_cookie_resolver():
    class SessionRequest(Request, kw_only=True):
        session: Cookie[str]

    class Output(Object):
        session: str

    class CookieHandler(Handler):
        async def handle(self, request: SessionRequest) -> Response[Output]:
            return Response(Output(session=request.session))

    app = Fusion(routes=[Route(path="/session", methods=["GET"], handler=CookieHandler)])

    async with TestClient(app) as client:
        response = await client.get("/session", headers={"cookie": "session=tok123"})
        assert response.status_code == 200
        assert response.json() == {"session": "tok123"}


@pytest.mark.asyncio
async def test_path_param_resolver_with_none_value():
    from fusion.context import Context
    from fusion.resolvers import PathParamResolver

    sent: list = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(msg):
        sent.append(msg)

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "query_string": b"",
        "headers": [],
        "path_params": {},
    }
    async with Context(scope, receive, send):
        resolver = PathParamResolver(name="id", typ=int)
        with pytest.raises(msgspec.ValidationError):
            await resolver.resolve()
