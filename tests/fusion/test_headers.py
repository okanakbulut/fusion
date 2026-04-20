import pytest

from fusion import Cookie, Fusion, Handler, Header, Injectable, Object, Request, Response, Route
from fusion.testing import TestClient


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

    async with TestClient(app) as client:
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

    async with TestClient(app) as client:
        response = await client.get("/auth")
        assert response.status_code == 400
        assert response.headers["content-type"] == "application/problem+json"
        body = response.json()
        assert body["status"] == 400
        assert "authorization" in body["detail"].lower()


@pytest.mark.asyncio
async def test_cookie_resolver():
    class Input(Injectable):
        session: Cookie[str]

    class Output(Object):
        session: str

    class CookieHandler(Handler):
        inp: Input

        async def handle(self, request: Request) -> Response[Output]:
            return Response(Output(session=self.inp.session))

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
        "path_params": {},  # empty — no id param
    }
    async with Context(scope, receive, send):
        resolver = PathParamResolver(name="id", typ=int)
        name, value = await resolver.resolve()
    assert name == "id"
    assert value is None
