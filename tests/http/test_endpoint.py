# mypy: ignore-errors
# until PEP-695 implemented by mypy we need to ignore mypy errors
from typing import Any, Self

import msgspec
import pytest
from starlette.requests import Request
from starlette.testclient import TestClient

from fusion import Application, Context, Injectable, QueryParam, Route


# ideas: inject more parameters into the execute method
class Connection(Injectable, scope="request"):
    pass


class Pool(Injectable, scope="app"):
    @classmethod
    async def inject(cls, typ: type[Self], name: str, request: Request, ctx: Any) -> Self:
        return cls()


class ExampleService(Injectable):
    conn: Connection


@pytest.fixture
def app() -> Application:
    class InjectableEndpoint(msgspec.Struct):
        a: QueryParam[int]
        ss: ExampleService

        async def execute(self, ctx: Context) -> int:
            return self.a

    return Application(
        routes=[
            Route("/api/domain/resource/injectable_endpoint", "GET", command=InjectableEndpoint),
        ],
    )


@pytest.fixture
def client(app):
    with TestClient(app) as client:
        yield client


def test_injectable_endpoint(client):
    response = client.get("/api/domain/resource/injectable_endpoint?a=1")
    assert response.status_code == 200
    assert response.text == "1"
