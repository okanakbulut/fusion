import typing

import msgspec
from starlette.middleware import Middleware
from starlette.testclient import TestClient

from fusion import (
    Application,
    Endpoint,
    HeaderParam,
    HeaderStruct,
    QueryParam,
    QueryParamPartialStruct,
    QueryParamStruct,
    Route,
)
from fusion.http.middleware import ProblemDetail

# from fusion.http.request import Request


def test_query_param():
    class MyEndpoint(Endpoint):
        async def get(self, name: QueryParam[str]) -> str:
            return f"Hello {name}"

    client = TestClient(Application(routes=[Route("/", MyEndpoint, methods=["GET"])]))
    response = client.get("/?name=world")
    assert response.status_code == 200
    assert response.text == "Hello world"


def test_query_param_default():
    class MyEndpoint(Endpoint):
        async def get(self, name: QueryParam[str] = "world") -> str:
            return f"Hello {name}"

    client = TestClient(Application(routes=[Route("/", MyEndpoint, methods=["GET"])]))
    response = client.get("/")
    assert response.status_code == 200
    assert response.text == "Hello world"


def test_query_param_int():
    class MyEndpoint(Endpoint):
        async def get(self, number: QueryParam[int]) -> str:
            return f"Number: {number}"

    client = TestClient(
        Application(
            routes=[
                Route(
                    "/",
                    MyEndpoint,
                    methods=["GET"],
                    middleware=[
                        Middleware(ProblemDetail),
                    ],
                )
            ]
        )
    )
    response = client.get("/?number=42")
    assert response.status_code == 200
    assert response.text == "Number: 42"

    response = client.get("/?number=42.3")
    assert response.status_code == 400


def test_struct_query_param():
    class Person(msgspec.Struct):
        name: str
        title: str

    class MyEndpoint(Endpoint):
        async def get(self, person: QueryParamStruct[Person]) -> str:
            return f"Hello {person.title} {person.name}"

    app = Application(routes=[Route("/", MyEndpoint, methods=["GET"])])
    client = TestClient(app)
    response = client.get("/?name=John&title=Mr.")
    assert response.status_code == 200
    assert response.text == "Hello Mr. John"


def test_partial_struct_query_param():
    class Person(msgspec.Struct):
        name: str
        title: str

    class MyEndpoint(Endpoint):
        async def get(self, person: QueryParamPartialStruct[Person]) -> str:
            return f"Hello {person.title} {person.name}"

    app = Application(routes=[Route("/", MyEndpoint, methods=["GET"])])
    client = TestClient(app)
    response = client.get("/?person.name=John&person.title=Mr.")
    assert response.status_code == 200
    assert response.text == "Hello Mr. John"


def test_header_param_int():
    class MyEndpoint(Endpoint):
        async def get(self, number: HeaderParam[int]) -> str:
            return f"Number: {number}"

    client = TestClient(
        Application(
            routes=[
                Route(
                    "/",
                    MyEndpoint,
                    methods=["GET"],
                    middleware=[
                        Middleware(ProblemDetail),
                    ],
                )
            ]
        )
    )
    response = client.get("/", headers={"number": "42"})
    assert response.status_code == 200
    assert response.text == "Number: 42"

    response = client.get("/", headers={"number": "42.3"})
    assert response.status_code == 400


def test_struct_header_param():
    class Auth(msgspec.Struct):
        authorization: str
        x_api_version: str = msgspec.field(name="x-api-version")

    class MyEndpoint(Endpoint):
        async def get(self, auth: HeaderStruct[Auth]) -> str:
            return f"Auth {auth.authorization} {auth.x_api_version}"

    app = Application(routes=[Route("/", MyEndpoint, methods=["GET"])])
    client = TestClient(app)
    response = client.get("/", headers={"Authorization": "Bearer xyz", "X-Api-Version": "1.0"})
    assert response.status_code == 200
    assert response.text == "Auth Bearer xyz 1.0"
