# Fusion

> A modern, async-first ASGI web framework for Python with type-safe dependency injection.

[![Version](https://img.shields.io/badge/version-0.4.1-blue)](https://github.com/okanakbulut/fusion)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue)](https://www.python.org)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE.md)

---

> [!WARNING]
> **This project is under active development and is not production-ready.** APIs may change without notice between versions.

---

## Overview

Fusion is a lightweight ASGI web framework built on two pillars:

- **[msgspec](https://github.com/jcrist/msgspec)** — high-performance JSON serialization and validation
- **Type-hint-driven DI** — declare dependencies as annotated fields; Fusion resolves them automatically at request time

It is designed for Python 3.12+ and is async-first throughout.

---

## Installation

```bash
# From PyPI (once published)
pip install fusion

# From source
git clone https://github.com/okanakbulut/fusion.git
cd fusion
pip install -e .
```

---

## Quick Start

```python
# app.py
from fusion import Fusion, Get, Handler, Object, Request, Response


class Greeting(Object):
    message: str


class HelloHandler(Handler):
    async def handle(self, request: Request) -> Response[Greeting]:
        return Response(Greeting(message="Hello, World!"))


app = Fusion(routes=[Get("/hello", handler=HelloHandler)])
```

Run it:

```bash
pip install uvicorn
uvicorn app:app
```

```
GET /hello  →  {"message": "Hello, World!"}
```

---

## Core Concepts

### Objects (Data Models)

`Object` is a `msgspec.Struct`-backed base class for all serializable data. Define fields with standard type annotations:

```python
from fusion import Object

class User(Object):
    id: int
    name: str
    email: str
```

---

### Handlers

Subclass `Handler` and implement `async def handle(self, request: SomeRequest) -> SomeResponse`.

- The `request` parameter **must** be type-annotated with `Request` or a subclass of it.
- The return type determines how the response is serialized.

```python
from fusion import Handler, Object, Request, Response

class Output(Object):
    message: str

class MyHandler(Handler):
    async def handle(self, request: Request) -> Response[Output]:
        return Response(Output(message="ok"))
```

---

### Routing

Use `Route` for explicit method lists, or the shorthand helpers `Get`, `Post`, `Put`, `Delete`, `Patch`:

```python
from fusion import Fusion, Get, Post, Route

app = Fusion(routes=[
    Get("/items", handler=ListItemsHandler),
    Post("/items", handler=CreateItemHandler),
    Route("/items/{id:int}", methods=["GET", "DELETE"], handler=ItemHandler),
])
```

**Path parameter syntax:**

| Pattern | Matches |
|---|---|
| `{name}` | any string segment |
| `{id:int}` | integer |
| `{id:uuid}` | UUID |

---

### Request Parameters

Request-scoped parameters (`QueryParam`, `PathParam`, `Header`, `Cookie`, `RequestBody`) must be declared on a `Request` subclass — **not** directly on a `Handler`. The handler's `handle` method receives that subclass as its `request` argument.

#### Query Parameters

```python
from fusion import Fusion, Get, Handler, Object, QueryParam, Request, Response

class SearchRequest(Request):
    query: QueryParam[str]
    page: QueryParam[int]
    tags: QueryParam[list[str]]   # ?tags:list=a,b,c

class SearchResult(Object):
    query: str
    page: int

class SearchHandler(Handler):
    async def handle(self, request: SearchRequest) -> Response[SearchResult]:
        return Response(SearchResult(query=request.query, page=request.page))
```

#### Headers

```python
from fusion import Handler, Header, Object, Request, Response

class AuthRequest(Request):
    authorization: Header[str]
    user_id: Header[int]          # header value is coerced to the declared type

class AuthHandler(Handler):
    async def handle(self, request: AuthRequest) -> Response[Object]:
        token = request.authorization  # "Bearer ..."
        ...
```

#### Request Body

```python
from fusion import Body, Handler, Object, Request, Response

class User(Object):
    name: str
    email: str

class CreateUserRequest(Request):
    body: Body[User]

class CreateUserHandler(Handler):
    async def handle(self, request: CreateUserRequest) -> Response[User]:
        return Response(request.body)
```

---

### Dependency Injection

Use `@factory` to register a factory function for any type (e.g. a database connection). Declare the type as a field on an `Injectable`; Fusion calls the factory automatically.

```python
from fusion import Fusion, Get, Handler, Injectable, Object, Request, Response, factory

class Database:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

@factory
async def database_factory() -> Database:
    return Database("postgresql://localhost/mydb")

class StatusHandler(Handler):
    db: Database          # resolved from the factory above

    async def handle(self, request: Request) -> Response[Object]:
        # self.db is a Database instance
        ...
```

#### Lifecycle Management (async context managers)

Wrap a factory with `@asynccontextmanager` for per-request setup and teardown:

```python
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from fusion import factory

class Session:
    pass

@factory
@asynccontextmanager
async def session_factory() -> AsyncIterator[Session]:
    session = Session()
    try:
        yield session
    finally:
        await session.close()      # runs after the handler returns
```

---

### Middleware

Subclass `BaseMiddleware`, implement `async def handle(self, request)`, and attach it to a route:

```python
from fusion import Fusion, Get, Handler, Middleware, Object, Request, Response, Unauthorized
from fusion.middleware import BaseMiddleware

class AuthMiddleware(BaseMiddleware):
    async def handle(self, request: Request) -> Unauthorized | Response:
        token = request.headers.get("authorization", "")
        if not token.startswith("Bearer "):
            return Unauthorized(detail="Missing or invalid token")
        return await self.app.handle(request)

class ProtectedHandler(Handler):
    async def handle(self, request: Request) -> Response[Object]:
        ...

app = Fusion(routes=[
    Get("/protected", handler=ProtectedHandler, middlewares=[Middleware(AuthMiddleware)])
])
```

---

### Responses & Problem Details

Fusion follows [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457) for error responses. All error types serialize to `application/problem+json`.

| Class | Status | Use case |
|---|---|---|
| `Response[T]` | 200 | Success with body |
| `Created[T]` | 201 | Resource created |
| `NoContent` | 204 | Success, no body |
| `BadRequest` | 400 | Invalid input |
| `Unauthorized` | 401 | Authentication required |
| `Forbidden` | 403 | Permission denied |
| `NotFound` | 404 | Resource not found |
| `MethodNotAllowed` | 405 | Wrong HTTP method |
| `InternalServerError` | 500 | Unhandled error |
| `ValidationError` | 400 | Field-level validation errors |

#### Returning errors from a handler

```python
from fusion.responses import BadRequest, NotFound

class ItemHandler(Handler):
    async def handle(self, request: Request) -> NotFound | Response[Item]:
        item = db.get(item_id)
        if item is None:
            return NotFound(detail="Item not found")
        return Response(item)
```

#### Field-level validation errors

```python
from fusion.responses import FieldError, ValidationError

return ValidationError(
    detail="Validation failed",
    errors=[
        FieldError(field="email", message="invalid format"),
        FieldError(field="name", message="required"),
    ],
)
```

#### Custom problem types

```python
import typing
from fusion.responses import Problem

class OutOfStockProblem(Problem):
    type: typing.ClassVar[str] = "https://example.com/problems/out-of-stock"
    status: typing.ClassVar[int] = 409
    title: str = "Out of Stock"

# In a handler:
return OutOfStockProblem(detail="Item #42 is out of stock")
```

---

## Requirements

- Python 3.12+
- `msgspec >= 0.21.1`
- `typedprotocol >= 0.1.0`

---

## License

MIT — see [LICENSE.md](LICENSE.md).
