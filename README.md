# Fusion

> A modern, async-first ASGI web framework for Python with type-safe dependency injection.

[![PyPI](https://img.shields.io/pypi/v/fusion)](https://pypi.org/project/fusion/)
[![Python](https://img.shields.io/badge/python-3.14-blue)](https://www.python.org)
[![CI](https://github.com/okanakbulut/fusion/actions/workflows/ci.yml/badge.svg)](https://github.com/okanakbulut/fusion/actions/workflows/ci.yml)
[![Coverage](https://raw.githubusercontent.com/okanakbulut/fusion/main/coverage.svg)](https://github.com/okanakbulut/fusion/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE.md)

---

> [!WARNING]
> **This project is under active development and is not production-ready.** APIs may change without notice between versions.

---

## Overview

Fusion is a lightweight ASGI framework built on two pillars:

- **[msgspec](https://github.com/jcrist/msgspec)** — high-performance JSON serialization and validation
- **Explicit, type-driven binding** — every parameter names its source, and Fusion resolves it at call time

A handler is an ordinary `async def` function. Its signature is the contract: parameter markers say
where each value comes from, and the return annotation says what comes back. That one signature is
enough to serve the endpoint, generate its OpenAPI operation, and — for tool functions — publish an
MCP tool schema.

---

## Installation

```bash
pip install fusion

# From source
git clone https://github.com/okanakbulut/fusion.git
cd fusion && pip install -e .
```

---

## Quick start

```python
# app.py
from fusion import Fusion, Get, Object, Response


class Greeting(Object):
    message: str


async def hello() -> Response[Greeting]:
    """Say hello."""
    return Response(Greeting(message="Hello, World!"))


app = Fusion(routes=[Get("/hello", hello)])
```

```bash
pip install uvicorn
fusion serve app:app
```

```
GET /hello  →  {"message": "Hello, World!"}
```

---

## Core concepts

### Objects

`Object` is a `msgspec.Struct`-backed base class for serializable data.

```python
from fusion import Object, field


class User(Object):
    id: int
    name: str = field(min_length=1, description="Display name")
```

`field()` carries validation constraints (`ge`, `gt`, `le`, `lt`, `min_length`, `max_length`,
`pattern`) plus `description` and `deprecated`. All of them flow into generated JSON Schema, so
they show up in both your OpenAPI document and your MCP tool definitions.

---

### Handlers

A handler is an `async def` function. Nothing is inferred — each parameter carries a marker naming
its source, and unmarked parameters are rejected when the app is constructed.

```python
from fusion import Fusion, Get, Http, Inject, NotFound, Response


async def get_user(id: Http.Path[int], db: Inject[Database]) -> Response[User] | NotFound:
    """Fetch one user."""
    user = await db.fetch(id)
    if user is None:
        return NotFound(detail=f"no user {id}")
    return Response(user)


app = Fusion(routes=[Get("/users/{id:int}", get_user)])
```

The docstring's first line becomes the OpenAPI `summary` (and an MCP tool's description); the rest
becomes the `description`.

> **Parameter ordering.** Python forbids a non-default parameter after a defaulted one, so put
> `Inject` parameters before defaulted ones — `(q, db, limit=10)`, not `(q, limit=10, db)`. Order is
> otherwise irrelevant: the binder always calls with keyword arguments.

---

### Parameter markers

| Marker | Source |
|---|---|
| `Http.Path[T]` | a path segment |
| `Http.Query[T]` | a query-string parameter |
| `Http.Header[T]` | a request header |
| `Http.Cookie[T]` | a cookie |
| `Http.Body[T]` | the JSON request body |
| `Tool.Arg[T]` | one argument of a tool call |
| `Inject[T]` | a dependency — works under every transport |
| `FromContext[Request]` | the live request façade, for raw access |
| `Auth.Bearer` / `Auth.Basic` / `Auth.ApiKey` | a credential — documented as a security scheme, never as a parameter |

```python
async def search(
    q: Http.Query[str],
    authorization: Http.Header[str],
    page: Http.Query[int] = 1,
    tags: Http.Query[list[str]] = (),      # ?tags:list=a,b,c
) -> Response[list[User]]: ...
```

Values are coerced to the declared type. A missing required parameter, or one that fails
conversion, becomes a field error — and **all** of them are reported in a single 400 rather than
just the first:

```json
{
  "type": "about:blank", "status": 400, "title": "Bad Request",
  "errors": [
    {"field": "page", "location": "query", "message": "Expected `int`, got `str`"},
    {"field": "email", "location": "body", "message": "Expected `str`, got `int`"}
  ]
}
```

Header and cookie names are normalised, so `User-ID` binds to a parameter named `user_id`.

---

### Path parameters

| Pattern | Matches |
|---|---|
| `{name}` | any string segment |
| `{id:int}` | integer |
| `{id:uuid}` | UUID |

---

### Routing

```python
from fusion import Delete, Fusion, Get, Post, Route

app = Fusion(routes=[
    Get("/items", list_items),
    Post("/items", create_item),
    Route("/items/{id:int}", item_detail, methods=["GET", "DELETE"]),
])
```

`Route` also accepts `summary`, `description`, `tags`, `operation_id` and `keepalive`.

---

### Dependency injection

Register a factory for any type, then ask for it with `Inject[T]`.

```python
from fusion import Inject, factory


class Database:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn


@factory
async def database_factory() -> Database:
    return Database("postgresql://localhost/mydb")


async def status(db: Inject[Database]) -> Response[Object]:
    ...
```

A dependency is constructed **once per call** — two parameters asking for the same type share one
instance.

Wrap a factory in `@asynccontextmanager` for setup and teardown. Teardown runs after the response
is sent, and after a stream finishes:

```python
@factory
@asynccontextmanager
async def session_factory() -> AsyncIterator[Session]:
    session = Session()
    try:
        yield session
    finally:
        await session.close()
```

`Injectable` composes dependencies into a reusable group:

```python
class Deps(Injectable):
    db: Inject[Database]
    session: Inject[Session]


async def handler(deps: Inject[Deps]) -> Response[Object]: ...
```

---

### Server-sent events

An **async generator** handler is streamed as `text/event-stream`. Signal a pre-flight failure by
*yielding* a problem and returning — Fusion pulls the first item before committing a status line,
so you still get an ordinary error response rather than a 200 stream carrying an error:

```python
from collections.abc import AsyncIterator
from fusion import Event, Get, NotFound


async def order_events(
    order_id: Http.Path[int],
    db: Inject[Database],
    last_event_id: Http.Header[str] = "",
) -> AsyncIterator[Event[OrderEvent] | NotFound]:
    """Stream status changes for one order."""
    if await db.fetch_order(order_id) is None:
        yield NotFound(detail=f"order {order_id} not found")
        return

    async for change in db.watch_order(order_id, after=last_event_id):
        yield Event(data=change, id=str(change.seq), event=change.kind)


route = Get("/orders/{order_id:int}/events", order_events, keepalive=15.0)
```

Everything before the first `yield` is pre-flight:

| First item | Response |
|---|---|
| a `Problem` | that problem, buffered — no stream starts |
| an `Event` or object | `200 text/event-stream`, then the rest |
| nothing yielded | `200`, empty stream |
| an exception | the normal error path |

`Event` carries `data` plus optional `event`, `id` and `retry`; yielding a bare object emits a
data-only event. With `keepalive` set, Fusion emits SSE comments while idle so proxies do not reap
the connection, and cancels the generator when the client disconnects.

> Because an ASGI `receive()` has a single consumer, a handler that both reads a request body and
> then streams must read the body **before** the first `yield`.

---

### Middleware

Nothing is handed to a middleware to call. The chain runs on its own, and a middleware says what it
wants by the shape of the function. Either shape binds its parameters exactly as a handler does, so
it asks for the header or dependency it needs instead of digging through `Request`.

A plain `async def` is a **guard**: it runs before the route, and returning a response ends the
request there. Return nothing and the chain carries on.

```python
from fusion import Http, Unauthorized


async def require_token(authorization: Http.Header[str] = "") -> Unauthorized | None:
    if not authorization.startswith("Bearer "):
        return Unauthorized(detail="Missing or invalid token")
```

The annotation is not decoration: it is where the operation's `401` in the generated document comes
from, so `Any` is rejected at registration.

An `async def` that **yields** wraps the rest of the chain. Everything before the `yield` runs on
the way in, the response comes back through it, and everything after runs on the way out.

```python
async def timing() -> typing.AsyncIterator[None]:
    started = time.monotonic()
    response = yield                       # the route runs here
    response.headers["x-ms"] = f"{(time.monotonic() - started) * 1000:.1f}"


Get("/protected", handler, middlewares=[require_token, timing])
```

A route takes any number of middlewares — first in the list is outermost, so `[a, b]` runs `a`,
then `b`, then the route, and unwinds back out through `b` and `a`.

Because a middleware binds from the request the same way, what it declares is part of the route's
HTTP contract, and [OpenAPI generation](#openapi) documents it alongside the handler's own
parameters.

The `yield` is an ordinary suspension point, so `try`/`except`/`finally` around it work the way they
read: a failure downstream is thrown back in at the `yield`, and yielding a second value replaces
the response.

```python
async def as_problem() -> typing.AsyncIterator[NotFound | None]:
    try:
        yield
    except LookupError:
        yield NotFound(detail="gone")      # replaces the failure
```

A wrapper's *yield* type is what it can answer with — `AsyncIterator[None]` to pass through,
`AsyncIterator[NotFound | None]` to replace — and the `404` above is documented on every route that
uses it.

A middleware that needs configuration is a closure — no framework support required:

```python
class TooManyRequests(Problem):
    status_code: typing.ClassVar[int] = 429
    title: str = "Too Many Requests"


def rate_limit(per_minute: int):
    async def middleware(key: Http.Header[str] = "") -> TooManyRequests | None:
        if await too_many(key, per_minute):
            return TooManyRequests()

    return middleware


Get("/search", handler, middlewares=[rate_limit(per_minute=60)])
```

---

### Responses and problem details

Errors follow [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457) and serialize as
`application/problem+json`.

| Class | Status |
|---|---|
| `Response[T]` | 200 |
| `Created[T]` | 201 |
| `NoContent` | 204 |
| `BadRequest` | 400 |
| `Unauthorized` | 401 |
| `Forbidden` | 403 |
| `NotFound` | 404 |
| `MethodNotAllowed` | 405 |
| `InternalServerError` | 500 |
| `ValidationProblem` | 400 with field errors |

Return them from the union in your annotation:

```python
async def get_item(id: Http.Path[int]) -> Response[Item] | NotFound:
    item = db.get(id)
    return Response(item) if item else NotFound(detail="Item not found")
```

Custom problems set `type` and `status_code` as ClassVars:

```python
class OutOfStock(Problem):
    type: typing.ClassVar[str] = "https://example.com/problems/out-of-stock"
    status_code: typing.ClassVar[int] = 409
    title: str = "Out of Stock"
```

---

## MCP

A function whose non-injected parameters are all `Tool.Arg` can be published as a
[Model Context Protocol](https://modelcontextprotocol.io) tool. The input schema comes from the
signature and the description from the docstring.

```python
from fusion import Fusion, Inject, Response, Tool
from fusion.mcp import mcp_route


async def search_users(
    q: Tool.Arg[str],
    db: Inject[Database],
    limit: Tool.Arg[int] = 10,
) -> Response[list[User]]:
    """Search users by name."""
    return Response(await db.search(q, limit))


app = Fusion(routes=[mcp_route()], tools=[search_users])
```

`POST /mcp` serves `initialize`, `tools/list` and `tools/call` over JSON-RPC. A tool call gets its
own scope, so `Inject` dependencies and their teardown work exactly as they do over HTTP. Returning
a `Problem` produces a tool result with `isError: true`.

Mixing transports is rejected when the app is constructed, not at call time — registering a handler
with an `Http.*` marker as a tool raises immediately, naming the offending parameter.

---

## Return types

Every registered function — handler, middleware or tool — must declare what it can hand back, and
`Any` is rejected when the route is built. The generated document has exactly one source for an
operation's responses, so an annotation the generator cannot read is a hole in the spec rather than
a matter of style.

| Role | Legal | Rejected |
|---|---|---|
| handler | `Response[User] \| NotFound` | `Any`, no annotation, `None`, `dict`, a union with a stray arm |
| streaming handler | `AsyncIterator[Event[Tick] \| NotFound]` | bare `AsyncIterator`, `AsyncIterator[Any]` |
| guard middleware | `Unauthorized \| None`, or `None` alone | `Any`, no annotation |
| wrapper middleware | `AsyncIterator[NotFound \| None]`, `AsyncIterator[None]` | `AsyncIterator[Any]` |
| tool | `Response[User]` | `Any`, `None`, a type with no status |

`None` is legal only for middleware, where it means "I did not answer — carry on". A handler that
returns nothing has no response to document, so it is rejected.

Every arm must carry a status code, which is why `-> dict` fails: it would document a bare `200`
with no content. The one thing this cannot check is a *wrong* annotation — nothing verifies that a
middleware typed `AsyncIterator[None]` never yields a `NotFound`.

---

## OpenAPI

```python
from fusion import openapi_route

app = Fusion(routes=[Get("/users/{id:int}", get_user), openapi_route()])
```

`GET /openapi.json` serves an OpenAPI 3.1 document; `app.openapi(title=..., version=...)` returns it
directly. Parameters, their locations, request bodies, per-status responses and shared component
schemas are all derived from the signatures — including `text/event-stream` operations for streaming
handlers. `Inject` parameters never appear: they are not part of the wire contract.

An operation is generated from the **whole chain**, not the handler alone. A middleware binds from
the request exactly as a handler does, so a header it declares is a header this route requires, and
it is documented as one:

```python
async def require_key(x_api_key: Http.Header[str]) -> Unauthorized | None:
    ...

Get("/users/{id:int}", get_user, middlewares=[require_key])
# parameters: id (path, required), x_api_key (header, required)
# responses:  200, 404 from the handler, 401 from the guard
```

A guard's return annotation contributes its statuses, since it answers requests on its own. A
wrapper's annotation describes its generator rather than a response, so it is left out — annotate a
replacement it can yield on the handler if it needs documenting. A header both a handler and a
middleware declare is documented once.

---

## Authentication and authorization

A credential is a parameter like any other — a marker names the scheme that carries it, and that
same declaration is what the OpenAPI document describes. Nothing is configured.

```python
from fusion import Auth, Response, requires


async def get_me(token: Auth.Bearer) -> Response[User]:
    ...                                    # token arrives with "Bearer " stripped
```

| Marker | Scheme | Read from |
|---|---|---|
| `Auth.Bearer` | `http` / `bearer` | `Authorization`, prefix removed |
| `Auth.Basic` | `http` / `basic` | `Authorization`, decoded to `Credentials(username, password)` |
| `Auth.ApiKey` | `apiKey` in header | the header named after the parameter — `x_api_key` → `x-api-key` |
| `Auth.ApiKeyQuery` | `apiKey` in query | the query parameter of that name |
| `Auth.ApiKeyCookie` | `apiKey` in cookie | the cookie of that name |

A missing or malformed credential is **401**, not the 422 a required header would give you — the
resolver raises rather than reporting a missing field.

### Roles

`@requires` declares what an operation needs; roles are AND-ed and stacking the decorator unions
them.

```python
@requires("items:write")
async def update_item(id: Http.Path[int], token: Auth.Bearer) -> Response[Item] | Forbidden:
    ...
```

The framework never decides what a role *means*. It asks the authorizer you hand the application:

```python
class RoleChecker:
    async def authorize(self, roles: frozenset[str]) -> bool:
        request = Request()                          # ambient — nothing is plumbed in
        granted = self._cache.get(...) or await self._store.roles_for(...)
        return roles <= granted


app = Fusion(routes=[...], authorizer=RoleChecker())
```

Returning `False` produces `403 Forbidden` naming the missing roles. Verification, hierarchies,
wildcards and caching all live in your implementation — the framework only asks.

The check runs where the decorated function runs: on a handler it runs after every middleware, on a
middleware it runs at that link, so authentication guards on the outside are reached first.

Two mistakes fail before a request ever arrives: `@requires()` with no roles, and an application
holding a role-protected route but no `authorizer`. A route declaring roles with no `Auth.*`
credential anywhere in its chain is rejected too — a security requirement is keyed by a scheme
name, so roles with no credential could be enforced but never documented.

### What lands in the document

```yaml
components:
  securitySchemes:
    bearerAuth: {type: http, scheme: bearer}
paths:
  /items/{id}:
    patch:
      security: [{bearerAuth: ["items:write"]}]
      responses:
        "401": {...}      # implied by the credential
        "403": {...}      # implied by the roles
```

401 and 403 are added because the framework produces them itself — facts about the operation, not
claims a return annotation has to make. Credentials never appear under `parameters`: OpenAPI
describes them as schemes, and ignores a header parameter named `Authorization` outright.

Not covered: OAuth2 and OpenID Connect flows, OR-ed alternatives ("admin *or* owner"), optional
credentials with an anonymous fallback, and resource-level permissions — deciding whether you may
edit *this* document needs the document, so it stays in the handler.

---

## Sub-requests

`app.execute()` runs one route in-process and hands back what it answered:

```python
result = await app.execute("POST", "/users", body=b'{"name":"ada"}')
result.status    # int
result.headers   # dict[str, str]
result.body      # bytes
```

The request is synthesised and driven down the ordinary ASGI path, so routing, middleware,
authorization and validation behave exactly as they would for a real call. Two consequences worth
knowing:

- **It never raises.** A route that blows up comes back as a captured `500`, an unknown path as a
  `404`, a bad body as a `400`. One bad sub-request cannot take a batch down with it.
- **Authorization is per route.** The calling route's own credentials grant nothing; each
  sub-request re-runs the target's middleware and role checks.

Headers are inherited from the request in progress, with per-call `headers` merged over them by wire
name, so a credential carries into each sub-request unless an item overrides it. `execute` also
works outside a request, with nothing to inherit.

Two refusals, both returned as captured `500`s rather than raised: a route that streams (an
`EventStream` has no transport to be captured into) and nesting deeper than
`Fusion.MAX_SUBREQUEST_DEPTH`, so a route executing itself fails fast instead of exhausting the
stack.

### Batch endpoints

The envelope is your contract, not fusion's — fusion ships only `execute`:

```python
async def batch(body: Http.Body[BatchIn], request: FromContext[Request]) -> Response[BatchOut]:
    """Run several routes in one round trip."""
    app = request.scope["app"]

    async def run(item: SubRequest) -> SubResponse:
        result = await app.execute(item.method, item.path, headers=item.headers, body=item.body)
        return SubResponse(id=item.id, status=result.status, body=msgspec.Raw(result.body))

    return Response(BatchOut(responses=list(await asyncio.gather(*map(run, body.requests)))))
```

Because the envelope is always a `200` carrying per-item statuses, the batch operation's own
document stays complete and accurate. `msgspec.Raw` is worth reaching for on both sides: an item's
body passes through undecoded, and a sub-response splices into the envelope without a re-encode.
Declare it as a plain `Raw` with a `default_factory` — `Raw | None` collapses to null when decoding,
and a `Raw` *default* cannot be rendered into a JSON Schema.

A worked example lives in [tests/fusion/test_batch.py](tests/fusion/test_batch.py).

---

## Testing

```python
from fusion.testing import TestClient


async def test_hello():
    async with TestClient(app) as client:
        response = await client.get("/hello")
        assert response.json() == {"message": "Hello, World!"}
```

`TestClient` runs the application's lifespan; `LifespanManager` exposes the yielded state.

---

## CLI

```bash
fusion serve myapp:app
fusion serve myapp:app --host 127.0.0.1 --port 9000 --reload
```

---

## Requirements

- Python 3.14+
- `msgspec >= 0.21.1`
- `typedprotocol >= 0.1.0`

---

## License

MIT — see [LICENSE.md](LICENSE.md).
