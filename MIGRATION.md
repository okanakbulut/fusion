# Migration guide — class handlers → function handlers

> **Temporary document.** This covers the one-time move from the class-based API to the
> function-based one. Fold what's useful into the README and delete this once the migration is done.

Every endpoint used to need two classes: a `Request` subclass holding request-scoped parameters and
a `Handler` subclass holding dependencies, wired together by introspecting `handle`'s `request:`
annotation. Now a handler is a single `async def` function whose signature carries both.

There is **no compatibility shim** — `Handler` is gone rather than deprecated, so everything below
is a required change, not a recommendation. In exchange the same signature now also generates your
OpenAPI operations and MCP tool schemas.

---

## 1. Import map

| Before | After |
|---|---|
| `Handler` | *(removed — write a function)* |
| `PathParam[T]` | `Http.Path[T]` |
| `QueryParam[T]` | `Http.Query[T]` |
| `Header[T]` | `Http.Header[T]` |
| `Cookie[T]` | `Http.Cookie[T]` |
| `Body[T]` / `RequestBody[T]` | `Http.Body[T]` |
| `db: Database` (bare annotation) | `db: Inject[Database]` |
| `request: Request` (handler arg) | `request: FromContext[Request]` |
| `fusion.renderers` | *(removed — was never invoked)* |
| `BaseMiddleware` / `Middleware(cls, ...)` | *(removed — write a function; see §6)* |
| `Fusion.dispatch(path, method, params)` | `Fusion.execute(method, path, ...)` — see §8 |
| `fusion.types.Match`, `HttpRoute.match` | *(removed — never implemented)* |
| `InjectableResolver`, `FactoryResolver` | `DependencyResolver` |
| `fusion.resolvers.__factories__`, `has_factory` | *(removed — see §3)* |

New exports: `Http`, `Tool`, `Inject`, `FromContext`, `Auth`, `requires`, `Authorizer`,
`Credentials`, `Event`, `EventStream`, `ToolDef`, `Transport`, `Signature`, `bind`, `field`,
`openapi_route`.

`Object`, `Injectable`, `Response`, the `Problem` family, `factory`, `Route` and the verb
shorthands all keep their names.

---

## 2. Handlers

Collapse the two classes into one function. Parameters that lived on the `Request` subclass and
dependencies that lived on the `Handler` become parameters side by side.

**Before**

```python
class UserRequest(Request, kw_only=True):
    id: PathParam[int]
    verbose: QueryParam[bool] = False

class GetUserHandler(Handler):
    db: Database

    async def handle(self, request: UserRequest) -> Response[User] | NotFound:
        user = await self.db.fetch(request.id)
        if user is None:
            return NotFound(detail="no such user")
        return Response(user)

app = Fusion(routes=[Get("/users/{id:int}", handler=GetUserHandler)])
```

**After**

```python
async def get_user(
    id: Http.Path[int],
    db: Inject[Database],
    verbose: Http.Query[bool] = False,
) -> Response[User] | NotFound:
    """Fetch one user."""
    user = await db.fetch(id)
    if user is None:
        return NotFound(detail="no such user")
    return Response(user)

app = Fusion(routes=[Get("/users/{id:int}", get_user)])
```

Three things to note:

- `self.db` becomes the `db` parameter; `request.id` becomes the `id` parameter.
- **Put `Inject` parameters before defaulted ones.** Python forbids a non-default parameter after a
  defaulted one, so `(id, db, verbose=False)` compiles and `(id, verbose=False, db)` does not.
  Order is otherwise irrelevant — the binder always calls with keyword arguments.
- The docstring is no longer decoration: its first line becomes the OpenAPI `summary`, the rest the
  `description`, and for tools it becomes the description the model reads.

### Handlers that only needed the raw request

**Before**

```python
class PingHandler(Handler):
    async def handle(self, request: Request) -> Response[Out]:
        return Response(Out(agent=request.headers.get("user_agent")))
```

**After** — drop the parameter entirely if you don't use it, or ask for the façade explicitly:

```python
async def ping(request: FromContext[Request]) -> Response[Out]:
    return Response(Out(agent=request.headers.get("user_agent")))
```

`Request` is now a live view over the active context with no fields of its own. **Subclassing it to
declare parameters no longer does anything** — those go in the signature.

---

## 3. Dependency injection

A bare annotation no longer resolves. `Inject[T]` is now the only way to ask for a dependency, on
both function parameters and `Injectable` fields.

```python
# before
class Deps(Injectable):
    db: Database
    session: Session

# after
class Deps(Injectable):
    db: Inject[Database]
    session: Inject[Session]
```

### Factories move onto an object

`@factory` no longer registers anything. It marks a function as producing its return type, and the
factories for an application are collected from an object you hand it.

**Before** — the registry was process-wide and filled as a side effect of importing the module,
which is why wiring an application meant an import that referenced no name:

```python
# di.py
@factory
async def database_factory() -> Database:
    return Database(DSN)

@factory
@asynccontextmanager
async def session_factory() -> AsyncIterator[Session]:
    async with Database(DSN).session() as s:   # no way to ask for the Database
        yield s

# app.py
import myapp.di  # noqa: F401
app = Fusion(routes=[...])
```

**After** — the factories are methods on a plain `Object`, and the application is given that object:

```python
# di.py
class Deps(Object):
    dsn: str

    @factory
    async def database(self) -> Database:
        return Database(self.dsn)

    @factory
    @asynccontextmanager
    async def session(self, db: Inject[Database]) -> AsyncIterator[Session]:
        async with db.session() as s:
            yield s

# app.py
from .di import Deps
app = Fusion(routes=[...], factories=Deps(dsn=DSN))
```

Per application, mechanically:

- Collect every `@factory` function into one class inheriting `Object`, and give each one `self`.
- Delete the `import ...  # noqa: F401` lines whose only job was executing those registrations.
- Pass the instance as `Fusion(factories=...)`. Several objects can be passed as a list.
- Move the module-level constants the factories closed over onto the class as fields. They are
  validated like any other `Object` field.
- Drop any import of `fusion.resolvers.__factories__` or `has_factory`; both are gone.

Two things you get in exchange. **A factory may now declare dependencies of its own** — it binds like
a handler, so `Inject[...]` in its signature resolves through the same per-call cache. And **a missing
factory is refused when the application is constructed**, naming the route and the parameter, instead
of reaching a caller as a 500 on whichever request hits that route first:

```
ValueError: Route '/thing' injects Db for 'db', but this application was built without a
factory for it. Add an @factory method producing Db to the object you pass as
Fusion(factories=...).
```

### Test doubles replace a subclass, not a global

```python
# before — mutated state every other test shared, and needed save/restore around it
__factories__[Database] = fake_db_factory

# after
class FakeDeps(Deps):
    @factory
    async def database(self) -> Database:
        return FakeDatabase()

app = Fusion(routes=[...], factories=FakeDeps(dsn="unused"))
```

An override has to **reuse the method name** — that is what makes it replace the one above it in the
MRO. Overriding under a different name leaves both factories claiming the same type, which is an
error naming both.

If you kept a fixture that saved and restored `__factories__` between tests, delete it rather than
adapting it. Two applications now hold two objects, so there is nothing left to leak.

### Import order still does not matter

The previous release lifted the requirement that a factory be registered before its consumers were
defined. That still holds, for a better reason: factory methods live in a class body, so there is no
ordering between a factory and the handler that injects it to get wrong. The
injectable-vs-factory question is settled once when the application is constructed rather than on
first use.

### One `Route` object belongs to one set of factories

Wiring happens on the resolvers a `Route` owns, so a module-level `ROUTES = [...]` shared by an app
and its tests is only safe while both wire it the same way. Two applications built from the **same**
factories object may share the list — the answer for every type is the same one, so there is nothing
to disagree about. Two applications built from **different** factories objects may not, and it is
reported at construction rather than becoming a route served by whichever application was built last:

```
ValueError: Route '/thing' is already wired by another application whose factory for Db is a
different one, which would leave it built by whichever application was constructed last. A Route
wired by two sets of factories belongs to one of them - build the routes for each.
```

An app under test alongside the real one is exactly that case — `FakeDeps()` is a different object
from `Deps()` — so give the application under test its own route list. Sharing a handler *function*
is always free: every `Get("/x", handler)` builds its own resolvers.

An `Injectable` carries no such restriction. Its resolver table is built once per class and belongs
to no application, so `settle` copies it per injection site: any number of applications may inject
the same `Injectable`, each building its fields from its own factories, and none of them writes
anything to the class.

---

## 4. Routing

`Route` and the verb shorthands take a function where they took a class. The handler may be passed
positionally, which reads better than the old keyword form:

```python
Get("/users/{id:int}", get_user)                  # preferred
Get("/users/{id:int}", handler=get_user)          # still works
Route("/items/{id:int}", item, methods=["GET", "DELETE"])
```

New optional keywords on `Route` and every shorthand: `summary`, `description`, `tags`,
`operation_id` (all feed OpenAPI) and `keepalive` (streaming only).

---

## 5. Behaviour changes

These are the ones that will not raise an error — code keeps running and does something different.
Read this section even if everything else compiles.

### `methods=[...]` now registers every method

Previously only `methods[0]` was registered and the rest were silently dropped, despite the README
documenting otherwise. `Route("/items/{id}", h, methods=["GET", "DELETE"])` used to answer **GET
only**; it now answers both.

If you relied on the old behaviour — for instance a second `Route` registering `DELETE` on the same
path — the later registration now overwrites the first. Check any path with more than one `Route`.

### A dependency is built once per call

Two parameters of the same type used to invoke the factory twice; they now share one instance.
**If your factory has side effects** — incrementing a counter, opening a second connection, writing
an audit row — the count changes. This is also the largest performance difference in the refactor.

### Falsy response content

`Response.__call__` used `self.content or ""`, so `Response(content=0)` serialized as `""`,
`Response(content=[])` as `""`, and an empty struct likewise. They now serialize as `0`, `[]` and
`{}`. Any client that special-cased the empty string needs updating.

### Optional headers and cookies actually work

`Http.Header[str] = ""` used to fail with `Expected 'str', got 'null'` when the header was absent —
the resolver converted `None` instead of falling back to the default. Absent headers and cookies now
fall through to the parameter's default, matching how query parameters always behaved.

### Missing required values are validation errors

A missing required parameter used to surface as a type-conversion error or a `TypeError`. It is now
a proper field error, so the response is a 400 naming the parameter:

```json
{"field": "q", "location": "query", "message": "Missing required value"}
```

Errors are also aggregated across **all** parameters — including dependencies, which previously
failed fast with a different error shape — so one 400 reports every problem with the request.

### `Problem.status` → `Problem.status_code`

Custom problem subclasses must be updated. The RFC 9457 wire member stays `status`; only the class
attribute is renamed, so response bodies are unchanged.

```python
class OutOfStock(Problem):
    type: typing.ClassVar[str] = "https://example.com/problems/out-of-stock"
    status_code: typing.ClassVar[int] = 409     # was: status
    title: str = "Out of Stock"
```

### `field(description=...)` and `deprecated` now reach the schema

Both were accepted and silently discarded before. They now appear in generated JSON Schema, so
descriptions you had already written will start showing up in OpenAPI and MCP output.

### Contexts may nest

`Context.__aenter__` no longer raises `RuntimeError("Nested context is not allowed")`. This is what
lets an MCP tool call run inside the HTTP request that carried it. `Fusion.dispatch` also unwinds
its sub-context properly now — previously anything it opened was never closed.

---

## 6. Middleware

Middleware followed handlers out of classes. `BaseMiddleware` and the `Middleware(cls, ...)`
deferred-construction wrapper are both gone, and `middlewares=` now takes functions directly.
Nothing is passed along the chain to call: a middleware is a guard or a wrapper, told apart by
whether it yields.

**Before**

```python
class AuthMiddleware(BaseMiddleware):
    async def handle(self, request: Request):
        if not request.headers.get("authorization", "").startswith("Bearer "):
            return Unauthorized(detail="Missing or invalid token")
        return await self.app.handle(request)


class TimingMiddleware(BaseMiddleware):
    async def handle(self, request: Request):
        started = time.monotonic()
        response = await self.app.handle(request)
        response.headers["x-ms"] = f"{(time.monotonic() - started) * 1000:.1f}"
        return response


Get("/protected", handler, middlewares=[Middleware(AuthMiddleware), Middleware(TimingMiddleware)])
```

**After**

```python
async def require_token(authorization: Http.Header[str] = "") -> Unauthorized | None:
    if not authorization.startswith("Bearer "):
        return Unauthorized(detail="Missing or invalid token")


async def timing() -> typing.AsyncIterator[None]:
    started = time.monotonic()
    response = yield
    response.headers["x-ms"] = f"{(time.monotonic() - started) * 1000:.1f}"


Get("/protected", handler, middlewares=[require_token, timing])
```

- A middleware that only inspects the request loses the forwarding line entirely: return a response
  to answer, return nothing to fall through.
- A middleware that also needs the response becomes an async generator. `await self.app.handle(...)`
  becomes a bare `yield`, and what came back from that call comes back from the `yield`.
- The signature binds like a handler's: `Http.*` markers, `Inject[...]` and `FromContext[Request]`
  all work, so reaching for `request.headers` is now optional rather than the only way in.
- Constructor arguments (`Middleware(Tagging, tag="hello")`) become a closure returning the
  middleware: `middlewares=[tagging(tag="hello")]`.
- Ordering is unchanged — first in the list is outermost.
- New, and not expressible before: `try`/`except`/`finally` around the `yield`. A failure in the
  route is thrown back in at that point, and yielding a second value replaces the response.
- Return annotations are now mandatory and checked at registration: `Any` is rejected for handlers,
  middleware and tools alike, and every arm must carry a status code. A guard says `Unauthorized |
  None`, a wrapper says `AsyncIterator[NotFound | None]`. This is what makes the generated document
  complete rather than best-effort.
- Because a middleware's parameters bind from the request, they are now part of the generated
  OpenAPI operation: a header a middleware requires is documented on every route that uses it, and a
  guard's return annotation contributes its statuses. Under the old classes this was invisible to
  the generator, so a middleware that read a header silently made the document wrong.

---

## 7. What you get for it

None of this existed before the refactor.

**Declarative auth** — a credential is a marked parameter, so one declaration extracts it, produces
401 when it is absent, and describes itself in the document as a security scheme:

```python
@requires("items:write")
async def update_item(id: Http.Path[int], token: Auth.Bearer) -> Response[Item] | Forbidden: ...

app = Fusion(routes=[...], authorizer=RoleChecker())
```

`@requires` declares roles; an `Authorizer` you pass to the application decides, so verification and
caching stay yours. The generated operation carries `security: [{bearerAuth: ["items:write"]}]` plus
401 and 403. See the README for the whole surface and what it does not cover.

**Server-sent events** — an async generator handler is streamed as `text/event-stream`. Yield a
problem and return to fail before the stream starts:

```python
async def order_events(id: Http.Path[int]) -> AsyncIterator[Event[OrderEvent] | NotFound]:
    """Stream status changes."""
    if await db.fetch_order(id) is None:
        yield NotFound(detail="no such order")
        return
    async for change in db.watch_order(id):
        yield Event(data=change, id=str(change.seq), event=change.kind)

Get("/orders/{id:int}/events", order_events, keepalive=15.0)
```

**MCP tools** — a function whose non-injected parameters are all `Tool.Arg`:

```python
async def search_users(q: Tool.Arg[str], db: Inject[Database], limit: Tool.Arg[int] = 10) -> Response[list[User]]:
    """Search users by name."""
    return Response(await db.search(q, limit))

app = Fusion(routes=[mcp_route()], tools=[search_users])
```

**OpenAPI** — `Fusion(routes=[..., openapi_route()])` serves an OpenAPI 3.1 document at
`/openapi.json`, derived from the same signatures.

**Dependencies that build dependencies** — a factory binds like a handler, so it can inject what it
needs instead of reaching for a global:

```python
class Deps(Object):
    dsn: str

    @factory
    async def database(self) -> Database:
        return Database(self.dsn)

    @factory
    async def repo(self, db: Inject[Database]) -> Repository:
        return Repository(db)
```

Mixing transports is rejected when the app is constructed, not on the first call: registering a
handler carrying an `Http.*` marker as a tool raises immediately and names the parameter. So is an
unprovided dependency, and a cycle between two factories — `Fusion(...)` walks the whole graph before
it will build.

---

## 8. Troubleshooting

Real messages, and what they mean:

| Message | Cause | Fix |
|---|---|---|
| `module 'fusion' has no attribute 'Handler'` | class-based handler | rewrite as `async def` (§2) |
| `module 'fusion' has no attribute 'PathParam'` | flat marker name | `Http.Path[...]` (§1) |
| `No module named 'fusion.renderers'` | removed dead module | delete the import |
| `Parameter 'db' on 'h' is annotated <class 'DB'>, which carries no Fusion marker` | bare dependency | `Inject[DB]` (§3) |
| `Parameter 'q' on 'tool' uses a 'http' marker, which has no meaning here` | HTTP marker on a tool | use `Tool.Arg`, or register it as a route |
| `Tool 'x' is an async generator, but a tool call has no streaming result shape` | streaming tool | return a `Response`; streaming is HTTP-only |
| `type object 'Problem' has no attribute 'status'` | renamed attribute | `status_code` (§5) |
| `Handler 'H' must be defined with 'async def'` | **usually a class was passed** | the message is misleading here — if `H` is a class, rewrite it as a function (§2) |
| `cannot import name '__factories__' from 'fusion.resolvers'` | the global registry is gone | collect the factories onto an object (§3) |
| `Route '/x' injects DB for 'db', but this application was built without a factory for it` | factory never reached the app | pass the object as `Fusion(factories=...)` (§3) |
| `Fusion(factories=...) expects an instance, got the class 'Deps' itself` | passed the class | `Deps(...)` |
| `'Deps.b' produces DB, which 'Deps.a' already produces` | two factories for one type | keep one; to override, reuse the method name (§3) |
| `Factories cannot be resolved: DB needs Session needs DB` | factories depend on each other | break the cycle; one of them takes what it needs as a field |
| `Route '/x' is already wired by another application whose factory for DB is a different one` | one `Route` object, two different factories objects | build the route list per application (§3) |

---

## 9. Checklist

- [ ] Every `Handler` subclass is now an `async def` function
- [ ] Every `Request` subclass is deleted, its fields moved into signatures
- [ ] All markers namespaced: `Http.Path` / `Http.Query` / `Http.Header` / `Http.Cookie` / `Http.Body`
- [ ] Every dependency wrapped in `Inject[...]`, on functions *and* `Injectable` fields
- [ ] `Inject` parameters placed before defaulted ones
- [ ] Custom `Problem` subclasses use `status_code`
- [ ] Paths registered with `methods=[...]` re-checked for overwrites
- [ ] Factories with side effects re-checked against per-call caching
- [ ] Every `@factory` collected onto an `Object` and passed as `Fusion(factories=...)`
- [ ] Registration-only imports (`import ...di  # noqa: F401`) deleted
- [ ] Test doubles rewritten as subclasses; any `__factories__` save/restore fixture deleted
- [ ] Route lists shared by applications with *different* factories split per application
- [ ] Clients re-checked for falsy response content (`0`, `[]`, `{}` instead of `""`)
- [ ] `fusion.renderers` imports removed
- [ ] Tests updated — handlers defined inside test bodies are the bulk of the work

---

## 10. `dispatch` → `execute`

`Fusion.dispatch` is gone. It returned a live response object, could not send a body, and handed
back `None` for an unknown path — workable for calling one route, wrong for running a batch.

**Before**

```python
response = await request.scope["app"].dispatch("/inner", "GET", {"q": "hi"})
if response is None:
    ...                                  # you translate the miss into a 404 yourself
```

**After**

```python
result = await request.scope["app"].execute("GET", "/inner", query={"q": "hi"})
result.status, result.headers, result.body
```

| | `dispatch` | `execute` |
|---|---|---|
| arguments | `(path, method, params)` | `(method, path, headers=, query=, body=)` |
| returns | live `Response`, or `None` | `CapturedResponse` — status, headers, bytes |
| request body | not supported | `body=b"..."` |
| headers | whole parent scope reused | inherited, with per-call overrides |
| a route that raises | propagates into the caller | captured as a `500` |
| unknown path | `None` | captured as a `404` |
| streaming route | streamed into the parent's send | refused with a captured `500` |
| recursion | unbounded | bounded by `Fusion.MAX_SUBREQUEST_DEPTH` |

Mechanically it synthesises a request and drives the ordinary ASGI path rather than calling the
route directly, which is why the error mapping, middleware and per-route authorization all behave
exactly as they do for a real call.

Porting is usually three lines: swap the argument order, drop the `None` check, and read
`result.body` instead of the response object. If you were building a batch envelope, the shape of
one is worked through in `tests/fusion/test_batch.py`.
