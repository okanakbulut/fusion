"""OpenAPI 3.1 generation from route signatures.

Shares the schema generator with the tool transport, so an object documented
here and an object described in an MCP inputSchema can never disagree.
"""

import typing

import msgspec

from .annotations import FromContext
from .binding import Signature, response_arms, stream_arms, union_arms, yield_type
from .request import Request
from .responses import Forbidden, Problem, Response, Unauthorized
from .route import Route
from .security import SecurityResolver

OPENAPI_VERSION = "3.1.0"
REF_TEMPLATE = "#/components/schemas/{name}"

_PARAMETER_LOCATIONS = ("path", "query", "header", "cookie")


class SchemaCollector:
    """Collects every type the document references, so each is defined once."""

    def __init__(self) -> None:
        self._types: list[typing.Any] = []

    def add(self, typ: typing.Any) -> None:
        if typ is None or typ is typing.Any:
            return
        if typ not in self._types:
            self._types.append(typ)

    def build(self) -> tuple[dict[typing.Any, dict], dict[str, dict]]:
        if not self._types:
            return {}, {}
        schemas, components = msgspec.json.schema_components(self._types, ref_template=REF_TEMPLATE)
        return dict(zip(self._types, schemas, strict=True)), dict(components)


def _signatures(route: Route) -> tuple[Signature, ...]:
    """Everything that shapes the operation, handler first.

    A middleware binds from the request exactly as the handler does, so a header
    it declares is one this operation requires - leaving it out would document a
    request the route rejects.
    """
    return (route.signature, *route.middlewares)


def _credentials(route: Route) -> list[SecurityResolver]:
    """Credential resolvers across the whole chain, in declaration order."""
    found: list[SecurityResolver] = []
    for signature in _signatures(route):
        for resolver in signature.credentials:
            if isinstance(resolver, SecurityResolver):
                found.append(resolver)
    return found


def _roles(route: Route) -> list[str]:
    """Roles the chain requires, as one sorted set: every check must pass."""
    roles: set[str] = set()
    for signature in _signatures(route):
        roles |= signature.roles
    return sorted(roles)


def _security(route: Route) -> list[dict[str, list[str]]]:
    """The operation's security requirement.

    Every scheme in the chain is required, so they share one entry - OpenAPI
    reads a single object as AND.  Roles ride on each of them: they qualify the
    operation, and a requirement object is the only place the document has to
    put them.
    """
    credentials = _credentials(route)
    if not credentials:
        return []
    roles = _roles(route)
    return [{resolver.scheme_name: list(roles) for resolver in credentials}]


def _response_arms(route: Route) -> list[tuple[int, typing.Any, bool]]:
    """Every response the operation can produce, as ``(status, content, is_stream)``.

    A guard middleware answers requests on its own, so its return annotation
    documents statuses the handler never mentions.  A wrapper's annotation
    describes its generator rather than a response, and is skipped.

    A credential adds 401 and a role adds 403 whether or not anyone annotated
    them: the framework produces both itself, so they are facts about the
    operation rather than claims about the handler.
    """
    signature = route.signature
    if signature.is_asyncgen:
        arms: list[tuple[int, typing.Any, bool]] = stream_arms(signature.return_type)
    else:
        arms = [
            (status, content, False) for status, content in response_arms(signature.return_type)
        ]

    documented = {status for status, _, _ in arms}
    for middleware in route.middlewares:
        # A wrapper's *yield* type is what it can answer with; its return type
        # only describes the generator.
        produced = (
            yield_type(middleware.return_type)
            if middleware.is_asyncgen
            else (middleware.return_type)
        )
        for status, content in response_arms(produced):
            if status not in documented:
                documented.add(status)
                arms.append((status, content, False))

    for problem in _implied_problems(route):
        if problem.status_code not in documented:
            documented.add(problem.status_code)
            arms.append((problem.status_code, None, False))
    return arms


def _implied_problems(route: Route) -> tuple[type[Problem], ...]:
    """Problems the framework itself can return for this operation."""
    implied: tuple[type[Problem], ...] = ()
    if _credentials(route):
        implied += (Unauthorized,)
    if _roles(route):
        implied += (Forbidden,)
    return implied


def _collect(route: Route, collector: SchemaCollector) -> None:
    for signature in _signatures(route):
        for resolver in signature.resolvers.values():
            if resolver.location in _PARAMETER_LOCATIONS or resolver.location == "body":
                collector.add(resolver.typ)

    for _, content, _ in _response_arms(route):
        collector.add(content)


def _parameters(route: Route, schemas: dict[typing.Any, dict]) -> list[dict]:
    parameters = []
    seen: set[tuple[str, str]] = set()
    for signature in _signatures(route):
        for name, resolver in signature.resolvers.items():
            if resolver.location not in _PARAMETER_LOCATIONS:
                continue
            # OpenAPI keys a parameter by name and location, so a header the
            # handler and a middleware both declare is documented once.
            if (name, resolver.location) in seen:
                continue
            seen.add((name, resolver.location))
            parameters.append(
                {
                    "name": name,
                    "in": resolver.location,
                    "required": name in signature.required,
                    "schema": schemas.get(resolver.typ, {}),
                }
            )
    return parameters


def _request_body(route: Route, schemas: dict[typing.Any, dict]) -> dict | None:
    for signature in _signatures(route):
        for resolver in signature.resolvers.values():
            if resolver.location == "body":
                return {
                    "required": resolver.name in signature.required,
                    "content": {"application/json": {"schema": schemas.get(resolver.typ, {})}},
                }
    return None


def _responses(route: Route, schemas: dict[typing.Any, dict]) -> dict[str, dict]:
    responses: dict[str, dict] = {}

    for status, content, is_stream in _response_arms(route):
        if content is None:
            media = "application/problem+json"
            body = {"$ref": REF_TEMPLATE.format(name="Problem")}
        elif is_stream:
            media = "text/event-stream"
            body = schemas.get(content, {})
        else:
            media = "application/json"
            body = schemas.get(content, {})
        responses[str(status)] = {
            "description": "",
            "content": {media: {"schema": body}},
        }

    return responses or {"200": {"description": ""}}


def _openapi_path(path: str) -> str:
    """Rewrite ``{id:int}`` as OpenAPI's ``{id}``; the type lives in the schema."""
    out = []
    for segment in path.split("/"):
        if segment.startswith("{") and segment.endswith("}"):
            out.append("{" + segment[1:-1].split(":", 1)[0] + "}")
        else:
            out.append(segment)
    return "/".join(out)


def generate(
    routes: typing.Iterable[Route],
    *,
    title: str = "Fusion",
    version: str = "0.1.0",
    description: str | None = None,
) -> dict[str, typing.Any]:
    """Build an OpenAPI document from the application's routes."""
    routes = list(routes)

    collector = SchemaCollector()
    for route in routes:
        _collect(route, collector)
    schemas, components = collector.build()

    paths: dict[str, dict[str, typing.Any]] = {}
    for route in routes:
        operation: dict[str, typing.Any] = {
            "operationId": route.operation_id,
            "responses": _responses(route, schemas),
        }
        if route.summary:
            operation["summary"] = route.summary
        if route.description:
            operation["description"] = route.description
        if route.tags:
            operation["tags"] = list(route.tags)
        if parameters := _parameters(route, schemas):
            operation["parameters"] = parameters
        if body := _request_body(route, schemas):
            operation["requestBody"] = body
        if security := _security(route):
            operation["security"] = security

        entry = paths.setdefault(_openapi_path(route.path), {})
        for method in route.methods:
            entry[method.value.lower()] = dict(operation)

    info: dict[str, typing.Any] = {"title": title, "version": version}
    if description:
        info["description"] = description

    components.setdefault("Problem", _problem_schema())

    document_components: dict[str, typing.Any] = {"schemas": components}
    if schemes := _security_schemes(routes):
        document_components["securitySchemes"] = schemes

    return {
        "openapi": OPENAPI_VERSION,
        "info": info,
        "paths": paths,
        "components": document_components,
    }


def _security_schemes(routes: typing.Iterable[Route]) -> dict[str, dict]:
    """Every credential the application declares, defined once."""
    schemes: dict[str, dict] = {}
    for route in routes:
        for resolver in _credentials(route):
            schemes.setdefault(resolver.scheme_name, resolver.scheme())
    return schemes


def _problem_schema() -> dict[str, typing.Any]:
    return {
        "type": "object",
        "title": "Problem",
        "description": "RFC 9457 problem details.",
        "properties": {
            "type": {"type": "string"},
            "status": {"type": "integer"},
            "title": {"type": "string"},
            "detail": {"type": ["string", "null"]},
            "instance": {"type": ["string", "null"]},
        },
        "required": ["type", "status", "title"],
    }


async def openapi_endpoint(request: FromContext[Request]) -> Response[typing.Any]:
    """OpenAPI schema for this application."""
    return Response(content=request.scope["app"].openapi(), media_type="application/json")


def openapi_route(path: str = "/openapi.json") -> Route:
    """A route serving the generated schema.  Add it to ``Fusion(routes=[...])``."""
    from .types import Method

    return Route(path=path, handler=openapi_endpoint, method=Method.GET)
