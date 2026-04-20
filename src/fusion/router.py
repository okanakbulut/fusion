import re
import typing

import msgspec

from .context import Context
from .protocols import HttpRequest, HttpRoute
from .responses import BadRequest, InternalServerError, MethodNotAllowed, NotFound
from .types import Method, Receive, Scope, Send

# regex to match path segments like "{path_param[:(int|uuid|/regex_pattern/)]}"
segment_type_pattern = re.compile(r"^\{([a-zA-Z_][a-zA-Z0-9_]*)(?::(int|uuid|.+))?\}$")
supported_types: dict[str, type] = {"str": str, "int": int, "uuid": str}  # Extend as needed
type_patterns = {
    "int": re.compile(r"^\d+$"),
    "uuid": re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
    ),
}

MAX_PATH_DEPTH = 50


class PathSegment(msgspec.Struct, frozen=True):
    """Represents a segment of a route path, which can be static or a parameterized segment."""

    name: str
    pattern: re.Pattern[str] | None

    @classmethod
    def create(cls, segment: str):
        name = segment
        pattern = None
        if match := segment_type_pattern.match(segment):
            name = match.group(1)
            if match.group(2) is None:
                pattern = re.compile(r"^.+$")
            else:
                pattern = type_patterns.get(match.group(2), re.compile(f"^{match.group(2)}$"))

        return cls(name=name, pattern=pattern)

    def match(self, segment: str) -> tuple[bool, str, typing.Any]:
        if self.pattern:
            if not self.pattern.match(segment):
                return False, "", None

            return True, self.name, segment
        return self.name == segment, "", None


class RouteNode(msgspec.Struct):
    """
    Represents a node in the route tree, holding routes and child nodes.
            api/v1
           /       \
        users     items
        /            \
    {id}            {item_id}
(GET -> GetUserHandler)   (GET -> GetItemHandler)
    
    """

    routes: dict[Method, HttpRoute] = msgspec.field(default_factory=lambda: dict())
    children: dict[PathSegment, typing.Self] = msgspec.field(default_factory=lambda: dict())


class TreeRouter:
    def __init__(self, routes: list[HttpRoute]) -> None:
        self.root = RouteNode()
        for route in routes:
            self._insert_route(route)

    def _insert_route(self, route: HttpRoute[typing.Any, typing.Any]) -> None:
        current_node = self.root

        for segment in route.path.strip("/").split("/"):
            path_segment = PathSegment.create(segment)

            if path_segment not in current_node.children:
                current_node.children[path_segment] = RouteNode()

            current_node = current_node.children[path_segment]

        current_node.routes[route.method] = route

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        async with Context(scope, receive, send) as ctx:
            path_params: dict[str, typing.Any] = {}  # To store extracted path parameters
            path_segments = ctx.path.strip("/").split("/")

            if len(path_segments) > MAX_PATH_DEPTH:
                return await NotFound(detail="Route not found")(scope, receive, send)

            current_node = self.root

            for segment in path_segments:
                matched_child = None
                for path_segment, child_node in current_node.children.items():
                    is_match, name, value = path_segment.match(segment)
                    if is_match:
                        matched_child = child_node
                        if name:
                            path_params[name] = value

                        break

                if matched_child is None:
                    return await NotFound(detail="Route not found")(scope, receive, send)

                current_node = matched_child

            # If we reached here, we found a matching route
            if route := current_node.routes.get(ctx.method):
                scope["path_params"] = path_params
                try:
                    request_class = route.get_request_class()
                    request = await request_class.instance()
                    response = await route.handle(request)
                except (ValueError, msgspec.ValidationError) as exc:
                    response = BadRequest(detail=str(exc))
                except Exception:
                    # TODO: log the exception here
                    response = InternalServerError()
                return await response(scope, receive, send)

            return await MethodNotAllowed()(scope, receive, send)
