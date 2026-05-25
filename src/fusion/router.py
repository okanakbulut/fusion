import re
import typing

import msgspec

from .protocols import HttpRequest
from .route import Route
from .types import Method

# regex to match path segments like "{path_param[:(int|uuid|/regex_pattern/)]}"
segment_type_pattern = re.compile(r"^\{([a-zA-Z_][a-zA-Z0-9_]*)(?::(int|uuid|.+))?\}$")
supported_types: dict[str, type] = {"str": str, "int": int, "uuid": str}  # Extend as needed
type_patterns = {
    "int": re.compile(r"^\d+$"),
    "uuid": re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
    ),
    "pk": re.compile(r"^.+-[a-zA-Z0-9]{4,}$"),
}

# Apps register transforms at startup, e.g.:
#   type_transforms["pk"] = lambda s: parse_public_key(s)[1]
# Fusion ships this empty — the framework itself does not register any transforms.
type_transforms: dict[str, typing.Callable[[str], typing.Any]] = {}

MAX_PATH_DEPTH = 50


class PathSegment(msgspec.Struct, frozen=True):
    """Represents a segment of a route path, which can be static or a parameterized segment."""

    name: str
    pattern: re.Pattern[str] | None
    type_name: str | None = None

    @classmethod
    def create(cls, segment: str):
        name = segment
        pattern = None
        type_name = None
        if match := segment_type_pattern.match(segment):
            name = match.group(1)
            constraint = match.group(2)
            if constraint is None:
                pattern = re.compile(r"^.+$")
            else:
                pattern = type_patterns.get(constraint, re.compile(f"^{constraint}$"))
                if constraint in type_patterns:
                    type_name = constraint

        return cls(name=name, pattern=pattern, type_name=type_name)

    def match(self, segment: str) -> tuple[bool, str, typing.Any]:
        if self.pattern:
            if not self.pattern.match(segment):
                return False, "", None

            if self.type_name is not None:
                transform = type_transforms.get(self.type_name)
                if transform is not None:
                    try:
                        return True, self.name, transform(segment)
                    except Exception:
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

    routes: dict[Method, Route] = msgspec.field(default_factory=lambda: dict())
    children: dict[PathSegment, typing.Self] = msgspec.field(default_factory=lambda: dict())


class TreeRouter:
    def __init__(self, routes: list[Route]) -> None:
        self.root = RouteNode()
        for route in routes:
            self._insert_route(route)

    def _insert_route(self, route: Route[typing.Any, typing.Any]) -> None:
        current_node = self.root

        for segment in route.path.strip("/").split("/"):
            path_segment = PathSegment.create(segment)

            if path_segment not in current_node.children:
                current_node.children[path_segment] = RouteNode()

            current_node = current_node.children[path_segment]

        current_node.routes[route.method] = route

    def resolve(
        self, path: str, method: Method
    ) -> tuple[Route[typing.Any, typing.Any], dict[str, typing.Any]] | None:
        """Return (route, path_params) for the given path and method, or None."""
        path_params: dict[str, typing.Any] = {}
        path_segments = path.strip("/").split("/")

        if len(path_segments) > MAX_PATH_DEPTH:
            return None

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
                return None

            current_node = matched_child

        route = current_node.routes.get(method)
        if route is None:
            return None

        return route, path_params

    def _has_path(self, path: str) -> bool:
        """Return True if path traversal reaches a node in the tree (any method)."""
        path_segments = path.strip("/").split("/")
        current_node = self.root

        for segment in path_segments:
            matched_child = None
            for path_segment, child_node in current_node.children.items():
                is_match, _, _ = path_segment.match(segment)
                if is_match:
                    matched_child = child_node
                    break

            if matched_child is None:
                return False

            current_node = matched_child

        return True
