"""MCP server: tools/list and tools/call over JSON-RPC, mounted as an HTTP route."""

import typing

import msgspec

from .annotations import FromContext
from .exceptions import ValidationException
from .request import Request
from .responses import Response
from .route import Route
from .tools import ToolDef, result_content
from .types import Method

PROTOCOL_VERSION = "2025-06-18"

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

_encoder = msgspec.json.Encoder()


def _error(request_id: typing.Any, code: int, message: str) -> dict[str, typing.Any]:
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


def _result(request_id: typing.Any, result: typing.Any) -> dict[str, typing.Any]:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def server_info(app: typing.Any) -> dict[str, typing.Any]:
    return {
        "protocolVersion": PROTOCOL_VERSION,
        "capabilities": {"tools": {"listChanged": False}},
        "serverInfo": {"name": "fusion", "version": _version()},
    }


def _version() -> str:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("fusion")
    except PackageNotFoundError:  # pragma: no cover - only when running from a source tree
        return "0.0.0"


async def call_tool(
    tools: dict[str, ToolDef], params: typing.Mapping[str, typing.Any]
) -> dict[str, typing.Any]:
    """Execute one tools/call, mapping the handler's return onto a tool result."""
    name = params.get("name")
    tool = tools.get(name) if isinstance(name, str) else None
    if tool is None:
        return {
            "content": [{"type": "text", "text": f"Unknown tool: {name!r}"}],
            "isError": True,
        }

    arguments = params.get("arguments")
    if arguments is None:
        arguments = {}
    # Checked before any falsy-to-{} coercion, or `"arguments": []` slips through.
    if not isinstance(arguments, dict):
        return {
            "content": [{"type": "text", "text": "'arguments' must be an object"}],
            "isError": True,
        }

    try:
        returned = await tool.call(arguments)
    except ValidationException as exc:
        detail = exc.detail or "; ".join(f"{e.field}: {e.message}" for e in (exc.errors or []))
        return {"content": [{"type": "text", "text": detail}], "isError": True}

    payload, is_error = result_content(returned)
    text = payload if isinstance(payload, str) else _encoder.encode(payload).decode()
    return {"content": [{"type": "text", "text": text}], "isError": is_error}


async def dispatch(app: typing.Any, message: typing.Any) -> dict[str, typing.Any] | None:
    """Handle one JSON-RPC message.  Returns None for a notification."""
    if not isinstance(message, dict):
        return _error(None, INVALID_REQUEST, "Request must be a JSON object")

    request_id = message.get("id")
    is_notification = "id" not in message
    method = message.get("method")

    if not isinstance(method, str):
        return None if is_notification else _error(request_id, INVALID_REQUEST, "Missing method")

    params = message.get("params")
    if params is None:
        params = {}

    if method == "initialize":
        result: typing.Any = server_info(app)
    elif method == "notifications/initialized":
        return None
    elif method == "tools/list":
        result = {"tools": [tool.definition() for tool in app.tools.values()]}
    elif method == "tools/call":
        if not isinstance(params, dict):
            return _error(request_id, INVALID_PARAMS, "'params' must be an object")
        result = await call_tool(app.tools, params)
    else:
        return None if is_notification else _error(request_id, METHOD_NOT_FOUND, method)

    return None if is_notification else _result(request_id, result)


async def mcp_endpoint(request: FromContext[Request]) -> Response[typing.Any]:
    """Model Context Protocol endpoint."""
    app = request.scope["app"]
    try:
        message = msgspec.json.decode(await request.body())
    except msgspec.DecodeError as exc:
        return _as_response(_error(None, PARSE_ERROR, str(exc)))

    if isinstance(message, list):
        replies = [r for m in message if (r := await dispatch(app, m)) is not None]
        return _as_response(replies or None)

    return _as_response(await dispatch(app, message))


def _as_response(payload: typing.Any) -> Response[typing.Any]:
    if payload is None:
        # A notification-only request gets an accepted-but-empty reply.
        return Response(content=None, media_type="application/json")
    return Response(content=payload, media_type="application/json")


def mcp_route(path: str = "/mcp") -> Route:
    """A route serving the MCP endpoint.  Add it to ``Fusion(routes=[...])``."""
    return Route(path=path, handler=mcp_endpoint, method=Method.POST)
