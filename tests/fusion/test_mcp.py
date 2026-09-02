import pytest

from fusion import Fusion, Inject, NotFound, Object, Response, Tool, factory
from fusion.mcp import PROTOCOL_VERSION, mcp_route

from .conftest import client_for


class User(Object):
    id: int
    name: str


async def search(q: Tool.Arg[str], limit: Tool.Arg[int] = 10) -> Response[list[User]]:
    """Search users by name."""
    return Response([User(id=1, name=q)][:limit])


async def missing(id: Tool.Arg[int]) -> Response[User] | NotFound:
    """Always reports the user as missing."""
    return NotFound(detail=f"no user {id}")


def build_app() -> Fusion:
    return Fusion(routes=[mcp_route()], tools=[search, missing])


async def rpc(client, method, params=None, request_id=1):
    payload = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        payload["params"] = params
    return (await client.post("/mcp", json=payload)).json()


@pytest.mark.asyncio
async def test_initialize_advertises_tools():
    async with client_for(build_app()) as client:
        result = (await rpc(client, "initialize"))["result"]

    assert result["protocolVersion"] == PROTOCOL_VERSION
    assert result["capabilities"]["tools"] == {"listChanged": False}
    assert result["serverInfo"]["name"] == "fusion"


@pytest.mark.asyncio
async def test_tools_list():
    async with client_for(build_app()) as client:
        tools = (await rpc(client, "tools/list"))["result"]["tools"]

    by_name = {t["name"]: t for t in tools}
    assert by_name.keys() == {"search", "missing"}
    assert by_name["search"]["description"] == "Search users by name."
    assert by_name["search"]["inputSchema"]["required"] == ["q"]


@pytest.mark.asyncio
async def test_tools_call_returns_content():
    async with client_for(build_app()) as client:
        result = (await rpc(client, "tools/call", {"name": "search", "arguments": {"q": "ada"}}))[
            "result"
        ]

    assert result["isError"] is False
    assert result["content"] == [{"type": "text", "text": '[{"id":1,"name":"ada"}]'}]


@pytest.mark.asyncio
async def test_a_problem_return_becomes_an_error_result():
    async with client_for(build_app()) as client:
        result = (await rpc(client, "tools/call", {"name": "missing", "arguments": {"id": 3}}))[
            "result"
        ]

    assert result["isError"] is True
    assert "no user 3" in result["content"][0]["text"]


@pytest.mark.asyncio
async def test_missing_argument_is_reported_as_an_error_result():
    async with client_for(build_app()) as client:
        result = (await rpc(client, "tools/call", {"name": "search", "arguments": {}}))["result"]

    assert result["isError"] is True
    assert "Missing required value" in result["content"][0]["text"]


@pytest.mark.asyncio
async def test_bad_argument_type_is_reported_as_an_error_result():
    async with client_for(build_app()) as client:
        result = (await rpc(client, "tools/call", {"name": "search", "arguments": {"q": 1}}))[
            "result"
        ]

    assert result["isError"] is True


@pytest.mark.asyncio
async def test_unknown_tool():
    async with client_for(build_app()) as client:
        result = (await rpc(client, "tools/call", {"name": "nope", "arguments": {}}))["result"]

    assert result["isError"] is True
    assert "Unknown tool" in result["content"][0]["text"]


@pytest.mark.asyncio
async def test_arguments_must_be_an_object():
    async with client_for(build_app()) as client:
        result = (await rpc(client, "tools/call", {"name": "search", "arguments": []}))["result"]

    assert result["isError"] is True
    assert "must be an object" in result["content"][0]["text"]


@pytest.mark.asyncio
async def test_unknown_method_is_a_jsonrpc_error():
    async with client_for(build_app()) as client:
        reply = await rpc(client, "tools/nope")

    assert reply["error"]["code"] == -32601


@pytest.mark.asyncio
async def test_missing_method_is_an_invalid_request():
    async with client_for(build_app()) as client:
        reply = (await client.post("/mcp", json={"jsonrpc": "2.0", "id": 1})).json()

    assert reply["error"]["code"] == -32600


@pytest.mark.asyncio
async def test_non_object_message_is_an_invalid_request():
    async with client_for(build_app()) as client:
        reply = (await client.post("/mcp", json="nope")).json()

    assert reply["error"]["code"] == -32600


@pytest.mark.asyncio
async def test_malformed_json_is_a_parse_error():
    async with client_for(build_app()) as client:
        reply = (
            await client.post(
                "/mcp", content=b"{oops", headers={"content-type": "application/json"}
            )
        ).json()

    assert reply["error"]["code"] == -32700


@pytest.mark.asyncio
async def test_notifications_get_no_reply():
    async with client_for(build_app()) as client:
        body = (
            await client.post(
                "/mcp", json={"jsonrpc": "2.0", "method": "notifications/initialized"}
            )
        ).json()
        unknown = (await client.post("/mcp", json={"jsonrpc": "2.0", "method": "who/knows"})).json()

    assert body is None
    assert unknown is None


@pytest.mark.asyncio
async def test_batched_requests():
    async with client_for(build_app()) as client:
        replies = (
            await client.post(
                "/mcp",
                json=[
                    {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
                    {"jsonrpc": "2.0", "method": "notifications/initialized"},
                    {"jsonrpc": "2.0", "id": 2, "method": "initialize"},
                ],
            )
        ).json()

    assert [r["id"] for r in replies] == [1, 2]


@pytest.mark.asyncio
async def test_tool_call_over_mcp_gets_dependency_injection():
    class Database:
        url = "postgres://test"

    @factory
    async def db_factory() -> Database:
        return Database()

    async def whoami(db: Inject[Database]) -> Response[str]:
        """Report the database URL."""
        return Response(content=db.url)

    app = Fusion(routes=[mcp_route()], tools=[whoami])
    async with client_for(app) as client:
        result = (await rpc(client, "tools/call", {"name": "whoami", "arguments": {}}))["result"]

    assert result["content"][0]["text"] == "postgres://test"


@pytest.mark.asyncio
async def test_params_must_be_an_object_for_tools_call():
    async with client_for(build_app()) as client:
        reply = (
            await client.post(
                "/mcp",
                json={"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": []},
            )
        ).json()

    assert reply["error"]["code"] == -32602


@pytest.mark.asyncio
async def test_tools_call_defaults_absent_arguments_to_empty():
    async def no_args() -> Response[str]:
        """Takes nothing."""
        return Response(content="fine")

    app = Fusion(routes=[mcp_route()], tools=[no_args])
    async with client_for(app) as client:
        result = (await rpc(client, "tools/call", {"name": "no_args"}))["result"]

    assert result == {"content": [{"type": "text", "text": "fine"}], "isError": False}
