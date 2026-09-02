import typing
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from fusion import (
    Event,
    Fusion,
    Get,
    Http,
    Inject,
    NotFound,
    Object,
    Response,
    Tool,
    ToolDef,
    factory,
    field,
)
from fusion.tools import ToolContext, build_input_schema, result_content


class User(Object):
    id: int
    name: str


class Filters(Object):
    active: bool = True


async def search(q: Tool.Arg[str], limit: Tool.Arg[int] = 10) -> Response[list[User]]:
    """Search users by name.

    Matches on a case-insensitive prefix.
    """
    return Response([User(id=1, name=q)][:limit])


def test_tool_definition_uses_name_and_docstring():
    tool = ToolDef(search)
    definition = tool.definition()

    assert definition["name"] == "search"
    assert definition["description"].startswith("Search users by name.")
    assert definition["inputSchema"]["properties"].keys() == {"q", "limit"}


def test_input_schema_shape():
    schema = build_input_schema(ToolDef(search).signature)

    assert schema["type"] == "object"
    assert schema["required"] == ["q"]
    assert schema["additionalProperties"] is False
    assert schema["properties"]["limit"]["default"] == 10
    assert "$ref" not in schema  # the root object is inlined, not referenced


def test_input_schema_inlines_root_and_keeps_nested_defs():
    async def tool(filters: Tool.Arg[Filters]) -> Response[User]: ...

    schema = build_input_schema(ToolDef(tool).signature)

    assert schema["properties"]["filters"] == {"$ref": "#/$defs/Filters"}
    assert "Filters" in schema["$defs"]


def test_input_schema_carries_field_constraints():
    class Query(Object):
        text: str = field(min_length=2, description="what to look for")

    async def tool(query: Tool.Arg[Query]) -> Response[User]: ...

    schema = build_input_schema(ToolDef(tool).signature)
    prop = schema["$defs"]["Query"]["properties"]["text"]

    assert prop["minLength"] == 2
    assert prop["description"] == "what to look for"


def test_injected_params_are_not_in_the_schema():
    class Database:
        pass

    @factory
    async def db_factory() -> Database:
        return Database()

    async def tool(q: Tool.Arg[str], db: Inject[Database]) -> Response[User]: ...

    schema = build_input_schema(ToolDef(tool).signature)
    assert schema["properties"].keys() == {"q"}


def test_http_marker_is_rejected_on_a_tool():
    async def tool(id: Http.Path[int]) -> Response[User]: ...

    with pytest.raises(TypeError, match="'http' marker"):
        Fusion(tools=[tool])


def test_async_generator_is_rejected_as_a_tool():
    async def tool(q: Tool.Arg[str]) -> AsyncIterator[Event[User]]:
        yield Event(data=User(id=1, name=q))

    with pytest.raises(TypeError, match="no streaming result shape"):
        Fusion(tools=[tool])


def test_duplicate_tool_names_are_rejected():
    with pytest.raises(ValueError, match="Duplicate tool name"):
        Fusion(tools=[search, ToolDef(search)])


def test_tool_can_be_renamed_and_redescribed():
    tool = ToolDef(search, name="find_users", description="Custom.")
    assert tool.definition()["name"] == "find_users"
    assert tool.definition()["description"] == "Custom."


@pytest.mark.asyncio
async def test_calling_a_tool_binds_arguments():
    result = await ToolDef(search).call({"q": "ada", "limit": 1})
    assert [(u.id, u.name) for u in result.content] == [(1, "ada")]


@pytest.mark.asyncio
async def test_tool_call_gets_dependency_injection_and_teardown():
    events: list[str] = []

    class Session:
        pass

    @factory
    @asynccontextmanager
    async def session_factory() -> AsyncIterator[Session]:
        events.append("open")
        try:
            yield Session()
        finally:
            events.append("close")

    async def tool(q: Tool.Arg[str], session: Inject[Session]) -> Response[str]:
        events.append("call")
        return Response(content=q)

    await ToolDef(tool).call({"q": "x"})

    assert events == ["open", "call", "close"]


@pytest.mark.asyncio
async def test_tool_context_carries_no_send_channel():
    async with ToolContext({"a": 1}) as ctx:
        assert ctx.arguments == {"a": 1}
        with pytest.raises(RuntimeError, match="no ASGI send channel"):
            await ctx.send({})


def test_result_content_maps_responses_and_problems():
    payload, is_error = result_content(Response(content={"a": 1}))
    assert (payload, is_error) == ({"a": 1}, False)

    payload, is_error = result_content(NotFound(detail="gone"))
    assert is_error is True
    assert payload["status"] == 404

    assert result_content("plain") == ("plain", False)
