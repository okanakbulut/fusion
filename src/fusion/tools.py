"""The tool-call transport: a flat argument dict in, one result out.

Shared by the MCP server and, later, an in-process agent loop - a ``tools/call``
and a model's ``tool_use`` block carry the same shape, so they are two entry
points into this one transport rather than two transports.
"""

import inspect
import typing

import msgspec

from .binding import Returns, Signature, bind, check_returns
from .context import Context
from .responses import Problem, Response
from .types import Transport

ARGUMENT = "argument"


def build_input_schema(signature: Signature) -> dict[str, typing.Any]:
    """Derive a JSON Schema for a tool's arguments from its signature.

    The parameters are collected into a throwaway struct purely so msgspec can
    generate the schema - constraints and descriptions declared with ``field()``
    ride along for free.
    """
    parameters = inspect.signature(signature.func).parameters
    fields: list[tuple[str, typing.Any, typing.Any]] = []

    for name, resolver in signature.resolvers.items():
        if resolver.location != ARGUMENT:
            continue
        default = parameters[name].default
        fields.append(
            (
                name,
                resolver.typ,
                msgspec.NODEFAULT if default is inspect.Parameter.empty else default,
            )
        )

    # kw_only, or a defaulted parameter followed by a required one is rejected -
    # and parameter order is not the caller's concern for a keyword-argument
    # protocol anyway.
    struct = msgspec.defstruct(f"{signature.name}_arguments", fields, kw_only=True)
    _, components = msgspec.json.schema_components([struct], ref_template="#/$defs/{name}")

    # schema_components returns the root as a $ref; inline it, keeping any
    # nested struct definitions alongside.
    components = dict(components)
    schema = dict(components.pop(struct.__name__))
    schema.pop("title", None)
    schema.setdefault("type", "object")
    schema.setdefault("properties", {})
    schema["additionalProperties"] = False
    if components:
        schema["$defs"] = components
    return schema


class ToolDef:
    """A function exposed as a callable tool."""

    __slots__ = ("description", "name", "schema", "signature")

    def __init__(
        self,
        func: typing.Callable[..., typing.Any],
        *,
        name: str | None = None,
        description: str | None = None,
    ) -> None:
        signature = Signature.of(func, transport=Transport.TOOL)
        if signature.is_asyncgen:
            raise TypeError(
                f"Tool {signature.name!r} is an async generator, but a tool call has no "
                f"streaming result shape. Use a coroutine returning a Response."
            )
        check_returns(signature, Returns.TOOL)
        self.signature = signature
        self.name = name or signature.name
        self.description = description or signature.doc
        self.schema = build_input_schema(signature)

    def definition(self) -> dict[str, typing.Any]:
        """The tool as it appears in an MCP ``tools/list`` reply."""
        return {
            "name": self.name,
            "description": self.description or "",
            "inputSchema": self.schema,
        }

    async def call(self, arguments: typing.Mapping[str, typing.Any]) -> typing.Any:
        """Invoke the tool with ``arguments`` in a fresh tool context."""
        async with ToolContext(arguments):
            kwargs = await bind(self.signature)
            return await self.signature.func(**kwargs)


class ToolContext(Context):
    """Request-scoped context for one tool call.

    Subclassing ``Context`` is what makes dependency injection and async
    context-manager teardown work for tool calls without touching a single
    resolver: they all read the same context variable.
    """

    def __init__(self, arguments: typing.Mapping[str, typing.Any]) -> None:
        async def _receive() -> dict[str, typing.Any]:  # pragma: no cover
            return {"type": "http.disconnect"}

        async def _send(message: typing.Any) -> None:  # pragma: no cover
            raise RuntimeError("A tool call has no ASGI send channel")

        super().__init__({"type": "tool"}, _receive, _send)
        self.arguments = dict(arguments)


def result_content(value: typing.Any) -> tuple[typing.Any, bool]:
    """Map a handler's return value onto an MCP tool result.

    Returns the payload and whether it represents an error.
    """
    if isinstance(value, Problem):
        return value.body, True
    if isinstance(value, Response):
        return value.content, False
    return value, False
