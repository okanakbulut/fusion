import typing
from collections.abc import AsyncIterator

import pytest

from fusion import Event, Http, Inject, Object, Response, Tool
from fusion.binding import Signature, bind
from fusion.context import Context
from fusion.exceptions import ValidationException
from fusion.resolvers import Marker, build_resolvers, marker_of
from fusion.types import Transport


class Out(Object):
    value: str


async def handler(id: Http.Path[int], q: Http.Query[str] = "x") -> Response[Out] | None:
    """Summary line.

    Longer description spanning
    two lines.
    """
    return Response(Out(value=q))


def test_signature_captures_metadata():
    signature = Signature.of(handler, transport=Transport.HTTP)

    assert signature.name == "handler"
    assert signature.summary == "Summary line."
    assert signature.description == "Longer description spanning\ntwo lines."
    assert signature.required == {"id"}
    assert signature.is_asyncgen is False
    assert set(signature.resolvers) == {"id", "q"}


def test_signature_detects_async_generators():
    async def streamer(id: Http.Path[int]) -> AsyncIterator[Event[Out]]:
        yield Event(data=Out(value="x"))

    signature = Signature.of(streamer, transport=Transport.HTTP)
    assert signature.is_asyncgen is True


def test_signature_without_a_docstring():
    async def bare(id: Http.Path[int]) -> Response[Out]: ...

    signature = Signature.of(bare, transport=Transport.HTTP)
    assert signature.summary is None
    assert signature.description is None


def test_signature_with_a_single_line_docstring():
    async def one(id: Http.Path[int]) -> Response[Out]:
        """Just a summary."""

    signature = Signature.of(one, transport=Transport.HTTP)
    assert signature.summary == "Just a summary."
    assert signature.description is None


def test_signature_repr():
    signature = Signature.of(handler, transport=Transport.HTTP)
    assert repr(signature) == "<Signature handler transport=http>"


def test_signature_rejects_a_sync_function():
    def sync(id: Http.Path[int]) -> Response[Out]: ...

    with pytest.raises(TypeError, match="async def"):
        Signature.of(sync, transport=Transport.HTTP)


def test_signature_without_a_return_annotation():
    async def bare(id: Http.Path[int]): ...

    signature = Signature.of(bare, transport=Transport.HTTP)
    assert signature.return_type is typing.Any


# --- marker decoding ---------------------------------------------------------


def test_marker_of_reads_the_payload():
    marker = marker_of(Http.Query[int])
    assert isinstance(marker, Marker)
    assert marker.transport is Transport.HTTP


def test_marker_of_returns_none_for_plain_annotations():
    assert marker_of(int) is None
    assert marker_of(list[int]) is None
    assert marker_of(typing.Annotated[int, "not a marker"]) is None


def test_build_resolvers_skips_classvars():
    hints = {"x": typing.ClassVar[int], "return": int}
    assert build_resolvers(hints, allowed=frozenset(Transport), owner="T") == {}


def test_build_resolvers_reports_the_owner_and_parameter():
    with pytest.raises(TypeError, match="Parameter 'db' on 'Thing'"):
        build_resolvers({"db": int}, allowed=frozenset(Transport), owner="Thing")


def test_build_resolvers_rejects_a_foreign_transport():
    with pytest.raises(TypeError, match="'tool' marker"):
        build_resolvers(
            {"q": Tool.Arg[str]},
            allowed=frozenset({Transport.HTTP, Transport.ANY}),
            owner="Thing",
        )


def test_inject_is_allowed_under_every_transport():
    class Thing:
        pass

    for transport in (Transport.HTTP, Transport.TOOL):
        resolvers = build_resolvers(
            {"x": Inject[Thing]},
            allowed=frozenset({transport, Transport.ANY}),
            owner="Thing",
        )
        assert set(resolvers) == {"x"}


# --- binding -----------------------------------------------------------------


def _http_context(path_params=None, query=b""):
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "query_string": query,
        "headers": [],
        "path_params": path_params or {},
    }

    async def receive():  # pragma: no cover
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):  # pragma: no cover
        pass

    return Context(scope, receive, send)


@pytest.mark.asyncio
async def test_bind_omits_absent_values_so_python_defaults_apply():
    """An absent optional parameter contributes no kwarg at all.

    Passing None instead would force a conversion and defeat the function's
    own default, so the resolver signals MISSING and bind drops the argument.
    """
    signature = Signature.of(handler, transport=Transport.HTTP)

    async with _http_context(path_params={"id": "7"}):
        assert await bind(signature) == {"id": 7}
        assert (await handler(**await bind(signature))).content.value == "x"


@pytest.mark.asyncio
async def test_bind_uses_a_supplied_value_over_the_default():
    signature = Signature.of(handler, transport=Transport.HTTP)

    async with _http_context(path_params={"id": "7"}, query=b"q=given"):
        assert await bind(signature) == {"id": 7, "q": "given"}


@pytest.mark.asyncio
async def test_bind_aggregates_every_failure():
    async def two_bad(a: Http.Query[int], b: Http.Query[int]) -> Response[Out]: ...

    signature = Signature.of(two_bad, transport=Transport.HTTP)

    async with _http_context(query=b"a=x&b=y"):
        with pytest.raises(ValidationException) as excinfo:
            await bind(signature)

    assert {error.field for error in excinfo.value.errors} == {"a", "b"}


@pytest.mark.asyncio
async def test_bind_reports_a_missing_required_value():
    async def needs(a: Http.Query[int]) -> Response[Out]: ...

    signature = Signature.of(needs, transport=Transport.HTTP)

    async with _http_context():
        with pytest.raises(ValidationException) as excinfo:
            await bind(signature)

    assert excinfo.value.errors[0].message == "Missing required value"


@pytest.mark.asyncio
async def test_resolver_without_a_context_raises():
    from fusion.resolvers import QueryParamResolver

    with pytest.raises(RuntimeError, match="No context available"):
        await QueryParamResolver(name="q", typ=str).resolve()


# --- resolver edge cases -----------------------------------------------------


@pytest.mark.asyncio
async def test_injecting_a_type_with_no_provider():
    from fusion.resolvers import DependencyResolver

    class Orphan:
        pass

    async with _http_context():
        with pytest.raises(RuntimeError, match="not an Injectable subclass"):
            await DependencyResolver(name="x", typ=Orphan).resolve()


@pytest.mark.asyncio
async def test_a_dependency_is_cached_per_context():
    from fusion import Injectable
    from fusion.resolvers import DependencyResolver

    class Deps(Injectable):
        pass

    async with _http_context():
        first = (await DependencyResolver(name="a", typ=Deps).resolve())[1]
        second = (await DependencyResolver(name="b", typ=Deps).resolve())[1]

    assert first is second


@pytest.mark.asyncio
async def test_a_dependency_kind_is_decided_once_and_reused():
    """An Injectable no application wired still settles itself, once."""
    from fusion import Injectable
    from fusion.resolvers import DependencyResolver

    class Deps(Injectable):
        pass

    resolver = DependencyResolver(name="a", typ=Deps)
    assert resolver.from_factory is None

    async with _http_context():
        await resolver.resolve()
    assert resolver.from_factory is False

    async with _http_context():
        await resolver.resolve()
    assert resolver.from_factory is False


@pytest.mark.asyncio
async def test_body_resolver_reports_malformed_json_in_the_fallback_path():
    """The second decode also has to cope with syntactically invalid JSON."""
    from fusion.resolvers import RequestBodyResolver

    class Model(Object):
        a: int

    async def receive():
        return {"type": "http.request", "body": b"{bad", "more_body": False}

    ctx = Context({"type": "http"}, receive, None)
    async with ctx:
        with pytest.raises(ValidationException):
            await RequestBodyResolver(name="body", typ=Model).resolve()


@pytest.mark.asyncio
async def test_body_resolver_applies_struct_defaults_for_absent_fields():
    from fusion.object import field
    from fusion.resolvers import RequestBodyResolver

    class Model(Object):
        a: int
        b: int = 5
        c: list[int] = field(default_factory=list)

    async def receive():
        # A string where an int is declared: strict decoding fails, so the
        # per-field walk runs and coerces leniently.
        return {"type": "http.request", "body": b'{"a": "1"}', "more_body": False}

    ctx = Context({"type": "http"}, receive, None)
    async with ctx:
        _name, value = await RequestBodyResolver(name="body", typ=Model).resolve()

    assert (value.a, value.b, value.c) == (1, 5, [])


def test_validation_exception_message_comes_from_its_errors():
    from fusion.responses import FieldError

    exc = ValidationException(errors=[FieldError(field="a", location="query", message="bad")])
    assert str(exc) == "a: bad"
    assert str(ValidationException(detail="plain")) == "plain"


# --- return annotations must be documentable ------------------------------


class _Payload(Object):
    id: int


def _get(handler: typing.Any) -> typing.Any:
    from fusion import Get

    return Get("/x", handler)


def test_a_handler_returning_any_is_rejected():
    async def handler() -> typing.Any: ...

    with pytest.raises(TypeError, match="Handler 'handler' is annotated to return Any"):
        _get(handler)


def test_a_handler_returning_none_is_rejected():
    async def handler() -> None: ...

    with pytest.raises(TypeError, match="may not return None"):
        _get(handler)


def test_a_handler_returning_a_type_without_a_status_is_rejected():
    """`-> dict` would silently document a bare 200 with no content."""

    async def handler() -> dict: ...

    with pytest.raises(TypeError, match="carries no status code"):
        _get(handler)


def test_one_stray_arm_rejects_the_whole_union():
    async def handler() -> Response[_Payload] | bool: ...

    with pytest.raises(TypeError, match="carries no status code"):
        _get(handler)


def test_a_stream_must_say_what_it_yields():
    async def handler() -> typing.AsyncIterator:
        yield _Payload(id=1)  # pragma: no cover

    with pytest.raises(TypeError, match="must declare what it yields"):
        _get(handler)


def test_a_stream_yielding_any_is_rejected():
    async def handler() -> typing.AsyncIterator[typing.Any]:
        yield _Payload(id=1)  # pragma: no cover

    with pytest.raises(TypeError, match="annotated to yield Any"):
        _get(handler)


def test_a_stream_may_yield_data_or_a_problem():
    """Both are documentable: one is the payload, the other answers pre-flight."""
    from fusion import Event, NotFound

    async def handler() -> typing.AsyncIterator[Event[_Payload] | NotFound]:
        yield Event(data=_Payload(id=1))  # pragma: no cover

    assert _get(handler).signature.is_asyncgen


def test_a_guard_may_return_none_but_not_any():
    from fusion import Get, Unauthorized

    async def allowed() -> Unauthorized | None: ...

    async def bare() -> typing.Any: ...

    async def handler() -> Response[_Payload]: ...

    assert Get("/x", handler, middlewares=[allowed]) is not None
    with pytest.raises(TypeError, match="Middleware 'bare' is annotated to return Any"):
        Get("/x", handler, middlewares=[bare])


def test_a_wrapper_must_declare_what_it_can_yield():
    from fusion import Get

    async def loose() -> typing.AsyncIterator[typing.Any]:
        yield  # pragma: no cover

    async def handler() -> Response[_Payload]: ...

    with pytest.raises(TypeError, match="Middleware 'loose' is annotated to yield Any"):
        Get("/x", handler, middlewares=[loose])


def test_a_tool_returning_any_is_rejected():
    from fusion import Tool
    from fusion.tools import ToolDef

    async def tool(name: Tool.Arg[str]) -> typing.Any: ...

    with pytest.raises(TypeError, match="Tool 'tool' is annotated to return Any"):
        ToolDef(tool)


def test_both_spellings_of_a_none_yield_are_accepted():
    """`typing.AsyncIterator[None]` normalises to NoneType; `collections.abc` does not."""
    import collections.abc

    from fusion import Get

    async def typing_spelling() -> typing.AsyncIterator[None]:
        yield  # pragma: no cover

    async def abc_spelling() -> collections.abc.AsyncIterator[None]:
        yield  # pragma: no cover

    async def handler() -> Response[_Payload]: ...

    assert Get("/x", handler, middlewares=[typing_spelling, abc_spelling]) is not None


@pytest.mark.asyncio
async def test_a_resolver_answers_the_same_with_or_without_a_context_argument():
    """``bind`` passes the context, but the argument stays optional."""
    from fusion.resolvers import PathParamResolver, QueryParamResolver
    from fusion.security import BearerResolver

    resolvers = [
        QueryParamResolver(name="q", typ=str),
        PathParamResolver(name="id", typ=int),
        BearerResolver(name="token", typ=str),
    ]

    async with _http_context(path_params={"id": "7"}, query=b"q=given") as ctx:
        ctx.scope["headers"] = [(b"authorization", b"Bearer abc")]
        for resolver in resolvers:
            assert await resolver.resolve() == await resolver.resolve(ctx)


@pytest.mark.asyncio
async def test_a_resolver_written_before_the_context_argument_still_binds():
    """The old ``resolve(self)`` signature is what makes the argument optional."""
    from fusion.resolvers import MISSING, Resolver

    class Legacy(Resolver):
        location: typing.ClassVar[str] = "query"

        async def resolve(self) -> tuple[str, typing.Any]:  # type: ignore[override]
            return self.name, self.context.query_params.get(self.name, MISSING)

    signature = Signature.of(handler, transport=Transport.HTTP)
    signature.resolvers["q"] = Legacy(name="q", typ=str)

    async with _http_context(path_params={"id": "7"}, query=b"q=legacy"):
        assert await bind(signature) == {"id": 7, "q": "legacy"}
