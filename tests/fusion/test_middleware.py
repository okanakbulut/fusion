import typing

import pytest

from fusion import (
    EventStream,
    FromContext,
    Fusion,
    Get,
    Http,
    Inject,
    Injectable,
    NotFound,
    Object,
    Request,
    Response,
    Unauthorized,
)

from .conftest import client_for


class Item(Object):
    id: int


async def get_item() -> Response[Item]:
    return Response(Item(id=1))


async def require_bearer(authorization: Http.Header[str] = "") -> Unauthorized | None:
    """A guard: returning a response ends the request, returning nothing falls through."""
    if not authorization.startswith("Bearer "):
        return Unauthorized(detail="Unknown authentication method")


@pytest.mark.asyncio
async def test_a_guard_can_end_the_request():
    app = Fusion(routes=[Get("/items", get_item, middlewares=[require_bearer])])
    async with client_for(app) as client:
        response = await client.get("/items")

    assert response.status_code == 401
    assert response.json()["detail"] == "Unknown authentication method"


@pytest.mark.asyncio
async def test_a_guard_returning_nothing_falls_through():
    app = Fusion(routes=[Get("/items", get_item, middlewares=[require_bearer])])
    async with client_for(app) as client:
        response = await client.get("/items", headers={"authorization": "Bearer x"})

    assert response.status_code == 200
    assert response.json() == {"id": 1}


@pytest.mark.asyncio
async def test_a_wrapper_sees_the_response_on_the_way_out():
    async def tagging() -> typing.AsyncIterator[None]:
        response = yield
        response.headers = {"x-tag": "hello"}

    app = Fusion(routes=[Get("/items", get_item, middlewares=[tagging])])
    async with client_for(app) as client:
        response = await client.get("/items")

    assert response.headers["x-tag"] == "hello"
    assert response.json() == {"id": 1}


@pytest.mark.asyncio
async def test_a_wrapper_replaces_the_response_by_yielding_a_second_one():
    async def censoring() -> typing.AsyncIterator[NotFound | None]:
        yield
        yield NotFound(detail="nothing here")

    app = Fusion(routes=[Get("/items", get_item, middlewares=[censoring])])
    async with client_for(app) as client:
        response = await client.get("/items")

    assert response.status_code == 404
    assert response.json()["detail"] == "nothing here"


@pytest.mark.asyncio
async def test_middlewares_run_outermost_first():
    order: list[str] = []

    async def first() -> typing.AsyncIterator[None]:
        order.append("first")
        yield
        order.append("first done")

    async def second() -> typing.AsyncIterator[None]:
        order.append("second")
        yield
        order.append("second done")

    app = Fusion(routes=[Get("/items", get_item, middlewares=[first, second])])
    async with client_for(app) as client:
        await client.get("/items")

    assert order == ["first", "second", "second done", "first done"]


@pytest.mark.asyncio
async def test_a_guard_short_circuiting_skips_the_middlewares_after_it():
    reached: list[str] = []

    async def deny() -> Unauthorized:
        return Unauthorized(detail="no")

    async def inner() -> typing.AsyncIterator[None]:
        reached.append("inner")
        yield

    app = Fusion(routes=[Get("/items", get_item, middlewares=[deny, inner])])
    async with client_for(app) as client:
        assert (await client.get("/items")).status_code == 401

    assert reached == []


@pytest.mark.asyncio
async def test_middleware_factory_carries_configuration():
    """A middleware needing arguments is a closure - no framework support required."""

    def tagging(tag: str, suffix: str = "!") -> typing.Callable[..., typing.Any]:
        async def middleware() -> typing.AsyncIterator[None]:
            response = yield
            response.headers = {"x-tag": tag + suffix}

        return middleware

    app = Fusion(routes=[Get("/items", get_item, middlewares=[tagging(tag="hello")])])
    async with client_for(app) as client:
        response = await client.get("/items")

    assert response.headers["x-tag"] == "hello!"


@pytest.mark.asyncio
async def test_middleware_binds_the_same_sources_as_a_handler():
    seen: dict[str, typing.Any] = {}

    class Clock(Injectable):
        async def now(self) -> str:
            return "noon"

    async def observing(
        request: FromContext[Request],
        clock: Inject[Clock],
        item_id: Http.Path[int],
        verbose: Http.Query[bool] = False,
    ) -> typing.AsyncIterator[None]:
        seen.update(method=request.method, now=await clock.now(), item_id=item_id, verbose=verbose)
        yield

    async def get_one(item_id: Http.Path[int]) -> Response[Item]:
        return Response(Item(id=item_id))

    app = Fusion(routes=[Get("/items/{item_id:int}", get_one, middlewares=[observing])])
    async with client_for(app) as client:
        response = await client.get("/items/7?verbose=true")

    assert response.json() == {"id": 7}
    assert seen == {"method": "GET", "now": "noon", "item_id": 7, "verbose": True}


@pytest.mark.asyncio
async def test_middleware_wraps_a_streaming_handler():
    """A wrapper runs before the stream opens and hands the stream straight back."""
    seen: list[typing.Any] = []

    async def observing() -> typing.AsyncIterator[None]:
        stream = yield
        seen.append(stream)

    async def events() -> typing.AsyncIterator[Item]:
        yield Item(id=1)

    app = Fusion(routes=[Get("/events", events, middlewares=[observing])])
    async with client_for(app) as client:
        response = await client.get("/events")

    assert isinstance(seen[0], EventStream)
    assert 'data: {"id":1}' in response.text


@pytest.mark.asyncio
async def test_a_failure_downstream_is_thrown_back_into_the_wrapper():
    cleaned: list[str] = []

    async def guarding() -> typing.AsyncIterator[NotFound | None]:
        try:
            yield
        except LookupError:
            yield NotFound(detail="gone")
        finally:
            cleaned.append("done")

    async def boom() -> Response[Item]:
        raise LookupError("no such item")

    app = Fusion(routes=[Get("/items", boom, middlewares=[guarding])])
    async with client_for(app) as client:
        response = await client.get("/items")

    assert response.status_code == 404
    assert response.json()["detail"] == "gone"
    assert cleaned == ["done"]


@pytest.mark.asyncio
async def test_a_failure_the_wrapper_does_not_answer_still_propagates():
    cleaned: list[str] = []

    async def watching() -> typing.AsyncIterator[None]:
        try:
            yield
        finally:
            cleaned.append("done")

    async def boom() -> Response[Item]:
        raise RuntimeError("kaboom")

    app = Fusion(routes=[Get("/items", boom, middlewares=[watching])])
    async with client_for(app) as client:
        response = await client.get("/items")

    assert response.status_code == 500
    assert cleaned == ["done"]


@pytest.mark.asyncio
async def test_a_swallowed_failure_without_a_replacement_still_propagates():
    async def swallowing() -> typing.AsyncIterator[None]:
        try:
            yield
        except RuntimeError:
            pass

    async def boom() -> Response[Item]:
        raise RuntimeError("kaboom")

    app = Fusion(routes=[Get("/items", boom, middlewares=[swallowing])])
    async with client_for(app) as client:
        assert (await client.get("/items")).status_code == 500


@pytest.mark.asyncio
async def test_a_wrapper_that_never_yields_is_an_error(caplog):
    async def forgetful() -> typing.AsyncIterator[None]:
        if False:  # pragma: no cover - makes the function a generator without yielding
            yield

    app = Fusion(routes=[Get("/items", get_item, middlewares=[forgetful])])
    async with client_for(app) as client:
        assert (await client.get("/items")).status_code == 500

    assert "finished without reaching its 'yield'" in caplog.text


@pytest.mark.asyncio
async def test_a_wrapper_that_yields_before_the_route_runs_is_an_error(caplog):
    """The first yield is where the chain runs; answering early is a guard's job."""

    async def eager() -> typing.AsyncIterator[Unauthorized | None]:
        yield Unauthorized(detail="too soon")

    app = Fusion(routes=[Get("/items", get_item, middlewares=[eager])])
    async with client_for(app) as client:
        assert (await client.get("/items")).status_code == 500

    assert "The first 'yield' must be bare" in caplog.text
