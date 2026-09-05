import typing
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from fusion import Fusion, Get, Inject, Injectable, Object, Response, factory
from fusion.di import PROVIDES, collect_factories

from .conftest import client_for


class Out(Object):
    value: typing.Any


class Database:
    def __init__(self, url: str = "postgres://test") -> None:
        self.url = url


class Session:
    def __init__(self, db: Database) -> None:
        self.db = db


class Deps(Object):
    url: str = "postgres://test"

    @factory
    async def database(self) -> Database:
        return Database(self.url)


@pytest.mark.asyncio
async def test_factory_backed_dependency_is_injected():
    async def handler(db: Inject[Database]) -> Response[Out]:
        return Response(Out(value=db.url))

    app = Fusion(routes=[Get("/db", handler)], factories=Deps())
    async with client_for(app) as client:
        assert (await client.get("/db")).json() == {"value": "postgres://test"}


@pytest.mark.asyncio
async def test_injectable_subclass_is_built_from_its_own_markers():
    class Group(Injectable):
        db: Inject[Database]

    async def handler(deps: Inject[Group]) -> Response[Out]:
        return Response(Out(value=deps.db.url))

    app = Fusion(routes=[Get("/deps", handler)], factories=Deps())
    async with client_for(app) as client:
        assert (await client.get("/deps")).json() == {"value": "postgres://test"}


@pytest.mark.asyncio
async def test_a_factory_may_declare_dependencies_of_its_own():
    class Layered(Deps):
        @factory
        async def session(self, db: Inject[Database]) -> Session:
            return Session(db)

    async def handler(session: Inject[Session]) -> Response[Out]:
        return Response(Out(value=session.db.url))

    app = Fusion(routes=[Get("/s", handler)], factories=Layered())
    async with client_for(app) as client:
        assert (await client.get("/s")).json() == {"value": "postgres://test"}


@pytest.mark.asyncio
async def test_dependency_is_constructed_once_per_request():
    async def handler(a: Inject[Database], b: Inject[Database]) -> Response[Out]:
        return Response(Out(value=a is b))

    app = Fusion(routes=[Get("/twice", handler)], factories=Deps())
    async with client_for(app) as client:
        assert (await client.get("/twice")).json() == {"value": True}


@pytest.mark.asyncio
async def test_each_request_gets_a_fresh_dependency():
    calls: list[int] = []

    class Counting(Object):
        @factory
        async def database(self) -> Database:
            calls.append(1)
            return Database()

    async def handler(db: Inject[Database]) -> Response[Out]:
        return Response(Out(value=id(db)))

    app = Fusion(routes=[Get("/db", handler)], factories=Counting())
    async with client_for(app) as client:
        await client.get("/db")
        await client.get("/db")

    assert calls == [1, 1]


@pytest.mark.asyncio
async def test_async_context_manager_factory_tears_down_after_the_response():
    events: list[str] = []

    class Managed(Object):
        @factory
        @asynccontextmanager
        async def session(self) -> AsyncIterator[Session]:
            events.append("open")
            try:
                yield Session(Database())
            finally:
                events.append("close")

    async def handler(session: Inject[Session]) -> Response[Out]:
        events.append("handle")
        return Response(Out(value="ok"))

    app = Fusion(routes=[Get("/s", handler)], factories=Managed())
    async with client_for(app) as client:
        await client.get("/s")

    assert events == ["open", "handle", "close"]


@pytest.mark.asyncio
async def test_teardown_runs_even_when_the_handler_raises():
    events: list[str] = []

    class Managed(Object):
        @factory
        @asynccontextmanager
        async def session(self) -> AsyncIterator[Session]:
            try:
                yield Session(Database())
            finally:
                events.append("close")

    async def handler(session: Inject[Session]) -> Response[Out]:
        raise RuntimeError("boom")

    app = Fusion(routes=[Get("/s", handler)], factories=Managed())
    async with client_for(app) as client:
        assert (await client.get("/s")).status_code == 500

    assert events == ["close"]


def test_injecting_an_unprovided_type_is_refused_at_construction():
    class Orphan:
        pass

    async def handler(x: Inject[Orphan]) -> Response[Out]:
        return Response(Out(value=None))

    with pytest.raises(ValueError, match="built without a factory"):
        Fusion(routes=[Get("/x", handler)], factories=Deps())


def test_an_unprovided_type_behind_an_injectable_is_refused_too():
    class Orphan:
        pass

    class Group(Injectable):
        x: Inject[Orphan]

    async def handler(deps: Inject[Group]) -> Response[Out]:
        return Response(Out(value=None))

    with pytest.raises(ValueError, match="built without a factory"):
        Fusion(routes=[Get("/x", handler)], factories=Deps())


def test_a_factory_cycle_names_the_loop():
    class Cyclic(Object):
        @factory
        async def database(self, session: Inject[Session]) -> Database:
            return Database()

        @factory
        async def session(self, db: Inject[Database]) -> Session:
            return Session(db)

    async def handler(db: Inject[Database]) -> Response[Out]:
        return Response(Out(value=None))

    with pytest.raises(ValueError, match="Database needs Session needs Database"):
        Fusion(routes=[Get("/x", handler)], factories=Cyclic())


def test_two_factories_for_one_type_is_refused():
    class Twice(Object):
        @factory
        async def one(self) -> Database:
            return Database()

        @factory
        async def two(self) -> Database:
            return Database()

    with pytest.raises(ValueError, match="already produces"):
        Fusion(factories=Twice())


def test_passing_the_class_instead_of_an_instance_says_so():
    with pytest.raises(TypeError, match=r"Pass Deps\(\.\.\.\)"):
        Fusion(factories=Deps)


def test_a_factory_may_not_use_a_transport_marker():
    from fusion import Http

    class Bad(Object):
        @factory
        async def database(self, q: Http.Query[str]) -> Database:
            return Database()

    with pytest.raises(TypeError, match="has no meaning here"):
        Fusion(factories=Bad())


@pytest.mark.asyncio
async def test_a_subclass_replaces_the_factory_it_overrides():
    class Fake(Deps):
        @factory
        async def database(self) -> Database:
            return Database("fake://")

    async def handler(db: Inject[Database]) -> Response[Out]:
        return Response(Out(value=db.url))

    real = Fusion(routes=[Get("/db", handler)], factories=Deps())
    fake = Fusion(routes=[Get("/db", handler)], factories=Fake())

    async with client_for(real) as client:
        assert (await client.get("/db")).json() == {"value": "postgres://test"}
    async with client_for(fake) as client:
        assert (await client.get("/db")).json() == {"value": "fake://"}


@pytest.mark.asyncio
async def test_several_objects_are_merged():
    class Sessions(Object):
        @factory
        async def session(self, db: Inject[Database]) -> Session:
            return Session(db)

    async def handler(session: Inject[Session]) -> Response[Out]:
        return Response(Out(value=session.db.url))

    app = Fusion(routes=[Get("/s", handler)], factories=[Deps(), Sessions()])
    async with client_for(app) as client:
        assert (await client.get("/s")).json() == {"value": "postgres://test"}


def test_one_route_object_may_not_be_wired_by_two_sets_of_factories():
    async def handler(db: Inject[Database]) -> Response[Out]:
        return Response(Out(value=db.url))

    routes = [Get("/db", handler)]
    Fusion(routes=routes, factories=Deps())

    with pytest.raises(ValueError, match="belongs to one of them"):
        Fusion(routes=routes, factories=Deps())


@pytest.mark.asyncio
async def test_one_route_object_may_be_wired_twice_from_the_same_factories():
    """The guard is against two answers for one type, not against two applications."""

    async def handler(db: Inject[Database]) -> Response[Out]:
        return Response(Out(value=db.url))

    deps = Deps()
    routes = [Get("/db", handler)]
    Fusion(routes=routes, factories=deps)

    async with client_for(Fusion(routes=routes, factories=deps)) as client:
        assert (await client.get("/db")).json() == {"value": "postgres://test"}


@pytest.mark.asyncio
async def test_two_applications_may_inject_the_same_injectable():
    """An Injectable's resolver table is class-level, so it belongs to no application."""

    class Group(Injectable):
        db: Inject[Database]

    async def handler(deps: Inject[Group]) -> Response[Out]:
        return Response(Out(value=deps.db.url))

    first = Fusion(routes=[Get("/deps", handler)], factories=Deps(url="one"))
    second = Fusion(routes=[Get("/deps", handler)], factories=Deps(url="two"))

    async with client_for(first) as client:
        assert (await client.get("/deps")).json() == {"value": "one"}
    async with client_for(second) as client:
        assert (await client.get("/deps")).json() == {"value": "two"}


def test_a_shared_route_reaching_a_factory_through_an_injectable_is_refused_too():
    """The disagreement is the same one; only the resolver it is reached through differs."""

    class Group(Injectable):
        db: Inject[Database]

    async def handler(deps: Inject[Group]) -> Response[Out]:
        return Response(Out(value=deps.db.url))

    routes = [Get("/deps", handler)]
    Fusion(routes=routes, factories=Deps(url="one"))

    with pytest.raises(ValueError, match="Group is already wired"):
        Fusion(routes=routes, factories=Deps(url="two"))


@pytest.mark.asyncio
async def test_a_shared_route_reaching_an_injectable_may_be_wired_from_one_object():
    class Group(Injectable):
        db: Inject[Database]

    async def handler(deps: Inject[Group]) -> Response[Out]:
        return Response(Out(value=deps.db.url))

    deps = Deps()
    routes = [Get("/deps", handler)]
    Fusion(routes=routes, factories=deps)

    async with client_for(Fusion(routes=routes, factories=deps)) as client:
        assert (await client.get("/deps")).json() == {"value": "postgres://test"}


def test_wiring_leaves_the_injectable_class_untouched():
    """Settling a class table would decide how it is built everywhere, forever."""
    from fusion.resolvers import DependencyResolver

    class Group(Injectable):
        db: Inject[Database]

    async def handler(deps: Inject[Group]) -> Response[Out]:
        return Response(Out(value=deps.db.url))

    route = Get("/deps", handler)
    Fusion(routes=[route], factories=Deps())

    field = Group.__resolvers__["db"]
    assert isinstance(field, DependencyResolver)
    assert field.provider is None
    assert field.from_factory is None

    site = route.signature.resolvers["deps"]
    assert isinstance(site, DependencyResolver)
    assert site.fields is not None
    assert site.fields["db"] is not field
    assert site.fields["db"].provider is not None


@pytest.mark.asyncio
async def test_a_nested_injectable_is_settled_per_site_as_well():
    class Inner(Injectable):
        db: Inject[Database]

    class Outer(Injectable):
        inner: Inject[Inner]

    async def handler(outer: Inject[Outer]) -> Response[Out]:
        return Response(Out(value=outer.inner.db.url))

    first = Fusion(routes=[Get("/n", handler)], factories=Deps(url="one"))
    second = Fusion(routes=[Get("/n", handler)], factories=Deps(url="two"))

    async with client_for(first) as client:
        assert (await client.get("/n")).json() == {"value": "one"}
    async with client_for(second) as client:
        assert (await client.get("/n")).json() == {"value": "two"}


def test_factory_requires_a_return_annotation():
    with pytest.raises(ValueError, match="return type annotation"):

        @factory
        async def bad():  # type: ignore[no-untyped-def]
            return 1


def test_factory_unwraps_async_iterator_annotations():
    class Thing:
        pass

    class Managed(Object):
        @factory
        @asynccontextmanager
        async def thing(self) -> AsyncIterator[Thing]:
            yield Thing()

    assert getattr(Managed.thing, PROVIDES) is Thing
    assert list(collect_factories(Managed())) == [Thing]


def test_no_factories_at_all_is_fine_when_nothing_is_injected():
    async def handler() -> Response[Out]:
        return Response(Out(value="ok"))

    assert Fusion(routes=[Get("/ok", handler)]).routes


def test_injectable_rejects_an_unmarked_field():
    with pytest.raises(TypeError, match="carries no Fusion marker"):

        class Bad(Injectable):
            db: Database
