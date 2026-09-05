"""Factories: how a type the application does not own gets built.

An ``Injectable`` builds itself, so nothing has to be registered for it.  Every
other type needs something that knows how, and that something is a ``@factory``
method on an object the application is handed.  Marking a method records the
type it produces on the function itself, so importing the module that defines it
wires nothing - the object is what an application is given, and ``settle`` is
what connects the two.

Nothing here survives construction.  ``settle`` points each ``Inject[T]``
resolver at the factory that answers it and returns; the application keeps no
registry, and a request never looks one up.
"""

import typing
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager

from .binding import Signature
from .injectable import Injectable
from .resolvers import DependencyResolver, Resolver, build_resolvers
from .types import Transport

T = typing.TypeVar("T")
type Constructor[T] = typing.Callable[..., typing.Awaitable[T] | AbstractAsyncContextManager[T]]

PROVIDES = "__fusion_provides__"
"""Where ``factory`` records the type a function produces.  Reading it back off
the function is what replaces a registry: the declaration travels with the
function, the way ``requires`` carries its roles."""


def _get_factory_type(return_annotation: typing.Any) -> type[typing.Any]:
    origin = typing.get_origin(return_annotation)
    if origin in {AsyncIterator, AbstractAsyncContextManager}:
        args = typing.get_args(return_annotation)
        if not args:  # pragma: no cover
            raise ValueError("Factory return type must specify the produced value type")
        return typing.cast(type[typing.Any], args[0])
    return typing.cast(type[typing.Any], return_annotation)


def factory(func: Constructor[T]) -> Constructor[T]:
    """Mark a function as producing its return type.

    The produced type is stamped on the function rather than written to a
    registry, so a module full of factories has no effect until the object
    carrying them reaches ``Fusion(factories=...)``.
    """
    if "return" not in func.__annotations__:
        raise ValueError("Factory function must have a return type annotation")

    setattr(func, PROVIDES, _get_factory_type(func.__annotations__["return"]))
    return func


def collect_factories(bundles: typing.Any) -> dict[type[typing.Any], Signature]:
    """Index every ``@factory`` reachable from ``bundles`` by what it produces.

    ``bundles`` is one object or several; several are merged left to right, and
    two factories for one type is an error rather than the last one quietly
    winning.
    """
    if bundles is None:
        return {}

    if isinstance(bundles, type):
        raise TypeError(
            f"Fusion(factories=...) expects an instance, got the class "
            f"{bundles.__name__!r} itself. Pass {bundles.__name__}(...)."
        )

    if not isinstance(bundles, (list, tuple)):
        bundles = [bundles]

    collected: dict[type[typing.Any], Signature] = {}
    origin: dict[type[typing.Any], str] = {}

    for bundle in bundles:
        for name, produced in _stamped(type(bundle)):
            where = f"{type(bundle).__name__}.{name}"
            if produced in collected:
                raise ValueError(
                    f"{where!r} produces {produced.__name__}, which {origin[produced]!r} "
                    f"already produces. Keep one, or give each application its own object."
                )
            collected[produced] = _factory_signature(getattr(bundle, name), where)
            origin[produced] = where

    return collected


def _stamped(cls: type[typing.Any]) -> typing.Iterator[tuple[str, type[typing.Any]]]:
    """``(name, produced type)`` for each ``@factory`` on ``cls``, nearest class first.

    Walking the MRO in order and skipping a name already seen is what makes a
    subclass's factory replace the one it overrides - which is how a test swaps a
    real dependency for a fake without touching anything another test shares.
    """
    seen: set[str] = set()
    for klass in cls.__mro__:
        for name, value in vars(klass).items():
            produced = getattr(value, PROVIDES, None)
            if produced is not None and name not in seen:
                seen.add(name)
                yield name, produced


def _factory_signature(bound: typing.Any, where: str) -> Signature:
    """A factory's signature, built without ``Signature.of``'s async-def check.

    ``@asynccontextmanager`` returns a plain function, so a factory with teardown
    is neither a coroutine function nor an async generator function, and ``of``
    would turn it away for not being 'async def'.  What it produces was settled
    by ``@factory`` from the return annotation instead.

    Binding the method before inspecting it is what drops ``self``: it appears in
    neither the type hints nor the parameter list, so nothing downstream needs a
    special case for it.
    """
    hints = typing.get_type_hints(bound, include_extras=True)
    return Signature(
        bound,
        resolvers=build_resolvers(hints, allowed=frozenset({Transport.ANY}), owner=where),
        return_type=hints.get("return", typing.Any),
        transport=Transport.ANY,
        is_asyncgen=False,
    )


def settle(
    resolvers: dict[str, Resolver],
    factories: dict[type[typing.Any], Signature],
    owner: str,
    chain: tuple[type[typing.Any], ...] = (),
) -> None:
    """Point every ``Inject[T]`` at what will build it, or refuse to run at all.

    Walks the graph rather than one signature: an ``Injectable``'s fields and a
    factory's own parameters are dependencies too.  Settling here is what lets
    the request path skip the question - a resolver reaches its first call
    already holding the factory that answers it - and what turns a dependency
    cycle from a recursion that exhausts the stack into a sentence naming the
    loop.
    """
    for resolver in resolvers.values():
        if not isinstance(resolver, DependencyResolver):
            continue

        typ = resolver.typ
        if typ in chain:
            loop = " needs ".join(t.__name__ for t in (*chain, typ))
            raise ValueError(f"Factories cannot be resolved: {loop}.")

        if isinstance(typ, type) and issubclass(typ, Injectable):
            resolver.from_factory = False
            settle(typ.__resolvers__, factories, typ.__name__, (*chain, typ))
            continue

        provider = factories.get(typ)
        if provider is None:
            raise ValueError(
                f"{owner} injects {typ.__name__} for {resolver.name!r}, but this application "
                f"was built without a factory for it. Add an @factory method producing "
                f"{typ.__name__} to the object you pass as Fusion(factories=...)."
            )

        if resolver.provider is not None and resolver.provider is not provider:
            raise ValueError(
                f"{owner} is already wired by another application, which would leave "
                f"{typ.__name__} built by whichever constructed it last. A Route belongs to "
                f"one application - build the routes for each."
            )

        resolver.from_factory = True
        resolver.provider = provider
        settle(provider.resolvers, factories, f"Factory for {typ.__name__}", (*chain, typ))


__all__ = ["Injectable", "collect_factories", "factory", "settle"]
