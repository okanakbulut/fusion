import sys
import typing

from .context import Context, current
from .object import Object
from .resolvers import MISSING, Resolver, build_resolvers
from .types import Transport


class Injectable(Object):
    """Base class for all injectable objects.

    Subclasses declare their fields with explicit markers; the resolver table is
    built once at class creation and reused for every instantiation.
    """

    __transports__: typing.ClassVar[frozenset[Transport]] = frozenset(Transport)
    """Which transports this class's fields may draw from.  ``Request`` narrows
    this to HTTP; a plain ``Injectable`` accepts any."""

    __resolvers__: typing.ClassVar[dict[str, Resolver]] = {}

    def __init_subclass__(cls, **kwargs: typing.Any) -> None:
        super().__init_subclass__(**kwargs)

        global_ns = sys.modules[cls.__module__].__dict__
        type_hints = typing.get_type_hints(
            cls, globalns=global_ns, localns=vars(cls), include_extras=True
        )
        cls.__resolvers__ = build_resolvers(
            type_hints, allowed=cls.__transports__, owner=cls.__name__
        )

    @classmethod
    async def instance(
        cls, ctx: Context | None = None, resolvers: dict[str, Resolver] | None = None
    ) -> typing.Self:
        """Build an instance, resolving each field from ``ctx``.

        ``resolvers`` is the table an application settled for one injection
        site; it stands in for the class's own because ``__resolvers__`` is
        shared by every application in the process and so is never wired.
        Omitting it builds from the class table, which is what an ``Injectable``
        instantiated outside any application has.
        """
        params: dict[str, typing.Any] = {}
        ctx = ctx or current()
        if resolvers is None:
            resolvers = cls.__resolvers__
        for resolver in resolvers.values():
            name, value = await resolver.resolve(ctx)
            if value is not MISSING:
                params[name] = value
        return cls(**params)
