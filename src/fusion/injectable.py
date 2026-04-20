import sys
import typing

from .object import Object
from .protocols import AnnotationResolver


class Injectable(Object):
    """Base class for all injectable objects."""

    __allowed_annotations__: typing.ClassVar[set[typing.Any]] = set()
    __resolvers__: typing.ClassVar[dict[str, AnnotationResolver[typing.Any]]] = {}

    def __init_subclass__(cls, **kwargs: typing.Any) -> None:
        super().__init_subclass__(**kwargs)

        global_ns = sys.modules[cls.__module__].__dict__
        type_hints = typing.get_type_hints(
            cls, globalns=global_ns, localns=vars(cls), include_extras=True
        )

        from .resolvers import FactoryResolver, InjectableResolver, has_factory

        resolvers: dict[str, AnnotationResolver[typing.Any]] = {}
        for attr_name, annotation in type_hints.items():
            origin = typing.get_origin(annotation)

            if origin in [typing.ClassVar, type]:
                continue

            if origin is None:
                # Allow bare Injectable types (implicit Inject)
                if isinstance(annotation, type) and issubclass(annotation, Injectable):
                    resolvers[attr_name] = InjectableResolver(name=attr_name, typ=annotation)
                    continue
                if isinstance(annotation, type) and has_factory(annotation):
                    resolvers[attr_name] = FactoryResolver(name=attr_name, typ=annotation)
                    continue
                raise TypeError(f"Type hint {annotation} is not a valid type")

            if cls.__allowed_annotations__ and origin not in cls.__allowed_annotations__:
                raise TypeError(f"Type hint {annotation} is not allowed in {cls.__name__}")

            try:
                annotated = origin.__value__
                metadata = typing.cast(dict[str, typing.Any], annotated.__metadata__[0])
                resolver_class = typing.cast(
                    type[AnnotationResolver[typing.Any]], metadata["resolver"]
                )
            except Exception as exc:
                raise TypeError(f"Type hint {annotation} is not a valid type") from exc

            args = typing.get_args(annotation)
            inner_type = args[0] if args else typing.Any
            resolvers[attr_name] = resolver_class(name=attr_name, typ=inner_type)

        cls.__resolvers__ = resolvers

    @classmethod
    async def instance(cls) -> typing.Self:
        params: dict[str, typing.Any] = {}
        for resolver in cls.__resolvers__.values():
            name, value = await resolver.resolve()
            params[name] = value
        return cls(**params)
