from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from functools import wraps
from typing import Any, Callable, ClassVar, Self, TypeVar, get_origin

from msgspec import Struct as Object

from fusion.resolvers import (
    Constructor,
    FactoryResolver,
    InjectableResolver,
    Resolver,
    __factories__,
)

T = TypeVar("T")


class Injectable(Object):
    __resolvers__: ClassVar[list[Resolver]]

    def __init_subclass__(cls, *args, **kwargs):
        cls.__resolvers__ = build_resolvers(cls.__annotations__)
        super().__init_subclass__(*args, **kwargs)

    @classmethod
    async def instance(cls) -> Self:
        """Create an instance of the class with all dependencies resolved."""
        params = {}
        for resolver in cls.__resolvers__:
            name, value = await resolver.resolve()
            params[name] = value

        return cls(**params)


def factory(func: Constructor) -> Constructor:
    """Decorator to register a factory function for a type."""
    if "return" not in func.__annotations__:
        raise ValueError("Factory function must have a return type annotation")
    # Register the factory function
    return_annotation = func.__annotations__["return"]
    origin = get_origin(return_annotation)
    return_type = return_annotation.__args__[0] if origin is AsyncIterator else return_annotation
    __factories__[return_type] = func
    return func


def build_resolvers(annotations: dict[str, Any]) -> list[Resolver]:
    resolvers = []
    for name, annotation in annotations.items():
        origin = get_origin(annotation)
        if not origin:
            if issubclass(annotation, Injectable):
                resolvers.append(InjectableResolver(name=name, typ=annotation))
            elif annotation in __factories__:
                resolvers.append(FactoryResolver(name=name, typ=annotation))
            else:
                raise ValueError(f"Invalid annotation for {name}: {annotation}")
            continue
        # skip if annotation is ClassVar
        if origin is ClassVar:
            continue

        if len(annotation.__args__) != 1:
            raise ValueError(f"Invalid annotation for {name}: {annotation}")

        typ = annotation.__args__[0]
        annotated = origin.__value__
        if not annotated:
            raise ValueError(f"Invalid annotation for {name}: {annotation}")

        if not hasattr(annotated, "__metadata__"):
            raise ValueError(f"Invalid annotation for {name}: {annotation}")

        metadata = annotated.__metadata__[0]
        DependencyResolver = metadata.get("resolver", None)
        if not DependencyResolver:
            raise ValueError(f"No resolver found for {name}: {annotation}")

        if not issubclass(DependencyResolver, Resolver):
            raise ValueError(f"Invalid resolver for {name}: {annotation}")

        resolvers.append(DependencyResolver(name=name, typ=typ))

    return resolvers


def inject(func: Callable) -> Callable:
    """Decorator to mark a function as an injector."""
    resolvers = build_resolvers(func.__annotations__)

    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        params = {}
        for resolver in resolvers:
            name, value = await resolver.resolve()
            params[name] = value
        # Call the original function with resolved parameters
        return await func(self, **params)

    return wrapper
