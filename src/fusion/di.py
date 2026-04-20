import typing
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager

from .injectable import Injectable
from .resolvers import __factories__

T = typing.TypeVar("T")
type Constructor[T] = typing.Callable[[], typing.Awaitable[T] | AbstractAsyncContextManager[T]]


def _get_factory_type(return_annotation: typing.Any) -> type[typing.Any]:
    origin = typing.get_origin(return_annotation)
    if origin in {AsyncIterator, AbstractAsyncContextManager}:
        args = typing.get_args(return_annotation)
        if not args:  # pragma: no cover
            raise ValueError("Factory return type must specify the produced value type")
        return typing.cast(type[typing.Any], args[0])
    return typing.cast(type[typing.Any], return_annotation)


def factory(func: Constructor[T]) -> Constructor[T]:
    """Register a factory function by its produced type."""
    if "return" not in func.__annotations__:
        raise ValueError("Factory function must have a return type annotation")

    produced_type = _get_factory_type(func.__annotations__["return"])
    __factories__[produced_type] = func
    return func


__all__ = ["Injectable", "factory"]
