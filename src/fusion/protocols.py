import typing
from abc import abstractmethod

from .request import Request
from .responses import Object, Response


@typing.runtime_checkable
class Injectable(typing.Protocol):
    @classmethod
    async def instance(cls) -> typing.Self:
        ...


@typing.runtime_checkable
class HttpHandler(typing.Protocol):
    async def handle(self, request: Request) -> Response:
        ...


@typing.runtime_checkable
class InjectableHandler(typing.Protocol):
    @classmethod
    async def instance(cls) -> typing.Self:
        ...

    async def handle(self, *args, **kwargs) -> Response:
        ...


T = typing.TypeVar("T")


class Resolver(Object, typing.Generic[T]):
    """Base class for resolvers."""

    name: str
    typ: typing.Type[T]

    @abstractmethod
    async def resolve(self, request: Request) -> tuple[str, T | None]:
        """Resolve the dependency."""
        raise NotImplementedError("Subclasses must implement this method")
