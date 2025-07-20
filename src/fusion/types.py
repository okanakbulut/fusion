import typing

from fusion.responses import Response

AppType = typing.TypeVar("AppType")

Scope = typing.MutableMapping[str, typing.Any]
Message = typing.MutableMapping[str, typing.Any]

Receive = typing.Callable[[], typing.Awaitable[Message]]
Send = typing.Callable[[Message], typing.Awaitable[None]]

Lifespan = typing.Callable[[AppType], typing.AsyncContextManager[typing.Mapping[str, typing.Any]]]
R = typing.TypeVar("R", bound=Response, covariant=True)


@typing.runtime_checkable
class Injectable(typing.Protocol):
    @classmethod
    async def instance(cls) -> typing.Self:
        ...


@typing.runtime_checkable
class HttpHandler(typing.Protocol[R]):
    async def handle(self, *args, **kwargs) -> R:
        ...


@typing.runtime_checkable
class InjectableHandler(typing.Protocol[R]):
    @classmethod
    async def instance(cls) -> typing.Self:
        ...

    async def handle(self, *args, **kwargs) -> R:
        ...
