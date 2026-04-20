import enum
import typing

AppType = typing.TypeVar("AppType")

Scope = typing.MutableMapping[str, typing.Any]
Message = typing.MutableMapping[str, typing.Any]

Receive = typing.Callable[[], typing.Awaitable[Message]]
Send = typing.Callable[[Message], typing.Awaitable[None]]

Lifespan = typing.Callable[[AppType], typing.AsyncContextManager[typing.Mapping[str, typing.Any]]]


class Method(enum.StrEnum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"
    OPTIONS = "OPTIONS"
    HEAD = "HEAD"


class Match(enum.IntEnum):
    NONE = 0
    PARTIAL = 1
    FULL = 2
