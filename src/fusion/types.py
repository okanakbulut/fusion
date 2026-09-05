import enum
import typing

AppType = typing.TypeVar("AppType")

Scope = typing.MutableMapping[str, typing.Any]
Message = typing.MutableMapping[str, typing.Any]

Receive = typing.Callable[[], typing.Awaitable[Message]]
Send = typing.Callable[[Message], typing.Awaitable[None]]
type ASGIApp = typing.Callable[[Scope, Receive, Send], typing.Awaitable[None]]

Lifespan = typing.Callable[[AppType], typing.AsyncContextManager[typing.Mapping[str, typing.Any]]]


class Transport(enum.StrEnum):
    """Where a parameter's value comes from.

    ``ANY`` marks a parameter that resolves identically under every transport,
    such as an injected dependency.
    """

    HTTP = "http"
    TOOL = "tool"
    ANY = "any"


class Method(enum.StrEnum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"
    OPTIONS = "OPTIONS"
    HEAD = "HEAD"
