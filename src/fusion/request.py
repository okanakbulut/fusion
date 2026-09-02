import typing

from .context import context
from .types import Receive, Scope, Send


class Request:
    """Live view of the active HTTP request.

    Every attribute reads through to the ambient context, so a ``Request`` holds
    no state of its own and is cheap to construct.  Declare it as
    ``FromContext[Request]`` to get at anything the markers do not cover.
    """

    @property
    def scope(self) -> Scope:
        return context.get().scope

    @property
    def receive(self) -> Receive:
        return context.get().receive

    @property
    def send(self) -> Send:
        return context.get().send

    @property
    def method(self) -> str:
        return context.get().method

    @property
    def path(self) -> str:
        return context.get().path

    @property
    def headers(self) -> dict[str, str]:
        return context.get().headers

    @property
    def cookies(self) -> dict[str, str]:
        return context.get().cookies

    @property
    def query_params(self) -> dict[str, typing.Any | list[typing.Any]]:
        return context.get().query_params

    @property
    def path_params(self) -> dict[str, typing.Any]:
        return context.get().path_params

    async def body(self) -> bytes:
        return await context.get().body()
