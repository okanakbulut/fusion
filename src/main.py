import contextlib
import typing
from contextlib import asynccontextmanager

from fusion import Fusion, Injectable, Route, factory
from fusion.handler import Handler
from fusion.responses import Object, Response

AppType = typing.TypeVar("AppType", bound="Fusion")

Lifespan = typing.Callable[[AppType], typing.AsyncContextManager[typing.Mapping[str, typing.Any]]]


# default lifespan
@contextlib.asynccontextmanager
async def default_lifespan(app) -> typing.AsyncIterator[typing.Any]:
    yield dict(foo="foo", bar="bar")  # Example state, can be empty


class Connection:
    pass


@factory
@asynccontextmanager
async def create_connection() -> typing.AsyncIterator[Connection]:
    """Factory function to create a Connection instance."""
    yield Connection()


# example usage
class User(Object):
    id: int
    name: str
    address: str = "Unknown"


class UserService(Injectable):
    connection: Connection

    async def get_user(self, user_id: int) -> User:
        return User(id=user_id, name="John Doe", address="123 Main St")


class GetUser(Handler):
    # user_service: UserService

    async def handle(self) -> Response[User]:
        # return Response(await self.user_service.get_user(user_id))
        return Response(User(id=101, name="John Doe", address="123 Main St"))


app = Fusion(
    routes=[
        Route("/users", methods=["GET"], handler=GetUser),
    ],
    # middlewares=[Middleware(BaseMiddleware)],
)
