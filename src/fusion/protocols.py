import graphlib
import inspect
from typing import Any, Callable, Literal, Protocol, Self, runtime_checkable

from starlette.requests import Request


class Context:
    "Context object that will be passed into the endpoint."

    pass


@runtime_checkable
class CommandProtocol(Protocol):
    "Command protocol we will use to define commands"

    async def execute(self, ctx: Context) -> Any:
        """
        Executes the command with the given context.
        """
        ...


@runtime_checkable
class Injector[T](Protocol):
    @classmethod
    async def inject(cls, typ: type[T], name: str, request: Request, ctx: Any) -> T:
        ...


class Meta(type):
    def __new__(
        cls,
        name,
        bases,
        klass,
        scope: Literal["app", "request"] = "request",
    ):
        return super().__new__(cls, name, bases, klass)


class Injectable(metaclass=Meta):
    "Base class for injectable objects."

    @classmethod
    async def inject(cls, typ: type[Self], name: str, request: Request, ctx: Any) -> Self:
        return cls()


_providers = {}


def provider(fn, scope: Literal["app", "request"] = "request"):
    """
    A decorator to register a function as a provider.

    Example:
    ```python
    @provider
    async def connection(request: Request, ctx: Any, pool:Inject[Pool]) -> Connection:
        return Connection()

    @provider(scope="app")
    async def pool(request: Request, ctx: Any) -> Pool:
        return Pool()

    @injectable(scope="app")
    class UserService:
        conn: Inject[Connection]

        async def findUser(self, id: int) -> User:
            return await self.conn.query("SELECT * FROM users WHERE id = ?", id)

        @classmethod
        async def instance(cls) -> Self:

    ```

    """
    signature = inspect.signature(fn)

    # Injectable.__providers__.append(fn)
    return_type = signature.return_annotation
    dependencies = [(p.name, p.annotation) for p in signature.parameters.values()]

    def wrapper(*args, **kwargs):
        args = {}
        for name, typ in dependencies:
            injector = _providers[typ]
            args[name] = _providers[typ](typ, name, *args, **kwargs)
        return fn(*args, **kwargs)

    _providers[return_type] = wrapper
    return wrapper
