import inspect
from functools import partial, singledispatchmethod
from types import GenericAlias
from typing import (
    Any,
    Awaitable,
    Callable,
    Sequence,
    Type,
    TypeAliasType,
)

from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.routing import Route as StarletteRoute
from starlette.routing import compile_path
from starlette.types import ASGIApp, Receive, Scope, Send

from fusion.http.response import Response
from fusion.protocols import CommandProtocol, Injector


class Route(StarletteRoute):
    """
    Represents a route in the API.
    """

    def __init__(
        self,
        path: str,
        method: str,
        command: Type[CommandProtocol],
        *,
        name: str | None = None,
        include_in_schema: bool = True,
        middleware: Sequence[Middleware] | None = None,
    ) -> None:
        """
        Routing http request to a command.

        Args:
            path (str): The path of the route.
            method (str): The HTTP method of the route.
            command (Type[CommandProtocol]): The command associated with the route.
            name (str, optional): The name of the route. Defaults to None.
            include_in_schema (bool, optional): Includes route in API schema by default.
            middleware (Sequence[Middleware], optional): Middleware for route, defaults to None.
        """
        assert path.startswith("/"), "Routed paths must start with '/'"  # nosec
        self.path = path
        self.endpoint = self.wrap(command)
        self.app = self.endpoint
        self.name = command.__name__ if name is None else name
        self.include_in_schema = include_in_schema
        self.methods = {method.upper()}

        if middleware is not None:
            for cls, options in reversed(middleware):
                self.app = cls(app=self.app, **options)

        self.path_regex, self.path_format, self.param_convertors = compile_path(path)

    def wrap(self, Command: Type[CommandProtocol]) -> ASGIApp:
        """
        Wrap the given command with the route.
        """
        injectors: dict[str, Callable[[Request, Any], Awaitable[Any]]] = self._inspect(Command)

        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            """
            Handle the incoming request.
            """
            request = Request(scope=scope, receive=receive, send=send)
            async with request.app.context_factory(scope) as ctx:
                command = await self.inject(Command, injectors, request, ctx)
                result = await command.execute(ctx)
                response = Response(result, media_type="application/json")
                await response(scope, receive, send)

        return app

    def _inspect(
        self, Command: Type[CommandProtocol]
    ) -> dict[str, Callable[[Request, Any], Awaitable[Any]]]:
        """
        Find the injectors for the command.
        """
        injectors = {}
        for name, annotation in Command.__annotations__.items():
            if isinstance(annotation, GenericAlias):
                origin = annotation.__origin__
                args = annotation.__args__
                if args and isinstance(origin, TypeAliasType):
                    if metadata := origin.__value__.__metadata__:
                        injector = metadata[0]
                        if issubclass(injector, Injector):
                            typ = args[0]
                            injectors[name] = partial(injector.inject, typ, name)
            elif issubclass(annotation, Injector):
                injectors[name] = partial(annotation.inject, annotation, name)

        return injectors

    async def inject(
        self,
        Command: Type[CommandProtocol],
        injectors: dict[str, Callable[[Request, Any], Awaitable[Any]]],
        request: Request,
        ctx: Any,
    ) -> CommandProtocol:
        """
        Inject the command with the request.
        """
        attributes = {}
        for name, injector in injectors.items():
            attributes[name] = await injector(request, ctx)

        return Command(**attributes)

    async def handle(self, scope: Scope, receive: Receive, send: Send) -> None:
        """
        Handle the incoming request.
        """
        await self.app(scope, receive, send)
