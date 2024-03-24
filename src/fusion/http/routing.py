import typing

from starlette.middleware import Middleware
from starlette.routing import Route as StarletteRoute
from starlette.types import Receive, Scope, Send

from fusion.di import ExecutionContext
from fusion.http.endpoint import Endpoint
from fusion.http.request import Request


class Route(StarletteRoute):
    def __init__(
        self,
        path: str,
        endpoint: type[Endpoint],
        *,
        methods: typing.Optional[typing.List[str]] = None,
        name: typing.Optional[str] = None,
        include_in_schema: bool = True,
        middleware: typing.Optional[typing.Sequence[Middleware]] = None,
    ) -> None:
        async def wrapped(scope: Scope, receive: Receive, send: Send) -> None:
            nonlocal endpoint
            request = Request(scope, receive, send)
            async with ExecutionContext() as ctx:
                ctx[Request] = request
                ep = await endpoint.instance(ctx)
                response = await ep.dispatch(request)
                await response(scope, receive, send)

        super().__init__(
            path,
            endpoint,
            methods=methods,
            name=name,
            include_in_schema=include_in_schema,
            middleware=middleware,
        )
        self.app = wrapped
        if middleware is not None:
            for cls, args, kwargs in reversed(middleware):
                self.app = cls(app=self.app, *args, **kwargs)
