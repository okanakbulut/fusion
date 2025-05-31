from typing import Type

from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.routing import Route as StarletteRoute
from starlette.routing import Router as StarletteRouter
from starlette.types import Receive, Scope, Send

from fusion.context import Context
from fusion.endpoints import HttpEndpoint


class Route(StarletteRoute):
    """Custom route that integrates dependency injection."""

    endpoint: HttpEndpoint

    def __init__(self, path: str, endpoint: Type[HttpEndpoint]):
        super().__init__(path=path, endpoint=endpoint, methods=endpoint.methods)

    async def handle(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["method"] not in self.endpoint.methods:
            headers = {"Allow": ", ".join(self.endpoint.methods)}
            if "app" in scope:
                raise HTTPException(status_code=405, headers=headers)
            else:
                response = PlainTextResponse("Method Not Allowed", status_code=405, headers=headers)
            await response(scope, receive, send)
        else:
            request = Request(scope, receive, send)
            async with Context(request):
                endpoint = (
                    await self.endpoint.instance()
                )  # initialize the endpoint with dependencies resolved
                method = getattr(endpoint, scope["method"].lower())
                response = await method()
                await response(scope, receive, send)


class Router(StarletteRouter):
    ...
