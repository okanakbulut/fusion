from dataclasses import asdict

from starlette.types import ASGIApp, Receive, Scope, Send

from fusion.http.exceptions import HttpException, InternalServerError
from fusion.http.response import Response


class ProblemDetail:
    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        try:
            await self.app(scope, receive, send)
        except Exception as exc:
            await self.handle_exception(scope, receive, send, exc)

    async def handle_exception(self, scope: Scope, receive: Receive, send: Send, exc: Exception):
        # Handle the exception here
        if isinstance(exc, HttpException):
            response = Response(asdict(exc), status_code=exc.status)
        else:
            response = Response(InternalServerError(str(exc)), status_code=500)
        await response(scope, receive, send)
