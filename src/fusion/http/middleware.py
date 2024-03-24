from dataclasses import asdict

from fusion.http.exceptions import HttpException, InternalServerError
from fusion.http.response import Response


class ProblemDetail:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        try:
            await self.app(scope, receive, send)
        except Exception as exc:
            await self.handle_exception(scope, receive, send, exc)

    async def handle_exception(self, scope, receive, send, exc):
        # Handle the exception here
        if isinstance(exc, HttpException):
            response = Response(asdict(exc), status_code=exc.status)
        else:
            response = Response(InternalServerError(str(exc)), status_code=500)
        await response(scope, receive, send)
