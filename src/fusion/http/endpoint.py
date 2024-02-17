import inspect
import typing
from typing import Any, Callable, ClassVar, Coroutine

import msgspec
from starlette.exceptions import HTTPException

from fusion.di import Injectable

# from starlette.requests import Request
from fusion.http.annotations import (
    Cookie,
    Header,
    PathParam,
    QueryParam,
    RequestBody,
    decode,
)
from fusion.http.request import Request
from fusion.http.response import Response


class Endpoint(Injectable):
    _allowed_methods: ClassVar[list[str]] = []

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        def wrap(
            handler: Callable[..., Any],
        ) -> Callable[[Endpoint, Request], Coroutine[Any, Any, Response]]:
            signature = inspect.signature(handler)
            params: list[inspect.Parameter] = []
            for _, param in signature.parameters.items():
                if param.name == "self":
                    continue
                if param.annotation is Request:
                    params.append(param)
                    continue
                if origin := getattr(param.annotation, "__origin__", None):
                    if origin in (QueryParam, PathParam, Header, RequestBody, Cookie):
                        params.append(param)
                        continue
                raise TypeError(f"Unsupported parameter type: {param.annotation}")

            async def wrapped(this: Endpoint, request: Request) -> Response:
                try:
                    args: list[typing.Any] = [await decode(param, request) for param in params]
                except msgspec.ValidationError as e:
                    return Response(str(e), status_code=400)

                return Response(await handler(this, *args))

            return wrapped

        for method in ("GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"):
            if handler := getattr(cls, method.lower(), None):
                setattr(cls, method.lower(), wrap(handler))
                cls._allowed_methods.append(method)

    async def method_not_allowed(self, request: Request) -> Response:
        headers = {"Allow": ", ".join(self._allowed_methods)}
        if "app" in request.scope:
            raise HTTPException(status_code=405, headers=headers)
        return Response("Method Not Allowed", status_code=405, headers=headers)

    async def dispatch(self, request: Request) -> Response:
        handler_name = (
            "get"
            if request.method == "HEAD" and not hasattr(self, "head")
            else request.method.lower()
        )

        handler: Callable[..., Any] = getattr(self, handler_name, self.method_not_allowed)
        # TODO: inject query params, path params, and body into handler
        return await handler(request)
        # result = await handler(request)
        # # TODO: decode result into response
        # return Response(result)
