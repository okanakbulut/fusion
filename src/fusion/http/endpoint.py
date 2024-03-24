import inspect
import re
import typing
from typing import Any, Callable, ClassVar, Coroutine

from fusion.di import Injectable
from fusion.http.decoder import Decoder
from fusion.http.exceptions import BadRequestException, HttpException, ValidationError
from fusion.http.request import Request
from fusion.http.response import Response


class Endpoint(Injectable):
    _allowed_methods: ClassVar[list[str]] = []

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Wrap endpoint methods to decode parameters and encode results."""
        super().__init_subclass__(**kwargs)

        def wrap(
            handler: Callable[..., Any],
        ) -> Callable[[Endpoint, Request], Coroutine[Any, Any, Response]]:
            signature = inspect.signature(handler)
            parameters: list[tuple[str, Decoder]] = []
            for _, param in signature.parameters.items():
                if param.name == "self":
                    continue
                elif origin := getattr(param.annotation, "__origin__", None):
                    if isinstance(origin, typing.TypeAliasType):
                        # if metadata := getattr(origin, "__metadata__", None):
                        DecoderType = origin.__value__.__metadata__[0]
                        if issubclass(DecoderType, Decoder):
                            parameters.append((param.name, DecoderType(param)))
                else:
                    raise TypeError(f"Unsupported parameter type: {param.annotation}")

            # scope: Scope, receive: Receive, send: Send
            async def wrapped(this: Endpoint, request: Request) -> Response:
                nonlocal parameters
                errors: list[ValidationError] = []
                args: list[typing.Any] = []

                for name, decoder in parameters:
                    try:
                        args.append(await decoder(request))
                    except ValidationError as error:
                        errors.append(error)

                if errors:
                    raise BadRequestException(errors=errors)
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
