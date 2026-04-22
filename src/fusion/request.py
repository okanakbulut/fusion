import typing

import msgspec

from .annotations import Cookie, Header, PathParam, QueryParam, RequestBody
from .context import context
from .exceptions import ValidationException
from .injectable import Injectable
from .responses import FieldError
from .types import Receive, Scope, Send


class Request(Injectable):
    """HTTP Request object.
    Provides access to request data such as path parameters, query parameters,
    headers, cookies, and body through annotations.
    Annotations supported:
    - PathParam
    - QueryParam
    - Header
    - Cookie
    - RequestBody

    Example:
    --------
    ```python
    from fusion.annotations import QueryParam, Header, RequestBody
    from fusion.request import Request

    class MyRequest(Request):
        user_id: QueryParam[int]
        auth_token: Header[str]
        data: RequestBody[MyDataModel]
    ```
    """

    __allowed_annotations__ = {Cookie, Header, PathParam, QueryParam, RequestBody}

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
    def headers(self) -> dict[str, str]:
        return context.get().headers

    @property
    def cookies(self) -> dict[str, str]:
        return context.get().cookies

    @property
    def query_params(self) -> dict[str, object | list[object]]:
        return context.get().query_params

    @property
    def path_params(self) -> dict[str, object]:
        return context.get().path_params

    async def body(self) -> bytes:
        return await context.get().body()

    @classmethod
    async def instance(cls) -> typing.Self:
        params: dict[str, typing.Any] = {}
        errors: list[FieldError] = []

        for resolver in cls.__resolvers__.values():
            try:
                name, value = await resolver.resolve()
                params[name] = value
            except ValidationException as exc:
                if exc.errors:
                    errors.extend(exc.errors)
                elif exc.detail:
                    location = getattr(resolver, "location", "unknown")
                    errors.append(
                        FieldError(field=resolver.name, location=location, message=exc.detail)
                    )
            except msgspec.ValidationError as exc:
                location = getattr(resolver, "location", "unknown")
                errors.append(FieldError(field=resolver.name, location=location, message=str(exc)))

        if errors:
            raise ValidationException(errors=errors)

        return cls(**params)
