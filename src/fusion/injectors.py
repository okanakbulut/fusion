from typing import Annotated, Any, Optional, Protocol, Self, cast, runtime_checkable

import msgspec
from starlette.requests import Request


# example of a service that can be injected
class CoreService:
    @classmethod
    async def inject(cls, typ: type[Self], name: str, request: Request, ctx: Any) -> Self:
        return cls()


def inject[T](typ: type[T], name: str, data: dict[str, Any]) -> T:
    if name in data:
        return msgspec.convert(data[name], typ, strict=False)
    else:
        return msgspec.convert(data, typ, strict=False)


class QueryParamInjector[T]:
    @classmethod
    async def inject(cls, typ: type[T], name: str, request: Request, ctx: Any) -> T:
        query_params = cast(dict[str, Any], request.query_params)
        return inject(typ, name, query_params)


class PathParamInjector[T]:
    @classmethod
    async def inject(cls, typ: type[T], name: str, request: Request, ctx: Any) -> T:
        path_params = cast(dict[str, Any], request.path_params)
        return inject(typ, name, path_params)


class HeaderInjector[T]:
    @classmethod
    async def inject(cls, typ: type[T], name: str, request: Request, ctx: Any) -> T:
        headers = cast(dict[str, Any], request.headers)
        return inject(typ, name, headers)


class RequestBodyInjector[T]:
    @classmethod
    async def inject(cls, typ: type[T], name: str, request: Request, ctx: Any) -> T:
        return msgspec.json.decode(await request.body(), type=typ)


class FieldInjector[T]:
    @classmethod
    async def inject(cls, typ: type[T], name: str, request: Request, ctx: Any) -> T:
        ...
