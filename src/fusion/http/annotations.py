import enum
import inspect
import typing

import msgspec

from fusion.exceptions import ValidationError

# from starlette.requests import Request
from fusion.http.request import Request

type QueryParam[T] = typing.Annotated[T, "queryparam"]
# type QueryParamFamily[T] = typing.Annotated[T, "queryparamfamily"]
type PathParam[T] = typing.Annotated[T, "pathparam"]
type Header[T] = typing.Annotated[T, "header"]
type RequestBody[T] = typing.Annotated[T, "requestbody"]
type Cookie[T] = typing.Annotated[T, "cookie"]


async def decode(param: inspect.Parameter, request: Request) -> typing.Any:
    if param.annotation is Request:
        return request

    origin = param.annotation.__origin__
    typ = param.annotation.__args__[0]
    if origin is RequestBody:
        return msgspec.json.decode(await request.body(), type=typ)

    typ_is_primitive = typ in (str, int, float, bool) or issubclass(typ, enum.Enum)
    if origin is QueryParam:
        data = request.query_params if typ_is_primitive else request.nested_query_params
    elif origin is PathParam:
        data = request.path_params
    elif origin is Header:
        data = request.headers
    else:
        data = request.cookies

    if param.name in data:
        return msgspec.convert(data[param.name], typ, strict=False)

    if typ_is_primitive:
        if param.default:
            return param.default
        raise ValidationError(f"Missing required parameter: {param.name}")

    return msgspec.convert(data, typ, strict=False)
