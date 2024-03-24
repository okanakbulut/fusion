import inspect
import re
import typing

import msgspec

from fusion.http.exceptions import HeaderError, QueryParamError, QueryPathError, RequestError
from fusion.http.request import Request


class RequestDecoder(msgspec.Struct):
    param: inspect.Parameter

    @property
    def annotation(self):
        return self.param.annotation.__args__[0]

    async def __call__(self, request: Request) -> typing.Any:
        if self.param.annotation is Request:
            return request
        try:
            body = await request.body()
            return msgspec.json.decode(body, type=self.param.annotation)
        except Exception as exc:
            error_message = str(exc)
            if match := re.match(r"^(.*) at (.*)$", error_message):
                detail, pointer = match.groups()
                raise RequestError(detail=detail, pointer=pointer)
            elif match := re.match(r"^Object missing required field `(.*)`$", error_message):
                (pointer,) = match.groups()
                raise RequestError(detail=error_message, pointer=pointer)
            raise RequestError(detail=error_message, pointer=self.param.name)


class QueryParamDecoder(RequestDecoder):
    async def __call__(self, request: Request) -> typing.Any:
        default = self.param.default if self.param.default != inspect.Parameter.empty else None
        value = request.query_params.get(self.param.name, default)
        try:
            return msgspec.convert(value, type=self.annotation, strict=False)
        except Exception as exc:
            error_message = str(exc)
            raise QueryParamError(detail=error_message, parameter=self.param.name)


class QueryParamStructDecoder(RequestDecoder):
    async def __call__(self, request: Request) -> typing.Any:
        return msgspec.convert(request.nested_query_params, self.annotation)


class QueryParamPartialStructDecoder(RequestDecoder):
    async def __call__(self, request: Request) -> typing.Any:
        return msgspec.convert(
            dict(request.nested_query_params[self.param.name]), self.annotation, strict=False
        )


class HeaderParamDecoder(RequestDecoder):
    async def __call__(self, request: Request) -> typing.Any:
        default = self.param.default if self.param.default != inspect.Parameter.empty else None
        value = request.headers.get(self.param.name, default)
        try:
            return msgspec.convert(value, type=self.annotation, strict=False)
        except Exception as exc:
            error_message = str(exc)
            raise HeaderError(detail=error_message, header=self.param.name)


class HeaderStructDecoder(RequestDecoder):
    async def __call__(self, request: Request) -> typing.Any:
        try:
            return msgspec.convert(request.headers, self.annotation)
        except Exception as exc:
            error_message = str(exc)
            raise HeaderError(detail=error_message, header=self.param.name)


class QueryPathDecoder(RequestDecoder):
    async def __call__(self, request: Request) -> typing.Any:
        try:
            return msgspec.convert(request.path_params.get(self.param.name), self.annotation)
        except Exception as exc:
            error_message = str(exc)
            raise QueryPathError(detail=error_message, parameter=self.param.name)
