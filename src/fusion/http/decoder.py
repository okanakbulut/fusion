import inspect
import re
import typing
from dataclasses import dataclass

import msgspec

from fusion.http.exceptions import HeaderError, QueryParamError
from fusion.http.request import Request


class Decoder(msgspec.Struct):
    param: inspect.Parameter

    @property
    def annotation(self):
        return self.param.annotation.__args__[0]

    async def __call__(self, request: Request) -> typing.Any:
        return request


class QueryParamDecoder(Decoder):
    async def __call__(self, request: Request) -> typing.Any:
        default = self.param.default if self.param.default != inspect.Parameter.empty else None
        value = request.query_params.get(self.param.name, default)
        try:
            return msgspec.convert(value, type=self.annotation, strict=False)
        except Exception as exc:
            error_message = str(exc)
            raise QueryParamError(detail=error_message, parameter=self.param.name)


class QueryParamStructDecoder(Decoder):
    async def __call__(self, request: Request) -> typing.Any:
        return msgspec.convert(request.nested_query_params, self.annotation)


class QueryParamPartialStructDecoder(Decoder):
    async def __call__(self, request: Request) -> typing.Any:
        return msgspec.convert(
            dict(request.nested_query_params[self.param.name]), self.annotation, strict=False
        )


class HeaderParamDecoder(Decoder):
    async def __call__(self, request: Request) -> typing.Any:
        default = self.param.default if self.param.default != inspect.Parameter.empty else None
        value = request.headers.get(self.param.name, default)
        try:
            return msgspec.convert(value, type=self.annotation, strict=False)
        except Exception as exc:
            error_message = str(exc)
            raise HeaderError(detail=error_message, header=self.param.name)


class HeaderStructDecoder(Decoder):
    async def __call__(self, request: Request) -> typing.Any:
        try:
            return msgspec.convert(request.headers, self.annotation)
        except Exception as exc:
            error_message = str(exc)
            raise HeaderError(detail=error_message)
