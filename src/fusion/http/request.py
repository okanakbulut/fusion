import typing
from urllib.parse import parse_qsl

from starlette.requests import Request as StarletteRequest

from fusion.http.types import QueryParams


class Request(StarletteRequest):
    _nested_query_params: typing.Optional[QueryParams] = None

    @property
    def nested_query_params(self) -> QueryParams:
        if not self._nested_query_params:
            query_string = self.scope["query_string"].decode("utf8")
            self._nested_query_params = QueryParams(parse_qsl(query_string))
        return self._nested_query_params
