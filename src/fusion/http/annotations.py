import typing

from fusion.http.decoder import (
    HeaderParamDecoder,
    HeaderStructDecoder,
    QueryParamDecoder,
    QueryParamPartialStructDecoder,
    QueryParamStructDecoder,
)

type HeaderParam[T] = typing.Annotated[T, HeaderParamDecoder]
type HeaderStruct[T] = typing.Annotated[T, HeaderStructDecoder]

type QueryParam[T] = typing.Annotated[T, QueryParamDecoder]
type QueryParamStruct[T] = typing.Annotated[T, QueryParamStructDecoder]
type QueryParamPartialStruct[T] = typing.Annotated[T, QueryParamPartialStructDecoder]
