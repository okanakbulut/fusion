from typing import Annotated

from .resolvers import (
    CookieResolver,
    HeaderResolver,
    PathParamResolver,
    QueryParamResolver,
    RequestBodyResolver,
)

type PathParam[T] = Annotated[T, {"resolver": PathParamResolver}]
type QueryParam[T] = Annotated[T, {"resolver": QueryParamResolver}]
type Header[T] = Annotated[T, {"resolver": HeaderResolver}]
type Cookie[T] = Annotated[T, {"resolver": CookieResolver}]
type RequestBody[T] = Annotated[T, {"resolver": RequestBodyResolver}]
