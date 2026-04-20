from typing import Annotated

from .renderers import BodyRenderer, CookieRenderer, HeaderRenderer
from .resolvers import (
    CookieResolver,
    HeaderResolver,
    InjectableResolver,
    PathParamResolver,
    QueryParamResolver,
    RequestBodyResolver,
)

type PathParam[T] = Annotated[T, {"resolver": PathParamResolver}]
type QueryParam[T] = Annotated[T, {"resolver": QueryParamResolver}]
type Header[T] = Annotated[T, {"resolver": HeaderResolver, "renderer": HeaderRenderer}]
type Cookie[T] = Annotated[T, {"resolver": CookieResolver, "renderer": CookieRenderer}]
type Body[T] = Annotated[T, {"resolver": RequestBodyResolver, "renderer": BodyRenderer}]
type RequestBody[T] = Annotated[T, {"resolver": RequestBodyResolver, "renderer": BodyRenderer}]
type Inject[T] = Annotated[T, {"resolver": InjectableResolver}]
