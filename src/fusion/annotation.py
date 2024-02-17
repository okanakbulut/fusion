from typing import Annotated

from fusion.injectors import (
    FieldInjector,
    HeaderInjector,
    PathParamInjector,
    QueryParamInjector,
    RequestBodyInjector,
)

type QueryParam[T] = Annotated[T, QueryParamInjector]
type PathParam[T] = Annotated[T, PathParamInjector]
type Header[T] = Annotated[T, HeaderInjector]
type RequestBody[T] = Annotated[T, RequestBodyInjector]
type Inject[T] = Annotated[T, FieldInjector]
