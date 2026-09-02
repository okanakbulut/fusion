from typing import Annotated

from .resolvers import (
    ContextResolver,
    CookieResolver,
    DependencyResolver,
    HeaderResolver,
    Marker,
    PathParamResolver,
    QueryParamResolver,
    RequestBodyResolver,
    ToolArgResolver,
)
from .security import (
    ApiKeyCookieResolver,
    ApiKeyQueryResolver,
    ApiKeyResolver,
    BasicResolver,
    BearerResolver,
    Credentials,
)
from .types import Transport


class Http:
    """Parameter sources for an HTTP request.

    Each marker names exactly one source; nothing falls back to another.
    """

    type Path[T] = Annotated[T, Marker(resolver=PathParamResolver, transport=Transport.HTTP)]
    type Query[T] = Annotated[T, Marker(resolver=QueryParamResolver, transport=Transport.HTTP)]
    type Header[T] = Annotated[T, Marker(resolver=HeaderResolver, transport=Transport.HTTP)]
    type Cookie[T] = Annotated[T, Marker(resolver=CookieResolver, transport=Transport.HTTP)]
    type Body[T] = Annotated[T, Marker(resolver=RequestBodyResolver, transport=Transport.HTTP)]


class Auth:
    """Credentials, named by the scheme that carries them.

    Each alias fixes its own type, so none of them takes a type argument: a
    bearer credential is a token, a basic one is a ``Credentials`` pair.  A
    credential is never listed among an operation's parameters - it is described
    as a security scheme instead.
    """

    type Bearer = Annotated[str, Marker(resolver=BearerResolver, transport=Transport.HTTP)]
    type Basic = Annotated[Credentials, Marker(resolver=BasicResolver, transport=Transport.HTTP)]
    type ApiKey = Annotated[str, Marker(resolver=ApiKeyResolver, transport=Transport.HTTP)]
    type ApiKeyQuery = Annotated[
        str, Marker(resolver=ApiKeyQueryResolver, transport=Transport.HTTP)
    ]
    type ApiKeyCookie = Annotated[
        str, Marker(resolver=ApiKeyCookieResolver, transport=Transport.HTTP)
    ]


class Tool:
    """Parameter sources for a tool call."""

    type Arg[T] = Annotated[T, Marker(resolver=ToolArgResolver, transport=Transport.TOOL)]


type Inject[T] = Annotated[T, Marker(resolver=DependencyResolver, transport=Transport.ANY)]
"""A dependency, resolved identically under every transport."""

type FromContext[T] = Annotated[T, Marker(resolver=ContextResolver, transport=Transport.ANY)]
"""A context-backed façade, such as ``Request``, constructed per call."""
