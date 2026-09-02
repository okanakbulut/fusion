import typing

from .binding import Returns, Signature, bind, check_returns
from .middleware import chain
from .protocols import HttpResponse
from .responses import EventStream
from .security import authorize
from .types import Method, Transport


class Endpoint:
    """Terminal handler of a route's middleware chain.

    Binds the signature against the active context, then follows whichever call
    convention was settled at registration - awaiting a coroutine, or wrapping an
    async generator as a stream.
    """

    __slots__ = ("keepalive", "signature")

    def __init__(self, signature: Signature, keepalive: float | None = None) -> None:
        check_returns(signature, Returns.HANDLER)
        self.signature = signature
        self.keepalive = keepalive

    async def handle(self) -> typing.Any:
        if problem := await authorize(self.signature.roles):
            return problem
        kwargs = await bind(self.signature)
        if self.signature.is_asyncgen:
            return EventStream(self.signature.func(**kwargs), keepalive=self.keepalive)
        return await self.signature.func(**kwargs)


class Route:
    __slots__ = (
        "description",
        "handler",
        "keepalive",
        "methods",
        "middlewares",
        "operation_id",
        "path",
        "signature",
        "summary",
        "tags",
    )

    path: str
    methods: tuple[Method, ...]
    signature: Signature
    middlewares: tuple[Signature, ...]
    """Signatures of the route's middlewares, kept because they are part of the
    operation's HTTP contract: a header a middleware binds is one this route
    requires, and a document that omitted it would describe a request the route
    rejects."""

    def __init__(
        self,
        path: str,
        handler: typing.Callable[..., typing.Any],
        method: Method | str | None = None,
        methods: typing.Sequence[Method | str] | None = None,
        middlewares: typing.Sequence[typing.Callable[..., typing.Any]] | None = None,
        *,
        keepalive: float | None = None,
        summary: str | None = None,
        description: str | None = None,
        tags: typing.Sequence[str] | None = None,
        operation_id: str | None = None,
    ) -> None:
        if methods:
            selected = list(methods)
        elif method is not None:
            selected = [method]
        else:
            raise ValueError("Either 'method' or 'methods' must be provided")

        self.path = path
        self.methods = tuple(Method(m.upper()) if isinstance(m, str) else m for m in selected)
        self.signature = Signature.of(handler, transport=Transport.HTTP)
        self.keepalive = keepalive
        self.summary = summary or self.signature.summary
        self.description = description or self.signature.description
        self.tags = tuple(tags or ())
        self.operation_id = operation_id or self.signature.name

        self.middlewares = tuple(
            Signature.of(middleware, transport=Transport.HTTP) for middleware in middlewares or ()
        )
        self._check_authorization()
        self.handler = chain(Endpoint(self.signature, keepalive), self.middlewares)

    def _check_authorization(self) -> None:
        """Roles are only a contract if the request says who is asking.

        An OpenAPI security requirement is keyed by a scheme name, so roles
        declared with no credential in the chain could be enforced but never
        documented.  Rejecting the combination here keeps the document complete.
        """
        signatures = (self.signature, *self.middlewares)
        if not any(signature.roles for signature in signatures):
            return
        if any(signature.credentials for signature in signatures):
            return
        raise TypeError(
            f"Route {self.path!r} requires roles but declares no credential. Add an Auth.* "
            f"parameter to the handler or one of its middlewares, so the requirement can be "
            f"documented as a security scheme."
        )

    @property
    def method(self) -> Method:
        """First registered method.  Kept for callers that assume one verb."""
        return self.methods[0]

    async def handle(self) -> HttpResponse:
        return await self.handler.handle()


def _shorthand(verb: Method) -> typing.Callable[..., Route]:
    def make(
        path: str,
        handler: typing.Callable[..., typing.Any],
        middlewares: typing.Sequence[typing.Callable[..., typing.Any]] | None = None,
        **kwargs: typing.Any,
    ) -> Route:
        return Route(path=path, handler=handler, method=verb, middlewares=middlewares, **kwargs)

    make.__name__ = verb.value.title()
    make.__qualname__ = make.__name__
    make.__doc__ = f"Route responding to HTTP {verb.value}."
    return make


Get = _shorthand(Method.GET)
Post = _shorthand(Method.POST)
Put = _shorthand(Method.PUT)
Delete = _shorthand(Method.DELETE)
Patch = _shorthand(Method.PATCH)
Options = _shorthand(Method.OPTIONS)
Head = _shorthand(Method.HEAD)
