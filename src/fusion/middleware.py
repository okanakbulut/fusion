"""Middleware as plain async functions.

Nothing is handed to a middleware to call: the chain runs on its own, and a
middleware says what it wants by the shape of the function.

A plain ``async def`` is a guard.  It runs before the route; returning a
response ends the request there, returning nothing falls through to whatever
comes next.

An ``async def`` that yields wraps the rest of the chain.  Everything before the
``yield`` runs on the way in, the route's response is handed back through the
``yield``, and everything after runs on the way out - including ``except`` and
``finally``, since a failure downstream is thrown back in at that point.
Yielding a second value replaces the response.

Either shape binds its parameters exactly as a handler does.
"""

import typing

from .binding import Returns, Signature, bind, check_returns
from .protocols import HttpHandler
from .security import authorize


class Link:
    """One middleware in a route's chain, bound to whatever follows it.

    The chain is assembled at registration, so a request pays only for binding
    the parameters each middleware actually declared.
    """

    __slots__ = ("app", "signature")

    def __init__(self, signature: Signature, app: HttpHandler) -> None:
        check_returns(signature, Returns.MIDDLEWARE)
        self.signature = signature
        self.app = app

    async def handle(self) -> typing.Any:
        if problem := await authorize(self.signature.roles):
            return problem
        kwargs = await bind(self.signature)
        if self.signature.is_asyncgen:
            return await self._wrap(self.signature.func(**kwargs))
        response = await self.signature.func(**kwargs)
        return response if response is not None else await self.app.handle()

    async def _wrap(self, generator: typing.AsyncGenerator[typing.Any, typing.Any]) -> typing.Any:
        """Run the rest of the chain between the two halves of ``generator``."""
        try:
            first = await anext(generator)
        except StopAsyncIteration:
            raise RuntimeError(
                f"Middleware {self.signature.name!r} finished without reaching its 'yield'. "
                f"To answer a request without running the route, write the middleware as a "
                f"plain 'async def' that returns a response."
            ) from None

        if first is not None:
            raise RuntimeError(
                f"Middleware {self.signature.name!r} yielded {first!r} before the route ran. "
                f"The first 'yield' must be bare - it is where the rest of the chain runs."
            )

        try:
            response = await self.app.handle()
        except Exception as exc:
            return await self._resume(generator, generator.athrow(exc), fallback=exc)
        return await self._resume(generator, generator.asend(response), fallback=response)

    async def _resume(
        self,
        generator: typing.AsyncGenerator[typing.Any, typing.Any],
        resumed: typing.Awaitable[typing.Any],
        *,
        fallback: typing.Any,
    ) -> typing.Any:
        """Finish the second half of a wrapper, honouring a replacement response.

        ``fallback`` is what stands when the middleware yields nothing further:
        the response the chain produced, or the exception it raised.
        """
        try:
            replacement = await resumed
        except StopAsyncIteration:
            replacement = None
        else:
            # A middleware that yielded a replacement is still suspended; close
            # it so its own cleanup runs before the response goes out.
            await generator.aclose()

        if replacement is None:
            if isinstance(fallback, BaseException):
                raise fallback from None
            return fallback
        return replacement


def chain(endpoint: HttpHandler, middlewares: typing.Sequence[Signature]) -> HttpHandler:
    """Wrap ``endpoint`` in ``middlewares``, first in the sequence outermost."""
    handler = endpoint
    for signature in reversed(middlewares):
        handler = Link(signature, handler)
    return handler
