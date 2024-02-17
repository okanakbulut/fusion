from contextlib import AbstractAsyncContextManager, asynccontextmanager
from types import SimpleNamespace
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Optional,
)

from starlette.applications import Starlette
from starlette.types import Scope

from fusion.protocols import Context


class Application(Starlette):
    "Main application for API service."

    def __init__(
        self,
        *args,
        context_factory: Optional[Callable[[Scope], AsyncIterator[Any]]] = None,
        **kwargs,
    ):
        """
        Initialize the Application object.

        Args:
            context_factory: A factory function to customize ctx object passed into endpoint.

            Note: for args and kwargs, see Starlette documentation.
        """

        async def default_context_factory(scope: Scope) -> AsyncIterator[Context]:
            # initialize ctx object
            yield Context()
            # clean up ctx object

        context_factory = context_factory or default_context_factory
        self.context_factory = asynccontextmanager(context_factory)
        super().__init__(*args, **kwargs)
