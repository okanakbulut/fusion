import abc
import typing

from .injectable import Injectable
from .protocols import HttpRequest, HttpResponse


class Handler[TRequest: HttpRequest, TResponse: HttpResponse](Injectable):
    """Base class for HTTP handlers.

    Only bare ``Injectable`` subclasses and factory-backed types may be used
    as class-level annotations.  Request-scoped annotations such as
    ``Header``, ``QueryParam``, ``PathParam``, ``Body`` / ``RequestBody`` and
    ``Cookie`` must be placed on a dedicated ``Injectable`` subclass that is
    then injected into the handler.
    """

    def __init_subclass__(cls, **kwargs: typing.Any) -> None:
        super().__init_subclass__(**kwargs)

        from .resolvers import FactoryResolver, InjectableResolver

        for attr_name, resolver in cls.__resolvers__.items():
            if not isinstance(resolver, (InjectableResolver, FactoryResolver)):
                raise TypeError(
                    f"Attribute '{attr_name}' on Handler subclass '{cls.__name__}' uses a "
                    f"request-scoped annotation which is not allowed directly on a Handler. "
                    f"Request-scoped annotations must be placed on a Request or Response subclass."
                )

    @abc.abstractmethod
    async def handle(self, request: TRequest) -> TResponse:
        """Execute the handler."""
        raise NotImplementedError("Subclasses must implement the handle method.")

    def get_request_class(self) -> type[TRequest]:
        """Get the request class inspected from the handle method."""
        return typing.get_type_hints(self.handle)["request"]
