from abc import abstractmethod

from .di import Injectable
from .request import Request
from .responses import Response


class Handler(Injectable):
    """Base class for HTTP handlers."""

    @abstractmethod
    async def handle(self, request: Request) -> Response:
        """Execute the handler."""
        raise NotImplementedError("Subclasses must implement the handle method.")
