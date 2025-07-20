import typing
from abc import abstractmethod

from fusion.di import Injectable, inject
from fusion.types import R


class Handler(Injectable, typing.Generic[R]):
    """Base class for HTTP handlers."""

    def __init_subclass__(cls, *args, **kwargs):
        super().__init_subclass__(*args, **kwargs)
        if not hasattr(cls, "handle"):
            raise TypeError(f"{cls.__name__} must implement the handle method.")
        cls.handle = inject(cls.handle)

    @abstractmethod
    async def handle(self, *args, **kwargs) -> R:
        """Execute the handler."""
        raise NotImplementedError("Subclasses must implement the handle method.")
