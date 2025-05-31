from starlette.applications import Starlette

from fusion.routing import Router


class Fusion(Starlette):
    """Fusion application that integrates dependency injection."""

    def __init__(self, routes, middleware=None, lifespan=None):
        super().__init__(routes=routes, middleware=middleware, lifespan=lifespan)
        self.router = Router(routes=routes, lifespan=lifespan)
