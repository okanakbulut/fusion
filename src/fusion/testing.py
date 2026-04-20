import asyncio
import contextlib
import typing

import httpx

from .types import ASGIApp, Message, Receive, Scope, Send


class LifespanManager(contextlib.AsyncExitStack):
    def __init__(self, app: ASGIApp) -> None:
        super().__init__()
        self.state: dict[str, typing.Any] = dict()
        self.app = self.wrap_app(app)
        self.task: asyncio.Task[None]
        self.receive_queue: asyncio.Queue[Message] = asyncio.Queue()
        self.send_queue: asyncio.Queue[Message] = asyncio.Queue()

    def wrap_app(self, app: ASGIApp) -> ASGIApp:
        async def wrapped_app(scope: Scope, receive: Receive, send: Send) -> None:
            scope["state"] = self.state
            await app(scope, receive, send)

        return wrapped_app

    async def __aenter__(self) -> typing.Self:
        await super().__aenter__()
        scope = {"type": "lifespan"}
        # Queues for communication with the app

        async def receive() -> Message:
            return await self.receive_queue.get()

        async def send(message: Message) -> None:
            await self.send_queue.put(message)

        app_coro = typing.cast(
            typing.Coroutine[typing.Any, typing.Any, None], self.app(scope, receive, send)
        )
        self.task = asyncio.create_task(app_coro)
        # Send startup event
        await self.receive_queue.put({"type": "lifespan.startup"})
        while True:
            message = await self.send_queue.get()
            if message["type"] == "lifespan.startup.complete":
                break
            elif message["type"] == "lifespan.startup.failed":
                raise RuntimeError("Lifespan startup failed")

        return self

    async def __aexit__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        # Send shutdown event
        await self.receive_queue.put({"type": "lifespan.shutdown"})
        while True:
            message = await self.send_queue.get()
            if message["type"] == "lifespan.shutdown.complete":
                break
            elif message["type"] == "lifespan.shutdown.failed":
                raise RuntimeError("Lifespan shutdown failed")
        await self.task
        await super().__aexit__(*args, **kwargs)


@contextlib.asynccontextmanager
async def TestClient(
    app: ASGIApp, base_url: str = "http://testserver", **kwargs: typing.Any
) -> typing.AsyncIterator[httpx.AsyncClient]:
    async with LifespanManager(app) as lifespan:
        async with httpx.AsyncClient(
            base_url=base_url,
            transport=httpx.ASGITransport(app=lifespan.app),
            **kwargs,
        ) as client:
            yield client
