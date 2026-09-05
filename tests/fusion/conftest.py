import contextlib
import typing

import httpx
import pytest

from fusion import Fusion


@contextlib.asynccontextmanager
async def client_for(app: Fusion) -> typing.AsyncIterator[httpx.AsyncClient]:
    """An HTTP client bound directly to an app, without a real server."""
    async with httpx.AsyncClient(
        base_url="http://testserver", transport=httpx.ASGITransport(app=app)
    ) as client:
        yield client


@pytest.fixture
def make_client():
    return client_for


@pytest.fixture(autouse=True)
def _isolate_factories():
    """Keep @factory registrations from leaking between tests.

    The registry is a process-wide dict, so a factory registered by one test is
    visible to every later one unless it is restored.
    """
    from fusion.resolvers import __factories__

    saved = dict(__factories__)
    yield
    __factories__.clear()
    __factories__.update(saved)
