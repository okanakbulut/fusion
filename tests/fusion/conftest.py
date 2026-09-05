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
