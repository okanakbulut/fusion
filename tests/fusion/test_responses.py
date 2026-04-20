"""Tests for fusion's response types.

Covers Response, Created, NoContent, Problem and its subclasses,
custom headers, and the renderer utilities.
"""

import pytest

from fusion import Object
from fusion.renderers import BodyRenderer, CookieRenderer, HeaderRenderer, RenderResult
from fusion.responses import (
    BadRequest,
    Created,
    Forbidden,
    InternalServerError,
    MethodNotAllowed,
    NoContent,
    NotFound,
    Problem,
    Response,
    Unauthorized,
    ValidationError,
)

# ---------------------------------------------------------------------------
# ASGI helpers
# ---------------------------------------------------------------------------


async def _call(response) -> tuple[int, dict[str, str], bytes]:
    """Call an ASGI response and collect (status, headers, body)."""
    sent: list[dict] = []

    async def send(msg):
        sent.append(msg)

    scope: dict = {}
    receive = None
    await response(scope, receive, send)

    start = sent[0]
    body_msg = sent[1]
    headers = {k.decode(): v.decode() for k, v in start["headers"]}
    return start["status"], headers, body_msg["body"]


# ---------------------------------------------------------------------------
# Response
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_response_200_with_body():
    class Out(Object):
        x: int

    status, headers, body = await _call(Response(Out(x=1)))
    assert status == 200
    assert b'"x":1' in body
    assert headers["content-type"] == "application/json"


@pytest.mark.asyncio
async def test_response_with_custom_headers():
    class Out(Object):
        x: int

    r = Response(Out(x=1), headers={"x-request-id": "abc"})
    status, headers, _ = await _call(r)
    assert headers["x-request-id"] == "abc"


@pytest.mark.asyncio
async def test_response_none_content():
    status, _, body = await _call(Response(None))
    assert status == 200
    assert body == b'""'


@pytest.mark.asyncio
async def test_created_status_201():
    class Out(Object):
        id: int

    status, _, _ = await _call(Created(Out(id=42)))
    assert status == 201


@pytest.mark.asyncio
async def test_no_content_status_204():
    status, _, _ = await _call(NoContent(None))
    assert status == 204


# ---------------------------------------------------------------------------
# Problem (RFC-9457)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_not_found_problem():
    status, headers, body = await _call(NotFound())
    assert status == 404
    assert headers["content-type"] == "application/problem+json"
    import json

    data = json.loads(body)
    assert data["type"] == "about:blank"
    assert data["title"] == "Not Found"
    assert data["status"] == 404


@pytest.mark.asyncio
async def test_bad_request_with_detail():
    status, _, body = await _call(BadRequest(detail="invalid input"))
    assert status == 400
    import json

    data = json.loads(body)
    assert data["detail"] == "invalid input"


@pytest.mark.asyncio
async def test_unauthorized_problem():
    status, _, _ = await _call(Unauthorized())
    assert status == 401


@pytest.mark.asyncio
async def test_forbidden_problem():
    status, _, _ = await _call(Forbidden())
    assert status == 403


@pytest.mark.asyncio
async def test_method_not_allowed_problem():
    status, _, _ = await _call(MethodNotAllowed())
    assert status == 405


@pytest.mark.asyncio
async def test_internal_server_error_problem():
    status, _, _ = await _call(InternalServerError())
    assert status == 500


@pytest.mark.asyncio
async def test_custom_problem_subclass():
    class OutOfStock(Problem):
        type: str = "https://example.com/out-of-stock"
        status: int = 409
        title: str = "Out of Stock"

    status, _, body = await _call(OutOfStock(detail="item #1 unavailable"))
    assert status == 409
    import json

    data = json.loads(body)
    assert data["type"] == "https://example.com/out-of-stock"


@pytest.mark.asyncio
async def test_validation_error_includes_field_errors():
    from fusion.responses import FieldError

    r = ValidationError(
        detail="failed",
        errors=[FieldError(field="email", message="invalid")],
    )
    _, _, body = await _call(r)
    import json

    data = json.loads(body)
    assert data["errors"] == [{"field": "email", "message": "invalid"}]


@pytest.mark.asyncio
async def test_problem_with_instance():
    r = BadRequest(detail="x", instance="/api/users/1")
    _, _, body = await _call(r)
    import json

    data = json.loads(body)
    assert data["instance"] == "/api/users/1"


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------


def test_body_renderer():
    class Item(Object):
        value: int

    item = Item(value=7)
    r: RenderResult = BodyRenderer(attr_name="value", attr_type=int).render(item)
    assert r.body is not None
    assert b"7" in r.body
    assert r.headers is not None
    assert any(b"application/json" in v for _, v in r.headers)


def test_header_renderer_with_value():
    class Item(Object):
        token: str

    item = Item(token="abc")
    r: RenderResult = HeaderRenderer(attr_name="token", attr_type=str).render(item)
    assert r.headers is not None
    assert r.headers[0] == (b"token", b"abc")


def test_header_renderer_without_value():
    class Item(Object):
        token: str | None = None

    item = Item()
    r: RenderResult = HeaderRenderer(attr_name="token", attr_type=str).render(item)
    assert r.headers is None


def test_cookie_renderer_with_value():
    class Item(Object):
        session: str

    item = Item(session="xyz")
    r: RenderResult = CookieRenderer(attr_name="session", attr_type=str).render(item)
    assert r.cookies is not None
    assert r.cookies[0] == (b"session", b"xyz")


def test_cookie_renderer_without_value():
    class Item(Object):
        session: str | None = None

    item = Item()
    r: RenderResult = CookieRenderer(attr_name="session", attr_type=str).render(item)
    assert r.cookies is None
