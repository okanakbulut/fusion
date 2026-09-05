import typing

import pytest

from fusion import (
    BadRequest,
    Created,
    FieldError,
    Forbidden,
    InternalServerError,
    MethodNotAllowed,
    NoContent,
    NotFound,
    Object,
    Problem,
    Response,
    Unauthorized,
    ValidationProblem,
)
from fusion.responses import raw_headers


class Payload(Object):
    value: int


async def render(response) -> tuple[dict, list[dict]]:
    """Drive a response's ASGI call, returning (start, body messages)."""
    messages: list[dict] = []

    async def send(message):
        messages.append(message)

    await response({}, None, send)
    return messages[0], messages[1:]


@pytest.mark.asyncio
async def test_response_defaults_to_200_json():
    start, body = await render(Response(Payload(value=1)))

    assert start["status"] == 200
    assert dict(start["headers"])[b"content-type"] == b"application/json"
    assert body[0]["body"] == b'{"value":1}'


@pytest.mark.asyncio
async def test_response_includes_content_length():
    start, body = await render(Response(Payload(value=1)))
    assert dict(start["headers"])[b"content-length"] == str(len(body[0]["body"])).encode()


@pytest.mark.asyncio
async def test_response_custom_headers_and_media_type():
    start, _ = await render(
        Response(Payload(value=1), headers={"x-a": "b"}, media_type="application/x-thing")
    )
    headers = dict(start["headers"])

    assert headers[b"x-a"] == b"b"
    assert headers[b"content-type"] == b"application/x-thing"


@pytest.mark.asyncio
async def test_created_is_201():
    start, _ = await render(Created(Payload(value=1)))
    assert start["status"] == 201


@pytest.mark.asyncio
async def test_no_content_is_204_with_no_body():
    start, body = await render(NoContent())
    assert start["status"] == 204
    assert body[0]["body"] == b""
    assert not any(k == b"content-type" for k, _ in start["headers"])


@pytest.mark.asyncio
async def test_no_content_keeps_custom_headers():
    start, _ = await render(NoContent(headers={"x-a": "b"}))
    assert dict(start["headers"])[b"x-a"] == b"b"


@pytest.mark.parametrize(
    ("cls", "status", "title"),
    [
        (NotFound, 404, "Not Found"),
        (BadRequest, 400, "Bad Request"),
        (Unauthorized, 401, "Unauthorized"),
        (Forbidden, 403, "Forbidden"),
        (MethodNotAllowed, 405, "Method Not Allowed"),
        (InternalServerError, 500, "Internal Server Error"),
    ],
)
@pytest.mark.asyncio
async def test_problem_statuses(cls, status, title):
    start, body = await render(cls())

    assert start["status"] == status
    assert dict(start["headers"])[b"content-type"] == b"application/problem+json"
    assert f'"title":"{title}"'.encode() in body[0]["body"]


@pytest.mark.asyncio
async def test_problem_body_uses_the_rfc_status_member():
    """The class attribute is status_code; the wire member stays `status`."""
    _start, body = await render(NotFound(detail="gone"))

    assert NotFound.status_code == 404
    assert b'"status":404' in body[0]["body"]
    assert b'"detail":"gone"' in body[0]["body"]


@pytest.mark.asyncio
async def test_validation_problem_includes_errors():
    problem = ValidationProblem(
        detail="bad", errors=[FieldError(field="a", location="query", message="m")]
    )
    _, body = await render(problem)

    assert b'"errors"' in body[0]["body"]
    assert b'"field":"a"' in body[0]["body"]


@pytest.mark.asyncio
async def test_custom_problem_subclass():
    class Teapot(Problem):
        type: typing.ClassVar[str] = "https://example.com/teapot"
        status_code: typing.ClassVar[int] = 418
        title: str = "I'm a teapot"

    start, body = await render(Teapot(detail="short and stout"))

    assert start["status"] == 418
    assert b"https://example.com/teapot" in body[0]["body"]


def test_raw_headers_omits_content_length_when_unknown():
    headers = raw_headers("text/event-stream", None, None)
    assert headers == [(b"content-type", b"text/event-stream")]


def test_raw_headers_appends_custom_headers():
    headers = raw_headers("application/json", {"x-a": "b"}, 3)
    assert headers == [
        (b"content-type", b"application/json"),
        (b"content-length", b"3"),
        (b"x-a", b"b"),
    ]
