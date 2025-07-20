import typing

import msgspec


class Object(msgspec.Struct, gc=False):
    ...


class Error(Object, omit_defaults=True):
    """
    Represents an error response's body.
    {
        "code": "error_code",
        "message": "Error message",
        "details": [
            {"field": "value"},
            {"field2": "value2"}
        ]
    }
    """

    code: str
    message: str
    details: list[dict[str, str]] | None = None


T = typing.TypeVar("T", bound=Object)


class Response(Object, typing.Generic[T]):
    encoder: typing.ClassVar[msgspec.json.Encoder] = msgspec.json.Encoder()
    status_code: typing.ClassVar[int] = 200

    content: T | str | None = None
    headers: typing.Mapping[str, str] | None = None
    media_type: str = "application/json"

    async def __call__(self, scope, receive, send) -> None:
        body = self.encoder.encode(self.content or "")
        raw_headers = [
            (b"content-type", self.media_type.encode("latin-1")),
            (b"content-length", str(len(body)).encode("latin-1")),
        ]

        if self.headers:
            for k, v in self.headers.items():
                raw_headers.append((k.encode("latin-1"), v.encode("latin-1")))

        await send(
            {
                "type": "http.response.start",
                "status": self.status_code,
                "headers": raw_headers,
            }
        )

        await send({"type": "http.response.body", "body": body})


class Created(Response[T]):
    status_code: typing.ClassVar[int] = 201


class NoContent(Response[T]):
    status_code: typing.ClassVar[int] = 204


class BadRequest(Response[Error]):
    status_code: typing.ClassVar[int] = 400


class Unauthorized(Response[Error]):
    status_code: typing.ClassVar[int] = 401


class Forbidden(Response[Error]):
    status_code: typing.ClassVar[int] = 403


class NotFound(Response[Error]):
    status_code: typing.ClassVar[int] = 404


class InternalServerError(Response[Error]):
    status_code: typing.ClassVar[int] = 500
