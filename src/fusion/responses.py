import typing

import msgspec


class Object(msgspec.Struct, gc=False): ...


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

        await send({
            "type": "http.response.start",
            "status": self.status_code,
            "headers": raw_headers,
        })

        await send({"type": "http.response.body", "body": body})


class Created(Response):
    status_code: typing.ClassVar[int] = 201


class NoContent(Response):
    status_code: typing.ClassVar[int] = 204


class Problem(Object, omit_defaults=True):
    """Base RFC-9457 ASGI error response. Subclass and set type/status as ClassVars, title as field default."""

    encoder: typing.ClassVar[msgspec.json.Encoder] = msgspec.json.Encoder()
    type: typing.ClassVar[str] = "about:blank"
    status: typing.ClassVar[int] = 500
    title: str
    detail: typing.Optional[str] = None
    instance: typing.Optional[str] = None

    @property
    def body(self) -> dict:
        return dict(
            type=self.type,
            status=self.status,
            title=self.title,
            detail=self.detail,
            instance=self.instance,
        )

    async def __call__(self, scope, receive, send) -> None:
        body = self.encoder.encode(self.body)
        headers = [
            (b"content-type", b"application/problem+json"),
            (b"content-length", str(len(body)).encode("latin-1")),
        ]
        await send({"type": "http.response.start", "status": self.status, "headers": headers})
        await send({"type": "http.response.body", "body": body})


class NotFound(Problem):
    status: typing.ClassVar[int] = 404
    title: str = "Not Found"


class BadRequest(Problem):
    status: typing.ClassVar[int] = 400
    title: str = "Bad Request"


class Unauthorized(Problem):
    status: typing.ClassVar[int] = 401
    title: str = "Unauthorized"


class Forbidden(Problem):
    status: typing.ClassVar[int] = 403
    title: str = "Forbidden"


class MethodNotAllowed(Problem):
    status: typing.ClassVar[int] = 405
    title: str = "Method Not Allowed"


class InternalServerError(Problem):
    status: typing.ClassVar[int] = 500
    title: str = "Internal Server Error"


class FieldError(Object):
    field: str
    message: str


class ValidationError(BadRequest):
    errors: list[FieldError] | None = None

    @property
    def body(self) -> dict:
        problem = super().body
        problem.update(errors=self.errors)
        return problem
