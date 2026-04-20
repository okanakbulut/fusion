import typing

import msgspec

from .object import Object


class RenderResult(Object, kw_only=True):
    body: typing.Optional[bytes] = None
    headers: typing.Optional[list[tuple[bytes, bytes]]] = None
    cookies: typing.Optional[list[tuple[bytes, bytes]]] = None


class Renderer(Object):
    """Base class for renderers."""

    encoder: typing.ClassVar[msgspec.json.Encoder] = msgspec.json.Encoder()

    attr_name: str
    attr_type: type[typing.Any]

    def render(self, obj: typing.Any) -> RenderResult:
        """Render the given object."""
        raise NotImplementedError("Subclasses must implement the render method.")


class BodyRenderer(Renderer):
    def render(self, obj: typing.Any) -> RenderResult:
        """Render the body attribute of the given object."""
        value = getattr(obj, self.attr_name)
        body = self.encoder.encode(value)
        headers = [
            (b"Content-Type", b"application/json"),
            (b"Content-Length", str(len(body)).encode()),
        ]

        return RenderResult(body=body, headers=headers)


class HeaderRenderer(Renderer):
    def render(self, obj: typing.Any) -> RenderResult:
        headers = None
        if value := getattr(obj, self.attr_name, None):
            headers = [(self.attr_name.encode(), str(value).encode())]
        return RenderResult(headers=headers)


class CookieRenderer(Renderer):
    def render(self, obj: typing.Any) -> RenderResult:
        cookies = None
        if value := getattr(obj, self.attr_name, None):
            cookies = [(self.attr_name.encode(), str(value).encode())]
        return RenderResult(cookies=cookies)
