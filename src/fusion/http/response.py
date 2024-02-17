from typing import Any, Mapping

from msgspec import json, msgpack, yaml
import msgspec
from starlette.responses import Response as StarletteResponse



class Response(StarletteResponse):
    """
    Represents an HTTP response.
    """

    def render(self, content: Any) -> bytes:
        """
        Render the content into bytes.

        Args:
            content (Any): The content to be rendered.

        Returns:
            bytes: The rendered content as bytes.
        """
        if content is None:
            return b""

        if isinstance(content, bytes):
            return content

        if isinstance(content, str):
            return content.encode("utf-8")

        return msgspec.json.encode(content)
