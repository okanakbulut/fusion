from typing import ClassVar


class HttpException(Exception):
    """Base class for HTTP errors."""

    status_code: ClassVar[int] = 500

    def __init__(self, code: str, message: str, details: list[dict[str, str]] | None = None):
        super().__init__(message)
        self.code = code
        self.details = details

    @property
    def message(self) -> str:
        """Get the error message."""
        return self.args[0]


class NotFound(HttpException):
    """Exception raised when a resource is not found."""

    status_code: ClassVar[int] = 404

    def __init__(
        self,
        message: str = "Resource not found",
        details: list[dict[str, str]] | None = None,
    ):
        super().__init__("not_found", message, details)


class BadRequest(HttpException):
    """Exception raised for bad requests."""

    status_code: ClassVar[int] = 400

    def __init__(
        self,
        message: str = "Bad request",
        details: list[dict[str, str]] | None = None,
    ):
        super().__init__("bad_request", message, details)


class Unauthorized(HttpException):
    """Exception raised for unauthorized access."""

    status_code: ClassVar[int] = 401

    def __init__(
        self,
        message: str = "Unauthorized",
        details: list[dict[str, str]] | None = None,
    ):
        super().__init__("unauthorized", message, details)


class Forbidden(HttpException):
    """Exception raised for forbidden access."""

    status_code: ClassVar[int] = 403

    def __init__(
        self,
        message: str = "Forbidden",
        details: list[dict[str, str]] | None = None,
    ):
        super().__init__("forbidden", message, details)


class InternalServerError(HttpException):
    """Exception raised for internal server errors."""

    status_code: ClassVar[int] = 500

    def __init__(
        self,
        message: str = "Internal server error",
        details: list[dict[str, str]] | None = None,
    ):
        super().__init__("internal_server_error", message, details)
