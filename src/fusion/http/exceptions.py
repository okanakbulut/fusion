from dataclasses import asdict, dataclass, field


@dataclass(slots=True)
class InternalServerError(Exception):
    """
    Exception raised when an internal server error occurs.
    """

    detail: str
    status: int = field(default=500, init=False)
    title: str = field(default="Internal Server Error", init=False)


@dataclass
class ValidationError(Exception):
    """
    Error object to be returned in the JSON response.

    Args:
        detail: A human-readable explanation specific to this occurrence of the problem.
    """

    detail: str


@dataclass(slots=True)
class QueryParamError(ValidationError):
    parameter: str


@dataclass(slots=True)
class HeaderError(ValidationError):
    header: str


@dataclass(slots=True)
class RequestError(ValidationError):
    pointer: str


@dataclass
class HttpException(Exception):
    """
    Base class for all HTTP exceptions. Will return a JSON response with the 400 status code.
    """

    status: int


@dataclass(slots=True)
class BadRequestException(HttpException):
    """
    Exception raised when a request fails validation. will result in a 400 status code.
    """

    title: str = field(default="Bad Request", init=False)
    status: int = field(default=400, init=False)
    errors: list[ValidationError] = field(default_factory=list)
