from .responses import FieldError


class ValidationException(Exception):
    def __init__(self, errors: list[FieldError] | None = None, detail: str | None = None) -> None:
        self.errors = errors
        self.detail = detail
