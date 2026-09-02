from .responses import FieldError, Problem


class ValidationException(Exception):
    """Raised when request or argument values fail to bind.

    Carries either a single ``detail`` or per-field ``errors``; the message is
    derived from whichever is present so tracebacks and logs are readable.
    """

    def __init__(self, errors: list[FieldError] | None = None, detail: str | None = None) -> None:
        self.errors = errors
        self.detail = detail
        super().__init__(detail or "; ".join(f"{e.field}: {e.message}" for e in errors or ()))


class ProblemException(Exception):
    """Raised to answer a request with a prepared problem, from anywhere.

    A resolver returns a value, so it has no way to *return* a 401; raising is
    how a missing credential reaches the response layer without being mistaken
    for a validation failure.
    """

    def __init__(self, problem: Problem) -> None:
        self.problem = problem
        super().__init__(problem.detail or problem.title)
