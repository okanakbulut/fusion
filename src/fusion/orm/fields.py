import typing

from fusion.object import Field, NODEFAULT

T = typing.TypeVar("T")


class DBField(Field, frozen=True):
    db_type: str | None = None


class _DbNow:
    pass


class _DbUuid:
    pass


_DB_NOW = _DbNow()
_DB_UUID = _DbUuid()


def db_now() -> _DbNow:
    return _DB_NOW


def db_uuid() -> _DbUuid:
    return _DB_UUID


def field(
    *,
    name: str | None = None,
    description: str | None = None,
    deprecated: bool | None = None,
    default: typing.Any = NODEFAULT,
    default_factory: typing.Callable[[], T] | None = None,
    ge: int | float | None = None,
    gt: int | float | None = None,
    le: int | float | None = None,
    lt: int | float | None = None,
    min_length: int | None = None,
    max_length: int | None = None,
    pattern: str | None = None,
    db_type: str | None = None,
) -> typing.Any:
    return DBField(
        name=name,
        description=description,
        deprecated=deprecated,
        default=default,
        default_factory=default_factory,
        ge=ge,
        gt=gt,
        le=le,
        lt=lt,
        min_length=min_length,
        max_length=max_length,
        pattern=pattern,
        db_type=db_type,
    )
