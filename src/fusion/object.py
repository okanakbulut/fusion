import annotationlib
import enum
import typing

import msgspec


class _NoDefault(enum.Enum):
    NODEFAULT = enum.auto()


NODEFAULT = _NoDefault.NODEFAULT


T = typing.TypeVar("T")


class Field(msgspec.Struct, frozen=True):
    name: str | None = None
    description: str | None = None
    deprecated: bool | None = False
    default: typing.Any | None = NODEFAULT
    default_factory: typing.Callable[[], typing.Any] | None = None
    # validation parameters for json decoding
    ge: int | float | None = None  # greater than or equal
    gt: int | float | None = None  # greater than
    le: int | float | None = None  # less than or equal
    lt: int | float | None = None  # less than
    min_length: int | None = None
    max_length: int | None = None
    pattern: str | None = None


def field(
    *,
    name: str | None = None,
    description: str | None = None,
    deprecated: bool | None = None,
    default: typing.Any | None = NODEFAULT,
    default_factory: typing.Callable[[], T] | None = None,
    ge: int | float | None = None,
    gt: int | float | None = None,
    le: int | float | None = None,
    lt: int | float | None = None,
    min_length: int | None = None,
    max_length: int | None = None,
    pattern: str | None = None,
) -> typing.Any:
    return Field(
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
    )


_CONSTRAINT_ATTRS = ("ge", "gt", "le", "lt", "min_length", "max_length", "pattern")
_META_ATTRS = (*_CONSTRAINT_ATTRS, "description")


def _constraints_of(field_info: Field) -> dict[str, typing.Any]:
    """Collect the msgspec.Meta settings declared on a Field, in declaration order.

    ``description`` and ``deprecated`` are carried through as well as the
    validation constraints: they are what make a generated tool or OpenAPI
    schema self-describing.
    """
    meta = {attr: value for attr in _META_ATTRS if (value := getattr(field_info, attr)) is not None}
    if field_info.deprecated:
        meta["extra_json_schema"] = {"deprecated": True}
    return meta


@typing.dataclass_transform(field_specifiers=(field,))
class MetaObject(msgspec.StructMeta):  # type: ignore[misc]
    def __new__(
        cls,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, typing.Any],
        **kwargs: typing.Any,
    ) -> type:
        # Python 3.14 (PEP 649): annotations are stored as __annotate__ callable in the
        # namespace, not as a dict. Call it with Format.VALUE to get own resolved annotations
        # before super().__new__, so we need only a single call (avoiding the double-call
        # pattern that polluted msgspec's internal per-class-name state for inherited defaults).
        annotate_func = namespace.get("__annotate_func__")
        if annotate_func is not None:
            try:
                own_annotations: dict[str, typing.Any] = annotate_func(annotationlib.Format.VALUE)
            except Exception:
                own_annotations = dict(namespace.get("__annotations__", {}))
        else:
            own_annotations = dict(namespace.get("__annotations__", {}))

        fields: dict[str, Field] = {}
        own_resolved: dict[str, typing.Any] = {}

        for key, annotation in own_annotations.items():
            if typing.get_origin(annotation) is typing.ClassVar:
                own_resolved[key] = annotation
                continue

            field_info = namespace.get(key, NODEFAULT)
            if field_info is NODEFAULT:
                fields[key] = Field()
                own_resolved[key] = annotation
            elif isinstance(field_info, Field):
                fields[key] = field_info
                namespace.pop(key)

                constraints = _constraints_of(field_info)
                if constraints:
                    own_resolved[key] = typing.Annotated[annotation, msgspec.Meta(**constraints)]
                else:
                    own_resolved[key] = annotation

                if field_info.default is not NODEFAULT:
                    namespace[key] = msgspec.field(
                        name=field_info.name,
                        default=field_info.default,
                    )
                elif field_info.default_factory or field_info.name is not None:
                    namespace[key] = msgspec.field(
                        name=field_info.name,
                        default_factory=field_info.default_factory,  # type: ignore
                    )
            else:
                fields[key] = Field(default=field_info)
                own_resolved[key] = annotation

        namespace["__annotations__"] = own_resolved
        namespace.pop("__annotate_func__", None)
        namespace["__fields__"] = fields
        return super().__new__(cls, name, bases, namespace, **kwargs)


class Object(metaclass=MetaObject, kw_only=True):
    """Base for every struct the framework defines or a user declares.

    Instances carry no ``__dict__`` and no ``__weakref__``: a struct is meant to
    be small.  Declare ``class Foo(Object, weakref=True)`` on the rare class that
    needs to be the key of a weak mapping - it costs one pointer per instance,
    and ``_utils.cached_property`` explains when that is worth it.
    """

    __fields__: typing.ClassVar[dict[str, Field]]
