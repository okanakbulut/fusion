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
                own_annotations: dict[str, typing.Any] = annotate_func(
                    annotationlib.Format.VALUE
                )
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

                constraints: dict[str, typing.Any] = {}
                if field_info.ge is not None:
                    constraints["ge"] = field_info.ge
                if field_info.gt is not None:
                    constraints["gt"] = field_info.gt
                if field_info.le is not None:
                    constraints["le"] = field_info.le
                if field_info.lt is not None:
                    constraints["lt"] = field_info.lt
                if field_info.min_length is not None:
                    constraints["min_length"] = field_info.min_length
                if field_info.max_length is not None:
                    constraints["max_length"] = field_info.max_length
                if field_info.pattern is not None:
                    constraints["pattern"] = field_info.pattern

                if constraints:
                    own_resolved[key] = typing.Annotated[
                        annotation, msgspec.Meta(**constraints)
                    ]
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
        if "__annotate_func__" in namespace:
            del namespace["__annotate_func__"]
        namespace["__fields__"] = fields
        return super().__new__(cls, name, bases, namespace, **kwargs)


class Object(metaclass=MetaObject, kw_only=True):
    __fields__: typing.ClassVar[dict[str, Field]]
