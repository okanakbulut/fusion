import re
import types
import typing

from fusion.object import MetaObject, Object

from .column import Column


def _camel_to_snake(name: str) -> str:
    s1 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    return re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s1).lower()


def _pluralize(name: str) -> str:
    if name.endswith(("s", "x", "z", "ch", "sh")):
        return name + "es"
    if name.endswith("y") and len(name) > 1 and name[-2] not in "aeiou":
        return name[:-1] + "ies"
    return name + "s"


def _derive_table_name(class_name: str) -> str:
    return _pluralize(_camel_to_snake(class_name))


def _unwrap_optional(annotation: typing.Any) -> typing.Any | None:
    """Return the inner type if annotation is `T | None`, else None."""
    if typing.get_origin(annotation) is types.UnionType:
        args = typing.get_args(annotation)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return non_none[0]
    return None


def _is_model_class(tp: typing.Any) -> bool:
    return isinstance(tp, type) and issubclass(tp, Model) and tp is not Model


def _fk_col_type(target: type, optional: bool) -> typing.Any:
    """Derive FK column type from target model's id annotation."""
    hints = typing.get_type_hints(target)
    id_type = hints.get("id", int)
    inner = _unwrap_optional(id_type) or id_type
    return (inner | None) if optional else inner  # type: ignore[operator]


def _resolve_namespace_annotations(namespace: dict[str, typing.Any]) -> dict[str, typing.Any]:
    """Get the real annotations from a class namespace, handling Python 3.14 lazy annotations."""
    annotate_func = namespace.get("__annotate_func__")
    if annotate_func is not None:
        import annotationlib

        return annotate_func(annotationlib.Format.VALUE)
    return dict(namespace.get("__annotations__", {}))


def _inject_relationship_fields(
    namespace: dict[str, typing.Any],
) -> tuple[list[typing.Any], list[str]]:
    """Scan annotations, inject {field}_id columns.

    Returns (auto_constraints, relationship_field_names).
    """
    from .constraints import ForeignKey

    annotations = _resolve_namespace_annotations(namespace)
    extra_constraints: list[typing.Any] = []
    relationship_fields: list[str] = []
    injected: dict[str, typing.Any] = {}

    for field_name, annotation in annotations.items():
        inner = _unwrap_optional(annotation)
        if inner is not None and _is_model_class(inner):
            relationship_fields.append(field_name)
            fk_col = f"{field_name}_id"
            extra_constraints.append(ForeignKey(fk_col, inner))
            if fk_col not in annotations:
                injected[fk_col] = _fk_col_type(inner, optional=True)
                namespace[fk_col] = None
            continue
        if _is_model_class(annotation):
            relationship_fields.append(field_name)
            fk_col = f"{field_name}_id"
            extra_constraints.append(ForeignKey(fk_col, annotation))
            if fk_col not in annotations:
                injected[fk_col] = _fk_col_type(annotation, optional=False)

    if injected:
        namespace["__annotations__"] = {**annotations, **injected}
        namespace.pop("__annotate_func__", None)

    return extra_constraints, relationship_fields


class MetaModel(MetaObject):
    def __new__(
        cls,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, typing.Any],
        **kwargs: typing.Any,
    ) -> type:
        table_override: str | None = namespace.pop("__table__", None)
        schema_override: str | None = namespace.pop("__schema__", None)
        db_constraints: list[typing.Any] = list(namespace.pop("__constraints__", []))
        db_indexes: list[typing.Any] = list(namespace.pop("__indexes__", []))

        auto_fk, rel_fields = _inject_relationship_fields(namespace)
        db_constraints = auto_fk + db_constraints

        kwargs.setdefault("kw_only", True)
        kwargs.setdefault("frozen", True)
        new_cls = super().__new__(cls, name, bases, namespace, **kwargs)

        new_cls.__table_name__ = table_override or _derive_table_name(name)  # type: ignore[attr-defined]
        new_cls.__schema__ = schema_override  # type: ignore[attr-defined]
        new_cls.__db_constraints__ = db_constraints  # type: ignore[attr-defined]
        new_cls.__db_indexes__ = db_indexes  # type: ignore[attr-defined]
        new_cls.__relationship_fields__ = frozenset(rel_fields)  # type: ignore[attr-defined]

        return new_cls

    def __getattribute__(cls, name: str) -> typing.Any:
        try:
            fields: dict[str, typing.Any] = super().__getattribute__("__fields__")
            if name in fields:
                table: str = super().__getattribute__("__table_name__")
                return Column(name, table=table)
        except AttributeError:  # pragma: no cover
            pass
        return super().__getattribute__(name)


class Model(Object, metaclass=MetaModel):
    __table_name__: typing.ClassVar[str]
    __schema__: typing.ClassVar[str | None]
    __db_constraints__: typing.ClassVar[list[typing.Any]]
    __db_indexes__: typing.ClassVar[list[typing.Any]]
    __relationship_fields__: typing.ClassVar[frozenset[str]]

    def replace(self, **kwargs: typing.Any) -> typing.Self:
        import msgspec.structs

        rel = self.__relationship_fields__
        # Setting a relationship field → sync the FK column
        for key in list(kwargs):
            if key in rel:
                obj = kwargs[key]
                kwargs[f"{key}_id"] = obj.id if obj is not None else None
        # Setting a FK column directly → clear the stale relationship field
        for key in list(kwargs):
            rel_name = key.removesuffix("_id")
            if rel_name != key and rel_name in rel and rel_name not in kwargs:
                kwargs[rel_name] = None

        return msgspec.structs.replace(self, **kwargs)  # type: ignore[arg-type]

    @classmethod
    def select(cls, *columns: str) -> SelectQuery:
        from .query import SelectQuery

        return SelectQuery(cls, columns)

    @classmethod
    def insert(cls) -> InsertQuery:
        from .query import InsertQuery

        return InsertQuery(cls)

    @classmethod
    def update(cls) -> UpdateQuery:
        from .query import UpdateQuery

        return UpdateQuery(cls)

    @classmethod
    def delete(cls) -> DeleteQuery:
        from .query import DeleteQuery

        return DeleteQuery(cls)


if typing.TYPE_CHECKING:  # pragma: no cover
    from .query import DeleteQuery, InsertQuery, SelectQuery, UpdateQuery
