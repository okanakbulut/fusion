import typing
import uuid

if typing.TYPE_CHECKING:  # pragma: no cover
    from fusion.orm.model import Model

_PY_TO_PG: dict[type, str] = {
    str: "TEXT",
    int: "INTEGER",
    float: "DOUBLE PRECISION",
    bool: "BOOLEAN",
    uuid.UUID: "UUID",
}


def _resolve_pg_type(model: type[Model], field_name: str) -> str:
    import types

    from fusion.orm.fields import DBField

    f = model.__fields__.get(field_name)
    if isinstance(f, DBField) and f.db_type is not None:
        return f.db_type

    hints = {}
    try:
        import sys
        import typing as _typing

        hints = _typing.get_type_hints(model, globalns=sys.modules[model.__module__].__dict__)
    except Exception:  # pragma: no cover
        hints = getattr(model, "__annotations__", {})

    annotation = hints.get(field_name)
    if annotation is None:  # pragma: no cover
        return "TEXT"

    # Unwrap Optional / X | None
    origin = typing.get_origin(annotation)
    if origin is typing.Union or str(origin) == "types.UnionType":
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        annotation = args[0] if args else str

    return _PY_TO_PG.get(annotation, "TEXT")


def _is_nullable(model: type[Model], field_name: str) -> bool:
    import sys
    import types

    try:
        import typing as _typing

        hints = _typing.get_type_hints(model, globalns=sys.modules[model.__module__].__dict__)
    except Exception:  # pragma: no cover
        hints = getattr(model, "__annotations__", {})

    annotation = hints.get(field_name)
    if annotation is None:  # pragma: no cover
        return True

    origin = typing.get_origin(annotation)
    if origin is typing.Union or str(origin) == "types.UnionType":
        return type(None) in typing.get_args(annotation)
    return False


def _field_default(model: type[Model], field_name: str) -> str | None:
    from fusion.orm.fields import DBField, _DbNow, _DbUuid

    f = model.__fields__.get(field_name)
    if not isinstance(f, DBField):
        return None
    if isinstance(f.default, _DbNow):
        return "NOW()"
    if isinstance(f.default, _DbUuid):
        return "gen_random_uuid()"
    return None


def _serialize_id_column(model: type[Model]) -> dict[str, typing.Any]:
    from fusion.orm.fields import DBField

    f = model.__fields__.get("id")
    pg_type = f.db_type if isinstance(f, DBField) and f.db_type is not None else "SERIAL"
    col: dict[str, typing.Any] = {"type": pg_type, "nullable": False, "primary_key": True}
    default = _field_default(model, "id")
    if default is not None:
        col["default"] = default
    return col


def _serialize_columns(model: type[Model]) -> dict[str, typing.Any]:
    rel_fields = getattr(model, "__relationship_fields__", frozenset())
    columns: dict[str, typing.Any] = {}
    for field_name in model.__fields__:
        if field_name in rel_fields:
            continue
        if field_name == "id":
            columns["id"] = _serialize_id_column(model)
            continue
        pg_type = _resolve_pg_type(model, field_name)
        nullable = _is_nullable(model, field_name)
        col_def: dict[str, typing.Any] = {"type": pg_type, "nullable": nullable}
        default = _field_default(model, field_name)
        if default is not None:
            col_def["default"] = default
        columns[field_name] = col_def
    return columns


def _serialize_constraints(model: type[Model]) -> list[dict[str, typing.Any]]:
    from fusion.orm.constraints import CheckConstraint, ForeignKey, UniqueConstraint

    constraints: list[dict[str, typing.Any]] = []
    for c in model.__db_constraints__:
        if isinstance(c, UniqueConstraint):
            constraints.append({"type": "unique", "columns": list(c.columns)})
        elif isinstance(c, ForeignKey):
            fk_entry: dict[str, typing.Any] = {
                "type": "foreign_key",
                "column": c.column,
                "references_table": c.target.__table_name__,
                "references_column": c.target_column,
            }
            ref_schema = getattr(c.target, "__schema__", None)
            if ref_schema is not None:
                fk_entry["references_schema"] = ref_schema
            if c.on_delete is not None:
                fk_entry["on_delete"] = c.on_delete
            constraints.append(fk_entry)
        elif isinstance(c, CheckConstraint):
            constraints.append({"type": "check", "name": c.name, "expression": c.expression})
    return constraints


def _serialize_indexes(model: type[Model]) -> list[dict[str, typing.Any]]:
    from fusion.orm.constraints import Index

    indexes: list[dict[str, typing.Any]] = []
    for idx in model.__db_indexes__:
        if isinstance(idx, Index):
            entry: dict[str, typing.Any] = {"columns": list(idx.columns)}
            if idx.method is not None:
                entry["method"] = idx.method
            indexes.append(entry)
    return indexes


def serialize(models: list[type[Model]]) -> dict[str, typing.Any]:
    tables: dict[str, typing.Any] = {}

    for model in models:
        table_name = model.__table_name__
        table_def: dict[str, typing.Any] = {
            "columns": _serialize_columns(model),
            "constraints": _serialize_constraints(model),
            "indexes": _serialize_indexes(model),
        }
        schema = getattr(model, "__schema__", None)
        if schema is not None:
            table_def["schema"] = schema
        tables[table_name] = table_def

    extensions: list[str] = []
    for model in models:
        for ext in getattr(model, "__db_extensions__", []):
            if ext not in extensions:
                extensions.append(ext)

    result: dict[str, typing.Any] = {"version": 1, "tables": tables}
    if extensions:
        result["extensions"] = extensions
    return result
