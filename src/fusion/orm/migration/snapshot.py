import typing

if typing.TYPE_CHECKING:  # pragma: no cover
    from fusion.orm.model import Model

_PY_TO_PG: dict[type, str] = {
    str: "TEXT",
    int: "INTEGER",
    float: "DOUBLE PRECISION",
    bool: "BOOLEAN",
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


def serialize(models: list[type[Model]]) -> dict[str, typing.Any]:
    from fusion.orm.constraints import ForeignKey, Index, UniqueConstraint

    tables: dict[str, typing.Any] = {}

    for model in models:
        table_name = model.__table_name__
        columns: dict[str, typing.Any] = {}

        for field_name in model.__fields__:
            if field_name == "id":
                columns["id"] = {
                    "type": "SERIAL",
                    "nullable": False,
                    "primary_key": True,
                }
                continue

            pg_type = _resolve_pg_type(model, field_name)
            nullable = _is_nullable(model, field_name)
            col_def: dict[str, typing.Any] = {
                "type": pg_type,
                "nullable": nullable,
            }
            default = _field_default(model, field_name)
            if default is not None:
                col_def["default"] = default

            columns[field_name] = col_def

        constraints: list[dict[str, typing.Any]] = []
        for c in model.__db_constraints__:
            if isinstance(c, UniqueConstraint):
                constraints.append({"type": "unique", "columns": list(c.columns)})
            elif isinstance(c, ForeignKey):
                constraints.append(
                    {
                        "type": "foreign_key",
                        "column": c.column,
                        "references_table": c.target.__table_name__,
                        "references_column": c.target_column,
                    }
                )

        indexes: list[dict[str, typing.Any]] = []
        for idx in model.__db_indexes__:
            if isinstance(idx, Index):
                entry: dict[str, typing.Any] = {"columns": list(idx.columns)}
                if idx.method is not None:
                    entry["method"] = idx.method
                indexes.append(entry)

        tables[table_name] = {
            "columns": columns,
            "constraints": constraints,
            "indexes": indexes,
        }

    return {"version": 1, "tables": tables}
