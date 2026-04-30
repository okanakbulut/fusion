import typing


def _with_schema(change: dict[str, typing.Any], schema: str | None) -> dict[str, typing.Any]:
    if schema:
        change["schema"] = schema
    return change


def _diff_table(
    table_name: str,
    before_def: dict[str, typing.Any],
    after_def: dict[str, typing.Any],
    *,
    allow_drop: bool,
) -> list[dict[str, typing.Any]]:
    changes: list[dict[str, typing.Any]] = []
    schema = after_def.get("schema") or before_def.get("schema")
    before_cols: dict[str, typing.Any] = before_def.get("columns", {})
    after_cols: dict[str, typing.Any] = after_def.get("columns", {})

    for col_name, col_def in after_cols.items():
        if col_name not in before_cols:
            changes.append(
                _with_schema(
                    {
                        "op": "add_column",
                        "table": table_name,
                        "column": col_name,
                        "type": col_def["type"],
                        "nullable": col_def.get("nullable", True),
                        "default": col_def.get("default"),
                    },
                    schema,
                )
            )

    for col_name in before_cols:
        if col_name not in after_cols:
            op = "drop_column" if allow_drop else "drop_column_blocked"
            changes.append(
                _with_schema({"op": op, "table": table_name, "column": col_name}, schema)
            )

    for constraint in after_def.get("constraints", []):
        if constraint not in before_def.get("constraints", []):
            changes.append(
                _with_schema(
                    {"op": "add_constraint", "table": table_name, "constraint": constraint},
                    schema,
                )
            )

    for index in after_def.get("indexes", []):
        if index not in before_def.get("indexes", []):
            changes.append(
                _with_schema({"op": "create_index", "table": table_name, "index": index}, schema)
            )

    return changes


def diff(
    before: dict[str, typing.Any],
    after: dict[str, typing.Any],
    *,
    allow_drop: bool = False,
) -> list[dict[str, typing.Any]]:
    changes: list[dict[str, typing.Any]] = []

    before_tables: dict[str, typing.Any] = before.get("tables", {})
    after_tables: dict[str, typing.Any] = after.get("tables", {})

    for table_name, table_def in after_tables.items():
        if table_name not in before_tables:
            changes.append(
                _with_schema(
                    {"op": "create_table", "table": table_name, "columns": table_def["columns"]},
                    table_def.get("schema"),
                )
            )

    for table_name, table_def in before_tables.items():
        if table_name not in after_tables:
            op = "drop_table" if allow_drop else "drop_table_blocked"
            changes.append(_with_schema({"op": op, "table": table_name}, table_def.get("schema")))

    for table_name in after_tables:
        if table_name in before_tables:
            changes.extend(
                _diff_table(
                    table_name,
                    before_tables[table_name],
                    after_tables[table_name],
                    allow_drop=allow_drop,
                )
            )

    return changes
