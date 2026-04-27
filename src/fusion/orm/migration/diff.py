import typing


def diff(
    before: dict[str, typing.Any],
    after: dict[str, typing.Any],
    *,
    allow_drop: bool = False,
) -> list[dict[str, typing.Any]]:
    changes: list[dict[str, typing.Any]] = []

    before_tables: dict[str, typing.Any] = before.get("tables", {})
    after_tables: dict[str, typing.Any] = after.get("tables", {})

    # Added tables
    for table_name, table_def in after_tables.items():
        if table_name not in before_tables:
            changes.append(
                {"op": "create_table", "table": table_name, "columns": table_def["columns"]}
            )

    # Dropped tables
    for table_name in before_tables:
        if table_name not in after_tables:
            op = "drop_table" if allow_drop else "drop_table_blocked"
            changes.append({"op": op, "table": table_name})

    # Modified tables
    for table_name in after_tables:
        if table_name not in before_tables:
            continue

        before_def = before_tables[table_name]
        after_def = after_tables[table_name]
        before_cols: dict[str, typing.Any] = before_def.get("columns", {})
        after_cols: dict[str, typing.Any] = after_def.get("columns", {})

        # Added columns
        for col_name, col_def in after_cols.items():
            if col_name not in before_cols:
                changes.append(
                    {
                        "op": "add_column",
                        "table": table_name,
                        "column": col_name,
                        "type": col_def["type"],
                        "nullable": col_def.get("nullable", True),
                        "default": col_def.get("default"),
                    }
                )

        # Dropped columns
        for col_name in before_cols:
            if col_name not in after_cols:
                op = "drop_column" if allow_drop else "drop_column_blocked"
                changes.append({"op": op, "table": table_name, "column": col_name})

        # Added constraints
        before_constraints = before_def.get("constraints", [])
        after_constraints = after_def.get("constraints", [])
        for constraint in after_constraints:
            if constraint not in before_constraints:
                changes.append(
                    {"op": "add_constraint", "table": table_name, "constraint": constraint}
                )

        # Added indexes
        before_indexes = before_def.get("indexes", [])
        after_indexes = after_def.get("indexes", [])
        for index in after_indexes:
            if index not in before_indexes:
                changes.append({"op": "create_index", "table": table_name, "index": index})

    return changes
