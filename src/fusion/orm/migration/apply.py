import typing


class BlockedOperationError(Exception):
    pass


def _table_ref(table: str, schema: str | None) -> str:
    return f'"{schema}"."{table}"' if schema else f'"{table}"'


def _col_ddl(name: str, col_def: dict[str, typing.Any]) -> str:
    pg_type = col_def["type"]
    nullable = col_def.get("nullable", True)
    primary_key = col_def.get("primary_key", False)
    default = col_def.get("default")

    null_clause = "NOT NULL" if not nullable else "NULL"
    default_clause = f" DEFAULT {default}" if default is not None else ""
    pk_clause = " PRIMARY KEY" if primary_key else ""

    return f'"{name}" {pg_type}{pk_clause} {null_clause}{default_clause}'


def to_ddl(changes: list[dict[str, typing.Any]]) -> list[str]:
    statements: list[str] = []

    for change in changes:
        op = change["op"]
        schema = change.get("schema")

        if op == "create_table":
            table = _table_ref(change["table"], schema)
            col_parts = [_col_ddl(n, d) for n, d in change["columns"].items()]
            cols_sql = ",\n    ".join(col_parts)
            statements.append(f"CREATE TABLE {table} (\n    {cols_sql}\n)")

        elif op == "add_column":
            table = _table_ref(change["table"], schema)
            col = change["column"]
            pg_type = change["type"]
            nullable = change.get("nullable", True)
            default = change.get("default")

            if not nullable and default is None:
                raise BlockedOperationError(
                    f"Cannot add NOT NULL column '{col}' without a default value. "
                    "Set a db_default sentinel on the field."
                )

            null_clause = "NULL" if nullable else "NOT NULL"
            default_clause = f" DEFAULT {default!r}" if default is not None else ""
            statements.append(
                f'ALTER TABLE {table} ADD COLUMN "{col}" {pg_type} {null_clause}{default_clause}'
            )

        elif op == "drop_column_blocked":
            col = change["column"]
            raise BlockedOperationError(
                f"drop_column '{col}' on '{change['table']}' is blocked. "
                "Re-run with --drop to allow destructive operations."
            )

        elif op == "drop_column":
            table = _table_ref(change["table"], schema)
            col = change["column"]
            statements.append(f'ALTER TABLE {table} DROP COLUMN "{col}"')

        elif op == "drop_table_blocked":
            raise BlockedOperationError(
                f"drop_table '{change['table']}' is blocked. "
                "Re-run with --drop to allow destructive operations."
            )

        elif op == "drop_table":
            table = _table_ref(change["table"], schema)
            statements.append(f"DROP TABLE {table}")

        elif op == "add_constraint":
            table = _table_ref(change["table"], schema)
            c = change["constraint"]
            c_type = c["type"]

            if c_type == "unique":
                cols = ", ".join(f'"{col}"' for col in c["columns"])
                statements.append(f"ALTER TABLE {table} ADD UNIQUE ({cols})")

            elif c_type == "foreign_key":
                fk_col = c["column"]
                ref_table = c["references_table"]
                ref_col = c["references_column"]
                statements.append(
                    f'ALTER TABLE {table} ADD FOREIGN KEY ("{fk_col}") '
                    f'REFERENCES "{ref_table}" ("{ref_col}")'
                )

        elif op == "create_index":
            table_name = change["table"]
            idx = change["index"]
            cols = ", ".join(f'"{col}"' for col in idx["columns"])
            method_clause = f" USING {idx['method']}" if idx.get("method") else ""
            idx_name = f"idx_{table_name}_{'_'.join(idx['columns'])}"
            tref = _table_ref(table_name, schema)
            statements.append(f'CREATE INDEX "{idx_name}" ON {tref}{method_clause} ({cols})')

    return statements
