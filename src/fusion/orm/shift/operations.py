"""Fusion shift operation classes — safe, idempotent DDL generation."""

from __future__ import annotations

import hashlib
import typing
from dataclasses import dataclass

if typing.TYPE_CHECKING:  # pragma: no cover
    import asyncpg

from fusion.orm.shift.state import ColumnState, SchemaError, SchemaState, TableState

ColumnDef = dict[str, typing.Any]
ConstraintDef = dict[str, typing.Any]


def _table_ref(table: str, schema: str | None) -> str:
    return f'"{schema}"."{table}"' if schema else f'"{table}"'


def _col_ddl(name: str, defn: ColumnDef) -> str:
    pg_type = defn["type"]
    nullable = defn.get("nullable", True)
    primary_key = defn.get("primary_key", False)
    default = defn.get("default")

    null_clause = "" if nullable else " NOT NULL"
    default_clause = f" DEFAULT {default}" if default is not None else ""

    if primary_key:
        return f'"{name}" {pg_type} PRIMARY KEY{default_clause}'

    return f'"{name}" {pg_type}{null_clause}{default_clause}'


def _do_block(inner_sql: str) -> str:
    return (
        "DO $$ BEGIN\n"
        f"    {inner_sql};\n"
        "EXCEPTION WHEN duplicate_object OR duplicate_table THEN NULL;\n"
        "END $$"
    )


# ---------------------------------------------------------------------------
# Table operations
# ---------------------------------------------------------------------------


@dataclass
class CreateTable:
    table: str
    columns: dict[str, ColumnDef]
    schema: str | None = None

    def to_ddl(self) -> str:
        tref = _table_ref(self.table, self.schema)
        col_parts = [_col_ddl(name, defn) for name, defn in self.columns.items()]
        cols = ",\n    ".join(col_parts)
        return f"CREATE TABLE IF NOT EXISTS {tref} (\n    {cols}\n)"

    def apply(self, state: SchemaState) -> None:
        cols = {}
        for name, defn in self.columns.items():
            cols[name] = ColumnState(
                type=defn["type"],
                nullable=defn.get("nullable", True),
                default=defn.get("default"),
                primary_key=defn.get("primary_key", False),
            )
        state.tables[self.table] = TableState(columns=cols, schema=self.schema)


@dataclass
class DropTable:
    table: str
    schema: str | None = None

    def to_ddl(self) -> str:
        return f"DROP TABLE IF EXISTS {_table_ref(self.table, self.schema)}"

    def apply(self, state: SchemaState) -> None:
        if self.table not in state.tables:
            raise SchemaError(f"Table '{self.table}' does not exist.")
        del state.tables[self.table]


@dataclass
class RenameTable:
    old_name: str
    new_name: str

    def to_ddl(self) -> str:
        return f'ALTER TABLE "{self.old_name}" RENAME TO "{self.new_name}"'

    def apply(self, state: SchemaState) -> None:
        if self.old_name not in state.tables:
            raise SchemaError(f"Table '{self.old_name}' does not exist.")
        state.tables[self.new_name] = state.tables.pop(self.old_name)


# ---------------------------------------------------------------------------
# Column operations
# ---------------------------------------------------------------------------


@dataclass
class AddColumn:
    table: str
    column: str
    type: str
    nullable: bool
    default: str | None = None
    schema: str | None = None

    def to_ddl(self) -> str:
        tref = _table_ref(self.table, self.schema)
        null_clause = " NOT NULL" if not self.nullable else ""
        default_clause = f" DEFAULT {self.default}" if self.default is not None else ""
        return (
            f'ALTER TABLE {tref} ADD COLUMN IF NOT EXISTS "{self.column}" '
            f"{self.type}{null_clause}{default_clause}"
        )

    def apply(self, state: SchemaState) -> None:
        if self.table not in state.tables:
            raise SchemaError(f"Table '{self.table}' does not exist.")
        state.tables[self.table].columns[self.column] = ColumnState(
            type=self.type,
            nullable=self.nullable,
            default=self.default,
        )


@dataclass
class DropColumn:
    table: str
    column: str
    schema: str | None = None

    def to_ddl(self) -> str:
        tref = _table_ref(self.table, self.schema)
        return f'ALTER TABLE {tref} DROP COLUMN IF EXISTS "{self.column}"'

    def apply(self, state: SchemaState) -> None:
        if self.table not in state.tables:
            raise SchemaError(f"Table '{self.table}' does not exist.")
        cols = state.tables[self.table].columns
        if self.column not in cols:
            raise SchemaError(f"Column '{self.column}' does not exist on table '{self.table}'.")
        del cols[self.column]


@dataclass
class AlterColumn:
    table: str
    column: str
    type: str | None = None
    nullable: bool | None = None
    default: str | None = None
    schema: str | None = None

    def to_ddl(self) -> str:
        tref = _table_ref(self.table, self.schema)
        clauses: list[str] = []

        if self.type is not None:
            clauses.append(f'ALTER COLUMN "{self.column}" TYPE {self.type}')
        if self.nullable is False:
            clauses.append(f'ALTER COLUMN "{self.column}" SET NOT NULL')
        elif self.nullable is True:
            clauses.append(f'ALTER COLUMN "{self.column}" DROP NOT NULL')
        if self.default is not None:
            clauses.append(f'ALTER COLUMN "{self.column}" SET DEFAULT {self.default}')

        return f"ALTER TABLE {tref} {', '.join(clauses)}"

    def apply(self, state: SchemaState) -> None:
        if self.table not in state.tables:
            raise SchemaError(f"Table '{self.table}' does not exist.")
        cols = state.tables[self.table].columns
        if self.column not in cols:
            raise SchemaError(f"Column '{self.column}' does not exist on table '{self.table}'.")
        col = cols[self.column]
        if self.type is not None:
            col.type = self.type
        if self.nullable is not None:
            col.nullable = self.nullable
        if self.default is not None:
            col.default = self.default


@dataclass
class RenameColumn:
    table: str
    old_name: str
    new_name: str
    schema: str | None = None

    def to_ddl(self) -> str:
        tref = _table_ref(self.table, self.schema)
        return f'ALTER TABLE {tref} RENAME COLUMN "{self.old_name}" TO "{self.new_name}"'

    def apply(self, state: SchemaState) -> None:
        if self.table not in state.tables:
            raise SchemaError(f"Table '{self.table}' does not exist.")
        cols = state.tables[self.table].columns
        if self.old_name not in cols:
            raise SchemaError(f"Column '{self.old_name}' does not exist on table '{self.table}'.")
        cols[self.new_name] = cols.pop(self.old_name)


# ---------------------------------------------------------------------------
# Constraint operations
# ---------------------------------------------------------------------------


@dataclass
class AddConstraint:
    table: str
    constraint: ConstraintDef
    schema: str | None = None

    def to_ddl(self) -> str:
        tref = _table_ref(self.table, self.schema)
        c = self.constraint
        c_type = c["type"]

        if c_type == "unique":
            cols_sql = ", ".join(f'"{col}"' for col in c["columns"])
            name = c.get("name") or f"{self.table}_{'_'.join(c['columns'])}_key"
            inner = f'ALTER TABLE {tref} ADD CONSTRAINT "{name}" UNIQUE ({cols_sql})'

        elif c_type == "foreign_key":
            col = c["column"]
            ref_table = c["references_table"]
            ref_col = c["references_column"]
            ref_schema = c.get("references_schema")
            ref_tref = f'"{ref_schema}"."{ref_table}"' if ref_schema else f'"{ref_table}"'
            on_delete = c.get("on_delete")
            on_delete_clause = f" ON DELETE {on_delete}" if on_delete else ""
            name = c.get("name") or f"{self.table}_{col}_fkey"
            inner = (
                f'ALTER TABLE {tref} ADD CONSTRAINT "{name}" '
                f'FOREIGN KEY ("{col}") REFERENCES {ref_tref} ("{ref_col}"){on_delete_clause}'
            )

        elif c_type == "check":
            expr = c["expression"]
            name = c.get("name") or f"{self.table}_chk_{hashlib.md5(expr.encode()).hexdigest()[:8]}"
            inner = f'ALTER TABLE {tref} ADD CONSTRAINT "{name}" CHECK ({expr})'

        else:
            raise ValueError(f"Unknown constraint type: {c_type!r}")

        return _do_block(inner)

    def apply(self, state: SchemaState) -> None:
        if self.table not in state.tables:
            raise SchemaError(f"Table '{self.table}' does not exist.")
        c = self.constraint
        c_type = c["type"]
        if c_type == "unique":
            name = c.get("name") or f"{self.table}_{'_'.join(c['columns'])}_key"
        elif c_type == "foreign_key":
            name = c.get("name") or f"{self.table}_{c['column']}_fkey"
        elif c_type == "check":
            name = c["name"]
        else:
            raise ValueError(f"Unknown constraint type: {c_type!r}")
        stored = dict(c)
        stored["name"] = name
        state.tables[self.table].constraints.append(stored)


@dataclass
class DropConstraint:
    table: str
    name: str
    schema: str | None = None

    def to_ddl(self) -> str:
        tref = _table_ref(self.table, self.schema)
        return f'ALTER TABLE {tref} DROP CONSTRAINT IF EXISTS "{self.name}"'

    def apply(self, state: SchemaState) -> None:
        if self.table not in state.tables:
            raise SchemaError(f"Table '{self.table}' does not exist.")
        constraints = state.tables[self.table].constraints
        state.tables[self.table].constraints = [
            c for c in constraints if c.get("name") != self.name
        ]


# ---------------------------------------------------------------------------
# Index operations
# ---------------------------------------------------------------------------


@dataclass
class AddIndex:
    table: str
    columns: list[str]
    name: str | None = None
    method: str | None = None
    schema: str | None = None

    def to_ddl(self) -> str:
        tref = _table_ref(self.table, self.schema)
        idx_name = self.name or f"idx_{self.table}_{'_'.join(self.columns)}"
        cols_sql = ", ".join(f'"{col}"' for col in self.columns)
        method_clause = f" USING {self.method}" if self.method else ""
        return f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON {tref}{method_clause} ({cols_sql})'

    def apply(self, state: SchemaState) -> None:
        if self.table not in state.tables:
            raise SchemaError(f"Table '{self.table}' does not exist.")
        idx_name = self.name or f"idx_{self.table}_{'_'.join(self.columns)}"
        state.tables[self.table].indexes.append(
            {"name": idx_name, "columns": self.columns, "method": self.method}
        )


@dataclass
class DropIndex:
    name: str
    schema: str | None = None

    def to_ddl(self) -> str:
        return f'DROP INDEX IF EXISTS "{self.name}"'

    def apply(self, state: SchemaState) -> None:
        for table in state.tables.values():
            table.indexes = [i for i in table.indexes if i.get("name") != self.name]


# ---------------------------------------------------------------------------
# Extension / Schema operations
# ---------------------------------------------------------------------------


@dataclass
class AddExtension:
    name: str

    def to_ddl(self) -> str:
        return f'CREATE EXTENSION IF NOT EXISTS "{self.name}"'

    def apply(self, state: SchemaState) -> None:
        state.extensions.add(self.name)


@dataclass
class DropExtension:
    name: str

    def to_ddl(self) -> str:
        return f'DROP EXTENSION IF EXISTS "{self.name}"'

    def apply(self, state: SchemaState) -> None:
        state.extensions.discard(self.name)


@dataclass
class CreateSchema:
    name: str

    def to_ddl(self) -> str:
        return f'CREATE SCHEMA IF NOT EXISTS "{self.name}"'

    def apply(self, state: SchemaState) -> None:
        state.schemas.add(self.name)


# ---------------------------------------------------------------------------
# Escape hatches
# ---------------------------------------------------------------------------


@dataclass
class RunSQL:
    sql: str

    def to_ddl(self) -> str:
        return self.sql

    def apply(self, state: SchemaState) -> None:
        pass


@dataclass
class RunPython:
    fn: typing.Callable[..., typing.Awaitable[None]]

    def apply(self, state: SchemaState) -> None:
        pass


# ---------------------------------------------------------------------------
# Shift base class
# ---------------------------------------------------------------------------


class Shift:
    operations: list[typing.Any]
