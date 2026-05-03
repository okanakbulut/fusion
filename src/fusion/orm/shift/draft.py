"""Draft logic: model → SchemaState conversion and SchemaState diff."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from fusion.orm.model import Model

from fusion.orm.shift.operations import (
    AddColumn,
    AddConstraint,
    AddExtension,
    AddIndex,
    AlterColumn,
    CreateSchema,
    CreateTable,
    DropColumn,
    DropConstraint,
    DropExtension,
    DropIndex,
    DropTable,
)
from fusion.orm.shift.state import ColumnState, SchemaState, TableState


def models_to_schema_state(models: list[type[Model]]) -> SchemaState:
    from fusion.orm.shift.snapshot import serialize

    snapshot = serialize(models)
    state = SchemaState()

    # Collect extensions declared directly on model classes via __extensions__
    for model in models:
        for ext in getattr(model, "__extensions__", []):
            state.extensions.add(ext)

    for table_name, table_def in snapshot.get("tables", {}).items():
        schema = table_def.get("schema")
        if schema:
            state.schemas.add(schema)

        cols: dict[str, ColumnState] = {}
        for col_name, col_def in table_def["columns"].items():
            cols[col_name] = ColumnState(
                type=col_def["type"],
                nullable=col_def.get("nullable", True),
                default=col_def.get("default"),
                primary_key=col_def.get("primary_key", False),
            )

        constraints: list[dict[str, Any]] = []
        for c in table_def.get("constraints", []):
            c_type = c["type"]
            if c_type == "unique":
                name = c.get("name") or f"{table_name}_{'_'.join(c['columns'])}_key"
            elif c_type == "foreign_key":
                name = c.get("name") or f"{table_name}_{c['column']}_fkey"
            else:
                name = (
                    c.get("name")
                    or f"{table_name}_chk_{hashlib.md5(c.get('expression', '').encode()).hexdigest()[:8]}"
                )
            constraints.append({**c, "name": name})

        indexes: list[dict[str, Any]] = []
        for idx in table_def.get("indexes", []):
            cols_list = idx["columns"]
            name = idx.get("name") or f"idx_{table_name}_{'_'.join(cols_list)}"
            indexes.append({"name": name, "columns": cols_list, "method": idx.get("method")})

        state.tables[table_name] = TableState(
            columns=cols,
            constraints=constraints,
            indexes=indexes,
            schema=schema,
        )

    return state


def diff_states(current: SchemaState, target: SchemaState) -> list[Any]:  # noqa: C901
    ops: list[Any] = []

    for ext in sorted(target.extensions - current.extensions):
        ops.append(AddExtension(ext))
    for ext in sorted(current.extensions - target.extensions):
        ops.append(DropExtension(ext))

    for schema in sorted(target.schemas - current.schemas):
        ops.append(CreateSchema(schema))

    current_tables = set(current.tables)
    target_tables = set(target.tables)

    deferred_fks: list[tuple[str, Any, str | None]] = []

    for table in sorted(target_tables - current_tables):
        t = target.tables[table]
        col_defs: dict[str, Any] = {}
        for col_name, cs in t.columns.items():
            col_def: dict[str, Any] = {"type": cs.type, "nullable": cs.nullable}
            if cs.default is not None:
                col_def["default"] = cs.default
            if cs.primary_key:
                col_def["primary_key"] = True
            col_defs[col_name] = col_def
        ops.append(CreateTable(table, col_defs, schema=t.schema))

        for c in t.constraints:
            if c.get("type") == "foreign_key":
                deferred_fks.append((table, c, t.schema))
            else:
                ops.append(AddConstraint(table, c, schema=t.schema))
        for idx in t.indexes:
            ops.append(
                AddIndex(
                    table,
                    idx["columns"],
                    name=idx["name"],
                    method=idx.get("method"),
                    schema=t.schema,
                )
            )

    for table, c, schema in deferred_fks:
        ops.append(AddConstraint(table, c, schema=schema))

    for table in sorted(current_tables - target_tables):
        ops.append(DropTable(table, schema=current.tables[table].schema))

    for table in sorted(current_tables & target_tables):
        ct = current.tables[table]
        tt = target.tables[table]

        current_cols = set(ct.columns)
        target_cols = set(tt.columns)

        for col in sorted(target_cols - current_cols):
            cs = tt.columns[col]
            ops.append(AddColumn(table, col, cs.type, cs.nullable, cs.default, schema=tt.schema))

        for col in sorted(current_cols - target_cols):
            ops.append(DropColumn(table, col, schema=ct.schema))

        for col in sorted(current_cols & target_cols):
            cc = ct.columns[col]
            tc = tt.columns[col]
            kwargs: dict[str, Any] = {}
            if cc.type != tc.type:
                kwargs["type"] = tc.type
            if cc.nullable != tc.nullable:
                kwargs["nullable"] = tc.nullable
            if cc.default != tc.default and tc.default is not None:
                kwargs["default"] = tc.default
            if kwargs:
                ops.append(AlterColumn(table, col, schema=tt.schema, **kwargs))

        current_c_names = {c["name"] for c in ct.constraints}
        target_c_names = {c["name"] for c in tt.constraints}

        for c in tt.constraints:
            if c["name"] not in current_c_names:
                ops.append(AddConstraint(table, c, schema=tt.schema))
        for c in ct.constraints:
            if c["name"] not in target_c_names:
                ops.append(DropConstraint(table, c["name"], schema=ct.schema))

        current_i_names = {i["name"] for i in ct.indexes}
        target_i_names = {i["name"] for i in tt.indexes}

        for idx in tt.indexes:
            if idx["name"] not in current_i_names:
                ops.append(
                    AddIndex(
                        table,
                        idx["columns"],
                        name=idx["name"],
                        method=idx.get("method"),
                        schema=tt.schema,
                    )
                )
        for idx in ct.indexes:
            if idx["name"] not in target_i_names:
                ops.append(DropIndex(idx["name"], schema=ct.schema))

    return ops
