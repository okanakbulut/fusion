"""Tests for fusion.orm.shift schema reconstruction engine."""

import pytest

from fusion.orm.shift import (
    AddColumn,
    AddConstraint,
    AddExtension,
    AddIndex,
    AlterColumn,
    ColumnState,
    CreateSchema,
    CreateTable,
    DropColumn,
    DropConstraint,
    DropExtension,
    DropIndex,
    DropTable,
    RenameColumn,
    RenameTable,
    RunPython,
    RunSQL,
    SchemaError,
    SchemaState,
    TableState,
    replay_shifts,
)


def test_empty_schema_state_defaults():
    state = SchemaState()
    assert state.tables == {}
    assert state.extensions == set()
    assert state.schemas == set()


def test_replay_empty_paths_returns_empty_state():
    state = replay_shifts([])
    assert state.tables == {}
    assert state.extensions == set()
    assert state.schemas == set()


# ---------------------------------------------------------------------------
# CreateTable / DropTable
# ---------------------------------------------------------------------------


def test_create_table_apply_adds_table_with_columns():
    state = SchemaState()
    CreateTable(
        "users",
        {
            "id": {"type": "UUID", "nullable": False, "primary_key": True},
            "email": {"type": "TEXT", "nullable": False},
            "bio": {"type": "TEXT", "nullable": True},
            "role": {"type": "TEXT", "nullable": False, "default": "'member'"},
        },
    ).apply(state)

    assert "users" in state.tables
    t = state.tables["users"]
    assert t.schema is None
    assert t.columns["id"] == ColumnState(
        type="UUID", nullable=False, default=None, primary_key=True
    )
    assert t.columns["email"] == ColumnState(type="TEXT", nullable=False, default=None)
    assert t.columns["bio"] == ColumnState(type="TEXT", nullable=True, default=None)
    assert t.columns["role"] == ColumnState(type="TEXT", nullable=False, default="'member'")


def test_create_table_apply_preserves_schema():
    state = SchemaState()
    CreateTable(
        "matters",
        {"id": {"type": "UUID", "nullable": False, "primary_key": True}},
        schema="matters",
    ).apply(state)

    assert state.tables["matters"].schema == "matters"


def test_drop_table_apply_removes_table():
    state = SchemaState()
    CreateTable("users", {"id": {"type": "UUID", "nullable": False, "primary_key": True}}).apply(
        state
    )
    DropTable("users").apply(state)

    assert "users" not in state.tables


def test_drop_table_nonexistent_raises_schema_error():
    state = SchemaState()
    with pytest.raises(SchemaError, match="users"):
        DropTable("users").apply(state)


# ---------------------------------------------------------------------------
# AddColumn / DropColumn
# ---------------------------------------------------------------------------


def test_add_column_apply_adds_column_to_existing_table():
    state = SchemaState()
    CreateTable("users", {"id": {"type": "UUID", "nullable": False, "primary_key": True}}).apply(
        state
    )
    AddColumn("users", "bio", "TEXT", nullable=True).apply(state)

    assert state.tables["users"].columns["bio"] == ColumnState(
        type="TEXT", nullable=True, default=None
    )


def test_add_column_apply_with_default():
    state = SchemaState()
    CreateTable("users", {"id": {"type": "UUID", "nullable": False, "primary_key": True}}).apply(
        state
    )
    AddColumn("users", "role", "TEXT", nullable=False, default="'member'").apply(state)

    assert state.tables["users"].columns["role"] == ColumnState(
        type="TEXT", nullable=False, default="'member'"
    )


def test_add_column_nonexistent_table_raises_schema_error():
    state = SchemaState()
    with pytest.raises(SchemaError, match="users"):
        AddColumn("users", "bio", "TEXT", nullable=True).apply(state)


def test_drop_column_apply_removes_column():
    state = SchemaState()
    CreateTable(
        "users",
        {
            "id": {"type": "UUID", "nullable": False, "primary_key": True},
            "bio": {"type": "TEXT", "nullable": True},
        },
    ).apply(state)
    DropColumn("users", "bio").apply(state)

    assert "bio" not in state.tables["users"].columns
    assert "id" in state.tables["users"].columns


def test_drop_column_nonexistent_column_raises_schema_error():
    state = SchemaState()
    CreateTable("users", {"id": {"type": "UUID", "nullable": False, "primary_key": True}}).apply(
        state
    )
    with pytest.raises(SchemaError, match="bio"):
        DropColumn("users", "bio").apply(state)


def test_drop_column_nonexistent_table_raises_schema_error():
    state = SchemaState()
    with pytest.raises(SchemaError, match="users"):
        DropColumn("users", "bio").apply(state)


# ---------------------------------------------------------------------------
# AlterColumn
# ---------------------------------------------------------------------------


def test_alter_column_nonexistent_table_raises_schema_error():
    state = SchemaState()
    with pytest.raises(SchemaError, match="users"):
        AlterColumn("users", "status", type="TEXT").apply(state)


def test_alter_column_nonexistent_column_raises_schema_error():
    state = SchemaState()
    CreateTable("users", {"id": {"type": "UUID", "nullable": False, "primary_key": True}}).apply(
        state
    )
    with pytest.raises(SchemaError, match="status"):
        AlterColumn("users", "status", type="TEXT").apply(state)


def test_alter_column_apply_changes_type():
    state = SchemaState()
    CreateTable("users", {"status": {"type": "VARCHAR(50)", "nullable": True}}).apply(state)
    AlterColumn("users", "status", type="TEXT").apply(state)

    assert state.tables["users"].columns["status"].type == "TEXT"


def test_alter_column_apply_sets_not_null():
    state = SchemaState()
    CreateTable("users", {"status": {"type": "TEXT", "nullable": True}}).apply(state)
    AlterColumn("users", "status", nullable=False).apply(state)

    assert state.tables["users"].columns["status"].nullable is False


def test_alter_column_apply_drops_not_null():
    state = SchemaState()
    CreateTable(
        "users", {"status": {"type": "TEXT", "nullable": False, "default": "'active'"}}
    ).apply(state)
    AlterColumn("users", "status", nullable=True).apply(state)

    assert state.tables["users"].columns["status"].nullable is True


def test_alter_column_apply_sets_default():
    state = SchemaState()
    CreateTable("users", {"status": {"type": "TEXT", "nullable": True}}).apply(state)
    AlterColumn("users", "status", default="'active'").apply(state)

    assert state.tables["users"].columns["status"].default == "'active'"


def test_alter_column_apply_combined():
    state = SchemaState()
    CreateTable("users", {"status": {"type": "VARCHAR(50)", "nullable": True}}).apply(state)
    AlterColumn("users", "status", type="TEXT", nullable=False, default="'active'").apply(state)

    col = state.tables["users"].columns["status"]
    assert col == ColumnState(type="TEXT", nullable=False, default="'active'")


# ---------------------------------------------------------------------------
# RenameColumn
# ---------------------------------------------------------------------------


def test_rename_column_nonexistent_table_raises_schema_error():
    state = SchemaState()
    with pytest.raises(SchemaError, match="users"):
        RenameColumn("users", "old_name", "new_name").apply(state)


def test_rename_column_nonexistent_column_raises_schema_error():
    state = SchemaState()
    CreateTable("users", {"id": {"type": "UUID", "nullable": False, "primary_key": True}}).apply(
        state
    )
    with pytest.raises(SchemaError, match="old_name"):
        RenameColumn("users", "old_name", "new_name").apply(state)


def test_rename_column_apply_renames_key():
    state = SchemaState()
    CreateTable(
        "users",
        {
            "id": {"type": "UUID", "nullable": False, "primary_key": True},
            "old_name": {"type": "TEXT", "nullable": True},
        },
    ).apply(state)
    RenameColumn("users", "old_name", "new_name").apply(state)

    assert "old_name" not in state.tables["users"].columns
    assert state.tables["users"].columns["new_name"] == ColumnState(
        type="TEXT", nullable=True, default=None
    )


# ---------------------------------------------------------------------------
# RenameTable
# ---------------------------------------------------------------------------


def test_rename_table_apply_renames_key():
    state = SchemaState()
    CreateTable("users", {"id": {"type": "UUID", "nullable": False, "primary_key": True}}).apply(
        state
    )
    RenameTable("users", "accounts").apply(state)

    assert "users" not in state.tables
    assert "accounts" in state.tables


def test_rename_table_preserves_columns_and_schema():
    state = SchemaState()
    CreateTable(
        "matters",
        {
            "id": {"type": "UUID", "nullable": False, "primary_key": True},
            "title": {"type": "TEXT", "nullable": False},
        },
        schema="matters",
    ).apply(state)
    RenameTable("matters", "cases").apply(state)

    t = state.tables["cases"]
    assert t.schema == "matters"
    assert "id" in t.columns
    assert "title" in t.columns


def test_rename_table_nonexistent_raises_schema_error():
    state = SchemaState()
    with pytest.raises(SchemaError, match="users"):
        RenameTable("users", "accounts").apply(state)


# ---------------------------------------------------------------------------
# AddConstraint / DropConstraint
# ---------------------------------------------------------------------------


def test_add_constraint_apply_appends_to_constraints():
    state = SchemaState()
    CreateTable("users", {"id": {"type": "UUID", "nullable": False, "primary_key": True}}).apply(
        state
    )
    AddConstraint(
        "users", {"type": "unique", "columns": ["email"], "name": "uq_users_email"}
    ).apply(state)

    assert state.tables["users"].constraints == [
        {"type": "unique", "columns": ["email"], "name": "uq_users_email"}
    ]


def test_add_constraint_apply_resolves_auto_name_for_unique():
    state = SchemaState()
    CreateTable("users", {"id": {"type": "UUID", "nullable": False, "primary_key": True}}).apply(
        state
    )
    AddConstraint("users", {"type": "unique", "columns": ["email"]}).apply(state)

    assert state.tables["users"].constraints[0]["name"] == "users_email_key"


def test_add_constraint_apply_foreign_key_resolves_auto_name():
    state = SchemaState()
    CreateTable("posts", {"id": {"type": "UUID", "nullable": False, "primary_key": True}}).apply(
        state
    )
    AddConstraint(
        "posts",
        {
            "type": "foreign_key",
            "column": "user_id",
            "references_table": "users",
            "references_column": "id",
        },
    ).apply(state)

    assert state.tables["posts"].constraints[0]["name"] == "posts_user_id_fkey"


def test_add_constraint_apply_check():
    state = SchemaState()
    CreateTable("orders", {"amount": {"type": "INT", "nullable": False, "default": "0"}}).apply(
        state
    )
    AddConstraint(
        "orders", {"type": "check", "name": "chk_amount_positive", "expression": "amount > 0"}
    ).apply(state)

    assert state.tables["orders"].constraints[0]["name"] == "chk_amount_positive"


def test_add_constraint_apply_unknown_type_raises():
    state = SchemaState()
    CreateTable("users", {"id": {"type": "UUID", "nullable": False, "primary_key": True}}).apply(
        state
    )
    with pytest.raises(ValueError, match="Unknown constraint type"):
        AddConstraint("users", {"type": "unknown"}).apply(state)


def test_add_constraint_nonexistent_table_raises_schema_error():
    state = SchemaState()
    with pytest.raises(SchemaError, match="users"):
        AddConstraint("users", {"type": "unique", "columns": ["email"]}).apply(state)


def test_drop_constraint_nonexistent_table_raises_schema_error():
    state = SchemaState()
    with pytest.raises(SchemaError, match="users"):
        DropConstraint("users", "uq_email").apply(state)


def test_drop_constraint_apply_removes_by_name():
    state = SchemaState()
    CreateTable("users", {"id": {"type": "UUID", "nullable": False, "primary_key": True}}).apply(
        state
    )
    AddConstraint(
        "users", {"type": "unique", "columns": ["email"], "name": "uq_users_email"}
    ).apply(state)
    DropConstraint("users", "uq_users_email").apply(state)

    assert state.tables["users"].constraints == []


# ---------------------------------------------------------------------------
# AddIndex / DropIndex
# ---------------------------------------------------------------------------


def test_add_index_nonexistent_table_raises_schema_error():
    state = SchemaState()
    with pytest.raises(SchemaError, match="users"):
        AddIndex("users", ["email"]).apply(state)


def test_add_index_apply_appends_to_indexes():
    state = SchemaState()
    CreateTable("users", {"id": {"type": "UUID", "nullable": False, "primary_key": True}}).apply(
        state
    )
    AddIndex("users", ["email"], name="idx_users_email").apply(state)

    assert state.tables["users"].indexes == [
        {"name": "idx_users_email", "columns": ["email"], "method": None}
    ]


def test_add_index_apply_resolves_auto_name():
    state = SchemaState()
    CreateTable("users", {"id": {"type": "UUID", "nullable": False, "primary_key": True}}).apply(
        state
    )
    AddIndex("users", ["email"]).apply(state)

    assert state.tables["users"].indexes[0]["name"] == "idx_users_email"


def test_drop_index_apply_removes_by_name():
    state = SchemaState()
    CreateTable("users", {"id": {"type": "UUID", "nullable": False, "primary_key": True}}).apply(
        state
    )
    AddIndex("users", ["email"], name="idx_users_email").apply(state)
    DropIndex("idx_users_email").apply(state)

    assert state.tables["users"].indexes == []


# ---------------------------------------------------------------------------
# AddExtension / DropExtension / CreateSchema
# ---------------------------------------------------------------------------


def test_add_extension_apply_adds_to_extensions():
    state = SchemaState()
    AddExtension("pgcrypto").apply(state)

    assert state.extensions == {"pgcrypto"}


def test_drop_extension_apply_removes_from_extensions():
    state = SchemaState()
    AddExtension("pgcrypto").apply(state)
    DropExtension("pgcrypto").apply(state)

    assert state.extensions == set()


def test_create_schema_apply_adds_to_schemas():
    state = SchemaState()
    CreateSchema("matters").apply(state)

    assert state.schemas == {"matters"}


# ---------------------------------------------------------------------------
# RunSQL / RunPython — no-ops
# ---------------------------------------------------------------------------


def test_run_sql_apply_is_noop():
    state = SchemaState()
    CreateTable("users", {"id": {"type": "UUID", "nullable": False, "primary_key": True}}).apply(
        state
    )
    RunSQL("UPDATE users SET foo = 1").apply(state)

    assert list(state.tables.keys()) == ["users"]
    assert state.extensions == set()


def test_run_python_apply_is_noop():
    async def migrate(conn):
        pass

    state = SchemaState()
    CreateTable("users", {"id": {"type": "UUID", "nullable": False, "primary_key": True}}).apply(
        state
    )
    RunPython(migrate).apply(state)

    assert list(state.tables.keys()) == ["users"]
    assert state.extensions == set()


# ---------------------------------------------------------------------------
# Sequential renames + schema preservation
# ---------------------------------------------------------------------------


def test_two_sequential_renames_resolve_to_final_name():
    state = SchemaState()
    CreateTable("users", {"id": {"type": "UUID", "nullable": False, "primary_key": True}}).apply(
        state
    )
    RenameTable("users", "accounts").apply(state)
    RenameTable("accounts", "profiles").apply(state)

    assert "users" not in state.tables
    assert "accounts" not in state.tables
    assert "profiles" in state.tables


def test_schema_qualified_table_schema_preserved_through_rename():
    state = SchemaState()
    CreateTable(
        "matters",
        {"id": {"type": "UUID", "nullable": False, "primary_key": True}},
        schema="legal",
    ).apply(state)
    RenameTable("matters", "cases").apply(state)

    assert state.tables["cases"].schema == "legal"


# ---------------------------------------------------------------------------
# replay_shifts — file-based integration
# ---------------------------------------------------------------------------


def test_replay_shifts_applies_operations_from_file(tmp_path):
    shift_file = tmp_path / "20260501_000000_initial.py"
    shift_file.write_text(
        "from fusion.orm.shift import Shift, CreateTable\n\n"
        "class Initial(Shift):\n"
        "    operations = [\n"
        "        CreateTable('users', {'id': {'type': 'UUID', 'nullable': False, 'primary_key': True}}),\n"
        "    ]\n"
    )

    state = replay_shifts([shift_file])

    assert "users" in state.tables
    assert state.tables["users"].columns["id"] == ColumnState(
        type="UUID", nullable=False, default=None, primary_key=True
    )


def test_replay_shifts_sorts_files_by_filename(tmp_path):
    (tmp_path / "20260501_000001_add_email.py").write_text(
        "from fusion.orm.shift import Shift, AddColumn\n\n"
        "class AddEmail(Shift):\n"
        "    operations = [AddColumn('users', 'email', 'TEXT', nullable=True)]\n"
    )
    (tmp_path / "20260501_000000_initial.py").write_text(
        "from fusion.orm.shift import Shift, CreateTable\n\n"
        "class Initial(Shift):\n"
        "    operations = [\n"
        "        CreateTable('users', {'id': {'type': 'UUID', 'nullable': False, 'primary_key': True}}),\n"
        "    ]\n"
    )

    state = replay_shifts(list(tmp_path.glob("*.py")))

    assert "users" in state.tables
    assert "email" in state.tables["users"].columns


def test_replay_shifts_import_error_includes_filename(tmp_path):
    bad_file = tmp_path / "20260501_000000_bad.py"
    bad_file.write_text("this is not valid python syntax !!!")

    with pytest.raises(ImportError, match="20260501_000000_bad.py"):
        replay_shifts([bad_file])


def test_replay_shifts_no_shift_subclass_raises_import_error(tmp_path):
    empty_file = tmp_path / "20260501_000000_empty.py"
    empty_file.write_text("x = 1\n")

    with pytest.raises(ImportError, match="20260501_000000_empty.py"):
        replay_shifts([empty_file])
