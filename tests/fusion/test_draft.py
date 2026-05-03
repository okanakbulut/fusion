"""Tests for fusion draft command — schema diff and shift file generation."""

import argparse
import sys
import types

import pytest

from fusion.orm.model import Model


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_module(monkeypatch, module_name: str, **classes) -> str:
    for cls in classes.values():
        cls.__module__ = module_name
    mod = types.ModuleType(module_name)
    for name, cls in classes.items():
        setattr(mod, name, cls)
    monkeypatch.setitem(sys.modules, module_name, mod)
    return module_name


# ---------------------------------------------------------------------------
# Slice 1: _sanitize_slug
# ---------------------------------------------------------------------------


def test_sanitize_slug_replaces_spaces_with_underscores():
    from fusion.cli import _sanitize_slug

    assert _sanitize_slug("add status to users") == "add_status_to_users"


def test_sanitize_slug_lowercases():
    from fusion.cli import _sanitize_slug

    assert _sanitize_slug("AddStatus") == "addstatus"


def test_sanitize_slug_strips_non_alphanumeric():
    from fusion.cli import _sanitize_slug

    assert _sanitize_slug("add-status!") == "addstatus"


def test_sanitize_slug_combined():
    from fusion.cli import _sanitize_slug

    assert _sanitize_slug("Add Status to Users!") == "add_status_to_users"


# ---------------------------------------------------------------------------
# Slice 2: models_to_schema_state
# ---------------------------------------------------------------------------


def test_models_to_schema_state_basic_fields(monkeypatch):
    from fusion.orm.shift.draft import models_to_schema_state
    from fusion.orm.shift.state import ColumnState

    class User(Model):
        id: int | None = None
        email: str
        bio: str | None = None

    _make_module(monkeypatch, "_draft_test_basic", User=User)

    state = models_to_schema_state([User])

    assert "users" in state.tables
    t = state.tables["users"]
    assert t.columns["id"] == ColumnState(
        type="SERIAL", nullable=False, default=None, primary_key=True
    )
    assert t.columns["email"] == ColumnState(type="TEXT", nullable=False, default=None)
    assert t.columns["bio"] == ColumnState(type="TEXT", nullable=True, default=None)


def test_models_to_schema_state_with_extension(monkeypatch):
    from fusion.orm.shift.draft import models_to_schema_state

    class Doc(Model):
        __extensions__ = ["pgcrypto"]
        id: int | None = None
        content: str

    _make_module(monkeypatch, "_draft_test_ext", Doc=Doc)

    state = models_to_schema_state([Doc])

    assert "pgcrypto" in state.extensions


def test_models_to_schema_state_with_constraints(monkeypatch):
    from fusion.orm.constraints import UniqueConstraint
    from fusion.orm.shift.draft import models_to_schema_state

    class User(Model):
        __constraints__ = [UniqueConstraint("email")]
        id: int | None = None
        email: str

    _make_module(monkeypatch, "_draft_test_constraints", User=User)

    state = models_to_schema_state([User])

    assert len(state.tables["users"].constraints) == 1
    assert state.tables["users"].constraints[0]["name"] == "users_email_key"


def test_models_to_schema_state_with_indexes(monkeypatch):
    from fusion.orm.constraints import Index
    from fusion.orm.shift.draft import models_to_schema_state

    class User(Model):
        __indexes__ = [Index("email")]
        id: int | None = None
        email: str

    _make_module(monkeypatch, "_draft_test_indexes", User=User)

    state = models_to_schema_state([User])

    assert len(state.tables["users"].indexes) == 1
    assert state.tables["users"].indexes[0]["name"] == "idx_users_email"


def test_models_to_schema_state_column_with_default(monkeypatch):
    from fusion.orm.fields import field, db_now
    from fusion.orm.shift.draft import models_to_schema_state

    class Event(Model):
        id: int | None = None
        created_at: str = field(db_type="TIMESTAMPTZ", default=db_now())

    _make_module(monkeypatch, "_draft_test_default", Event=Event)

    state = models_to_schema_state([Event])

    assert state.tables["events"].columns["created_at"].default == "NOW()"


def test_models_to_schema_state_with_foreign_key_constraint(monkeypatch):
    from fusion.orm.constraints import ForeignKey
    from fusion.orm.shift.draft import models_to_schema_state

    class User(Model):
        id: int | None = None
        name: str

    class Post(Model):
        __constraints__ = [ForeignKey("author_id", User)]
        id: int | None = None
        author_id: int
        title: str

    User.__module__ = "_draft_test_fk"
    Post.__module__ = "_draft_test_fk"
    mod = types.ModuleType("_draft_test_fk")
    mod.User = User
    mod.Post = Post
    monkeypatch.setitem(sys.modules, "_draft_test_fk", mod)

    state = models_to_schema_state([Post])

    fk_constraint = next(c for c in state.tables["posts"].constraints if c["type"] == "foreign_key")
    assert fk_constraint["name"] == "posts_author_id_fkey"


def test_models_to_schema_state_with_check_constraint(monkeypatch):
    from fusion.orm.constraints import CheckConstraint
    from fusion.orm.shift.draft import models_to_schema_state

    class Order(Model):
        __constraints__ = [CheckConstraint("amount > 0", name="chk_amount_positive")]
        id: int | None = None
        amount: int

    _make_module(monkeypatch, "_draft_test_check", Order=Order)

    state = models_to_schema_state([Order])

    check = next(c for c in state.tables["orders"].constraints if c["type"] == "check")
    assert check["name"] == "chk_amount_positive"


def test_models_to_schema_state_with_schema(monkeypatch):
    from fusion.orm.shift.draft import models_to_schema_state

    class Report(Model):
        __schema__ = "analytics"
        id: int | None = None
        name: str

    _make_module(monkeypatch, "_draft_test_schema", Report=Report)

    state = models_to_schema_state([Report])

    assert state.tables["reports"].schema == "analytics"
    assert "analytics" in state.schemas


# ---------------------------------------------------------------------------
# Slice 3: diff_states — new table → CreateTable
# ---------------------------------------------------------------------------


def test_diff_states_new_table_creates_table(monkeypatch):
    from fusion.orm.shift.draft import diff_states, models_to_schema_state
    from fusion.orm.shift.state import SchemaState

    class User(Model):
        id: int | None = None
        email: str

    _make_module(monkeypatch, "_draft_test_ct", User=User)

    current = SchemaState()
    target = models_to_schema_state([User])
    ops = diff_states(current, target)

    assert len(ops) == 1
    assert ops[0].to_ddl() == (
        'CREATE TABLE IF NOT EXISTS "users" (\n'
        '    "id" SERIAL PRIMARY KEY,\n'
        '    "email" TEXT NOT NULL\n'
        ")"
    )


def test_diff_states_new_table_with_default_column():
    from fusion.orm.shift.draft import diff_states
    from fusion.orm.shift.state import ColumnState, SchemaState, TableState

    current = SchemaState()
    target = SchemaState(
        tables={
            "events": TableState(
                columns={
                    "id": ColumnState(
                        type="INTEGER", nullable=False, default=None, primary_key=True
                    ),
                    "created_at": ColumnState(type="TIMESTAMPTZ", nullable=False, default="NOW()"),
                }
            )
        }
    )
    ops = diff_states(current, target)

    assert len(ops) == 1
    assert ops[0].to_ddl() == (
        'CREATE TABLE IF NOT EXISTS "events" (\n'
        '    "id" INTEGER PRIMARY KEY,\n'
        '    "created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()\n'
        ")"
    )


def test_diff_states_new_table_with_constraints_and_indexes():
    from fusion.orm.shift.draft import diff_states
    from fusion.orm.shift.state import ColumnState, SchemaState, TableState

    current = SchemaState()
    target = SchemaState(
        tables={
            "users": TableState(
                columns={
                    "id": ColumnState(
                        type="INTEGER", nullable=False, default=None, primary_key=True
                    )
                },
                constraints=[{"type": "unique", "columns": ["email"], "name": "uq_users_email"}],
                indexes=[{"name": "idx_users_email", "columns": ["email"], "method": None}],
            )
        }
    )
    ops = diff_states(current, target)

    assert len(ops) == 3
    assert ops[0].to_ddl() == (
        'CREATE TABLE IF NOT EXISTS "users" (\n    "id" INTEGER PRIMARY KEY\n)'
    )
    assert ops[1].to_ddl() == (
        "DO $$ BEGIN\n"
        '    ALTER TABLE "users" ADD CONSTRAINT "uq_users_email" UNIQUE ("email");\n'
        "EXCEPTION WHEN duplicate_object OR duplicate_table THEN NULL;\n"
        "END $$"
    )
    assert ops[2].to_ddl() == 'CREATE INDEX IF NOT EXISTS "idx_users_email" ON "users" ("email")'


def test_diff_states_unchanged_constraints_and_indexes_produce_no_ops():
    from fusion.orm.shift.draft import diff_states
    from fusion.orm.shift.state import ColumnState, SchemaState, TableState

    shared_table = TableState(
        columns={"id": ColumnState(type="INTEGER", nullable=False, default=None, primary_key=True)},
        constraints=[{"type": "unique", "columns": ["email"], "name": "uq_users_email"}],
        indexes=[{"name": "idx_users_email", "columns": ["email"], "method": None}],
    )
    current = SchemaState(tables={"users": shared_table})
    target = SchemaState(tables={"users": shared_table})
    ops = diff_states(current, target)

    assert ops == []


def test_diff_states_no_changes_returns_empty(monkeypatch):
    from fusion.orm.shift.draft import diff_states, models_to_schema_state

    class User(Model):
        id: int | None = None
        email: str

    _make_module(monkeypatch, "_draft_test_noop", User=User)

    state = models_to_schema_state([User])
    ops = diff_states(state, state)

    assert ops == []


# ---------------------------------------------------------------------------
# Slice 4: diff_states — removed table → DropTable
# ---------------------------------------------------------------------------


def test_diff_states_removed_table_drops_table(monkeypatch):
    from fusion.orm.shift.draft import diff_states, models_to_schema_state
    from fusion.orm.shift.state import SchemaState

    class User(Model):
        id: int | None = None
        email: str

    _make_module(monkeypatch, "_draft_test_dt", User=User)

    current = models_to_schema_state([User])
    target = SchemaState()
    ops = diff_states(current, target)

    assert len(ops) == 1
    assert ops[0].to_ddl() == 'DROP TABLE IF EXISTS "users"'


# ---------------------------------------------------------------------------
# Slice 5: diff_states — column add / remove / alter
# ---------------------------------------------------------------------------


def test_diff_states_new_column_adds_column(monkeypatch):
    from fusion.orm.shift.draft import diff_states, models_to_schema_state
    from fusion.orm.shift.state import ColumnState, SchemaState, TableState

    current = SchemaState(
        tables={
            "users": TableState(
                columns={
                    "id": ColumnState(type="SERIAL", nullable=False, default=None, primary_key=True)
                }
            )
        }
    )

    class User(Model):
        id: int | None = None
        email: str

    _make_module(monkeypatch, "_draft_test_ac", User=User)
    target = models_to_schema_state([User])
    ops = diff_states(current, target)

    assert len(ops) == 1
    assert ops[0].to_ddl() == ('ALTER TABLE "users" ADD COLUMN IF NOT EXISTS "email" TEXT NOT NULL')


def test_diff_states_removed_column_drops_column(monkeypatch):
    from fusion.orm.shift.draft import diff_states, models_to_schema_state
    from fusion.orm.shift.state import ColumnState, SchemaState, TableState

    class User(Model):
        id: int | None = None

    _make_module(monkeypatch, "_draft_test_dc", User=User)
    target = models_to_schema_state([User])

    current = SchemaState(
        tables={
            "users": TableState(
                columns={
                    "id": ColumnState(
                        type="SERIAL", nullable=False, default=None, primary_key=True
                    ),
                    "email": ColumnState(type="TEXT", nullable=False, default=None),
                }
            )
        }
    )
    ops = diff_states(current, target)

    assert len(ops) == 1
    assert ops[0].to_ddl() == 'ALTER TABLE "users" DROP COLUMN IF EXISTS "email"'


def test_diff_states_column_type_change_alters_column(monkeypatch):
    from fusion.orm.shift.draft import diff_states
    from fusion.orm.shift.state import ColumnState, SchemaState, TableState

    current = SchemaState(
        tables={
            "users": TableState(
                columns={
                    "id": ColumnState(
                        type="INTEGER", nullable=False, default=None, primary_key=True
                    ),
                    "status": ColumnState(type="VARCHAR(50)", nullable=True, default=None),
                }
            )
        }
    )
    target = SchemaState(
        tables={
            "users": TableState(
                columns={
                    "id": ColumnState(
                        type="INTEGER", nullable=False, default=None, primary_key=True
                    ),
                    "status": ColumnState(type="TEXT", nullable=True, default=None),
                }
            )
        }
    )
    ops = diff_states(current, target)

    assert len(ops) == 1
    assert ops[0].to_ddl() == 'ALTER TABLE "users" ALTER COLUMN "status" TYPE TEXT'


def test_diff_states_column_nullable_change_alters_column(monkeypatch):
    from fusion.orm.shift.draft import diff_states
    from fusion.orm.shift.state import ColumnState, SchemaState, TableState

    current = SchemaState(
        tables={
            "users": TableState(
                columns={"status": ColumnState(type="TEXT", nullable=True, default=None)}
            )
        }
    )
    target = SchemaState(
        tables={
            "users": TableState(
                columns={"status": ColumnState(type="TEXT", nullable=False, default="'active'")}
            )
        }
    )
    ops = diff_states(current, target)

    assert len(ops) == 1
    assert ops[0].to_ddl() == (
        'ALTER TABLE "users" ALTER COLUMN "status" SET NOT NULL, ALTER COLUMN "status" SET DEFAULT \'active\''
    )


# ---------------------------------------------------------------------------
# Slice 6: diff_states — constraints
# ---------------------------------------------------------------------------


def test_diff_states_new_constraint_adds_constraint():
    from fusion.orm.shift.draft import diff_states
    from fusion.orm.shift.state import ColumnState, SchemaState, TableState

    base_cols = {"id": ColumnState(type="INTEGER", nullable=False, default=None, primary_key=True)}
    current = SchemaState(tables={"users": TableState(columns=base_cols)})
    target = SchemaState(
        tables={
            "users": TableState(
                columns=base_cols,
                constraints=[{"type": "unique", "columns": ["email"], "name": "uq_users_email"}],
            )
        }
    )
    ops = diff_states(current, target)

    assert len(ops) == 1
    assert ops[0].to_ddl() == (
        "DO $$ BEGIN\n"
        '    ALTER TABLE "users" ADD CONSTRAINT "uq_users_email" UNIQUE ("email");\n'
        "EXCEPTION WHEN duplicate_object OR duplicate_table THEN NULL;\n"
        "END $$"
    )


def test_diff_states_removed_constraint_drops_constraint():
    from fusion.orm.shift.draft import diff_states
    from fusion.orm.shift.state import ColumnState, SchemaState, TableState

    base_cols = {"id": ColumnState(type="INTEGER", nullable=False, default=None, primary_key=True)}
    current = SchemaState(
        tables={
            "users": TableState(
                columns=base_cols,
                constraints=[{"type": "unique", "columns": ["email"], "name": "uq_users_email"}],
            )
        }
    )
    target = SchemaState(tables={"users": TableState(columns=base_cols)})
    ops = diff_states(current, target)

    assert len(ops) == 1
    assert ops[0].to_ddl() == 'ALTER TABLE "users" DROP CONSTRAINT IF EXISTS "uq_users_email"'


def test_check_constraint_without_name_gets_stable_generated_name():
    # CheckConstraint(name="") used to produce CONSTRAINT "" in the DDL, which
    # Postgres rejects as a zero-length delimited identifier.  Verify that
    # models_to_schema_state assigns a non-empty, deterministic name.
    from fusion.orm.constraints import CheckConstraint
    from fusion.orm.shift.draft import models_to_schema_state

    class Order(Model):
        __constraints__ = [CheckConstraint("amount > 0")]  # name defaults to ""
        id: int | None = None
        amount: int

    state = models_to_schema_state([Order])
    check = next(c for c in state.tables["orders"].constraints if c["type"] == "check")
    assert check["name"], "constraint name must not be empty"
    # Stable: same expression always maps to the same name
    state2 = models_to_schema_state([Order])
    check2 = next(c for c in state2.tables["orders"].constraints if c["type"] == "check")
    assert check["name"] == check2["name"]


# ---------------------------------------------------------------------------
# Slice 7: diff_states — indexes
# ---------------------------------------------------------------------------


def test_diff_states_new_index_adds_index():
    from fusion.orm.shift.draft import diff_states
    from fusion.orm.shift.state import ColumnState, SchemaState, TableState

    base_cols = {"id": ColumnState(type="INTEGER", nullable=False, default=None, primary_key=True)}
    current = SchemaState(tables={"users": TableState(columns=base_cols)})
    target = SchemaState(
        tables={
            "users": TableState(
                columns=base_cols,
                indexes=[{"name": "idx_users_email", "columns": ["email"], "method": None}],
            )
        }
    )
    ops = diff_states(current, target)

    assert len(ops) == 1
    assert ops[0].to_ddl() == 'CREATE INDEX IF NOT EXISTS "idx_users_email" ON "users" ("email")'


def test_diff_states_removed_index_drops_index():
    from fusion.orm.shift.draft import diff_states
    from fusion.orm.shift.state import ColumnState, SchemaState, TableState

    base_cols = {"id": ColumnState(type="INTEGER", nullable=False, default=None, primary_key=True)}
    current = SchemaState(
        tables={
            "users": TableState(
                columns=base_cols,
                indexes=[{"name": "idx_users_email", "columns": ["email"], "method": None}],
            )
        }
    )
    target = SchemaState(tables={"users": TableState(columns=base_cols)})
    ops = diff_states(current, target)

    assert len(ops) == 1
    assert ops[0].to_ddl() == 'DROP INDEX IF EXISTS "idx_users_email"'


# ---------------------------------------------------------------------------
# Slice 8: diff_states — extensions
# ---------------------------------------------------------------------------


def test_diff_states_new_extension_adds_extension():
    from fusion.orm.shift.draft import diff_states
    from fusion.orm.shift.state import SchemaState

    current = SchemaState()
    target = SchemaState(extensions={"pgcrypto"})
    ops = diff_states(current, target)

    assert len(ops) == 1
    assert ops[0].to_ddl() == 'CREATE EXTENSION IF NOT EXISTS "pgcrypto"'


def test_diff_states_removed_extension_drops_extension():
    from fusion.orm.shift.draft import diff_states
    from fusion.orm.shift.state import SchemaState

    current = SchemaState(extensions={"pgcrypto"})
    target = SchemaState()
    ops = diff_states(current, target)

    assert len(ops) == 1
    assert ops[0].to_ddl() == 'DROP EXTENSION IF EXISTS "pgcrypto"'


# ---------------------------------------------------------------------------
# Slice 9: diff_states — new schema prepends CreateSchema before CreateTable
# ---------------------------------------------------------------------------


def test_diff_states_new_schema_prepends_create_schema(monkeypatch):
    from fusion.orm.shift.draft import diff_states, models_to_schema_state
    from fusion.orm.shift.state import SchemaState

    class Report(Model):
        __schema__ = "analytics"
        id: int | None = None
        name: str

    _make_module(monkeypatch, "_draft_test_cs", Report=Report)

    current = SchemaState()
    target = models_to_schema_state([Report])
    ops = diff_states(current, target)

    assert len(ops) == 2
    assert ops[0].to_ddl() == 'CREATE SCHEMA IF NOT EXISTS "analytics"'
    assert ops[1].to_ddl() == (
        'CREATE TABLE IF NOT EXISTS "analytics"."reports" (\n'
        '    "id" SERIAL PRIMARY KEY,\n'
        '    "name" TEXT NOT NULL\n'
        ")"
    )


# ---------------------------------------------------------------------------
# Slice 10: cmd_draft — no changes detected
# ---------------------------------------------------------------------------


def test_cmd_draft_no_changes_prints_message_and_writes_no_file(tmp_path, monkeypatch, capsys):
    from fusion.cli import cmd_draft

    class User(Model):
        id: int | None = None
        email: str

    _make_module(monkeypatch, "_draft_test_noop_cmd", User=User)

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    # Write a shift that creates users so current == target
    shift = migrations_dir / "20260501_000000_initial.py"
    shift.write_text(
        "from fusion.orm.shift import Shift, CreateTable\n\n"
        "class Initial(Shift):\n"
        "    operations = [\n"
        "        CreateTable('users', {\n"
        "            'id': {'type': 'SERIAL', 'nullable': False, 'primary_key': True},\n"
        "            'email': {'type': 'TEXT', 'nullable': False},\n"
        "        }),\n"
        "    ]\n"
    )

    cmd_draft(
        argparse.Namespace(
            module=["_draft_test_noop_cmd"],
            slug="no_op",
            migrations_dir=str(migrations_dir),
        )
    )

    out = capsys.readouterr().out
    assert "No changes detected" in out
    assert len(list(migrations_dir.glob("*.py"))) == 1  # only the existing shift


# ---------------------------------------------------------------------------
# Slice 11: cmd_draft — writes file, importable, correct DDL on replay
# ---------------------------------------------------------------------------


def test_cmd_draft_writes_correctly_named_file(tmp_path, monkeypatch, capsys):
    from fusion.cli import cmd_draft

    class User(Model):
        id: int | None = None
        email: str

    _make_module(monkeypatch, "_draft_test_write", User=User)

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    cmd_draft(
        argparse.Namespace(
            module=["_draft_test_write"],
            slug="initial",
            migrations_dir=str(migrations_dir),
        )
    )

    files = list(migrations_dir.glob("*.py"))
    assert len(files) == 1
    assert files[0].name.endswith("_initial.py")
    out = capsys.readouterr().out
    assert "Wrote" in out


def test_cmd_draft_generated_file_is_importable_and_has_correct_ddl(tmp_path, monkeypatch):
    from fusion.cli import cmd_draft
    from fusion.orm.shift.replay import replay_shifts

    class User(Model):
        id: int | None = None
        email: str

    _make_module(monkeypatch, "_draft_test_importable", User=User)

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    cmd_draft(
        argparse.Namespace(
            module=["_draft_test_importable"],
            slug="initial",
            migrations_dir=str(migrations_dir),
        )
    )

    files = sorted(migrations_dir.glob("*.py"))
    state = replay_shifts(files)

    assert "users" in state.tables
    assert state.tables["users"].columns["id"].primary_key is True
    assert state.tables["users"].columns["email"].type == "TEXT"

    # Verify full DDL via the operation
    from fusion.orm.shift.replay import _load_shift

    shift_cls = _load_shift(files[0])
    create_table_op = shift_cls.operations[0]
    assert create_table_op.to_ddl() == (
        'CREATE TABLE IF NOT EXISTS "users" (\n'
        '    "id" SERIAL PRIMARY KEY,\n'
        '    "email" TEXT NOT NULL\n'
        ")"
    )


# ---------------------------------------------------------------------------
# Slice 12: cmd_draft — first run with empty/missing migrations dir
# ---------------------------------------------------------------------------


def test_cmd_draft_first_run_generates_create_table_for_all_models(tmp_path, monkeypatch):
    from fusion.cli import cmd_draft
    from fusion.orm.shift.replay import replay_shifts

    class User(Model):
        id: int | None = None
        email: str

    class Post(Model):
        id: int | None = None
        title: str

    _make_module(monkeypatch, "_draft_test_first_run", User=User, Post=Post)

    migrations_dir = tmp_path / "migrations"
    # dir does not exist yet

    cmd_draft(
        argparse.Namespace(
            module=["_draft_test_first_run"],
            slug="initial",
            migrations_dir=str(migrations_dir),
        )
    )

    files = sorted(migrations_dir.glob("*.py"))
    assert len(files) == 1

    state = replay_shifts(files)
    assert "users" in state.tables
    assert "posts" in state.tables


# ---------------------------------------------------------------------------
# Slice 13: filename collision — same-second timestamp gets _1, _2 suffix
# ---------------------------------------------------------------------------


def test_generate_filename_returns_unique_name_on_collision(tmp_path):
    from fusion.cli import _generate_filename
    from unittest.mock import patch
    from datetime import datetime, timezone

    fixed_dt = datetime(2026, 5, 1, 14, 30, 12, tzinfo=timezone.utc)

    with patch("fusion.cli.datetime") as mock_dt:
        mock_dt.now.return_value = fixed_dt

        first = _generate_filename("add_status", tmp_path)
        assert first == "20260501_143012_add_status.py"

        (tmp_path / first).touch()
        second = _generate_filename("add_status", tmp_path)
        assert second == "20260501_143012_add_status_1.py"

        (tmp_path / second).touch()
        third = _generate_filename("add_status", tmp_path)
        assert third == "20260501_143012_add_status_2.py"


# ---------------------------------------------------------------------------
# Slice 14: cmd_draft — --migrations-dir flag + auto-creates missing dir
# ---------------------------------------------------------------------------


def test_cmd_draft_custom_migrations_dir(tmp_path, monkeypatch, capsys):
    from fusion.cli import cmd_draft

    class Item(Model):
        id: int | None = None
        name: str

    _make_module(monkeypatch, "_draft_test_custom_dir", Item=Item)

    custom_dir = tmp_path / "custom" / "shifts"
    # dir does not exist yet

    cmd_draft(
        argparse.Namespace(
            module=["_draft_test_custom_dir"],
            slug="initial",
            migrations_dir=str(custom_dir),
        )
    )

    assert custom_dir.exists()
    files = list(custom_dir.glob("*.py"))
    assert len(files) == 1


# ---------------------------------------------------------------------------
# Slice 15: cmd_draft — slug sanitization end-to-end
# ---------------------------------------------------------------------------


def test_cmd_draft_slug_with_spaces_produces_underscored_filename(tmp_path, monkeypatch, capsys):
    from fusion.cli import cmd_draft

    class Tag(Model):
        id: int | None = None
        label: str

    _make_module(monkeypatch, "_draft_test_slug_spaces", Tag=Tag)

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    cmd_draft(
        argparse.Namespace(
            module=["_draft_test_slug_spaces"],
            slug="add status to tags",
            migrations_dir=str(migrations_dir),
        )
    )

    files = list(migrations_dir.glob("*.py"))
    assert len(files) == 1
    assert "add_status_to_tags" in files[0].name


# ---------------------------------------------------------------------------
# Slice 16: cmd_draft — delta only (existing shifts + model change)
# ---------------------------------------------------------------------------


def test_cmd_draft_generates_only_delta_not_full_schema(tmp_path, monkeypatch):
    from fusion.cli import cmd_draft
    from fusion.orm.shift.replay import _load_shift

    class User(Model):
        id: int | None = None
        email: str
        bio: str | None = None

    _make_module(monkeypatch, "_draft_test_delta", User=User)

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    # Existing shift: users table with id + email
    existing = migrations_dir / "20260501_000000_initial.py"
    existing.write_text(
        "from fusion.orm.shift import Shift, CreateTable\n\n"
        "class Initial(Shift):\n"
        "    operations = [\n"
        "        CreateTable('users', {\n"
        "            'id': {'type': 'SERIAL', 'nullable': False, 'primary_key': True},\n"
        "            'email': {'type': 'TEXT', 'nullable': False},\n"
        "        }),\n"
        "    ]\n"
    )

    # Model now also has bio — only AddColumn should be drafted
    cmd_draft(
        argparse.Namespace(
            module=["_draft_test_delta"],
            slug="add_bio",
            migrations_dir=str(migrations_dir),
        )
    )

    new_files = sorted(f for f in migrations_dir.glob("*.py") if f != existing)
    assert len(new_files) == 1

    shift_cls = _load_shift(new_files[0])
    assert len(shift_cls.operations) == 1
    assert shift_cls.operations[0].to_ddl() == (
        'ALTER TABLE "users" ADD COLUMN IF NOT EXISTS "bio" TEXT'
    )


# ---------------------------------------------------------------------------
# Extra: _infer_modules_from_cwd and _resolve_modules coverage
# ---------------------------------------------------------------------------


def test_infer_modules_from_cwd_no_pyproject(tmp_path, monkeypatch):
    from fusion.cli import _infer_modules_from_cwd

    monkeypatch.chdir(tmp_path)
    result = _infer_modules_from_cwd()
    assert result == []


def test_infer_modules_from_cwd_with_pyproject(tmp_path, monkeypatch):
    from fusion.cli import _infer_modules_from_cwd

    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'test'\n")
    pkg = tmp_path / "myapp"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    monkeypatch.chdir(tmp_path)

    result = _infer_modules_from_cwd()
    assert "myapp" in result
    assert str(tmp_path) in sys.path


def test_resolve_modules_errors_when_no_modules_and_no_pyproject(tmp_path, monkeypatch, capsys):
    from fusion.cli import _resolve_modules

    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit) as exc_info:
        _resolve_modules(argparse.Namespace(module=[]))

    assert exc_info.value.code == 1
    assert "no module specified" in capsys.readouterr().err


def test_discover_models_handles_import_error(monkeypatch):
    """ImportError during walk_packages is silently ignored."""
    import pkgutil

    from fusion.cli import discover_models

    # Create a module with __path__ so it gets walked
    fake_mod = types.ModuleType("_fake_pkg_for_walk2")
    fake_mod.__path__ = ["/nonexistent"]
    monkeypatch.setitem(sys.modules, "_fake_pkg_for_walk2", fake_mod)

    # patch walk_packages to yield a submodule that always fails to import
    BAD_SUB = "_fake_pkg_for_walk2._bad_sub"

    def _bad_walk(path, prefix):
        yield None, BAD_SUB, False

    original_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __import__

    monkeypatch.setattr(pkgutil, "walk_packages", _bad_walk)

    # Patch importlib.import_module to fail only for the bad submodule
    import importlib as _importlib

    _real_import = _importlib.import_module

    def _selective_import(name, *args, **kwargs):
        if name == BAD_SUB:
            raise ImportError("bad submodule")
        return _real_import(name, *args, **kwargs)

    monkeypatch.setattr(_importlib, "import_module", _selective_import)

    # Should not raise — ImportError in walk_packages submodule is swallowed
    result = discover_models(["_fake_pkg_for_walk2"])
    assert result == []
