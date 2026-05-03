"""Tests for fusion.orm.shift operations — full SQL string assertions."""

import pytest

from fusion.orm.shift import (
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
    RenameColumn,
    RenameTable,
    RunPython,
    RunSQL,
    Shift,
)

# ---------------------------------------------------------------------------
# CreateTable
# ---------------------------------------------------------------------------


def test_create_table_basic():
    op = CreateTable(
        "users",
        {
            "id": {"type": "SERIAL", "nullable": False, "primary_key": True},
            "email": {"type": "TEXT", "nullable": False},
        },
    )
    assert op.to_ddl() == (
        'CREATE TABLE IF NOT EXISTS "users" (\n'
        '    "id" SERIAL PRIMARY KEY,\n'
        '    "email" TEXT NOT NULL\n'
        ")"
    )


def test_create_table_schema_qualified():
    op = CreateTable(
        "matters",
        {"id": {"type": "SERIAL", "nullable": False, "primary_key": True}},
        schema="matters",
    )
    assert op.to_ddl() == (
        'CREATE TABLE IF NOT EXISTS "matters"."matters" (\n    "id" SERIAL PRIMARY KEY\n)'
    )


def test_create_table_nullable_column():
    op = CreateTable(
        "users",
        {
            "id": {"type": "SERIAL", "nullable": False, "primary_key": True},
            "bio": {"type": "TEXT", "nullable": True},
        },
    )
    assert op.to_ddl() == (
        'CREATE TABLE IF NOT EXISTS "users" (\n    "id" SERIAL PRIMARY KEY,\n    "bio" TEXT\n)'
    )


def test_create_table_column_with_default():
    op = CreateTable(
        "events",
        {
            "id": {"type": "SERIAL", "nullable": False, "primary_key": True},
            "created_at": {"type": "TIMESTAMPTZ", "nullable": True, "default": "NOW()"},
        },
    )
    assert op.to_ddl() == (
        'CREATE TABLE IF NOT EXISTS "events" (\n'
        '    "id" SERIAL PRIMARY KEY,\n'
        '    "created_at" TIMESTAMPTZ DEFAULT NOW()\n'
        ")"
    )


def test_create_table_primary_key_with_default():
    op = CreateTable(
        "user_rates",
        {
            "id": {"type": "UUID", "nullable": False, "primary_key": True, "default": "gen_random_uuid()"},
            "rate": {"type": "NUMERIC(10,2)", "nullable": False},
        },
        schema="billing",
    )
    assert op.to_ddl() == (
        'CREATE TABLE IF NOT EXISTS "billing"."user_rates" (\n'
        '    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),\n'
        '    "rate" NUMERIC(10,2) NOT NULL\n'
        ")"
    )


# ---------------------------------------------------------------------------
# DropTable
# ---------------------------------------------------------------------------


def test_drop_table():
    op = DropTable("users")
    assert op.to_ddl() == 'DROP TABLE IF EXISTS "users"'


def test_drop_table_schema_qualified():
    op = DropTable("matters", schema="matters")
    assert op.to_ddl() == 'DROP TABLE IF EXISTS "matters"."matters"'


# ---------------------------------------------------------------------------
# AddColumn
# ---------------------------------------------------------------------------


def test_add_column_nullable():
    op = AddColumn("users", "bio", "TEXT", nullable=True)
    assert op.to_ddl() == 'ALTER TABLE "users" ADD COLUMN IF NOT EXISTS "bio" TEXT'


def test_add_column_not_null_with_default():
    op = AddColumn("users", "role", "TEXT", nullable=False, default="'admin'")
    assert op.to_ddl() == (
        'ALTER TABLE "users" ADD COLUMN IF NOT EXISTS "role" TEXT NOT NULL DEFAULT \'admin\''
    )


def test_add_column_not_null_without_default_generates_ddl():
    op = AddColumn("users", "role", "TEXT", nullable=False)
    assert op.to_ddl() == 'ALTER TABLE "users" ADD COLUMN IF NOT EXISTS "role" TEXT NOT NULL'


def test_add_column_schema_qualified():
    op = AddColumn("users", "bio", "TEXT", nullable=True, schema="auth")
    assert op.to_ddl() == 'ALTER TABLE "auth"."users" ADD COLUMN IF NOT EXISTS "bio" TEXT'


# ---------------------------------------------------------------------------
# DropColumn
# ---------------------------------------------------------------------------


def test_drop_column():
    op = DropColumn("users", "bio")
    assert op.to_ddl() == 'ALTER TABLE "users" DROP COLUMN IF EXISTS "bio"'


def test_drop_column_schema_qualified():
    op = DropColumn("users", "bio", schema="auth")
    assert op.to_ddl() == 'ALTER TABLE "auth"."users" DROP COLUMN IF EXISTS "bio"'


# ---------------------------------------------------------------------------
# AlterColumn
# ---------------------------------------------------------------------------


def test_alter_column_type_only():
    op = AlterColumn("users", "status", type="VARCHAR(255)")
    assert op.to_ddl() == 'ALTER TABLE "users" ALTER COLUMN "status" TYPE VARCHAR(255)'


def test_alter_column_set_not_null():
    op = AlterColumn("users", "status", nullable=False)
    assert op.to_ddl() == 'ALTER TABLE "users" ALTER COLUMN "status" SET NOT NULL'


def test_alter_column_drop_not_null():
    op = AlterColumn("users", "status", nullable=True)
    assert op.to_ddl() == 'ALTER TABLE "users" ALTER COLUMN "status" DROP NOT NULL'


def test_alter_column_set_default():
    op = AlterColumn("users", "status", default="'active'")
    assert op.to_ddl() == 'ALTER TABLE "users" ALTER COLUMN "status" SET DEFAULT \'active\''


def test_alter_column_type_and_nullable_combined():
    op = AlterColumn("users", "status", type="TEXT", nullable=False)
    assert op.to_ddl() == (
        'ALTER TABLE "users" ALTER COLUMN "status" TYPE TEXT, ALTER COLUMN "status" SET NOT NULL'
    )


# ---------------------------------------------------------------------------
# RenameColumn
# ---------------------------------------------------------------------------


def test_rename_column():
    op = RenameColumn("users", "old_name", "new_name")
    assert op.to_ddl() == 'ALTER TABLE "users" RENAME COLUMN "old_name" TO "new_name"'


def test_rename_column_schema_qualified():
    op = RenameColumn("users", "old_name", "new_name", schema="auth")
    assert op.to_ddl() == 'ALTER TABLE "auth"."users" RENAME COLUMN "old_name" TO "new_name"'


# ---------------------------------------------------------------------------
# RenameTable
# ---------------------------------------------------------------------------


def test_rename_table():
    op = RenameTable("users", "accounts")
    assert op.to_ddl() == 'ALTER TABLE "users" RENAME TO "accounts"'


# ---------------------------------------------------------------------------
# AddConstraint
# ---------------------------------------------------------------------------


def test_add_unique_constraint_auto_named():
    op = AddConstraint("users", {"type": "unique", "columns": ["email"]})
    assert op.to_ddl() == (
        "DO $$ BEGIN\n"
        '    ALTER TABLE "users" ADD CONSTRAINT "users_email_key" UNIQUE ("email");\n'
        "EXCEPTION WHEN duplicate_object OR duplicate_table THEN NULL;\n"
        "END $$"
    )


def test_add_unique_constraint_named():
    op = AddConstraint("users", {"type": "unique", "columns": ["email"], "name": "uq_users_email"})
    assert op.to_ddl() == (
        "DO $$ BEGIN\n"
        '    ALTER TABLE "users" ADD CONSTRAINT "uq_users_email" UNIQUE ("email");\n'
        "EXCEPTION WHEN duplicate_object OR duplicate_table THEN NULL;\n"
        "END $$"
    )


def test_add_unique_constraint_multi_column_auto_named():
    op = AddConstraint("users", {"type": "unique", "columns": ["email", "org_id"]})
    assert op.to_ddl() == (
        "DO $$ BEGIN\n"
        '    ALTER TABLE "users" ADD CONSTRAINT "users_email_org_id_key" UNIQUE ("email", "org_id");\n'
        "EXCEPTION WHEN duplicate_object OR duplicate_table THEN NULL;\n"
        "END $$"
    )


def test_add_foreign_key_constraint():
    op = AddConstraint(
        "posts",
        {
            "type": "foreign_key",
            "column": "user_id",
            "references_table": "users",
            "references_column": "id",
        },
    )
    assert op.to_ddl() == (
        "DO $$ BEGIN\n"
        '    ALTER TABLE "posts" ADD CONSTRAINT "posts_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "users" ("id");\n'
        "EXCEPTION WHEN duplicate_object OR duplicate_table THEN NULL;\n"
        "END $$"
    )


def test_add_foreign_key_constraint_with_on_delete():
    op = AddConstraint(
        "posts",
        {
            "type": "foreign_key",
            "column": "user_id",
            "references_table": "users",
            "references_column": "id",
            "on_delete": "CASCADE",
        },
    )
    assert op.to_ddl() == (
        "DO $$ BEGIN\n"
        '    ALTER TABLE "posts" ADD CONSTRAINT "posts_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "users" ("id") ON DELETE CASCADE;\n'
        "EXCEPTION WHEN duplicate_object OR duplicate_table THEN NULL;\n"
        "END $$"
    )


def test_add_check_constraint():
    op = AddConstraint(
        "orders",
        {"type": "check", "expression": "amount > 0", "name": "chk_amount_positive"},
    )
    assert op.to_ddl() == (
        "DO $$ BEGIN\n"
        '    ALTER TABLE "orders" ADD CONSTRAINT "chk_amount_positive" CHECK (amount > 0);\n'
        "EXCEPTION WHEN duplicate_object OR duplicate_table THEN NULL;\n"
        "END $$"
    )


def test_add_check_constraint_empty_name_generates_stable_name():
    # Empty name (as written by old draft output) must not produce CONSTRAINT ""
    # which Postgres rejects as a zero-length delimited identifier.
    op = AddConstraint(
        "orders",
        {"type": "check", "expression": "amount > 0", "name": ""},
    )
    ddl = op.to_ddl()
    assert 'CONSTRAINT ""' not in ddl
    assert "orders_chk_" in ddl


def test_add_check_constraint_missing_name_generates_stable_name():
    # Omitting 'name' entirely should also produce a valid, deterministic name.
    op1 = AddConstraint("orders", {"type": "check", "expression": "amount > 0"})
    op2 = AddConstraint("orders", {"type": "check", "expression": "amount > 0"})
    assert op1.to_ddl() == op2.to_ddl()
    assert 'CONSTRAINT ""' not in op1.to_ddl()


def test_add_unique_constraint_duplicate_table_exception_caught():
    # Unique constraints back a physical index; Postgres raises duplicate_table
    # (42P07) rather than duplicate_object (42710) when re-applied.  The DO
    # block must catch both so re-running a shift is idempotent.
    op = AddConstraint("users", {"type": "unique", "columns": ["email"]})
    assert "duplicate_table" in op.to_ddl()


# ---------------------------------------------------------------------------
# DropConstraint
# ---------------------------------------------------------------------------


def test_add_constraint_unknown_type_raises():
    with pytest.raises(ValueError, match="Unknown constraint type"):
        AddConstraint("users", {"type": "unknown"}).to_ddl()


def test_drop_constraint():
    op = DropConstraint("users", "users_email_key")
    assert op.to_ddl() == 'ALTER TABLE "users" DROP CONSTRAINT IF EXISTS "users_email_key"'


def test_drop_constraint_schema_qualified():
    op = DropConstraint("users", "users_email_key", schema="auth")
    assert op.to_ddl() == 'ALTER TABLE "auth"."users" DROP CONSTRAINT IF EXISTS "users_email_key"'


# ---------------------------------------------------------------------------
# AddIndex
# ---------------------------------------------------------------------------


def test_add_index_single_column():
    op = AddIndex("users", ["email"])
    assert op.to_ddl() == 'CREATE INDEX IF NOT EXISTS "idx_users_email" ON "users" ("email")'


def test_add_index_multi_column():
    op = AddIndex("users", ["email", "name"])
    assert op.to_ddl() == (
        'CREATE INDEX IF NOT EXISTS "idx_users_email_name" ON "users" ("email", "name")'
    )


def test_add_index_with_method_and_explicit_name():
    op = AddIndex("users", ["email"], name="custom_idx", method="GIN")
    assert op.to_ddl() == ('CREATE INDEX IF NOT EXISTS "custom_idx" ON "users" USING GIN ("email")')


def test_add_index_schema_qualified():
    op = AddIndex("users", ["email"], schema="auth")
    assert op.to_ddl() == (
        'CREATE INDEX IF NOT EXISTS "idx_users_email" ON "auth"."users" ("email")'
    )


# ---------------------------------------------------------------------------
# DropIndex
# ---------------------------------------------------------------------------


def test_drop_index():
    op = DropIndex("idx_users_email")
    assert op.to_ddl() == 'DROP INDEX IF EXISTS "idx_users_email"'


# ---------------------------------------------------------------------------
# AddExtension / DropExtension
# ---------------------------------------------------------------------------


def test_add_extension():
    op = AddExtension("pgcrypto")
    assert op.to_ddl() == 'CREATE EXTENSION IF NOT EXISTS "pgcrypto"'


def test_drop_extension():
    op = DropExtension("pgcrypto")
    assert op.to_ddl() == 'DROP EXTENSION IF EXISTS "pgcrypto"'


# ---------------------------------------------------------------------------
# CreateSchema
# ---------------------------------------------------------------------------


def test_create_schema():
    op = CreateSchema("matters")
    assert op.to_ddl() == 'CREATE SCHEMA IF NOT EXISTS "matters"'


# ---------------------------------------------------------------------------
# RunSQL
# ---------------------------------------------------------------------------


def test_run_sql_passthrough():
    op = RunSQL("SELECT 1; SELECT 2;")
    assert op.to_ddl() == "SELECT 1; SELECT 2;"


def test_run_sql_preserves_raw_string():
    sql = "UPDATE users SET status = 'active' WHERE status IS NULL"
    op = RunSQL(sql)
    assert op.to_ddl() == sql


# ---------------------------------------------------------------------------
# RunPython
# ---------------------------------------------------------------------------


def test_run_python_stores_callable():
    async def populate(conn):
        pass

    op = RunPython(populate)
    assert op.fn is populate


@pytest.mark.asyncio
async def test_run_python_calls_fn_with_connection():
    calls = []

    async def populate(conn):
        calls.append(conn)

    op = RunPython(populate)
    sentinel = object()
    await op.fn(sentinel)
    assert calls == [sentinel]


# ---------------------------------------------------------------------------
# Shift
# ---------------------------------------------------------------------------


def test_shift_holds_operations():
    class MyShift(Shift):
        operations = [
            CreateTable(
                "users", {"id": {"type": "SERIAL", "nullable": False, "primary_key": True}}
            ),
            AddColumn("users", "bio", "TEXT", nullable=True),
        ]

    assert len(MyShift.operations) == 2
    assert isinstance(MyShift.operations[0], CreateTable)
    assert isinstance(MyShift.operations[1], AddColumn)


def test_shift_importable_from_fusion_orm_shift():
    from fusion.orm.shift import Shift as ImportedShift

    assert ImportedShift is Shift
