"""Tests for fusion.orm.migration.apply — DDL SQL from diff changes."""

import pytest

from fusion.orm.migration.apply import BlockedOperationError, to_ddl


# ---------------------------------------------------------------------------
# CREATE TABLE
# ---------------------------------------------------------------------------


def test_create_table_ddl():
    change = {
        "op": "create_table",
        "table": "users",
        "columns": {
            "id": {"type": "SERIAL", "nullable": False, "primary_key": True},
            "email": {"type": "TEXT", "nullable": False},
        },
    }
    ddl = to_ddl([change])
    assert len(ddl) == 1
    assert "CREATE TABLE" in ddl[0]
    assert '"users"' in ddl[0]
    assert "SERIAL" in ddl[0]
    assert "PRIMARY KEY" in ddl[0]


def test_create_table_nullable_column():
    change = {
        "op": "create_table",
        "table": "posts",
        "columns": {
            "id": {"type": "SERIAL", "nullable": False, "primary_key": True},
            "body": {"type": "TEXT", "nullable": True},
        },
    }
    ddl = to_ddl([change])
    assert "NULL" in ddl[0]
    assert "NOT NULL" in ddl[0]


def test_create_table_with_default():
    change = {
        "op": "create_table",
        "table": "events",
        "columns": {
            "id": {"type": "SERIAL", "nullable": False, "primary_key": True},
            "created_at": {"type": "TIMESTAMPTZ", "nullable": True, "default": "NOW()"},
        },
    }
    ddl = to_ddl([change])
    assert "DEFAULT NOW()" in ddl[0]


# ---------------------------------------------------------------------------
# ADD COLUMN
# ---------------------------------------------------------------------------


def test_add_nullable_column_ddl():
    change = {
        "op": "add_column",
        "table": "users",
        "column": "bio",
        "type": "TEXT",
        "nullable": True,
        "default": None,
    }
    ddl = to_ddl([change])
    assert len(ddl) == 1
    assert "ALTER TABLE" in ddl[0]
    assert "ADD COLUMN" in ddl[0]
    assert '"bio"' in ddl[0]
    assert "NULL" in ddl[0]


def test_add_not_null_column_with_default_ddl():
    change = {
        "op": "add_column",
        "table": "users",
        "column": "role",
        "type": "TEXT",
        "nullable": False,
        "default": "user",
    }
    ddl = to_ddl([change])
    assert "NOT NULL" in ddl[0]
    assert "DEFAULT" in ddl[0]


def test_add_not_null_column_without_default_raises():
    change = {
        "op": "add_column",
        "table": "users",
        "column": "role",
        "type": "TEXT",
        "nullable": False,
        "default": None,
    }
    with pytest.raises(BlockedOperationError, match="NOT NULL.*default"):
        to_ddl([change])


# ---------------------------------------------------------------------------
# DROP COLUMN / TABLE — blocked ops raise
# ---------------------------------------------------------------------------


def test_drop_column_blocked_raises():
    change = {"op": "drop_column_blocked", "table": "users", "column": "bio"}
    with pytest.raises(BlockedOperationError, match="drop_column"):
        to_ddl([change])


def test_drop_table_blocked_raises():
    change = {"op": "drop_table_blocked", "table": "users"}
    with pytest.raises(BlockedOperationError, match="drop_table"):
        to_ddl([change])


def test_drop_column_allowed_ddl():
    change = {"op": "drop_column", "table": "users", "column": "bio"}
    ddl = to_ddl([change])
    assert "DROP COLUMN" in ddl[0]
    assert '"bio"' in ddl[0]


def test_drop_table_allowed_ddl():
    change = {"op": "drop_table", "table": "users"}
    ddl = to_ddl([change])
    assert "DROP TABLE" in ddl[0]
    assert '"users"' in ddl[0]


# ---------------------------------------------------------------------------
# ADD CONSTRAINT
# ---------------------------------------------------------------------------


def test_add_unique_constraint_ddl():
    change = {
        "op": "add_constraint",
        "table": "users",
        "constraint": {"type": "unique", "columns": ["email"]},
    }
    ddl = to_ddl([change])
    assert "ADD CONSTRAINT" in ddl[0] or "ADD UNIQUE" in ddl[0]
    assert '"email"' in ddl[0]


def test_add_foreign_key_constraint_ddl():
    change = {
        "op": "add_constraint",
        "table": "posts",
        "constraint": {
            "type": "foreign_key",
            "column": "user_id",
            "references_table": "users",
            "references_column": "id",
        },
    }
    ddl = to_ddl([change])
    assert "FOREIGN KEY" in ddl[0]
    assert "REFERENCES" in ddl[0]
    assert '"users"' in ddl[0]


# ---------------------------------------------------------------------------
# CREATE INDEX
# ---------------------------------------------------------------------------


def test_create_index_ddl():
    change = {
        "op": "create_index",
        "table": "posts",
        "index": {"columns": ["user_id"]},
    }
    ddl = to_ddl([change])
    assert "CREATE INDEX" in ddl[0]
    assert '"user_id"' in ddl[0]


def test_create_index_with_method_ddl():
    change = {
        "op": "create_index",
        "table": "posts",
        "index": {"columns": ["body"], "method": "GIN"},
    }
    ddl = to_ddl([change])
    assert "USING GIN" in ddl[0]


# ---------------------------------------------------------------------------
# Multiple changes → multiple DDL statements
# ---------------------------------------------------------------------------


def test_multiple_changes_produce_multiple_statements():
    changes = [
        {
            "op": "add_column",
            "table": "users",
            "column": "bio",
            "type": "TEXT",
            "nullable": True,
            "default": None,
        },
        {
            "op": "create_index",
            "table": "users",
            "index": {"columns": ["bio"]},
        },
    ]
    ddl = to_ddl(changes)
    assert len(ddl) == 2
