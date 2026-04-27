"""Tests for fusion.orm.migration.diff — diff two snapshots → ordered change list."""

from fusion.orm.migration.diff import diff


def _snap(*tables: tuple[str, dict]) -> dict:
    return {"version": 1, "tables": {name: schema for name, schema in tables}}


def _table(columns: dict, constraints=None, indexes=None) -> dict:
    return {
        "columns": columns,
        "constraints": constraints or [],
        "indexes": indexes or [],
    }


def _col(pg_type="TEXT", nullable=False, primary_key=False, default=None) -> dict:
    d: dict = {"type": pg_type, "nullable": nullable, "primary_key": primary_key}
    if default is not None:
        d["default"] = default
    return d


# ---------------------------------------------------------------------------
# Add table
# ---------------------------------------------------------------------------


def test_add_table_produces_create_table_change():
    before = _snap()
    after = _snap(("users", _table({"id": _col("SERIAL", primary_key=True), "email": _col()})))
    changes = diff(before, after)
    assert any(c["op"] == "create_table" and c["table"] == "users" for c in changes)


def test_create_table_includes_columns():
    before = _snap()
    after = _snap(("users", _table({"id": _col("SERIAL", primary_key=True), "email": _col()})))
    changes = diff(before, after)
    create = next(c for c in changes if c["op"] == "create_table")
    assert "email" in create["columns"]


# ---------------------------------------------------------------------------
# Add column
# ---------------------------------------------------------------------------


def test_add_nullable_column_produces_add_column_change():
    before = _snap(("users", _table({"id": _col("SERIAL", primary_key=True)})))
    after = _snap(
        ("users", _table({"id": _col("SERIAL", primary_key=True), "bio": _col("TEXT", nullable=True)}))
    )
    changes = diff(before, after)
    assert any(c["op"] == "add_column" and c["column"] == "bio" for c in changes)


def test_add_column_change_includes_type_and_nullable():
    before = _snap(("users", _table({"id": _col("SERIAL", primary_key=True)})))
    after = _snap(
        ("users", _table({"id": _col("SERIAL", primary_key=True), "bio": _col("TEXT", nullable=True)}))
    )
    changes = diff(before, after)
    add = next(c for c in changes if c["op"] == "add_column")
    assert add["type"] == "TEXT"
    assert add["nullable"] is True


# ---------------------------------------------------------------------------
# Drop column — blocked
# ---------------------------------------------------------------------------


def test_drop_column_without_flag_produces_blocked_change():
    before = _snap(
        ("users", _table({"id": _col("SERIAL", primary_key=True), "bio": _col(nullable=True)}))
    )
    after = _snap(("users", _table({"id": _col("SERIAL", primary_key=True)})))
    changes = diff(before, after)
    assert any(c["op"] == "drop_column_blocked" for c in changes)


def test_drop_column_with_flag_produces_drop_change():
    before = _snap(
        ("users", _table({"id": _col("SERIAL", primary_key=True), "bio": _col(nullable=True)}))
    )
    after = _snap(("users", _table({"id": _col("SERIAL", primary_key=True)})))
    changes = diff(before, after, allow_drop=True)
    assert any(c["op"] == "drop_column" and c["column"] == "bio" for c in changes)


# ---------------------------------------------------------------------------
# Drop table — blocked
# ---------------------------------------------------------------------------


def test_drop_table_without_flag_produces_blocked_change():
    before = _snap(("users", _table({"id": _col("SERIAL", primary_key=True)})))
    after = _snap()
    changes = diff(before, after)
    assert any(c["op"] == "drop_table_blocked" and c["table"] == "users" for c in changes)


def test_drop_table_with_flag_produces_drop_table_change():
    before = _snap(("users", _table({"id": _col("SERIAL", primary_key=True)})))
    after = _snap()
    changes = diff(before, after, allow_drop=True)
    assert any(c["op"] == "drop_table" and c["table"] == "users" for c in changes)


# ---------------------------------------------------------------------------
# Add constraint / index
# ---------------------------------------------------------------------------


def test_add_unique_constraint():
    before = _snap(("users", _table({"id": _col("SERIAL", primary_key=True)})))
    after = _snap(
        ("users", _table({"id": _col("SERIAL", primary_key=True)}, constraints=[{"type": "unique", "columns": ["email"]}]))
    )
    changes = diff(before, after)
    assert any(c["op"] == "add_constraint" for c in changes)


def test_add_index():
    before = _snap(("users", _table({"id": _col("SERIAL", primary_key=True)})))
    after = _snap(
        ("users", _table({"id": _col("SERIAL", primary_key=True)}, indexes=[{"columns": ["email"]}]))
    )
    changes = diff(before, after)
    assert any(c["op"] == "create_index" for c in changes)


# ---------------------------------------------------------------------------
# No changes produces empty list
# ---------------------------------------------------------------------------


def test_identical_snapshots_produce_no_changes():
    snap = _snap(("users", _table({"id": _col("SERIAL", primary_key=True)})))
    assert diff(snap, snap) == []
