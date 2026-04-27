"""Tests for fusion.orm.migration.snapshot — serialize Model classes to YAML."""

from fusion.orm.constraints import ForeignKey, Index, UniqueConstraint
from fusion.orm.fields import db_now, field
from fusion.orm.migration.snapshot import serialize
from fusion.orm.model import Model


class User(Model):
    id: int | None = None
    email: str
    username: str
    created_at: str | None = field(db_type="TIMESTAMPTZ", default=db_now())

    __constraints__ = [
        UniqueConstraint("email"),
        UniqueConstraint("username"),
    ]
    __indexes__ = [Index("email")]


class Post(Model):
    id: int | None = None
    user_id: int
    title: str = field(min_length=1, max_length=255)
    body: str | None = None
    tags: list[str] = field(db_type="TEXT[]", default_factory=list)

    __constraints__ = [ForeignKey("user_id", User)]
    __indexes__ = [Index("user_id", "created_at")]


# ---------------------------------------------------------------------------
# serialize() returns a dict
# ---------------------------------------------------------------------------


def test_serialize_returns_dict():
    result = serialize([User])
    assert isinstance(result, dict)


def test_serialize_has_version_key():
    result = serialize([User])
    assert "version" in result
    assert result["version"] == 1


def test_serialize_has_tables_key():
    result = serialize([User])
    assert "tables" in result


# ---------------------------------------------------------------------------
# Table-level structure
# ---------------------------------------------------------------------------


def test_serialize_includes_table_name():
    result = serialize([User])
    assert "users" in result["tables"]


def test_serialize_includes_all_models():
    result = serialize([User, Post])
    assert "users" in result["tables"]
    assert "posts" in result["tables"]


def test_table_has_columns_key():
    result = serialize([User])
    assert "columns" in result["tables"]["users"]


# ---------------------------------------------------------------------------
# Column structure
# ---------------------------------------------------------------------------


def test_id_column_is_serial_primary_key():
    result = serialize([User])
    id_col = result["tables"]["users"]["columns"]["id"]
    assert id_col["type"] == "SERIAL"
    assert id_col["primary_key"] is True
    assert id_col["nullable"] is False


def test_non_null_str_column():
    result = serialize([User])
    email_col = result["tables"]["users"]["columns"]["email"]
    assert email_col["type"] == "TEXT"
    assert email_col["nullable"] is False


def test_nullable_column():
    result = serialize([Post])
    body_col = result["tables"]["posts"]["columns"]["body"]
    assert body_col["nullable"] is True


def test_db_type_override_in_column():
    result = serialize([Post])
    tags_col = result["tables"]["posts"]["columns"]["tags"]
    assert tags_col["type"] == "TEXT[]"


def test_db_now_column_has_now_default():
    result = serialize([User])
    created_at = result["tables"]["users"]["columns"]["created_at"]
    assert created_at["default"] == "NOW()"


# ---------------------------------------------------------------------------
# Constraints in snapshot
# ---------------------------------------------------------------------------


def test_unique_constraints_in_snapshot():
    result = serialize([User])
    constraints = result["tables"]["users"]["constraints"]
    unique_types = [c for c in constraints if c["type"] == "unique"]
    cols = [tuple(c["columns"]) for c in unique_types]
    assert ("email",) in cols
    assert ("username",) in cols


def test_foreign_key_in_snapshot():
    result = serialize([Post])
    constraints = result["tables"]["posts"]["constraints"]
    fk = next(c for c in constraints if c["type"] == "foreign_key")
    assert fk["column"] == "user_id"
    assert fk["references_table"] == "users"
    assert fk["references_column"] == "id"


def test_indexes_in_snapshot():
    result = serialize([User])
    indexes = result["tables"]["users"]["indexes"]
    assert any("email" in idx["columns"] for idx in indexes)


def test_composite_index_in_snapshot():
    result = serialize([Post])
    indexes = result["tables"]["posts"]["indexes"]
    assert any(
        set(idx["columns"]) == {"user_id", "created_at"} for idx in indexes
    )


def test_db_uuid_column_has_gen_random_uuid_default():
    from fusion.orm.fields import db_uuid

    class Token(Model):
        id: int | None = None
        token: str | None = field(db_type="UUID", default=db_uuid())

    result = serialize([Token])
    token_col = result["tables"]["tokens"]["columns"]["token"]
    assert token_col["default"] == "gen_random_uuid()"


def test_unknown_python_type_defaults_to_text():
    from fusion.orm.migration.snapshot import _resolve_pg_type

    class Misc(Model):
        id: int | None = None
        data: list[str] = field(default_factory=list)

    # list[str] without db_type — falls back to TEXT
    pg_type = _resolve_pg_type(Misc, "data")
    assert pg_type == "TEXT"
