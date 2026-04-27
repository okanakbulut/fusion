"""Tests for fusion.orm.fields — DBField, field(), db_now(), db_uuid()."""

import msgspec
import pytest

from fusion.orm.fields import DBField, db_now, db_uuid, field
from fusion.object import Field


# ---------------------------------------------------------------------------
# DBField is a Field subclass
# ---------------------------------------------------------------------------


def test_dbfield_is_subclass_of_field():
    assert issubclass(DBField, Field)


def test_field_returns_dbfield():
    f = field()
    assert isinstance(f, DBField)


def test_dbfield_db_type_defaults_to_none():
    f = field()
    assert f.db_type is None


def test_field_accepts_db_type():
    f = field(db_type="JSONB")
    assert f.db_type == "JSONB"


def test_field_forwards_validation_constraints():
    f = field(min_length=3, max_length=100, db_type="TEXT")
    assert f.min_length == 3
    assert f.max_length == 100
    assert f.db_type == "TEXT"


def test_field_forwards_default():
    f = field(default="hello")
    assert f.default == "hello"


def test_field_forwards_default_factory():
    f = field(default_factory=list)
    assert f.default_factory is list


# ---------------------------------------------------------------------------
# DB sentinels
# ---------------------------------------------------------------------------


def test_db_now_returns_sentinel():
    s = db_now()
    assert s is not None


def test_db_uuid_returns_sentinel():
    s = db_uuid()
    assert s is not None


def test_db_now_instances_are_same_object():
    assert db_now() is db_now()


def test_db_uuid_instances_are_same_object():
    assert db_uuid() is db_uuid()


def test_db_now_and_db_uuid_are_distinct():
    assert db_now() is not db_uuid()


def test_db_now_is_dbfield_default_sentinel(sentinel_types):
    _DbNow, _DbUuid = sentinel_types
    assert isinstance(db_now(), _DbNow)


def test_db_uuid_is_dbfield_uuid_sentinel(sentinel_types):
    _DbNow, _DbUuid = sentinel_types
    assert isinstance(db_uuid(), _DbUuid)


# ---------------------------------------------------------------------------
# DBField validation constraints still work via msgspec
# ---------------------------------------------------------------------------


def test_dbfield_constraints_enforced_in_object():
    from fusion.object import Object

    class Product(Object):
        price: float = field(ge=0.0, db_type="DOUBLE PRECISION")

    with pytest.raises(msgspec.ValidationError):
        msgspec.json.decode(b'{"price": -1.0}', type=Product)

    p = msgspec.json.decode(b'{"price": 5.0}', type=Product)
    assert p.price == 5.0


@pytest.fixture
def sentinel_types():
    from fusion.orm import fields as f_module

    return f_module._DbNow, f_module._DbUuid
