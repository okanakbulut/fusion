"""Tests for fusion.orm.model — MetaModel, Model, table naming, Column descriptors."""

import msgspec
import pytest

from fusion.orm.column import Column
from fusion.orm.constraints import ForeignKey, Index, UniqueConstraint
from fusion.orm.fields import field
from fusion.orm.model import Model

# ---------------------------------------------------------------------------
# Table name derivation
# ---------------------------------------------------------------------------


def test_simple_name_is_snake_case_plural():
    class Post(Model):
        id: int | None = None
        title: str

    assert Post.__table_name__ == "posts"


def test_multi_word_name_is_snake_case_plural():
    class UserProfile(Model):
        id: int | None = None
        bio: str

    assert UserProfile.__table_name__ == "user_profiles"


def test_already_snake_name_is_pluralized():
    class audit_log(Model):
        id: int | None = None

    assert audit_log.__table_name__ == "audit_logs"


def test_table_name_override():
    class LegacyAudit(Model):
        __table__ = "tbl_audit"
        id: int | None = None

    assert LegacyAudit.__table_name__ == "tbl_audit"


# ---------------------------------------------------------------------------
# Class-level attribute access returns Column
# ---------------------------------------------------------------------------


def test_class_level_field_access_returns_column():
    class User(Model):
        id: int | None = None
        email: str

    assert isinstance(User.email, Column)


def test_column_carries_field_name():
    class User(Model):
        id: int | None = None
        email: str

    assert User.email.name == "email"


def test_instance_level_access_returns_value():
    class User(Model):
        id: int | None = None
        email: str

    u = msgspec.json.decode(b'{"id": 1, "email": "a@b.com"}', type=User)
    assert u.email == "a@b.com"


# ---------------------------------------------------------------------------
# DB metadata stored on class
# ---------------------------------------------------------------------------


def test_constraints_stored_on_class():
    class Membership(Model):
        user_id: int
        org_id: int

        __constraints__ = [UniqueConstraint("user_id", "org_id")]

    assert len(Membership.__db_constraints__) == 1
    assert isinstance(Membership.__db_constraints__[0], UniqueConstraint)


def test_indexes_stored_on_class():
    class Post(Model):
        id: int | None = None
        user_id: int

        __indexes__ = [Index("user_id")]

    assert len(Post.__db_indexes__) == 1
    assert isinstance(Post.__db_indexes__[0], Index)


def test_empty_constraints_and_indexes_by_default():
    class Simple(Model):
        id: int | None = None

    assert Simple.__db_constraints__ == []
    assert Simple.__db_indexes__ == []


# ---------------------------------------------------------------------------
# Model is a valid msgspec Struct — validation still works
# ---------------------------------------------------------------------------


def test_model_inherits_field_validation():
    class Post(Model):
        id: int | None = None
        title: str = field(min_length=1, max_length=255)

    with pytest.raises(msgspec.ValidationError):
        msgspec.json.decode(b'{"id": 1, "title": ""}', type=Post)


def test_model_is_frozen():
    class User(Model):
        id: int | None = None
        email: str

    u = msgspec.json.decode(b'{"id": 1, "email": "a@b.com"}', type=User)
    with pytest.raises((TypeError, AttributeError)):
        u.email = "b@c.com"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Table name derivation — edge cases
# ---------------------------------------------------------------------------


def test_pluralize_y_suffix():
    class Category(Model):
        id: int | None = None
        name: str

    assert Category.__table_name__ == "categories"


def test_pluralize_es_suffix():
    class Box(Model):
        id: int | None = None

    assert Box.__table_name__ == "boxes"


def test_class_level_non_field_access_returns_normally():
    class Widget(Model):
        id: int | None = None

    assert Widget.__table_name__ == "widgets"
