"""Tests for fusion.orm.constraints — ForeignKey, UniqueConstraint, Index."""

from fusion.orm.constraints import ForeignKey, Index, UniqueConstraint


# ---------------------------------------------------------------------------
# UniqueConstraint
# ---------------------------------------------------------------------------


def test_unique_constraint_single_column():
    uc = UniqueConstraint("email")
    assert uc.columns == ("email",)


def test_unique_constraint_composite():
    uc = UniqueConstraint("user_id", "org_id")
    assert uc.columns == ("user_id", "org_id")


# ---------------------------------------------------------------------------
# ForeignKey
# ---------------------------------------------------------------------------


def test_foreign_key_stores_column_and_target():
    class User:
        pass

    fk = ForeignKey("user_id", User)
    assert fk.column == "user_id"
    assert fk.target is User


def test_foreign_key_default_target_column_is_id():
    class User:
        pass

    fk = ForeignKey("user_id", User)
    assert fk.target_column == "id"


def test_foreign_key_custom_target_column():
    class User:
        pass

    fk = ForeignKey("user_id", User, target_column="uid")
    assert fk.target_column == "uid"


# ---------------------------------------------------------------------------
# Index
# ---------------------------------------------------------------------------


def test_index_single_column():
    idx = Index("email")
    assert idx.columns == ("email",)


def test_index_composite():
    idx = Index("user_id", "created_at")
    assert idx.columns == ("user_id", "created_at")


def test_index_default_method_is_none():
    idx = Index("email")
    assert idx.method is None


def test_index_custom_method():
    idx = Index("body", method="GIN")
    assert idx.method == "GIN"
