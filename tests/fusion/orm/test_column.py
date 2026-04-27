"""Tests for fusion.orm.column — Column descriptor and Condition objects."""

from fusion.orm.column import Column, Condition

# ---------------------------------------------------------------------------
# Column basic construction
# ---------------------------------------------------------------------------


def test_column_stores_name():
    col = Column("email")
    assert col.name == "email"


def test_column_with_table_prefix():
    col = Column("email", table="users")
    assert col.table == "users"


# ---------------------------------------------------------------------------
# Column operator overloads produce Condition objects
# ---------------------------------------------------------------------------


def test_column_eq_produces_condition():
    cond = Column("status") == "active"
    assert isinstance(cond, Condition)
    assert cond.column == "status"
    assert cond.lookup == "eq"
    assert cond.value == "active"


def test_column_ne_produces_condition():
    cond = Column("status") != "archived"
    assert isinstance(cond, Condition)
    assert cond.lookup == "ne"


def test_column_gt_produces_condition():
    cond = Column("score") > 10
    assert isinstance(cond, Condition)
    assert cond.lookup == "gt"
    assert cond.value == 10


def test_column_gte_produces_condition():
    cond = Column("score") >= 10
    assert cond.lookup == "gte"


def test_column_lt_produces_condition():
    cond = Column("score") < 100
    assert cond.lookup == "lt"


def test_column_lte_produces_condition():
    cond = Column("score") <= 100
    assert cond.lookup == "lte"


# ---------------------------------------------------------------------------
# Condition stores column name and value
# ---------------------------------------------------------------------------


def test_condition_stores_all_attrs():
    cond = Condition(column="email", lookup="eq", value="a@b.com")
    assert cond.column == "email"
    assert cond.lookup == "eq"
    assert cond.value == "a@b.com"
