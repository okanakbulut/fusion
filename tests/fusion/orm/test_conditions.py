"""Tests for fusion.orm.conditions — Q boolean composition."""

from fusion.orm.column import Condition
from fusion.orm.conditions import Q

# ---------------------------------------------------------------------------
# Q construction from kwargs
# ---------------------------------------------------------------------------


def test_q_from_equality_kwarg():
    q = Q(user_id=1)
    conds = q.conditions
    assert len(conds) == 1
    assert conds[0].column == "user_id"
    assert conds[0].lookup == "eq"
    assert conds[0].value == 1


def test_q_from_double_underscore_lookup():
    q = Q(score__gt=10)
    conds = q.conditions
    assert conds[0].column == "score"
    assert conds[0].lookup == "gt"
    assert conds[0].value == 10


def test_q_all_supported_lookups(lookup):
    _col, suffix = lookup
    q = Q(**{f"field__{suffix}": "x"})
    assert q.conditions[0].lookup == suffix


def test_q_multiple_kwargs_produces_multiple_conditions():
    q = Q(user_id=1, status="active")
    assert len(q.conditions) == 2


def test_q_from_condition_object():
    cond = Condition(column="email", lookup="ilike", value="%@example.com")
    q = Q(cond)
    assert q.conditions[0] is cond


# ---------------------------------------------------------------------------
# Q boolean operators return Q
# ---------------------------------------------------------------------------


def test_q_and_returns_q():
    result = Q(a=1) & Q(b=2)
    assert isinstance(result, Q)


def test_q_or_returns_q():
    result = Q(a=1) | Q(b=2)
    assert isinstance(result, Q)


def test_q_invert_returns_q():
    result = ~Q(a=1)
    assert isinstance(result, Q)


# ---------------------------------------------------------------------------
# Q tree structure
# ---------------------------------------------------------------------------


def test_q_and_sets_op():
    result = Q(a=1) & Q(b=2)
    assert result.op == "and"
    assert len(result.children) == 2


def test_q_or_sets_op():
    result = Q(a=1) | Q(b=2)
    assert result.op == "or"


def test_q_invert_sets_op():
    result = ~Q(a=1)
    assert result.op == "not"
    assert len(result.children) == 1


def test_q_leaf_has_no_children():
    q = Q(x=1)
    assert q.children == []
    assert q.op == "and"


def test_q_with_q_positional_arg():
    inner = Q(user_id=1)
    outer = Q(inner)
    assert len(outer.children) == 1
    assert outer.children[0] is inner


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


import pytest  # noqa: E402


@pytest.fixture(
    params=[
        ("field", "gt"),
        ("field", "gte"),
        ("field", "lt"),
        ("field", "lte"),
        ("field", "in"),
        ("field", "like"),
        ("field", "ilike"),
        ("field", "is_null"),
        ("field", "is_not_null"),
    ]
)
def lookup(request):
    col, suffix = request.param
    return col, suffix
