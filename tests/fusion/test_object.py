"""Tests for the Object base class and MetaObject metaclass.

Object is the schema foundation of fusion — every request/response model
inherits from it. These tests show how fields, constraints, defaults,
renames, and __post_init__ validation work during JSON deserialization.
"""

import typing

import msgspec
import pytest

from fusion.object import Field, Object, field

# ---------------------------------------------------------------------------
# Basic deserialization
# ---------------------------------------------------------------------------


def test_object_deserializes_from_json():
    class User(Object):
        id: int
        name: str

    user = msgspec.json.decode(b'{"id": 1, "name": "Alice"}', type=User)
    assert user.id == 1
    assert user.name == "Alice"


def test_object_fields_class_var_is_populated():
    class Item(Object):
        name: str
        price: float

    assert "name" in Item.__fields__
    assert "price" in Item.__fields__


def test_classvar_fields_are_ignored_by_metaclass():
    class Config(Object):
        debug: typing.ClassVar[bool] = False
        value: str

    assert "debug" not in Config.__fields__
    assert "value" in Config.__fields__


# ---------------------------------------------------------------------------
# Field defaults
# ---------------------------------------------------------------------------


def test_field_with_default_value():
    class Settings(Object):
        timeout: int = field(default=30)

    s = msgspec.json.decode(b"{}", type=Settings)
    assert s.timeout == 30


def test_field_with_default_factory():
    class Bag(Object):
        tags: list[str] = field(default_factory=list)

    b = msgspec.json.decode(b"{}", type=Bag)
    assert b.tags == []


# ---------------------------------------------------------------------------
# Field name remapping (JSON alias)
# ---------------------------------------------------------------------------


def test_field_name_renames_json_key():
    class Event(Object):
        created_at: str = field(name="createdAt", default="")

    e = msgspec.json.decode(b'{"createdAt": "2026-01-01"}', type=Event)
    assert e.created_at == "2026-01-01"


# ---------------------------------------------------------------------------
# Constraint enforcement via msgspec.Meta
# ---------------------------------------------------------------------------


def test_field_ge_constraint_enforced():
    class Product(Object):
        price: float = field(ge=0.0)

    with pytest.raises(msgspec.ValidationError):
        msgspec.json.decode(b'{"price": -1.0}', type=Product)

    p = msgspec.json.decode(b'{"price": 0.0}', type=Product)
    assert p.price == 0.0


def test_field_gt_constraint_enforced():
    class Qty(Object):
        count: int = field(gt=0)

    with pytest.raises(msgspec.ValidationError):
        msgspec.json.decode(b'{"count": 0}', type=Qty)


def test_field_le_constraint_enforced():
    class Score(Object):
        value: int = field(le=100)

    with pytest.raises(msgspec.ValidationError):
        msgspec.json.decode(b'{"value": 101}', type=Score)


def test_field_lt_constraint_enforced():
    class Score(Object):
        value: int = field(lt=100)

    with pytest.raises(msgspec.ValidationError):
        msgspec.json.decode(b'{"value": 100}', type=Score)


def test_field_min_length_constraint_enforced():
    class Username(Object):
        name: str = field(min_length=3)

    with pytest.raises(msgspec.ValidationError):
        msgspec.json.decode(b'{"name": "ab"}', type=Username)


def test_field_max_length_constraint_enforced():
    class Username(Object):
        name: str = field(max_length=10)

    with pytest.raises(msgspec.ValidationError):
        msgspec.json.decode(b'{"name": "toolongname!"}', type=Username)


def test_field_pattern_constraint_enforced():
    class Code(Object):
        value: str = field(pattern=r"^[A-Z]{3}$")

    with pytest.raises(msgspec.ValidationError):
        msgspec.json.decode(b'{"value": "abc"}', type=Code)

    c = msgspec.json.decode(b'{"value": "ABC"}', type=Code)
    assert c.value == "ABC"


# ---------------------------------------------------------------------------
# __post_init__ custom validation
# ---------------------------------------------------------------------------


def test_post_init_custom_validation_runs():
    class AgeRange(Object):
        min_age: int
        max_age: int

        def __post_init__(self):
            if self.min_age >= self.max_age:
                raise ValueError("min_age must be less than max_age")

    with pytest.raises((ValueError, msgspec.ValidationError)):
        msgspec.json.decode(b'{"min_age": 30, "max_age": 10}', type=AgeRange)

    a = msgspec.json.decode(b'{"min_age": 10, "max_age": 30}', type=AgeRange)
    assert a.min_age == 10


# ---------------------------------------------------------------------------
# Nested objects
# ---------------------------------------------------------------------------


def test_nested_object_deserializes():
    class Address(Object):
        street: str
        city: str

    class Person(Object):
        name: str
        address: Address

    p = msgspec.json.decode(
        b'{"name": "Bob", "address": {"street": "Main St", "city": "Springfield"}}',
        type=Person,
    )
    assert p.name == "Bob"
    assert p.address.city == "Springfield"


# ---------------------------------------------------------------------------
# Field info stored in __fields__
# ---------------------------------------------------------------------------


def test_field_info_stored_in_fields():
    class Item(Object):
        price: float = field(ge=0.0, description="Item price")

    f: Field = Item.__fields__["price"]
    assert f.ge == 0.0
    assert f.description == "Item price"


def test_plain_default_stored_as_field():
    class Item(Object):
        qty: int = 1

    assert "qty" in Item.__fields__


def test_metaclass_fallback_on_type_hints_failure():
    import sys

    from fusion.object import MetaObject

    original = sys.modules.get("fusion_test_sentinel")
    try:
        namespace = {
            "__module__": "fusion_test_sentinel",
            "__qualname__": "Orphan",
            "__annotations__": {"value": "MissingType"},
        }
        sys.modules["fusion_test_sentinel"] = None  # type: ignore[assignment]
        Orphan = MetaObject("Orphan", (Object,), namespace)
        assert "value" in Orphan.__annotations__
    finally:
        if original is None:
            sys.modules.pop("fusion_test_sentinel", None)
        else:
            sys.modules["fusion_test_sentinel"] = original
