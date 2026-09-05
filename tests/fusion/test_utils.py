"""The weak-mapping cached property, and the constraints its docstring claims."""

import gc
import weakref
from functools import cached_property as stdlib_cached_property

import pytest

from fusion import Object
from fusion._utils import cached_property


class Slotted:
    """No ``__dict__``, but weak-referenceable - what this helper is for."""

    __slots__ = ("__weakref__", "calls")

    def __init__(self) -> None:
        self.calls = 0

    @cached_property
    def value(self) -> int:
        """A doubled count."""
        self.calls += 1
        return self.calls * 2


def test_the_value_is_computed_once_per_instance():
    instance = Slotted()

    assert instance.value == 2
    assert instance.value == 2
    assert instance.calls == 1


def test_instances_do_not_share_a_cached_value():
    first, second = Slotted(), Slotted()

    assert first.value == 2
    assert second.value == 2
    assert first.calls == second.calls == 1


def test_the_docstring_survives_the_decoration():
    assert Slotted.value.__doc__ == "A doubled count."


def test_a_cached_value_is_released_with_its_instance():
    """The cache holds the owner weakly, so nothing outlives the instance."""
    instance = Slotted()
    assert instance.value == 2
    reference = weakref.ref(instance)

    del instance
    gc.collect()

    assert reference() is None


def test_it_serves_an_owner_the_stdlib_cannot():
    """A slotted class is exactly the case functools.cached_property rejects."""

    class Stdlib:
        __slots__ = ("__weakref__",)

        @stdlib_cached_property
        def value(self) -> int:
            return 1  # pragma: no cover

    with pytest.raises(TypeError, match="No '__dict__' attribute"):
        _ = Stdlib().value


def test_an_object_must_opt_into_weak_references():
    """Documented constraint: a plain Object is not weak-referenceable."""

    class Plain(Object):
        n: int

        @cached_property
        def doubled(self) -> int:
            return self.n * 2  # pragma: no cover

    with pytest.raises(TypeError, match="cannot create weak reference"):
        _ = Plain(n=2).doubled


def test_an_object_declaring_weakref_is_cacheable():
    class Weakly(Object, weakref=True):
        n: int

        @cached_property
        def doubled(self) -> int:
            return self.n * 2

    instance = Weakly(n=21)

    assert instance.doubled == 42
    assert instance.doubled == 42
