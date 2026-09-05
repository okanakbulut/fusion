"""A cached property for owners that cannot hold the value themselves."""

import functools
import typing
import weakref


def cached_property[T](func: typing.Callable[[typing.Any], T]) -> property:
    """Cache a computed value outside the instance, in a weak mapping.

    Prefer ``functools.cached_property``.  It writes the value into the
    instance dictionary, so every read after the first is a plain attribute
    lookup - about 20ns against 85ns here, because this one pays for a weak
    mapping lookup on every access.  ``Context`` uses the stdlib version for
    exactly that reason.

    This exists for the case the stdlib cannot serve: an owner with no
    ``__dict__`` to write into.  Any class defining ``__slots__`` qualifies, and
    every msgspec ``Struct`` - so every :class:`~fusion.object.Object` - is such
    a class.  ``functools.cached_property`` raises ``TypeError: No '__dict__'
    attribute`` there.

    Two conditions apply to the owner, and both fail loudly on first access:

    * **weak-referenceable.**  An ``Object`` is not, by default.  Declare it
      ``class Foo(Object, weakref=True)`` or the first read raises
      ``TypeError: cannot create weak reference``.
    * **hashable.**  A frozen struct is; a mutable one defines ``__eq__``
      without ``__hash__`` and is not.

    The cache holds no strong reference to the owner, so a cached value is
    released with the instance it belongs to.
    """
    cache: weakref.WeakKeyDictionary[typing.Any, T] = weakref.WeakKeyDictionary()

    @functools.wraps(func)
    def getter(instance: typing.Any) -> T:
        try:
            return cache[instance]
        except KeyError:
            pass
        # Computed outside the handler: a KeyError raised by func is the
        # caller's own, and chaining it to ours would misreport it.
        cache[instance] = value = func(instance)
        return value

    return property(getter)
