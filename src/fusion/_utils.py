import weakref


def cached_property(func):
    cache = weakref.WeakKeyDictionary()

    def getter(instance):
        if instance not in cache:
            cache[instance] = func(instance)
        return cache[instance]

    return property(getter)
