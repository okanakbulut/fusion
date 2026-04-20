import asyncio
import inspect
import re
import weakref
from collections import defaultdict
from functools import lru_cache
from urllib.parse import parse_qsl


@lru_cache(maxsize=128)
def parse_query_params(query_string):
    params = defaultdict(list)
    for name, value in parse_qsl(query_string, keep_blank_values=True):
        params[name].append(value)
    # flatten single-value lists
    return {k: v if len(v) > 1 else v[0] for k, v in params.items()}


def cached_property(func):
    cache = weakref.WeakKeyDictionary()

    if inspect.iscoroutinefunction(func):

        def getter(instance):
            if instance not in cache:
                cache[instance] = asyncio.create_task(func(instance))
            return cache[instance]

    else:

        def getter(instance):
            if instance not in cache:
                cache[instance] = func(instance)
            return cache[instance]

    return property(getter)


def route_path(scope) -> str:
    """Get the route path from the request."""
    path = scope.get("path", "")
    root_path = scope.get("root_path", "")
    route_path = re.sub(r"^" + root_path, "", path)
    return route_path
