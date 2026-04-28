import typing

from .column import Condition

KNOWN_LOOKUPS: frozenset[str] = frozenset(
    {
        "eq",
        "ne",
        "gt",
        "gte",
        "lt",
        "lte",
        "like",
        "ilike",
        "in",
        "is_null",
        "is_not_null",
        "startswith",
    }
)


class Q:
    def __init__(self, *conditions: Condition | Q, **kwargs: typing.Any) -> None:
        self._conditions: list[Condition] = []
        self.children: list[Q] = []
        self.op: str = "and"

        for arg in conditions:
            if isinstance(arg, Condition):
                self._conditions.append(arg)
            elif isinstance(arg, Q):
                self.children.append(arg)

        for key, value in kwargs.items():
            parts = key.split("__")
            if len(parts) > 1 and parts[-1] in KNOWN_LOOKUPS:
                lookup = parts[-1]
                rest = parts[:-1]
            else:
                lookup = "eq"
                rest = parts

            if len(rest) == 1:
                self._conditions.append(Condition(column=rest[0], lookup=lookup, value=value))
            else:
                self._conditions.append(
                    Condition(table_alias=rest[0], column=rest[1], lookup=lookup, value=value)
                )

    @property
    def conditions(self) -> list[Condition]:
        return self._conditions

    def __and__(self, other: Q) -> Q:
        q = Q()
        q.op = "and"
        q.children = [self, other]
        return q

    def __or__(self, other: Q) -> Q:
        q = Q()
        q.op = "or"
        q.children = [self, other]
        return q

    def __invert__(self) -> Q:
        q = Q()
        q.op = "not"
        q.children = [self]
        return q
