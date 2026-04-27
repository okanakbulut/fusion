import typing

from .column import Condition


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
            parts = key.split("__", 1)
            column = parts[0]
            lookup = parts[1] if len(parts) > 1 else "eq"
            self._conditions.append(Condition(column=column, lookup=lookup, value=value))

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
