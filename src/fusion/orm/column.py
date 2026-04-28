import typing


class Condition:
    def __init__(
        self, *, column: str, lookup: str, value: typing.Any, table_alias: str | None = None
    ) -> None:
        self.column = column
        self.lookup = lookup
        self.value = value
        self.table_alias = table_alias


class Column:
    def __init__(self, name: str, *, table: str | None = None) -> None:
        self.name = name
        self.table = table

    def __eq__(self, other: object) -> Condition:  # type: ignore[override]
        return Condition(column=self.name, lookup="eq", value=other)

    def __ne__(self, other: object) -> Condition:  # type: ignore[override]
        return Condition(column=self.name, lookup="ne", value=other)

    def __gt__(self, other: object) -> Condition:
        return Condition(column=self.name, lookup="gt", value=other)

    def __ge__(self, other: object) -> Condition:
        return Condition(column=self.name, lookup="gte", value=other)

    def __lt__(self, other: object) -> Condition:
        return Condition(column=self.name, lookup="lt", value=other)

    def __le__(self, other: object) -> Condition:
        return Condition(column=self.name, lookup="lte", value=other)

    def __hash__(self) -> int:
        return hash((self.name, self.table))
