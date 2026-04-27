import typing


class UniqueConstraint:
    def __init__(self, *columns: str) -> None:
        self.columns = columns


class ForeignKey:
    def __init__(self, column: str, target: type, *, target_column: str = "id") -> None:
        self.column = column
        self.target = target
        self.target_column = target_column


class Index:
    def __init__(self, *columns: str, method: str | None = None) -> None:
        self.columns = columns
        self.method = method
