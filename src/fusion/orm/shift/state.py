from __future__ import annotations

from dataclasses import dataclass, field


class SchemaError(Exception):
    pass


@dataclass
class ColumnState:
    type: str
    nullable: bool
    default: str | None
    primary_key: bool = False


@dataclass
class TableState:
    columns: dict[str, ColumnState] = field(default_factory=dict)
    constraints: list[dict] = field(default_factory=list)
    indexes: list[dict] = field(default_factory=list)
    schema: str | None = None


@dataclass
class SchemaState:
    tables: dict[str, TableState] = field(default_factory=dict)
    extensions: set[str] = field(default_factory=set)
    schemas: set[str] = field(default_factory=set)
