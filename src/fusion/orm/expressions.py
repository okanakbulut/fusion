import typing

# if typing.TYPE_CHECKING:  # pragma: no cover
#     from .query import SelectQuery
from .query import DeleteQuery, InsertQuery, SelectQuery, UpdateQuery


class Exp:
    """Raw SQL fragment — passed through without escaping or parameterization."""

    def __init__(self, sql: str) -> None:
        self.sql = sql


class UnionQuery:
    def __init__(self, queries: list[SelectQuery], *, all: bool = False) -> None:
        self._queries = queries
        self._all = all

    def build(self, params: list[typing.Any] | None = None) -> tuple[str, list[typing.Any]]:
        if params is None:
            params = []
        parts: list[str] = []

        for q in self._queries:
            sql, _ = q.build(params)
            parts.append(f"({sql})")

        keyword = "UNION ALL" if self._all else "UNION"
        return f" {keyword} ".join(parts), params

    async def fetch(self, conn: typing.Any) -> list[typing.Any]:
        sql, params = self.build()
        records = await conn.fetch(sql, *params)
        # Return raw dicts — union queries may not map cleanly to a single model
        return [dict(r.items()) for r in records]


type AnyQuery = SelectQuery | InsertQuery | UpdateQuery | DeleteQuery | UnionQuery | CTEQuery


class CTEQuery:
    def __init__(
        self,
        main: AnyQuery,
        ctes: dict[str, AnyQuery],
        *,
        recursive: bool = False,
    ) -> None:
        self._main = main
        self._ctes = ctes
        self._recursive = recursive

    def build(self, params: list[typing.Any] | None = None) -> tuple[str, list[typing.Any]]:
        if params is None:
            params = []
        cte_parts: list[str] = []

        for name, subquery in self._ctes.items():
            sql, _ = subquery.build(params)
            cte_parts.append(f"{name} AS ({sql})")

        main_sql, _ = self._main.build(params)

        keyword = "WITH RECURSIVE" if self._recursive else "WITH"
        with_clause = ", ".join(cte_parts)
        return f"{keyword} {with_clause} {main_sql}", params

    async def fetch(self, conn: typing.Any) -> list[typing.Any]:
        sql, params = self.build()
        records = await conn.fetch(sql, *params)
        return [dict(r.items()) for r in records]


def union(*queries: SelectQuery, all: bool = False) -> UnionQuery:
    return UnionQuery(list(queries), all=all)


def cte(*, main: AnyQuery, **named_ctes: AnyQuery) -> CTEQuery:
    return CTEQuery(main, named_ctes)


def recursive_cte(*, main: AnyQuery, **named_ctes: AnyQuery) -> CTEQuery:
    return CTEQuery(main, named_ctes, recursive=True)
