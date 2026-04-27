import re
import typing

if typing.TYPE_CHECKING:  # pragma: no cover
    from .query import SelectQuery


def _renumber_params(sql: str, offset: int) -> str:
    def replace(m: re.Match[str]) -> str:
        return f"${int(m.group(1)) + offset}"

    return re.sub(r"\$(\d+)", replace, sql)


class Exp:
    """Raw SQL fragment — passed through without escaping or parameterization."""

    def __init__(self, sql: str) -> None:
        self.sql = sql


class UnionQuery:
    def __init__(self, queries: list["SelectQuery"], *, all: bool = False) -> None:
        self._queries = queries
        self._all = all

    def build(self) -> tuple[str, list[typing.Any]]:
        all_params: list[typing.Any] = []
        parts: list[str] = []

        for q in self._queries:
            sql, params = q.build()
            sql = _renumber_params(sql, len(all_params))
            parts.append(f"({sql})")
            all_params.extend(params)

        keyword = "UNION ALL" if self._all else "UNION"
        return f" {keyword} ".join(parts), all_params

    async def fetch(self, conn: typing.Any) -> list[typing.Any]:
        sql, params = self.build()
        records = await conn.fetch(sql, *params)
        # Return raw dicts — union queries may not map cleanly to a single model
        return [dict(r.items()) for r in records]


class CTEQuery:
    def __init__(
        self,
        main: "SelectQuery",
        ctes: dict[str, "SelectQuery | UnionQuery"],
        *,
        recursive: bool = False,
    ) -> None:
        self._main = main
        self._ctes = ctes
        self._recursive = recursive

    def build(self) -> tuple[str, list[typing.Any]]:
        all_params: list[typing.Any] = []
        cte_parts: list[str] = []

        for name, subquery in self._ctes.items():
            sql, params = subquery.build()
            sql = _renumber_params(sql, len(all_params))
            cte_parts.append(f"{name} AS ({sql})")
            all_params.extend(params)

        main_sql, main_params = self._main.build()
        main_sql = _renumber_params(main_sql, len(all_params))
        all_params.extend(main_params)

        keyword = "WITH RECURSIVE" if self._recursive else "WITH"
        with_clause = ", ".join(cte_parts)
        return f"{keyword} {with_clause} {main_sql}", all_params

    async def fetch(self, conn: typing.Any) -> list[typing.Any]:
        sql, params = self.build()
        records = await conn.fetch(sql, *params)
        return [dict(r.items()) for r in records]


def union(*queries: "SelectQuery", all: bool = False) -> UnionQuery:
    return UnionQuery(list(queries), all=all)


def cte(*, main: "SelectQuery", **named_ctes: "SelectQuery | UnionQuery") -> CTEQuery:
    return CTEQuery(main, named_ctes)


def recursive_cte(*, main: "SelectQuery", **named_ctes: "SelectQuery | UnionQuery") -> CTEQuery:
    return CTEQuery(main, named_ctes, recursive=True)
