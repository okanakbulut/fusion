from datetime import date, datetime
from textwrap import indent
from typing import (
    Any,
    ClassVar,
    Dict,
    Optional,
    Self,
    Tuple,
    Union,
)

import msgspec

__all__ = [
    "Exp",
    "Q",
    "Join",
    "LeftJoin",
    "RightJoin",
    "FullJoin",
    "CrossJoin",
    "query",
    "cte",
    "union",
    "recursive_cte",
]


def _split_lookup(lookup: str) -> tuple[str, str, str]:
    """split to table, column and lookup"""
    lookups = {
        "contains",
        "startswith",
        "endswith",
        "range",
        "in",
        "isnull",
        "gte",
        "lte",
        "gt",
        "lt",
    }
    parts = lookup.split("__")
    if len(parts) == 1:  # just a column name
        return "", parts[0], ""
    elif len(parts) == 2 and parts[1] in lookups:  # column and lookup
        return "", parts[0], parts[1]
    elif len(parts) == 2:  # table and column
        return parts[0], parts[1], ""
    elif len(parts) == 3:  # table, column and lookup
        return parts[0], parts[1], parts[2]
    else:
        raise ValueError(f"Invalid lookup: {lookup}")


class Exp(msgspec.Struct, frozen=True):  # type: ignore[call-arg]
    """
    Represents an expression in a query, similar to Django's F object.

    An instance of this class can be used to reference a column in a table
    within a query. This is useful for performing operations on the field
    itself, such as comparisons or arithmetic operations.

    The expression is specified as a string argument when creating an instance
    of the Exp class.

    Example:
        Exp('column_name')
        Exp('table.column_name + 1')
    """

    expression: str


# type alias for Q arguments
_Q = Union[Tuple[str, "Q", "Q"], Tuple[str, "Q"], "Q"]


class Q:
    """
    A query condition that can be used to build a SQL query.
    """

    args: tuple[_Q, ...]
    kwargs: Dict[str, Any]

    __match_args__ = ("args", "kwargs")

    def __init__(self, *args: _Q, **kwargs: Any):
        """
        Query condition constructor that can be used to build a SQL query.

        Args:
            *args: positional arguments that can be another Q instance
            **kwargs: keyword arguments in a form of `[table__]field[__lookup]=value`

        Examples:
            >>> Q(user__name="John") | Q(user__name__startswith="Jane")
            ("user"."name" = $1 OR "user"."name" LIKE $2 || '%')
        """
        self.args = args or ()
        self.kwargs = kwargs or {}

    def __or__(self, other: "Q") -> "Q":
        return Q(("OR", self, other))

    def __and__(self, other: "Q") -> "Q":
        return Q(("AND", self, other))

    def __invert__(self) -> "Q":
        return Q(("NOT", self))

    def _render(self, values: Optional[list] = None) -> str:
        """
        Render the where condition into a SQL string.

        Args:
            values: Optional list of values to be used as placeholders in the SQL string.

        Returns:
            The rendered WHERE condition.

        Raises:
            ValueError: If an invalid argument or unsupported lookup is encountered.
        """
        values = [] if values is None else values

        def placeholder(value: Any):
            match value:
                case Query() as q:
                    subquery, _ = q.sql(values)
                    subquery = indent(subquery, "  ")
                    return f"(\n{subquery}\n)"
                case Exp() as exp:
                    return exp.expression
                case _:
                    values.append(value)
                    return f"${len(values)}"

        # evaluate positional arguments (Q instances)
        where_clauses: list[str] = []
        for arg in self.args:
            match arg:
                case ("OR", Q() as left, Q() as right):
                    where_clauses.append(f"({left._render(values)} OR {right._render(values)})")
                case ("AND", Q() as left, Q() as right):
                    where_clauses.append(f"({left._render(values)} AND {right._render(values)})")
                case ("NOT", Q() as q):
                    where_clauses.append(f"NOT ({q._render(values)})")
                case _:
                    raise ValueError(f"Invalid argument: {arg}")
        # evaluate keyword arguments (field lookups)
        for column, value in self.kwargs.items():
            table, column, lookup = _split_lookup(column)
            if table:
                column = f'"{table}"."{column}"'
            else:
                column = f'"{column}"'

            if not lookup:
                where_clauses.append(f"{column} = {placeholder(value)}")
                continue

            match lookup:
                case "contains":
                    where_clauses.append(f"{column} LIKE '%' || {placeholder(value)} || '%'")
                case "startswith":
                    where_clauses.append(f"{column} LIKE {placeholder(value)} || '%'")
                case "endswith":
                    where_clauses.append(f"{column} LIKE '%' || {placeholder(value)}")
                case "range":
                    start, end = value
                    where_clauses.append(
                        f"{column} BETWEEN {placeholder(start)} AND {placeholder(end)}"
                    )
                case "in":
                    if isinstance(value, Query):
                        where_clauses.append(f"{column} IN {placeholder(value)}")
                        continue

                    if not isinstance(value, (list, tuple, set)):
                        raise ValueError(f"Unsupported value for 'in' lookup: {value}")

                    item = value[0] if isinstance(value, (list, tuple)) else list(value)[0]
                    match item:  # to determine the type of the list
                        case int():
                            typ = "int"
                        case str():
                            typ = "text"
                        case float():
                            typ = "float"
                        case datetime():
                            typ = "timestamptz"
                        case date():
                            typ = "date"
                        case _:
                            raise ValueError(f"Unsupported value for 'in' lookup: {value}")
                    # we need to cast the list to the correct type for postgres
                    where_clauses.append(f"{column} = any({placeholder(value)}::{typ}[])")
                case "isnull":
                    if value:
                        where_clauses.append(f"{column} IS NULL")
                    else:
                        where_clauses.append(f"{column} IS NOT NULL")
                case "gte":
                    where_clauses.append(f"{column} >= {placeholder(value)}")
                case "lte":
                    where_clauses.append(f"{column} <= {placeholder(value)}")
                case "gt":
                    where_clauses.append(f"{column} > {placeholder(value)}")
                case "lt":
                    where_clauses.append(f"{column} < {placeholder(value)}")
                case _:
                    raise ValueError(f"Unsupported lookup: {lookup}")
                # TODO: add support for other operators like:
                #  __year, __year__gte, __year__lte etc. for date fields
        return " AND ".join(where_clauses)

    def __repr__(self) -> str:
        return self._render()



class Join(msgspec.Struct, frozen=True):  # type: ignore[call-arg]
    ""

    __join_type__: ClassVar[str] = "INNER"
    to: Union["Query", str]
    on: Optional[Q] = None
    using: Optional[str] = None

    def __post_init__(self):
        if self.on and self.using:
            raise ValueError("Cannot specify both 'on' and 'using' for a join condition")

        if not self.on and not self.using:
            raise ValueError("Missing join condition")

    def _render(self, alias: str, values: Optional[list] = None) -> str:
        values = [] if values is None else values
        match self.to:
            case str() as table:
                source = table
            case Query() as q:
                subquery, _ = q.sql(values)
                subquery = indent(subquery, "  ")
                source = f"(\n{subquery}\n)"
            case _:  # type[Model]
                raise ValueError("Invalid source for join")

        condition = f"ON ({self.on._render(values)})" if self.on else f" USING ({self.using})"
        return f'{self.__join_type__} JOIN {source} AS "{alias}" {condition}'


class LeftJoin(Join, frozen=True):  # type: ignore[call-arg]
    ""

    __join_type__ = "LEFT"


class RightJoin(Join, frozen=True):  # type: ignore[call-arg]
    ""

    __join_type__ = "RIGHT"


class FullJoin(Join, frozen=True):  # type: ignore[call-arg]
    ""

    __join_type__ = "FULL"


class CrossJoin(Join, frozen=True):  # type: ignore[call-arg]
    ""

    __join_type__ = "CROSS"


# DataSource is a type alias that can be either a model, a join or a query
DataSource = Union[Join, "Query", str]


class Query(msgspec.Struct, frozen=True):  # type: ignore[call-arg]
    "SQL query builder."

    sources: tuple[tuple[str, DataSource]]
    selections: tuple[Union[str, tuple[str, str]], ...] = msgspec.field(default_factory=tuple)
    conditions: tuple[Q, ...] = msgspec.field(default_factory=tuple)
    groupbys: tuple[str, ...] = msgspec.field(default_factory=tuple)
    orderbys: tuple[str, ...] = msgspec.field(default_factory=tuple)
    _limit: Optional[int] = None
    _offset: Optional[int] = None
    _distinct: Optional[str] = None

    def source(self, **sources: DataSource) -> Self:
        """
        Add a data source to the query.

        Args:
            **sources: Keyword arguments in a form of `alias=data_source`

        Returns:
            Query: New query object with the added data source.
        """
        return msgspec.structs.replace(self, sources=self.sources + tuple(sources.items()))  # type: ignore[no-any-return]

    def select(self, *args: str, **kwargs: Any) -> Self:
        """
        Add selections to the query.

        Args:
            *args: Variable number of string arguments representing column names.
            **kwargs: Keyword arguments in the form of `alias=column`.

        Returns:
            Query: New query object with the added selections.
        """
        selections = (
            self.selections
            + tuple(args)
            + tuple((alias, column) for alias, column in kwargs.items())
        )
        return msgspec.structs.replace(self, selections=selections)  # type: ignore[no-any-return]

    def where(self, *args: Q, **kwargs: Any) -> Self:
        """
        Add conditions to the query.

        Args:
            *args: Variable number of Q objects representing predefined conditions.
            **kwargs: Keyword arguments representing conditions in the form of
                `[table__]column[__lookup]=value`.

        Returns:
            Query: New query object with the added conditions.
        """
        conditions = self.conditions + tuple(args) + tuple(Q(**{k: v}) for k, v in kwargs.items())
        return msgspec.structs.replace(self, conditions=conditions)  # type: ignore[no-any-return]

    def group_by(self, *groups: str) -> Self:
        """
        Add columns to the GROUP BY clause.

        Args:
            *groups: Variable number of string arguments representing column names.

        Returns:
            Query: New query object with the added columns in the GROUP BY clause.
        """
        return msgspec.structs.replace(self, groupbys=self.groupbys + tuple(groups))  # type: ignore[no-any-return]

    def order_by(self, *orderbys: str) -> Self:
        """
        Add columns to the ORDER BY clause.

        Args:
            *orderbys: Variable number of string arguments representing column names.

        Returns:
            Query: New query object with the added columns in the ORDER BY clause.
        """
        return msgspec.structs.replace(self, orderbys=self.orderbys + tuple(orderbys))  # type: ignore[no-any-return]

    def __getitem__(self, key: slice) -> Self:
        """
        Slicing query object with [start:stop] syntax.

        Example:
            >>> query(u='user')[10:30]
            SELECT *
            FROM user AS "u"
            LIMIT $1
            OFFSET $2

            >>> query(u='user')[10:]
            SELECT *
            FROM user AS "u"
            OFFSET $1

            >>> query(u='user')[:10]
            SELECT *
            FROM user AS "u"
            LIMIT $1

        Invalid examples:
            >>> query(u='user')[10:30:2]
            Traceback (most recent call last):
                ...
            ValueError: Step is not supported

            >>> query(u='user')[10]
            Traceback (most recent call last):
                ...
            TypeError: Invalid index type
        """
        if not isinstance(key, slice):
            raise TypeError("Invalid index type")

        if key.step is not None:
            raise ValueError("Step is not supported")

        query = self
        if key.start is not None:
            query = query.offset(int(key.start))

        if key.stop is not None:
            limit = int(key.stop) - int(key.start) if key.start else int(key.stop)
            query = query.limit(limit)
        return query

    def limit(self, limit: int) -> Self:
        """
        Set the LIMIT clause for the query.

        Args:
            limit (int): The maximum number of rows to be returned.

        Returns:
            Query: New query object with the added LIMIT clauses.

        Raises:
            ValueError: If the limit is not a positive integer.

        Examples:
            >>> class User(Model): ...
            >>> query(u=User).limit(10)
            SELECT *
            FROM public.user AS "u"
            LIMIT $1

            >>> query(u=User).limit(-1)
            Traceback (most recent call last):
                ...
            ValueError: Limit must be a positive integer
        """

        if limit < 0:
            raise ValueError("Limit must be a positive integer")

        return msgspec.structs.replace(self, _limit=limit)  # type: ignore[no-any-return]

    def offset(self, offset: int) -> Self:
        """
        Set the OFFSET clause for the query.

        Args:
            offset (int): The number of rows to skip.

        Returns:
            Query: New query object with the added OFFSET clauses.

        Raises:
            ValueError: If the offset is not a positive integer.

        Examples:
            >>> class User(Model): ...
            >>> query(u=User).offset(10)
            SELECT *
            FROM public.user AS "u"
            OFFSET $1

            >>> query(u=User).offset(-1)
            Traceback (most recent call last):
                ...
            ValueError: Offset must be a positive integer
        """

        if offset < 0:
            raise ValueError("Offset must be a positive integer")

        return msgspec.structs.replace(self, _offset=offset)  # type: ignore[no-any-return]

    def distinct(self, on: Optional[str] = "") -> Self:
        """
        Add the DISTINCT clause to the query.

        Args:
            on (Optional[str]): The column name to apply the DISTINCT clause to.

        Returns:
            Query: New query object with the added DISTINCT clause.

        Examples:
            >>> class User(Model): ...
            >>> query(u=User).distinct()
            SELECT DISTINCT *
            FROM public.user AS "u"

            >>> query(u=User).distinct('name')
            SELECT DISTINCT ON(name) *
            FROM public.user AS "u"
        """
        return msgspec.structs.replace(self, _distinct=on)  # type: ignore[no-any-return]

    def sql(self, values: Optional[list] = None) -> tuple[str, list[Any]]:
        """
        Generate a SQL query based on the current state of the Query object.

        Args:
            values (Optional[list]): List of parameter values to be used in the query.

        Returns:
            tuple[str, list[Any]]: A tuple containing the generated SQL query
            and the list of parameter values.
        """
        # TODO: add indentation to the generated SQL query for better readability
        values = [] if values is None else values
        # Start with the SELECT clause
        selections: list[str] = []
        for selection in self.selections or ("*",):
            match selection:
                case str() as column:
                    selections.append(column)
                case (str() as alias, str() as column):
                    selections.append(f'{column} "{alias}"')
                case _:
                    raise ValueError(f"Invalid selection: {selection}")

        if self._distinct is None:
            select = "SELECT "
        elif self._distinct == "":
            select = "SELECT DISTINCT "
        else:
            select = f"SELECT DISTINCT ON({self._distinct}) "

        select_clause = select + ", ".join(selections)
        # build the FROM clause
        # first, add table names than add subqueries and joins
        tables, joins, subqueries = [], [], []
        for alias, source in self.sources:
            match source:
                case Join() as join:
                    join_text = f"\n{join._render(alias, values)}"
                    joins.append(indent(join_text, "  "))
                case Query() as q:
                    query, _ = q.sql(values)
                    query = indent(query, "  ")
                    subquery = f'(\n{query}\n) AS "{alias}"'
                    subqueries.append(subquery)
                case str() as table:
                    tables.append(f'{table} AS "{alias}"')
                case _:  # type[Model]
                    tables.append(f'{source.tablename()} AS "{alias}"')
        from_clause = f"\nFROM {', '.join(tables)}{'\n'.join(subqueries + joins)}"
        # build the WHERE clause
        where_clause = ""
        if self.conditions:
            where_clause = "\nWHERE " + " AND ".join(
                condition._render(values=values) for condition in self.conditions
            )

        # build the GROUP BY clause
        groupby_clause = "\nGROUP BY " + ", ".join(self.groupbys) if self.groupbys else ""
        # build the ORDER BY clause
        orderby_clause = ""
        if self.orderbys:
            orders = []
            for orderby in self.orderbys:
                if orderby.startswith("-"):
                    orders.append(f"{orderby[1:]} DESC")
                else:
                    orders.append(orderby)
            orderby_clause = "\nORDER BY " + ", ".join(orders)

        limit_clause = ""
        if self._limit is not None:
            values.append(self._limit)
            limit_clause = f"\nLIMIT ${len(values)}"

        # limit_clause = f"\nLIMIT {self.limit}" if self.limit is not None else ""
        offset_clause = ""
        if self._offset is not None:
            values.append(self._offset)
            offset_clause = f"\nOFFSET ${len(values)}"
        # offset_clause = f"\nOFFSET {self.offset}" if self.offset is not None else ""
        # Combine all the clauses into a SQL query
        query = (
            f"{select_clause}{from_clause}{where_clause}{groupby_clause}{orderby_clause}"
            f"{limit_clause}{offset_clause}"
        )
        # Return the query and list of parameters
        return query, values

    def __repr__(self) -> str:
        query, _ = self.sql()
        return query


def query(**sources: DataSource) -> Query:
    """
    Query function to create a new query object.

    Args:
        **sources: Data sources to be included in the query.

    Returns:
        Query: The created query object.

    Raises:
        ValueError: If no data source is provided.
    """
    if not sources:
        raise ValueError("At least one data source is required")
    return Query(sources=tuple(sources.items()))  # type: ignore[arg-type]


class With(msgspec.Struct, frozen=True):  # type: ignore[call-arg]
    """
    Non-recursive common table expression (CTE) with the given main query and additional CTEs.
    """

    main: Query
    ctes: tuple[tuple[str, Query]]

    def sql(self, values: Optional[list] = None) -> tuple[str, list[Any]]:
        """Generate the CTE query."""

        values = [] if values is None else values
        ctes = []
        for alias, q in self.ctes:
            query, _ = q.sql(values)
            query = indent(query, "  ")
            ctes.append(f'"{alias}" AS (\n{query}\n)')
        ctes_clause = ",\n".join(ctes)
        main_query, _ = self.main.sql(values)
        return f"WITH {ctes_clause}\n{main_query}", values

    def __repr__(self) -> str:
        query, _ = self.sql()
        return query


def cte(main: Query, **ctes: Query) -> With:
    """
    Create a common table expression (CTE) with the given main query and additional CTEs.

    Args:
        main (Query): The main query.
        **ctes (Query): Additional CTEs.

    Returns:
        With: The created CTE.

    Raises:
        ValueError: If no CTE is provided.

    Examples:
        >>> class User(Model): ...
        >>> cte(
        ...   main=query(cte='user_cte').select('cte.id','cte.name'),
        ...   user_cte=query(u=User).where(u__org_id=1)
        ... )
        WITH "user_cte" AS (
          SELECT *
          FROM public.user AS "u"
          WHERE "u"."org_id" = $1
        )
        SELECT cte.id, cte.name
        FROM user_cte AS "cte"
    """
    if not ctes:
        raise ValueError("At least one CTE must be provided")
    return With(main, ctes=tuple(ctes.items()))  # type: ignore[arg-type]


class QueryUnion(msgspec.Struct, frozen=True):  # type: ignore[call-arg]
    """
    Represents a UNION query.
    """

    queries: tuple[Query]
    all: Optional[bool] = False

    def sql(self, values: Optional[list] = None) -> tuple[str, list[Any]]:
        """Generate the SQL query for the QueryUnion object."""
        values = [] if values is None else values
        union = "\nUNION ALL\n" if self.all else "\nUNION\n"
        queries = []
        for q in self.queries:
            query, _ = q.sql(values)
            query = indent(query, "  ")
            queries.append(query)
        return union.join(queries), values

    def __repr__(self) -> str:
        query, _ = self.sql()
        return query


def union(*queries: Query, all: bool = False) -> QueryUnion:
    """
    Create a UNION query with the given queries.

    Args:
        *queries (Query): Variable number of queries.
        all (bool): If True, the UNION ALL operator is used, otherwise UNION is used.

    Returns:
        QueryUnion: The created QueryUnion object.

    Examples:
        >>> class User(Model): ...
        >>> union(
        ...     query(u=User).select("u.id", "u.name").where(u__org_id=123),
        ...     query(u=User).select("u.id", "u.name").where(u__org_id=247),
        ... )
          SELECT u.id, u.name
          FROM public.user AS "u"
          WHERE "u"."org_id" = $1
        UNION
          SELECT u.id, u.name
          FROM public.user AS "u"
          WHERE "u"."org_id" = $2
    """
    if len(queries) < 2:
        raise ValueError("At least two queries must be provided")
    return QueryUnion(tuple(queries), all)  # type: ignore[arg-type]


class RecursiveWith(msgspec.Struct, frozen=True):  # type: ignore[call-arg]
    """
    Recursive common table expression (CTE) with the given main query and base union query.
    """

    main: Query
    base: QueryUnion
    alias: str

    def sql(self, values: Optional[list] = None) -> tuple[str, list[Any]]:
        """Generate recursive CTE query"""
        values = [] if values is None else values
        base_query, _ = self.base.sql(values)
        base_query = indent(base_query, "  ")
        main_query, _ = self.main.sql(values)
        return f'WITH RECURSIVE "{self.alias}" AS (\n{base_query}\n)\n{main_query}', values

    def __repr__(self) -> str:
        query, _ = self.sql()
        return query


def recursive_cte(main: Query, **kwargs: QueryUnion) -> RecursiveWith:
    """
    Create a recursive common table expression (CTE) with the given main query and union query.

    Args:
        main (Query): The main query.
        **kwargs (QueryUnion): Union query.

    Returns:
        RecursiveWith: The created recursive CTE.

    Raises:
        ValueError: If provide union query count is not exactly one.

    Examples:
        >>> recursive_cte(
        ...     main=query(tree="search_tree").select("tree.id", "tree.link", "tree.data"),
        ...     search_tree=QueryUnion(
        ...         queries=(
        ...             query(t="tree").select("t.id", "t.link", "t.data"),
        ...             query(t="tree", st="search_tree")
        ...             .select("t.id", "t.link", "t.data")
        ...             .where(t__id=Exp('"st"."link"')),
        ...         ),
        ...         all=True,
        ...     ),
        ... )
        WITH RECURSIVE "search_tree" AS (
            SELECT t.id, t.link, t.data
            FROM tree AS "t"
          UNION ALL
            SELECT t.id, t.link, t.data
            FROM tree AS "t", search_tree AS "st"
            WHERE "t"."id" = "st"."link"
        )
        SELECT tree.id, tree.link, tree.data
        FROM search_tree AS "tree"
    """
    if len(kwargs) != 1:
        raise ValueError("Exactly one union query must be provided")
    alias, base = next(iter(kwargs.items()))
    return RecursiveWith(main, base, alias)


if __name__ == "__main__":
    import doctest

    doctest.testmod(verbose=True, raise_on_error=True)
