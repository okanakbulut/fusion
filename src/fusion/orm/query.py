import typing
from typing import Self

import pypika
from pypika import Order, Parameter, Table
from pypika import functions as fn
from pypika.dialects import PostgreSQLQuery
from pypika.terms import LiteralValue, ValueWrapper

from .column import Condition
from .conditions import Q
from .fields import _DbNow, _DbUuid

if typing.TYPE_CHECKING:  # pragma: no cover
    from .model import Model

_SENTINEL_TYPES = (_DbNow, _DbUuid)

_OnArg = tuple[typing.Any, typing.Any] | list[tuple[typing.Any, typing.Any]] | None

_JOIN_METHODS = {
    "inner": "join",
    "left": "left_join",
    "right": "right_join",
    "outer": "outer_join",
}

LOOKUP_OPS = {
    "eq": lambda col, p: col == p,
    "ne": lambda col, p: col != p,
    "gt": lambda col, p: col > p,
    "gte": lambda col, p: col >= p,
    "lt": lambda col, p: col < p,
    "lte": lambda col, p: col <= p,
    "like": lambda col, p: col.like(p),
    "ilike": lambda col, p: col.ilike(p),
    "in": lambda col, p: col.isin(p),
    "is_null": lambda col, _p: col.isnull(),
    "is_not_null": lambda col, _p: col.isnotnull(),
    "startswith": lambda col, p: col.like(p),
}


def _build_criterion(
    conditions: list[Condition],
    table: Table,
    params: list[typing.Any],
    alias_map: dict[str, Table] | None = None,
) -> pypika.Criterion | None:
    criteria: list[pypika.Criterion] = []
    for cond in conditions:
        if cond.table_alias:
            if not alias_map or cond.table_alias not in alias_map:
                raise ValueError(f"Unknown join alias: {cond.table_alias!r}")
            col = alias_map[cond.table_alias][cond.column]
        else:
            col = table[cond.column]
        op = LOOKUP_OPS.get(cond.lookup)
        if op is None:
            raise ValueError(f"Unknown lookup: {cond.lookup!r}")

        if cond.lookup in ("is_null", "is_not_null"):
            criteria.append(op(col, None))
        else:
            idx = len(params) + 1
            if cond.lookup == "in":
                params.append(list(cond.value))
                criteria.append(col.isin(Parameter(f"${idx}")))
            elif cond.lookup == "startswith":
                params.append(f"{cond.value}%")
                criteria.append(op(col, Parameter(f"${idx}")))
            else:
                params.append(cond.value)
                criteria.append(op(col, Parameter(f"${idx}")))

    if not criteria:
        return None
    result = criteria[0]
    for c in criteria[1:]:
        result = result & c
    return result


def _q_to_criterion(
    q: Q,
    table: Table,
    params: list[typing.Any],
    alias_map: dict[str, Table] | None = None,
) -> pypika.Criterion | None:
    if q.children:
        child_criteria = [_q_to_criterion(c, table, params, alias_map) for c in q.children]
        child_criteria = [c for c in child_criteria if c is not None]
        if not child_criteria:
            return None
        if q.op == "not":
            return child_criteria[0].negate()
        result = child_criteria[0]
        for c in child_criteria[1:]:
            if q.op == "or":
                result = result | c
            else:
                result = result & c
        return result
    return _build_criterion(q.conditions, table, params, alias_map)


def _where_arg_to_criterion(
    arg: Q | Condition,
    table: Table,
    params: list[typing.Any],
    alias_map: dict[str, Table] | None = None,
) -> pypika.Criterion | None:
    if isinstance(arg, Q):
        return _q_to_criterion(arg, table, params, alias_map)
    if isinstance(arg, Condition):
        return _build_criterion([arg], table, params, alias_map)
    return None


def _render_val(val: typing.Any, params: list) -> typing.Any:
    """Render an expression as a pypika term, wrapping scalars in ValueWrapper for SELECT."""
    if hasattr(val, "build"):
        sub_sql, _ = val.build(params)
        # SQL functions (e.g. Coalesce) render inline; subqueries need wrapping parens
        if getattr(val, "_is_sql_function", False):
            return LiteralValue(sub_sql)
        return LiteralValue(f"({sub_sql})")
    # scalar — wrap in ValueWrapper so the alias is rendered correctly in SELECT
    idx = len(params) + 1
    params.append(val)
    return ValueWrapper(Parameter(f"${idx}"))


def _render_insert_term(val: typing.Any, params: list) -> typing.Any:
    """Render an expression as a pypika term suitable for INSERT VALUES."""
    if hasattr(val, "build"):
        sub_sql, _ = val.build(params)
        if getattr(val, "_is_sql_function", False):
            return LiteralValue(sub_sql)
        return LiteralValue(f"({sub_sql})")
    # scalar — bare Parameter (no ValueWrapper) for INSERT
    idx = len(params) + 1
    params.append(val)
    return Parameter(f"${idx}")


class Query:
    def __init__(
        self,
        model: type | None = None,
        **exprs: typing.Any,
    ) -> None:
        self._model = model
        self._exprs = exprs
        self._wheres: list = []
        self._order: list = []
        self._groups: list = []
        self._limit_val: int | None = None
        self._offset_val: int | None = None
        self._joins: list = []

    def order_by(self, column: str, *, desc: bool = False) -> Self:
        new_q = self.__class__.__new__(self.__class__)
        new_q.__dict__ = {**self.__dict__, "_order": list(self._order)}
        new_q._order.append((column, desc))
        return new_q

    def group_by(self, *columns: str) -> Self:
        new_q = self.__class__.__new__(self.__class__)
        new_q.__dict__ = {**self.__dict__, "_groups": list(self._groups) + list(columns)}
        return new_q

    def limit(self, n: int) -> Self:
        new_q = self.__class__.__new__(self.__class__)
        new_q.__dict__ = {**self.__dict__}
        new_q._limit_val = n
        return new_q

    def offset(self, n: int) -> Self:
        new_q = self.__class__.__new__(self.__class__)
        new_q.__dict__ = {**self.__dict__}
        new_q._offset_val = n
        return new_q

    def where(self, *args: typing.Any, **kwargs: typing.Any) -> Self:
        new_q = self.__class__.__new__(self.__class__)
        new_q.__dict__ = {**self.__dict__, "_wheres": list(self._wheres)}
        for arg in args:
            new_q._wheres.append(arg)
        if kwargs:
            new_q._wheres.append(Q(**kwargs))
        return new_q

    def join(
        self,
        *,
        on: tuple | list | None = None,
        how: str = "inner",
        **target: typing.Any,
    ) -> Self:
        if len(target) != 1:
            raise ValueError("join() requires exactly one keyword argument: alias=model_or_query")
        alias, join_target = next(iter(target.items()))
        new_q = self.__class__.__new__(self.__class__)
        new_q.__dict__ = {**self.__dict__, "_joins": list(self._joins)}
        new_q._joins.append((alias, join_target, on, how))
        return new_q

    def build(self, params: list[typing.Any] | None = None) -> tuple[str, list[typing.Any]]:
        if params is None:
            params = []

        terms = []
        for alias, expr in self._exprs.items():
            term = _render_val(expr, params)
            terms.append(term.as_(alias))

        q = PostgreSQLQuery.select(*terms)

        subquery_joins: list[tuple[str, typing.Any, tuple | list | None, str]] = []

        if self._model is not None:
            table = _make_table(self._model)
            q = q.from_(table)

            alias_map: dict[str, Table] = {}
            for join_alias, join_target, on_arg, how in self._joins:
                join_fn_name = _JOIN_METHODS.get(how, "join")

                if hasattr(join_target, "build"):
                    subquery_joins.append((join_alias, join_target, on_arg, how))
                else:
                    join_table = _make_table(join_target)
                    alias_map[join_alias] = join_table
                    on_clause = _build_explicit_on(on_arg, table, join_table) or _infer_join_on(
                        self._model, join_target, table, join_table
                    )
                    join_fn = getattr(q, join_fn_name)
                    if on_clause is not None:
                        q = join_fn(join_table).on(on_clause)
                    else:
                        q = join_fn(join_table).on(LiteralValue("true"))

            for where_arg in self._wheres:
                criterion = _where_arg_to_criterion(where_arg, table, params, alias_map)
                if criterion is not None:
                    q = q.where(criterion)

            for col, is_desc in self._order:
                order = Order.desc if is_desc else Order.asc
                q = q.orderby(table[col], order=order)

            for col in self._groups:
                q = q.groupby(table[col])

        if self._limit_val is not None:
            q = q.limit(self._limit_val)
        if self._offset_val is not None:
            q = q.offset(self._offset_val)

        sql = q.get_sql(as_keyword=True)

        _JOIN_KEYWORDS = {
            "inner": "JOIN",
            "left": "LEFT JOIN",
            "right": "RIGHT JOIN",
            "outer": "FULL OUTER JOIN",
        }
        for join_alias, join_target, on_arg, how in subquery_joins:
            sub_sql, _ = join_target.build(params)
            join_kw = _JOIN_KEYWORDS.get(how, "JOIN")
            assert on_arg is not None, "subquery join requires an explicit on= pair"
            left_col, right_col = on_arg
            lp, rp = left_col.split("."), right_col.split(".")
            on_str = f'"{lp[0]}"."{lp[1]}"="{rp[0]}"."{rp[1]}"'
            sql += f' {join_kw} ({sub_sql}) "{join_alias}" ON {on_str}'

        return sql, params


class SelectQuery(Query):
    def __init__(
        self,
        model: type[Model] | None = None,
        /,
        *columns: str,
        **exprs: typing.Any,
    ) -> None:
        super().__init__(model, **exprs)
        self._columns = columns
        self._raw_wheres: list[str] = []
        self._prefetches: list[typing.Any] = []

    def where_raw(self, exp: typing.Any) -> SelectQuery:
        from .expressions import Exp

        q = SelectQuery.__new__(SelectQuery)
        q.__dict__ = {**self.__dict__, "_raw_wheres": list(self._raw_wheres)}
        if isinstance(exp, Exp):
            q._raw_wheres.append(exp.sql)
        return q

    def join(
        self,
        model: type[Model] | None = None,
        *,
        on: _OnArg = None,
        how: str = "inner",
        **alias_kwargs: type[Model],
    ) -> SelectQuery:
        if model is not None:
            alias = model.__name__.lower()
            target: type[Model] = model
        elif len(alias_kwargs) == 1:
            alias, target = next(iter(alias_kwargs.items()))
        else:
            raise ValueError("join() requires a positional model or exactly one alias kwarg")
        q = SelectQuery.__new__(SelectQuery)
        q.__dict__ = {**self.__dict__, "_joins": list(self._joins)}
        q._joins.append((target, alias, on, how))
        return q

    def prefetch(self, *models: type[Model]) -> SelectQuery:
        q = SelectQuery.__new__(SelectQuery)
        q.__dict__ = {**self.__dict__, "_prefetches": list(self._prefetches) + list(models)}
        return q

    def _build_base_query(
        self,
        table: Table,
        terms: list[typing.Any],
    ) -> typing.Any:
        import msgspec.structs

        if terms:
            q = PostgreSQLQuery.from_(table).select(*terms)
        elif self._prefetches:
            # pypika silently drops additional column selects when SELECT * is used,
            # so list the main table's columns explicitly whenever prefetches are present.
            assert self._model is not None
            rel_fields = getattr(self._model, "__relationship_fields__", frozenset())
            main_cols = [
                table[f.name]
                for f in msgspec.structs.fields(self._model)  # type: ignore[arg-type]
                if f.name not in rel_fields
            ]
            q = PostgreSQLQuery.from_(table).select(*main_cols)
        else:
            q = PostgreSQLQuery.from_(table).select("*")
        return q

    def _apply_model_clauses(
        self,
        q: typing.Any,
        table: Table,
        params: list[typing.Any],
    ) -> typing.Any:
        import msgspec.structs

        assert self._model is not None

        alias_map: dict[str, Table] = {}
        for join_model, alias, on_arg, how in self._joins:
            join_table = _make_table(join_model)
            alias_map[alias] = join_table
            on_clause = _build_explicit_on(on_arg, table, join_table) or _infer_join_on(
                self._model, join_model, table, join_table
            )
            join_fn = getattr(q, _JOIN_METHODS.get(how, "join"))
            if on_clause is not None:
                q = join_fn(join_table).on(on_clause)
            else:
                q = join_fn(join_table).on(LiteralValue("true"))  # type: ignore[arg-type]

        already_joined = {jm for jm, _, _, _ in self._joins}
        for prefetch_model in self._prefetches:
            rel_field, fk_constraint = _find_prefetch_relation(self._model, prefetch_model)
            join_table = _make_table(prefetch_model)
            if prefetch_model not in already_joined:
                on_clause = table[fk_constraint.column] == join_table[fk_constraint.target_column]
                q = q.left_join(join_table).on(on_clause)
            prefetch_rel_fields = getattr(prefetch_model, "__relationship_fields__", frozenset())
            for sf in msgspec.structs.fields(prefetch_model):  # type: ignore[arg-type]
                if sf.name not in prefetch_rel_fields:
                    q = q.select(join_table[sf.name].as_(f"{rel_field}__{sf.name}"))

        for where_arg in self._wheres:
            criterion = _where_arg_to_criterion(where_arg, table, params, alias_map)
            if criterion is not None:
                q = q.where(criterion)

        for raw_sql in self._raw_wheres:
            q = q.where(LiteralValue(raw_sql))

        for col, is_desc in self._order:
            order = Order.desc if is_desc else Order.asc
            q = q.orderby(table[col], order=order)

        for col in self._groups:
            q = q.groupby(table[col])

        return q

    def build(self, params: list[typing.Any] | None = None) -> tuple[str, list[typing.Any]]:
        if params is None:
            params = []

        # Build the SELECT projection terms
        terms: list[typing.Any] = []

        if self._model is not None:
            table: Table = _make_table(self._model)
            # Named columns (e.g. "id", "name")
            for c in self._columns:
                terms.append(table[c])
            # Expression projections (e.g. total=Coalesce("id"))
            for alias, expr in self._exprs.items():
                term = _render_val(expr, params)
                terms.append(term.as_(alias))
            q = self._build_base_query(table, terms)
            q = self._apply_model_clauses(q, table, params)
        else:
            # No model — free projection (no FROM)
            for alias, expr in self._exprs.items():
                term = _render_val(expr, params)
                terms.append(term.as_(alias))
            q = PostgreSQLQuery.select(*terms)

        if self._limit_val is not None:
            q = q.limit(self._limit_val)
        if self._offset_val is not None:
            q = q.offset(self._offset_val)

        return q.get_sql(as_keyword=True), params

    async def fetch(
        self,
        conn: typing.Any,
        *,
        raw: bool = False,
    ) -> list[typing.Any]:
        assert self._model is not None
        sql, params = self.build()
        records = await conn.fetch(sql, *params)
        if raw:
            return [dict(r.items()) for r in records]
        if self._prefetches:
            return [_row_to_model_with_prefetch(self._model, r, self._prefetches) for r in records]
        return [_row_to_model(self._model, r) for r in records]

    async def fetch_one(self, conn: typing.Any) -> typing.Any:
        assert self._model is not None
        sql, params = self.build()
        record = await conn.fetchrow(sql, *params)
        if record is None:
            return None
        return _row_to_model(self._model, record)

    def exists(self) -> ExistsExpression:
        return ExistsExpression(self)


class ExistsExpression:
    def __init__(self, query: SelectQuery) -> None:
        self._query = query
        self._negated = False

    def __invert__(self) -> ExistsExpression:
        e = ExistsExpression(self._query)
        e._negated = not self._negated
        return e


class InsertQuery(Query):
    def __init__(self, model: type[Model]) -> None:
        super().__init__(model)
        self._rows: list[dict[str, typing.Any]] = []
        self._subquery: typing.Any = None

    def values(self, rows: typing.Any = None, /, **kwargs: typing.Any) -> Self:
        if rows is not None and kwargs:
            raise ValueError("cannot pass both positional rows and kwargs")
        q = self.__class__.__new__(self.__class__)
        q.__dict__ = {**self.__dict__}
        if rows is None:
            # kwargs path — single row
            q._rows = [kwargs]
            q._subquery = None
        elif hasattr(rows, "build"):
            # subquery path — INSERT INTO ... SELECT ...
            q._rows = []
            q._subquery = rows
        elif isinstance(rows, list):
            if len(rows) == 0:
                raise ValueError("values() requires at least one row")
            if not isinstance(rows[0], dict):
                raise TypeError(
                    "values() expects a list of dicts, kwargs, or a query; "
                    f"got list of {type(rows[0]).__name__}"
                )
            q._rows = list(rows)
            q._subquery = None
        else:
            raise TypeError(
                f"values() expects a list of dicts, kwargs, or a query; got {type(rows).__name__}"
            )
        return q

    def _get_columns(self) -> list[str]:
        assert self._model is not None
        fields = self._model.__fields__
        rel_fields = getattr(self._model, "__relationship_fields__", frozenset())
        return [
            name
            for name, f in fields.items()
            if name != "id"
            and not isinstance(f.default, _SENTINEL_TYPES)
            and name not in rel_fields
        ]

    def build(self, params: list[typing.Any] | None = None) -> tuple[str, list[typing.Any]]:
        assert self._model is not None
        table = _make_table(self._model)
        columns = self._get_columns()

        if params is None:
            params = []

        if self._subquery is not None:
            sub_sql, _ = self._subquery.build(params)
            table_name = self._model.__table_name__
            schema = getattr(self._model, "__schema__", None)
            if schema:
                table_prefix = f'"{schema}"."{table_name}"'
            else:
                table_prefix = f'"{table_name}"'
            col_list = ",".join(f'"{c}"' for c in columns)
            sql = f"INSERT INTO {table_prefix} ({col_list}) {sub_sql} RETURNING *"
            return sql, params

        # Dict rows path
        q = PostgreSQLQuery.into(table).columns(*columns)
        for row in self._rows:
            terms: list[typing.Any] = []
            for col in columns:
                val = row.get(col)
                terms.append(_render_insert_term(val, params))
            q = q.insert(*terms)

        q = q.returning("*")  # type: ignore[attr-defined]
        return q.get_sql(), params

    async def fetch(self, conn: typing.Any) -> list[typing.Any]:
        assert self._model is not None
        sql, params = self.build()
        records = await conn.fetch(sql, *params)
        return [_row_to_model(self._model, r) for r in records]


class UpdateQuery(Query):
    def __init__(self, model: type[Model]) -> None:
        super().__init__(model)
        self._sets: dict[str, typing.Any] = {}

    def set(self, **kwargs: typing.Any) -> Self:
        q = self.__class__.__new__(self.__class__)
        q.__dict__ = {**self.__dict__, "_sets": {**self._sets, **kwargs}}
        return q

    def build(self, params: list[typing.Any] | None = None) -> tuple[str, list[typing.Any]]:
        assert self._model is not None
        table = _make_table(self._model)
        if params is None:
            params = []

        from .expressions import Exp

        q = PostgreSQLQuery.update(table)
        for col, val in self._sets.items():
            if isinstance(val, Exp):
                q = q.set(col, LiteralValue(val.sql))
            else:
                idx = len(params) + 1
                params.append(val)
                q = q.set(col, Parameter(f"${idx}"))

        for where_arg in self._wheres:
            criterion = _where_arg_to_criterion(where_arg, table, params)
            if criterion is not None:
                q = q.where(criterion)

        q = q.returning("*")  # type: ignore[attr-defined]
        return q.get_sql(), params

    async def fetch(self, conn: typing.Any) -> list[typing.Any]:
        assert self._model is not None
        sql, params = self.build()
        records = await conn.fetch(sql, *params)
        return [_row_to_model(self._model, r) for r in records]


class DeleteQuery(Query):
    def __init__(self, model: type[Model]) -> None:
        super().__init__(model)

    def build(self, params: list[typing.Any] | None = None) -> tuple[str, list[typing.Any]]:
        assert self._model is not None
        table = _make_table(self._model)
        if params is None:
            params = []

        q = PostgreSQLQuery.from_(table).delete()

        for where_arg in self._wheres:
            criterion = _where_arg_to_criterion(where_arg, table, params)
            if criterion is not None:
                q = q.where(criterion)

        q = q.returning("*")  # type: ignore[attr-defined]
        return q.get_sql(), params

    async def fetch(self, conn: typing.Any) -> list[typing.Any]:
        assert self._model is not None
        sql, params = self.build()
        records = await conn.fetch(sql, *params)
        return [_row_to_model(self._model, r) for r in records]


def _make_table(model: type[Model]) -> Table:
    schema = getattr(model, "__schema__", None)
    return Table(model.__table_name__, schema=schema)


def _build_explicit_on(
    on_arg: _OnArg,
    source_table: Table,
    target_table: Table,
) -> pypika.Criterion | None:
    if on_arg is None:
        return None
    pairs: list[tuple[typing.Any, typing.Any]] = on_arg if isinstance(on_arg, list) else [on_arg]
    criterion: pypika.Criterion | None = None
    for left_col, right_col in pairs:
        pair_criterion = source_table[left_col.name] == target_table[right_col.name]
        criterion = pair_criterion if criterion is None else criterion & pair_criterion
    return criterion


def _infer_join_on(
    source_model: type[Model],
    target_model: type[Model],
    source_table: Table,
    target_table: Table,
) -> pypika.Criterion | None:
    from .constraints import ForeignKey

    for constraint in source_model.__db_constraints__:
        if isinstance(constraint, ForeignKey) and constraint.target is target_model:
            return source_table[constraint.column] == target_table[constraint.target_column]
    return None


def _row_to_model(model: type[Model], record: typing.Any) -> typing.Any:
    import msgspec

    data = dict(record.items())
    return msgspec.convert(data, model)


def _find_prefetch_relation(
    source_model: type[Model],
    target_model: type[Model],
) -> tuple[str, typing.Any]:
    from .constraints import ForeignKey

    for constraint in source_model.__db_constraints__:
        if isinstance(constraint, ForeignKey) and constraint.target is target_model:
            rel_field = constraint.column.removesuffix("_id")
            return rel_field, constraint
    raise ValueError(f"No ForeignKey from {source_model.__name__} to {target_model.__name__} found")


def _row_to_model_with_prefetch(
    model: type[Model],
    record: typing.Any,
    prefetch_models: list[type[Model]],
) -> typing.Any:
    import msgspec

    data = dict(record.items())
    nested: dict[str, typing.Any] = {}

    for prefetch_model in prefetch_models:
        rel_field, _ = _find_prefetch_relation(model, prefetch_model)
        prefix = f"{rel_field}__"
        rel_data = {k[len(prefix) :]: v for k, v in data.items() if k.startswith(prefix)}
        # Remove prefixed keys from base data
        for k in list(rel_data):
            data.pop(f"{prefix}{k}", None)
        if any(v is not None for v in rel_data.values()):
            nested[rel_field] = msgspec.convert(rel_data, prefetch_model)

    data.update(nested)
    return msgspec.convert(data, model)
