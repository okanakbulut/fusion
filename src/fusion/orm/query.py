import typing

import pypika
from pypika import Order, Parameter, Table
from pypika import functions as fn
from pypika.dialects import PostgreSQLQuery
from pypika.terms import LiteralValue

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


class SelectQuery:
    def __init__(self, model: type[Model], columns: tuple[str, ...]) -> None:
        self._model = model
        self._columns = columns
        self._wheres: list[Q | Condition] = []
        self._raw_wheres: list[str] = []
        self._joins: list[tuple[type, str, _OnArg, str]] = []
        self._prefetches: list[type[Model]] = []
        self._order: list[tuple[str, bool]] = []
        self._limit_val: int | None = None
        self._offset_val: int | None = None

    def where(self, *args: Q | Condition, **kwargs: typing.Any) -> SelectQuery:
        q = SelectQuery.__new__(SelectQuery)
        q.__dict__ = {**self.__dict__, "_wheres": list(self._wheres)}
        for arg in args:
            q._wheres.append(arg)
        if kwargs:
            q._wheres.append(Q(**kwargs))
        return q

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

    def order_by(self, column: str, *, desc: bool = False) -> SelectQuery:
        q = SelectQuery.__new__(SelectQuery)
        q.__dict__ = {**self.__dict__, "_order": list(self._order)}
        q._order.append((column, desc))
        return q

    def limit(self, n: int) -> SelectQuery:
        q = SelectQuery.__new__(SelectQuery)
        q.__dict__ = {**self.__dict__}
        q._limit_val = n
        return q

    def offset(self, n: int) -> SelectQuery:
        q = SelectQuery.__new__(SelectQuery)
        q.__dict__ = {**self.__dict__}
        q._offset_val = n
        return q

    def build(self) -> tuple[str, list[typing.Any]]:
        import msgspec.structs

        table = _make_table(self._model)
        params: list[typing.Any] = []

        if self._columns:
            q = PostgreSQLQuery.from_(table).select(*[table[c] for c in self._columns])
        elif self._prefetches:
            # pypika silently drops additional column selects when SELECT * is used,
            # so list the main table's columns explicitly whenever prefetches are present.
            rel_fields = getattr(self._model, "__relationship_fields__", frozenset())
            main_cols = [
                table[f.name]
                for f in msgspec.structs.fields(self._model)  # type: ignore[arg-type]
                if f.name not in rel_fields
            ]
            q = PostgreSQLQuery.from_(table).select(*main_cols)
        else:
            q = PostgreSQLQuery.from_(table).select("*")

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
            for sf in msgspec.structs.fields(prefetch_model):  # type: ignore[arg-type]
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

        if self._limit_val is not None:
            q = q.limit(self._limit_val)
        if self._offset_val is not None:
            q = q.offset(self._offset_val)

        return q.get_sql(), params

    async def fetch(
        self,
        conn: typing.Any,
        *,
        raw: bool = False,
    ) -> list[typing.Any]:
        sql, params = self.build()
        records = await conn.fetch(sql, *params)
        if raw:
            return [dict(r.items()) for r in records]
        if self._prefetches:
            return [_row_to_model_with_prefetch(self._model, r, self._prefetches) for r in records]
        return [_row_to_model(self._model, r) for r in records]

    async def fetch_one(self, conn: typing.Any) -> typing.Any:
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


class InsertQuery:
    def __init__(self, model: type[Model]) -> None:
        self._model = model
        self._rows: list[typing.Any] = []

    def values(self, rows: typing.Any) -> InsertQuery:
        q = InsertQuery.__new__(InsertQuery)
        q.__dict__ = {**self.__dict__}
        if isinstance(rows, list):
            q._rows = list(rows)
        else:
            q._rows = [rows]
        return q

    def build(self) -> tuple[str, list[typing.Any]]:
        table = _make_table(self._model)
        fields = self._model.__fields__

        rel_fields = getattr(self._model, "__relationship_fields__", frozenset())
        columns = [
            name
            for name, f in fields.items()
            if name != "id"
            and not isinstance(f.default, _SENTINEL_TYPES)
            and name not in rel_fields
        ]

        params: list[typing.Any] = []
        q = PostgreSQLQuery.into(table).columns(*columns)

        for row in self._rows:
            row_params: list[typing.Any] = []
            for col in columns:
                val = getattr(row, col)
                row_params.append(val)
                params.append(val)
            base = len(params) - len(row_params) + 1
            q = q.insert(*[Parameter(f"${base + i}") for i in range(len(row_params))])

        q = q.returning("*")  # type: ignore[attr-defined]
        return q.get_sql(), params

    async def fetch(self, conn: typing.Any) -> list[typing.Any]:
        sql, params = self.build()
        records = await conn.fetch(sql, *params)
        return [_row_to_model(self._model, r) for r in records]


class UpdateQuery:
    def __init__(self, model: type[Model]) -> None:
        self._model = model
        self._sets: dict[str, typing.Any] = {}
        self._wheres: list[Q | Condition] = []

    def set(self, **kwargs: typing.Any) -> UpdateQuery:
        q = UpdateQuery.__new__(UpdateQuery)
        q.__dict__ = {**self.__dict__, "_sets": {**self._sets, **kwargs}}
        return q

    def where(self, *args: Q | Condition, **kwargs: typing.Any) -> UpdateQuery:
        q = UpdateQuery.__new__(UpdateQuery)
        q.__dict__ = {**self.__dict__, "_wheres": list(self._wheres)}
        for arg in args:
            q._wheres.append(arg)
        if kwargs:
            q._wheres.append(Q(**kwargs))
        return q

    def build(self) -> tuple[str, list[typing.Any]]:
        table = _make_table(self._model)
        params: list[typing.Any] = []

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
        sql, params = self.build()
        records = await conn.fetch(sql, *params)
        return [_row_to_model(self._model, r) for r in records]


class DeleteQuery:
    def __init__(self, model: type[Model]) -> None:
        self._model = model
        self._wheres: list[Q | Condition] = []

    def where(self, *args: Q | Condition, **kwargs: typing.Any) -> DeleteQuery:
        q = DeleteQuery.__new__(DeleteQuery)
        q.__dict__ = {**self.__dict__, "_wheres": list(self._wheres)}
        for arg in args:
            q._wheres.append(arg)
        if kwargs:
            q._wheres.append(Q(**kwargs))
        return q

    def build(self) -> tuple[str, list[typing.Any]]:
        table = _make_table(self._model)
        params: list[typing.Any] = []

        q = PostgreSQLQuery.from_(table).delete()

        for where_arg in self._wheres:
            criterion = _where_arg_to_criterion(where_arg, table, params)
            if criterion is not None:
                q = q.where(criterion)

        q = q.returning("*")  # type: ignore[attr-defined]
        return q.get_sql(), params

    async def fetch(self, conn: typing.Any) -> list[typing.Any]:
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
