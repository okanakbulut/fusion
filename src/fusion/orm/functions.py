from decimal import Decimal
from typing import Any
import pypika
from pypika import Parameter
from pypika import functions as fn

type Scalar = str | int | float | bool | Decimal | None


class Coalesce:
    """SQL COALESCE function. Accepts Function | Scalar args (subquery support added later)."""

    _is_sql_function = True  # marker: inline SQL, no wrapping parens needed

    def __init__(self, *args: Any) -> None:
        self._args = args

    def build(self, params: list[Any]) -> tuple[str, list[Any]]:
        from pypika.terms import LiteralValue

        terms = []
        for arg in self._args:
            if isinstance(arg, (str, int, float, bool, Decimal)) or arg is None:
                idx = len(params) + 1
                params.append(arg)
                terms.append(Parameter(f"${idx}"))
            elif hasattr(arg, "build"):
                sub_sql, _ = arg.build(params)
                # SQL functions render inline; subqueries need wrapping parens
                if getattr(arg, "_is_sql_function", False):
                    terms.append(LiteralValue(sub_sql))
                else:
                    terms.append(LiteralValue(f"({sub_sql})"))
            else:
                raise TypeError(f"Unsupported Coalesce arg type: {type(arg)}")
        sql = str(fn.Coalesce(*terms))
        return sql, params
