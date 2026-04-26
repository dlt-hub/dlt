from datetime import datetime  # noqa: I251
from typing import Any, List, Optional, Type, TYPE_CHECKING

from dlt.common.data_types.type_helpers import py_type_to_sc_type
from dlt.common.libs.sqlglot import sge, to_sqlglot_type, build_typed_literal

if TYPE_CHECKING:
    from dlt.extract.incremental import Incremental


def to_sqlglot_filter(
    incremental: "Incremental[Any]", apply_lag: bool = True
) -> Optional[sge.Expression]:
    """Build a sqlglot WHERE expression that filters rows to the current incremental window.

    Bounds come from `Incremental._resolve_bounds(apply_lag=apply_lag)` which works
    on both bound and unbound instances (falling back to `initial_value` / `end_value`).
    `range_start` / `range_end` decide endpoint inclusivity; the operator direction
    follows `last_value_func`. `on_cursor_value_missing` controls NULL handling
    (`"include"` ORs `cursor IS NULL`, `"exclude"` ANDs `cursor IS NOT NULL`,
    `"raise"` emits no NULL clause).

    Args:
        incremental (Incremental): The incremental instance whose cursor is filtered.
        apply_lag (bool): When True (default) lag is applied to the lower bound;
            when False the raw cached `start_value` is used.

    Returns:
        Optional[sge.Expression]: A sqlglot boolean expression suitable for use as a WHERE
            clause, or `None` when filtering is not possible: a JSONPath cursor (not a
            simple column), a custom `last_value_func` other than `min`/`max`, or no
            bounds and no NULL handling.
    """
    column_name = incremental.get_cursor_column_name()
    if column_name is None:
        # JSONPath cursor cannot be filtered in SQL
        return None

    lower_op: Type[sge.Binary]
    upper_op: Type[sge.Binary]
    if incremental.last_value_func is max:
        lower_op = sge.GTE if incremental.range_start == "closed" else sge.GT
        upper_op = sge.LT if incremental.range_end == "open" else sge.LTE
    elif incremental.last_value_func is min:
        lower_op = sge.LTE if incremental.range_start == "closed" else sge.LT
        upper_op = sge.GT if incremental.range_end == "open" else sge.GTE
    else:
        # custom last_value_func cannot be translated to SQL
        return None

    lower, upper = incremental._resolve_bounds(apply_lag=apply_lag)

    # type the literal: derive sqlglot DataType from cursor's Python type
    sqlglot_type: Optional[sge.DataType] = None
    param_type = incremental.get_incremental_value_type()
    if param_type is not Any:
        try:
            # tz-awareness comes from the actual bound value — needed so dialects that
            # split timestamp/timestamptz (bigquery DATETIME vs TIMESTAMP) emit the
            # right cast and don't reject the comparison
            timezone: Optional[bool] = None
            tz_sample = (
                lower
                if isinstance(lower, datetime)
                else (upper if isinstance(upper, datetime) else None)
            )
            if tz_sample is not None:
                timezone = tz_sample.tzinfo is not None
            sqlglot_type = to_sqlglot_type(
                dlt_type=py_type_to_sc_type(param_type), timezone=timezone
            )
        except TypeError:
            # unmappable Python type — emit untyped literal
            sqlglot_type = None

    column = sge.Column(this=sge.to_identifier(column_name, quoted=True))

    bound_clauses: List[sge.Expression] = []
    if lower is not None:
        bound_clauses.append(
            lower_op(this=column, expression=build_typed_literal(lower, sqlglot_type))
        )
    if upper is not None:
        bound_clauses.append(
            upper_op(this=column, expression=build_typed_literal(upper, sqlglot_type))
        )

    combined: Optional[sge.Expression] = None
    if len(bound_clauses) == 1:
        combined = bound_clauses[0]
    elif len(bound_clauses) >= 2:
        combined = sge.And(this=bound_clauses[0], expression=bound_clauses[1])

    # null handling — mirrors helpers.py:190-193 associativity: (a AND b) OR null
    if incremental.on_cursor_value_missing == "include":
        if combined is None:
            # nothing to weaken with OR — no useful filter
            return None
        null_clause = sge.Is(this=column, expression=sge.Null())
        combined = sge.Or(this=combined, expression=null_clause)
    elif incremental.on_cursor_value_missing == "exclude":
        not_null = sge.Not(this=sge.Is(this=column, expression=sge.Null()))
        if combined is None:
            combined = not_null
        else:
            combined = sge.And(this=combined, expression=not_null)

    return combined
