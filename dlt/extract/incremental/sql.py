from typing import Any, List, Optional, Type, TYPE_CHECKING

from dlt.common.data_types.type_helpers import py_type_to_sc_type
from dlt.common.libs.sqlglot import sge, to_sqlglot_type, build_typed_literal

from dlt.extract.exceptions import IncrementalUnboundError

if TYPE_CHECKING:
    from dlt.extract.incremental import Incremental


def to_sqlglot_filter(
    incremental: "Incremental[Any]", apply_lag: bool = True
) -> Optional[sge.Expression]:
    """Build a sqlglot WHERE expression that filters rows to the current incremental window.

    The filter is computed from the persisted state and current bounds:
    - The lower bound is `start_value` (lag-adjusted) when `apply_lag=True`,
      or the raw `state["start_value"]` snapshotted at `bind()` time when `apply_lag=False`.
    - The upper bound is `end_value` if set, otherwise `state["last_value"]` (which advances
      during the run), otherwise no upper bound clause is emitted.
    - `range_start` and `range_end` decide whether endpoints are inclusive (`closed`)
      or exclusive (`open`); the operator direction follows `last_value_func`.
    - `on_cursor_value_missing="include"` ORs `cursor IS NULL`, `"exclude"` ANDs
      `cursor IS NOT NULL`, `"raise"` emits no NULL clause.

    Args:
        incremental (Incremental): The incremental instance whose cursor is filtered.
        apply_lag (bool): When True (default) use the lag-adjusted instance `start_value`
            as the lower bound. When False, use the raw `start_value` from cached state;
            requires the incremental to have been bound.

    Returns:
        Optional[sge.Expression]: A sqlglot boolean expression suitable for use as a WHERE
            clause, or `None` when filtering is not possible: a JSONPath cursor (not a
            simple column), a custom `last_value_func` other than `min`/`max`, or no
            bounds and no NULL handling.

    Raises:
        IncrementalUnboundError: If `apply_lag=False` is requested and the incremental
            has not been bound (no cached state to read raw `start_value` from).
    """
    column_name = incremental.get_cursor_column_name()
    if column_name is None:
        # JSONPath cursor cannot be filtered in SQL
        return None

    # operator selection mirrors dlt/sources/sql_database/helpers.py:170-180
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

    # lower bound source
    if apply_lag:
        # `start_value` is set in bind() from `last_value` property; lag is auto-suppressed
        # when end_value is set (see Incremental.last_value lines 440-444)
        lower = incremental.start_value
    else:
        if incremental._cached_state is None:
            raise IncrementalUnboundError(incremental.cursor_path)
        lower = incremental._cached_state.get("start_value")

    # upper bound source: end_value > live last_value > None
    upper: Optional[Any]
    if incremental.end_value is not None:
        upper = incremental.end_value
    elif (
        incremental._cached_state is not None
        and incremental._cached_state.get("last_value") is not None
    ):
        upper = incremental._cached_state["last_value"]
    else:
        upper = None

    # type the literal: derive sqlglot DataType from cursor's Python type
    sqlglot_type: Optional[sge.DataType] = None
    param_type = incremental.get_incremental_value_type()
    if param_type is not Any:
        try:
            sqlglot_type = to_sqlglot_type(dlt_type=py_type_to_sc_type(param_type))
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
