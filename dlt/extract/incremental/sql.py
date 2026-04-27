from datetime import date, datetime  # noqa: I251
from typing import Any, List, Optional, Tuple, Type, TYPE_CHECKING

from dlt.common.data_types.type_helpers import py_type_to_sc_type
from dlt.common.data_writers.escape import format_datetime_value
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.libs.sqlglot import sge, to_sqlglot_type, build_typed_literal

if TYPE_CHECKING:
    from dlt.extract.incremental import Incremental


def _resolve_timestamp_cast(
    lower: Any,
    upper: Any,
    caps: Optional[DestinationCapabilitiesContext],
) -> Tuple[Optional[sge.DataType], Any, Any]:
    """Cast type and ISO-string bounds for a timestamp cursor."""
    dialect = caps.sqlglot_dialect if caps is not None else None
    precision = caps.timestamp_precision if caps is not None else 6

    # tz-awareness comes from the actual bound value — needed so dialects that
    # split timestamp/timestamptz (bigquery DATETIME vs TIMESTAMP) emit the
    # right cast and don't reject the comparison
    tz_sample = (
        lower if isinstance(lower, datetime) else (upper if isinstance(upper, datetime) else None)
    )
    timezone: Optional[bool] = None
    if tz_sample is not None:
        timezone = tz_sample.tzinfo is not None

    # naive cast when destination can't store tz-aware (dremio, athena) or its
    # tz-aware CAST rejects offset literals (clickhouse via the `_in_cast`
    # override); sqlite emits no cast, but its literal still needs UTC-naive
    # form to match TEXT-affinity column storage
    if timezone and caps is not None:
        cast_tz_ok = (
            caps.supports_tz_aware_datetime_in_cast
            if caps.supports_tz_aware_datetime_in_cast is not None
            else caps.supports_tz_aware_datetime
        )
        if not cast_tz_ok or dialect == "sqlite":
            timezone = False

    sqlglot_type = to_sqlglot_type(dlt_type="timestamp", timezone=timezone, precision=precision)

    naive = timezone is False
    if isinstance(lower, datetime):
        lower = format_datetime_value(lower, precision, no_tz=naive)
    if isinstance(upper, datetime):
        upper = format_datetime_value(upper, precision, no_tz=naive)

    # sqlite's CAST AS TIMESTAMP/TIMESTAMPTZ goes through NUMERIC affinity and
    # parses only leading digits — drop the cast so text comparison works
    if dialect == "sqlite":
        sqlglot_type = None

    return sqlglot_type, lower, upper


def to_sqlglot_filter(
    incremental: "Incremental[Any]",
    apply_lag: bool = True,
    destination_capabilities: Optional[DestinationCapabilitiesContext] = None,
) -> Optional[sge.Expression]:
    """Build a sqlglot WHERE expression that filters rows to the current incremental window.

    Args:
        incremental (Incremental[Any]): Incremental instance whose cursor is filtered.
            Works on bound and unbound instances.
        apply_lag (bool): When True, lag is applied to the lower bound;
            when False, the raw cached `start_value` is used.
        destination_capabilities (Optional[DestinationCapabilitiesContext]): Caps used
            to shape the timestamp cast and literal format (precision, tz handling,
            sqlite cast drop). When `None`, a generic tz-aware cast is emitted.

    Returns:
        Optional[sge.Expression]: A sqlglot boolean expression suitable for use as a
            WHERE clause, or `None` when filtering is not possible.
    """
    # bounds come from Incremental._resolve_bounds(apply_lag=apply_lag) which works on
    # bound and unbound instances (falling back to initial_value / end_value).
    # range_start / range_end decide endpoint inclusivity; the operator direction
    # follows last_value_func. on_cursor_value_missing controls NULL handling:
    #   "include" ORs `cursor IS NULL`
    #   "exclude" ANDs `cursor IS NOT NULL`
    #   "raise"   emits no NULL clause
    # returns None when filtering is not possible: JSONPath cursor (not a simple column),
    # custom last_value_func other than min/max, or no bounds and no NULL handling.
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

    # type the literal and format the bounds. branches by dlt cursor type:
    # - timestamp: caps drive precision + tz handling; sqlite drops the cast
    # - date:      ISO date string, plain CAST AS DATE; caps not consulted
    # - other:     primitive cast (int/str/float/decimal); caps not consulted
    sqlglot_type: Optional[sge.DataType] = None
    param_type = incremental.get_incremental_value_type()
    if param_type is not Any:
        try:
            dlt_type = py_type_to_sc_type(param_type)
        except TypeError:
            # unmappable Python type — emit untyped literal
            dlt_type = None

        if dlt_type == "timestamp":
            sqlglot_type, lower, upper = _resolve_timestamp_cast(
                lower, upper, destination_capabilities
            )
        elif dlt_type == "date":
            sqlglot_type = to_sqlglot_type(dlt_type="date")
            if isinstance(lower, date):
                lower = lower.isoformat()
            if isinstance(upper, date):
                upper = upper.isoformat()
        elif dlt_type is not None:
            sqlglot_type = to_sqlglot_type(dlt_type=dlt_type)

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
