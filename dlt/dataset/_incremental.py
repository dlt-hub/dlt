from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import Any, Callable, Optional, Tuple, Type, TYPE_CHECKING

import sqlglot.expressions as sge
from jsonpath_ng.exceptions import JSONPathError

from dlt.common.incremental.typing import TIncrementalRange
from dlt.common.jsonpath import extract_simple_field_name
from dlt.common.libs.sqlglot import (
    SQLGLOT_TO_DLT_TYPE_MAP,
    build_typed_literal,
    resolve_date_cast,
    resolve_timestamp_cast,
    to_sqlglot_type,
)
from dlt.common.schema.typing import TTableSchemaColumns

if TYPE_CHECKING:
    from dlt.common.destination.capabilities import DestinationCapabilitiesContext
    from dlt.extract.incremental import Incremental


_AGG_CURSOR_ALIAS = "__dlt_inc_cursor"


@dataclass(frozen=True)
class _RelationIncrementalContext:
    """Per-relation marker tying a `Relation` back to its `Incremental`."""

    incremental: Incremental[Any]
    cursor_column: sge.Column


def _build_incremental_aggregate(
    base_query: sge.Query,
    incremental: Incremental[Any],
    cursor_column: sge.Column,
    destination_capabilities: Optional[DestinationCapabilitiesContext] = None,
) -> sge.Select:
    """Build `SELECT <func>(alias) FROM (SELECT cursor AS alias FROM <filtered>)`."""
    if incremental.end_value is None and base_query.args.get("limit") is not None:
        raise ValueError(
            "LIMIT isn't supported on stateful `.incremental()` as state would "
            "advance past only the returned rows, silently skipping the rest on "
            "the next run. Remove it, or set `end_value=` to read a fixed range."
        )

    cursor_alias = sge.to_identifier(_AGG_CURSOR_ALIAS, quoted=True)
    if cursor_column.table:
        # qualified cursor (auto-join): replace projection inline so the join qualifier resolves
        inner = base_query.copy()
        inner.set(
            "expressions",
            [sge.Alias(this=cursor_column.copy(), alias=cursor_alias)],
        )
    else:
        # bare cursor: wrap base as subquery so GROUP BY, HAVING, and aliased computed cursors are preserved
        bare_cursor = sge.Column(this=cursor_column.this.copy())
        inner = sge.Select(expressions=[sge.Alias(this=bare_cursor, alias=cursor_alias)]).from_(
            base_query.copy().subquery()
        )

    agg_cls: Type[sge.AggFunc]
    if incremental.last_value_func is max:
        agg_cls = sge.Max
    elif incremental.last_value_func is min:
        agg_cls = sge.Min
    else:
        raise ValueError(
            "Incremental aggregate can only be built for `min` or `max` "
            f"`last_value_func`, got {incremental.last_value_func!r}."
        )

    outer_ref = sge.Column(this=cursor_alias.copy())
    agg_func: sge.AggFunc = agg_cls(this=outer_ref)
    agg: sge.Expression = agg_func
    if destination_capabilities is not None and destination_capabilities.null_safe_aggregate:
        agg = destination_capabilities.null_safe_aggregate(agg_func)
    return sge.Select(expressions=[agg]).from_(inner.subquery())


def parse_incremental_cursor_path(cursor_path: str) -> Tuple[Optional[str], str]:
    """Split `table.column` into parts, or return `(None, column)` for a bare field."""
    table_part, _, column_part = cursor_path.rpartition(".")
    try:
        column_name = extract_simple_field_name(column_part) if column_part else None
        # the table part must be a plain name too — rejects JSONPath roots, wildcards and indices
        if "." in cursor_path and extract_simple_field_name(table_part) != table_part:
            column_name = None
    except JSONPathError:
        column_name = None
    if column_name is None:
        raise ValueError(
            f"Incremental `cursor_path={cursor_path!r}` is not supported by"
            " `Relation.incremental()`, which accepts plain `column` or `table.column`"
            " cursors, not JSONPath expressions."
        )
    return table_part or None, column_name


def _build_incremental_condition(
    incremental: Incremental[Any],
    column_ref: sge.Column,
    sqlglot_type: Optional[sge.DataType],
    destination_capabilities: Optional[DestinationCapabilitiesContext] = None,
    range_start: Optional[TIncrementalRange] = None,
    range_end: Optional[TIncrementalRange] = None,
) -> Optional[sge.Expression]:
    """Build the WHERE condition for an Incremental cursor on `column_ref`.

    Args:
        incremental (Incremental): The incremental carrying the cursor range and
            `on_cursor_value_missing` policy.
        column_ref (sge.Column): Reference to the cursor column in the target query.
        sqlglot_type (Optional[sge.DataType]): SQLGlot data type used to CAST the
            range literals; pass `None` to skip casting.
        destination_capabilities (Optional[DestinationCapabilitiesContext]): Caps used
            to shape timestamp literal format and CAST.
        range_start (Optional[TIncrementalRange]): Overrides `incremental.range_start`.
        range_end (Optional[TIncrementalRange]): Overrides `incremental.range_end`.

    Returns:
        Optional[sge.Expression]: A boolean expression ready to be attached via
            `.where(...)`, or `None`.

    Raises:
        ValueError: If `incremental.last_value_func` is not `min` or `max`, or if
            `on_cursor_value_missing` is not one of `"include"`, `"exclude"`, `"raise"`.
    """
    range_start = range_start or incremental.range_start
    range_end = range_end or incremental.range_end
    last_value_func = incremental.last_value_func
    start_op_cls: Type[sge.Binary]
    end_op_cls: Type[sge.Binary]
    if last_value_func is max:
        start_op_cls = sge.GTE if range_start == "closed" else sge.GT
        end_op_cls = sge.LT if range_end == "open" else sge.LTE
    elif last_value_func is min:
        start_op_cls = sge.LTE if range_start == "closed" else sge.LT
        end_op_cls = sge.GT if range_end == "open" else sge.GTE
    else:
        raise ValueError(
            f"Incremental `last_value_func={last_value_func!r}` cannot be pushed "
            "down to SQL. Only `min` and `max` are supported by `Relation.incremental()`."
        )

    on_missing = incremental.on_cursor_value_missing
    if on_missing not in ("include", "exclude", "raise"):
        raise ValueError(
            "Incremental `on_cursor_value_missing="
            f"{on_missing!r}` is not supported by "
            "`Relation.incremental()`. Expected one of: 'include', 'exclude', 'raise'."
        )
    start_value, end_value = incremental.get_current_range(apply_lag=True)

    # coerce temporal range to the column type so literals render correctly
    dlt_type = SQLGLOT_TO_DLT_TYPE_MAP.get(sqlglot_type.this) if sqlglot_type is not None else None
    if dlt_type == "timestamp":
        sqlglot_type, start_value, end_value = resolve_timestamp_cast(
            start_value, end_value, destination_capabilities
        )
    elif dlt_type == "date":
        start_value, end_value = resolve_date_cast(start_value, end_value)

    range_expr: Optional[sge.Expression] = None
    if start_value is not None:
        start_literal = build_typed_literal(start_value, sqlglot_type)
        range_expr = start_op_cls(this=column_ref.copy(), expression=start_literal)

    if end_value is not None:
        end_literal = build_typed_literal(end_value, sqlglot_type)
        end_condition: sge.Expression = end_op_cls(this=column_ref.copy(), expression=end_literal)
        range_expr = (
            end_condition
            if range_expr is None
            else sge.And(this=range_expr, expression=end_condition)
        )

    if on_missing == "include":
        if range_expr is None:
            return None
        is_null = sge.Is(this=column_ref.copy(), expression=sge.Null())
        return sge.Or(this=range_expr, expression=is_null)

    # "exclude" or "raise" both pin nulls out via IS NOT NULL.
    # "raise" can't raise mid-query in SQL pushdown; so we warn users
    is_not_null = sge.Not(this=sge.Is(this=column_ref.copy(), expression=sge.Null()))
    if range_expr is None:
        return is_not_null
    return sge.And(this=range_expr, expression=is_not_null)


def apply_incremental(
    *,
    incremental: Incremental[Any],
    target_query: sge.Query,
    column_ref: sge.Column,
    column_lookup_columns: TTableSchemaColumns,
    destination_capabilities: Optional[DestinationCapabilitiesContext] = None,
    advance: bool = False,
    fetch_aggregate_scalar: Optional[Callable[[sge.Query], Any]] = None,
) -> Tuple[sge.Query, _RelationIncrementalContext]:
    """Attach incremental WHERE to `target_query`; with `advance=True`, advance state."""
    column_name = column_ref.name
    sqlglot_type = _sqlglot_type_for_column(column_lookup_columns, column_name)
    _maybe_warn_on_cursor_missing_raise(incremental, column_lookup_columns, column_name)

    range_start: Optional[TIncrementalRange] = None
    range_end: Optional[TIncrementalRange] = None

    # only a bound cursor is advanced/consumed; auto-advancing an unbound cursor would turn
    # a plain range filter into a boundary-dropping stateful read (breaks .incremental() composition)
    if advance and incremental._cached_state is not None:
        unique_cursor = incremental.end_value is None and incremental.is_unique_cursor()
        if unique_cursor:
            # unique cursor: no more rows can arrive at a seen value, take the boundary eagerly
            range_end = "closed"
            # with lag the range start deliberately re-reads the attribution window
            if not incremental.lag and incremental.unique_boundary_consumed():
                # the boundary row at the range start was loaded by a previous run, skip it
                range_start = "open"
        elif incremental.end_value is None:
            if incremental.range_start == "open" and incremental.range_end == "open":
                # an open start never replays the boundary row: take it eagerly,
                # otherwise it could never load
                range_end = "closed"

        if incremental.end_value is not None:
            new_value = incremental.end_value
        else:
            if fetch_aggregate_scalar is None:
                raise ValueError(
                    "`fetch_aggregate_scalar` is required when `advance=True` and"
                    " `incremental.end_value` is unset."
                )
            lower_condition = _build_incremental_condition(
                incremental,
                column_ref,
                sqlglot_type,
                destination_capabilities=destination_capabilities,
                range_start=range_start,
            )
            filtered_query = (
                target_query.where(lower_condition) if lower_condition is not None else target_query
            )
            agg_query = _build_incremental_aggregate(
                filtered_query,
                incremental,
                column_ref,
                destination_capabilities=destination_capabilities,
            )
            new_value = fetch_aggregate_scalar(agg_query)
            if new_value is not None and range_end != "closed" and incremental.range_end == "open":
                # deduplicated by the default warnings filter: once per cursor and process
                warnings.warn(
                    f"Rows at the boundary value `{new_value}` of cursor column"
                    f" {incremental.cursor_path!r} will not be included in this run. SQL"
                    " incremental does not support boundary deduplication, so the boundary row is"
                    " deferred until state advances past it. If the cursor column is unique,"
                    " declare it as primary_key on the incremental to load boundary values"
                    " eagerly. Otherwise set range_end='closed' and use write_disposition='merge'"
                    " with a primary_key to dedup the overlap.",
                    UserWarning,
                    stacklevel=2,
                )
        incremental.advance(new_value)

    condition = _build_incremental_condition(
        incremental,
        column_ref,
        sqlglot_type,
        destination_capabilities=destination_capabilities,
        range_start=range_start,
        range_end=range_end,
    )
    final_query = target_query.where(condition) if condition is not None else target_query
    ctx = _RelationIncrementalContext(incremental=incremental, cursor_column=column_ref.copy())
    return final_query, ctx


def raise_incomplete_cursor_column(cursor_path: str, location_label: str) -> None:
    raise ValueError(
        f"Incremental cursor `{cursor_path}` is not a materialized column on "
        f"{location_label}. Columns declared as hints without a `data_type` cannot "
        "be used as cursors. Use a column that exists at the destination."
    )


def _maybe_warn_on_cursor_missing_raise(
    incremental: Incremental[Any],
    columns_schema: TTableSchemaColumns,
    column_name: str,
) -> None:
    """Warn when `on_cursor_value_missing="raise"` is bound against a nullable cursor."""
    if incremental.on_cursor_value_missing != "raise":
        return
    column_schema = columns_schema.get(column_name) or {}
    if column_schema.get("nullable") is False:
        return
    warnings.warn(
        "Can't raise on NULL cursor values; rows with NULL "
        "cursors will be excluded. Set on_cursor_value_missing explicitly "
        "to silence.",
        UserWarning,
        stacklevel=5,
    )


def _sqlglot_type_for_column(
    columns: TTableSchemaColumns, column_name: str
) -> Optional[sge.DataType]:
    """Resolve the SQLGlot data type for `column_name` from a dlt columns schema."""
    column_schema = columns.get(column_name)
    if not column_schema:
        return None
    data_type = column_schema.get("data_type")
    if data_type is None:
        return None
    return to_sqlglot_type(
        dlt_type=data_type,
        precision=column_schema.get("precision"),
        timezone=column_schema.get("timezone"),
        nullable=column_schema.get("nullable"),
    )
