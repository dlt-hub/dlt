from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import Any, Optional, Tuple, Type, TYPE_CHECKING

import sqlglot.expressions as sge
from jsonpath_ng.exceptions import JSONPathError

from dlt.common.jsonpath import extract_simple_field_name
from dlt.common.libs.sqlglot import (
    SQLGLOT_TO_DLT_TYPE_MAP,
    build_typed_literal,
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
    ctx: _RelationIncrementalContext,
    destination_capabilities: Optional[DestinationCapabilitiesContext] = None,
) -> sge.Select:
    """Build `SELECT <func>(alias) FROM (SELECT cursor AS alias FROM <filtered>)`."""
    if ctx.incremental.end_value is None and base_query.args.get("limit") is not None:
        raise ValueError(
            "LIMIT isn't supported on stateful `.incremental()` as state would "
            "advance past only the returned rows, silently skipping the rest on "
            "the next run. Remove it, or set `end_value=` for a bounded read."
        )

    cursor_alias = sge.to_identifier(_AGG_CURSOR_ALIAS, quoted=True)
    if ctx.cursor_column.table:
        # qualified cursor (auto-join): replace projection inline so the join qualifier resolves
        inner = base_query.copy()
        inner.set(
            "expressions",
            [sge.Alias(this=ctx.cursor_column.copy(), alias=cursor_alias)],
        )
    else:
        # bare cursor: wrap base as subquery so GROUP BY, HAVING, and aliased computed cursors are preserved
        bare_cursor = sge.Column(this=ctx.cursor_column.this.copy())
        inner = sge.Select(expressions=[sge.Alias(this=bare_cursor, alias=cursor_alias)]).from_(
            base_query.copy().subquery()
        )

    agg_cls: Type[sge.AggFunc]
    if ctx.incremental.last_value_func is max:
        agg_cls = sge.Max
    elif ctx.incremental.last_value_func is min:
        agg_cls = sge.Min
    else:
        raise ValueError(
            "Incremental aggregate can only be built for `min` or `max` "
            f"`last_value_func`, got {ctx.incremental.last_value_func!r}."
        )

    outer_ref = sge.Column(this=cursor_alias.copy())
    agg_func: sge.AggFunc = agg_cls(this=outer_ref)
    agg: sge.Expression = agg_func
    if destination_capabilities is not None and destination_capabilities.null_safe_aggregate:
        agg = destination_capabilities.null_safe_aggregate(agg_func)
    return sge.Select(expressions=[agg]).from_(inner.subquery())


def _parse_incremental_cursor_path(cursor_path: str) -> Tuple[Optional[str], str]:
    """Split `table.column` into parts, or return `(None, column)` for a bare field."""
    if not cursor_path:
        raise ValueError("Incremental `cursor_path` must be a non-empty string.")

    # JSONPath wildcards, array indices, and `$` root markers cannot be pushed down to SQL
    if any(ch in cursor_path for ch in ("$", "[", "*")):
        raise ValueError(
            f"Incremental `cursor_path={cursor_path!r}` is a JSONPath expression. "
            "`Relation.incremental()` only supports plain `column` or `table.column` cursors."
        )

    invalid_msg = (
        f"Incremental `cursor_path={cursor_path!r}` is not a plain column identifier. "
        "Use `column` or `table.column`."
    )

    if "." in cursor_path:
        table_part, column_part = cursor_path.rsplit(".", 1)
        if not table_part:
            raise ValueError(invalid_msg)
    else:
        table_part, column_part = None, cursor_path

    try:
        column_name = extract_simple_field_name(column_part)
    except JSONPathError as e:
        raise ValueError(invalid_msg) from e
    if column_name is None:
        raise ValueError(invalid_msg)
    return table_part, column_name


def _build_incremental_condition(
    incremental: Incremental[Any],
    column_ref: sge.Column,
    sqlglot_type: Optional[sge.DataType],
    destination_capabilities: Optional[DestinationCapabilitiesContext] = None,
) -> Optional[sge.Expression]:
    """Build the WHERE condition for an Incremental cursor on `column_ref`.

    Args:
        incremental (Incremental): The incremental carrying cursor bounds, range, and
            `on_cursor_value_missing` policy.
        column_ref (sge.Column): Reference to the cursor column in the target query.
        sqlglot_type (Optional[sge.DataType]): SQLGlot data type used to CAST the
            bound literals; pass `None` to skip casting.
        destination_capabilities (Optional[DestinationCapabilitiesContext]): Caps used
            to shape timestamp literal format and CAST.

    Returns:
        Optional[sge.Expression]: A boolean expression ready to be attached via
            `.where(...)`, or `None`.

    Raises:
        ValueError: If `incremental.last_value_func` is not `min` or `max`, or if
            `on_cursor_value_missing` is not one of `"include"`, `"exclude"`, `"raise"`.
    """
    last_value_func = incremental.last_value_func
    start_op_cls: Type[sge.Binary]
    end_op_cls: Type[sge.Binary]
    if last_value_func is max:
        start_op_cls = sge.GTE if incremental.range_start == "closed" else sge.GT
        end_op_cls = sge.LT if incremental.range_end == "open" else sge.LTE
    elif last_value_func is min:
        start_op_cls = sge.LTE if incremental.range_start == "closed" else sge.LT
        end_op_cls = sge.GT if incremental.range_end == "open" else sge.GTE
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
    # XXX: Discard the upper here: when the cursor is bound and end_value isn't set,
    # resolve_bounds substitutes state["last_value"] as the upper.
    start_value, _ = incremental.resolve_bounds(apply_lag=True)
    end_value = incremental.end_value

    # caps-aware timestamp formatting
    if sqlglot_type is not None and SQLGLOT_TO_DLT_TYPE_MAP.get(sqlglot_type.this) == "timestamp":
        sqlglot_type, start_value, end_value = resolve_timestamp_cast(
            start_value, end_value, destination_capabilities
        )

    bounds: Optional[sge.Expression] = None
    if start_value is not None:
        start_literal = build_typed_literal(start_value, sqlglot_type)
        bounds = start_op_cls(this=column_ref.copy(), expression=start_literal)

    if end_value is not None:
        end_literal = build_typed_literal(end_value, sqlglot_type)
        end_condition: sge.Expression = end_op_cls(this=column_ref.copy(), expression=end_literal)
        bounds = end_condition if bounds is None else sge.And(this=bounds, expression=end_condition)

    if on_missing == "include":
        if bounds is None:
            return None
        is_null = sge.Is(this=column_ref.copy(), expression=sge.Null())
        return sge.Or(this=bounds, expression=is_null)

    # "exclude" or "raise" both pin nulls out via IS NOT NULL.
    # "raise" can't raise mid-query in SQL pushdown; so we warn users
    is_not_null = sge.Not(this=sge.Is(this=column_ref.copy(), expression=sge.Null()))
    if bounds is None:
        return is_not_null
    return sge.And(this=bounds, expression=is_not_null)


def _raise_incomplete_cursor_column(cursor_path: str, location_label: str) -> None:
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
        stacklevel=4,
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
