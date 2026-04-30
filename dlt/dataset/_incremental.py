from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import Any, Optional, Tuple, Type

import sqlglot.expressions as sge

from dlt.common.jsonpath import extract_simple_field_name
from dlt.common.libs.sqlglot import build_typed_literal, to_sqlglot_type
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.extract.incremental import Incremental


_INCREMENTAL_META_KEY = "dlt_incremental"
_AGG_CURSOR_ALIAS = "__dlt_inc_cursor"


@dataclass(frozen=True)
class _RelationIncrementalContext:
    """Private per-relation marker tying a `Relation` back to its `Incremental`.

    Set by `Relation.incremental()` and consumed by downstream lifecycle
    code (e.g. `dlthub` transformations) that needs to advance the cursor
    state after the relation executes.
    """

    incremental: Incremental[Any]
    cursor_column: sge.Column


def _build_incremental_aggregate(
    base_query: sge.Query,
    ctx: _RelationIncrementalContext,
) -> sge.Select:
    """Build `SELECT <func>(alias) FROM (SELECT cursor AS alias FROM <filtered>)`.

    The inner SELECT is rewritten to project the cursor under a stable alias so
    the outer aggregate can reference it without having to resolve the original
    table qualifier (which may live inside an auto-join).
    """
    inner = base_query.copy()
    # ORDER BY on the inner is meaningless for aggregation and some dialects
    # reject it inside a subquery without LIMIT.
    if inner.args.get("order") and inner.args.get("limit") is None:
        inner.set("order", None)

    inner.set(
        "expressions",
        [
            sge.Alias(
                this=ctx.cursor_column.copy(),
                alias=sge.to_identifier(_AGG_CURSOR_ALIAS, quoted=True),
            )
        ],
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

    outer_ref = sge.Column(this=sge.to_identifier(_AGG_CURSOR_ALIAS, quoted=True))
    return sge.Select(expressions=[agg_cls(this=outer_ref)]).from_(inner.subquery())


def _parse_incremental_cursor_path(cursor_path: str) -> Tuple[Optional[str], str]:
    """Split `table.column` into parts, or return `(None, column)` for a bare field.

    Rejects JSONPath expressions (wildcards, array indices, `$` root markers) that
    cannot be pushed down to SQL.
    """
    if not cursor_path:
        raise ValueError("Incremental `cursor_path` must be a non-empty string.")

    if any(ch in cursor_path for ch in ("$", "[", "*")):
        raise ValueError(
            f"Incremental `cursor_path={cursor_path!r}` is a JSONPath expression. "
            "`Relation.incremental()` only supports plain `column` or `table.column` cursors."
        )

    if "." in cursor_path:
        table_part, column_part = cursor_path.rsplit(".", 1)
        column_name = extract_simple_field_name(column_part)
        if column_name is None:
            raise ValueError(
                f"Incremental `cursor_path={cursor_path!r}` has a complex right-hand segment. "
                "Only simple identifiers are supported after the dot."
            )
        return table_part, column_name

    column_name = extract_simple_field_name(cursor_path)
    if column_name is None:
        raise ValueError(
            f"Incremental `cursor_path={cursor_path!r}` is not a plain column identifier."
        )
    return None, column_name


def _build_incremental_condition(
    incremental: Incremental[Any],
    column_ref: sge.Column,
    sqlglot_type: Optional[sge.DataType],
) -> sge.Expression:
    """Build the WHERE condition for an Incremental cursor on `column_ref`.

    Operator matrix (closed/open bounds):

    - `max` + closed start -> `>=`, open start -> `>`
    - `max` + open end -> `<`, closed end -> `<=`
    - `min` + closed start -> `<=`, open start -> `<`
    - `min` + open end -> `>`, closed end -> `>=`

    Args:
        incremental (Incremental): The incremental carrying cursor bounds, range, and
            `on_cursor_value_missing` policy.
        column_ref (sge.Column): Reference to the cursor column in the target query.
        sqlglot_type (Optional[sge.DataType]): SQLGlot data type used to CAST the
            bound literals; pass `None` to skip casting.

    Returns:
        sge.Expression: A boolean expression ready to be attached via `.where(...)`.
            The root node is tagged with `meta["dlt_incremental"] = True` so the
            caller can detect incremental filters later.

    Raises:
        ValueError: If `incremental.last_value_func` is not `min` or `max`.
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

    start_value = incremental.last_value
    end_value = incremental.end_value

    condition: Optional[sge.Expression] = None
    if start_value is not None:
        start_literal = build_typed_literal(start_value, sqlglot_type)
        condition = start_op_cls(this=column_ref.copy(), expression=start_literal)

    if end_value is not None:
        end_literal = build_typed_literal(end_value, sqlglot_type)
        end_condition: sge.Expression = end_op_cls(this=column_ref.copy(), expression=end_literal)
        condition = (
            end_condition
            if condition is None
            else sge.And(this=condition, expression=end_condition)
        )

    if condition is None:
        # no bounds at all: fall back to an always-true marker so the caller
        # still sees a WHERE clause tagged as incremental
        condition = sge.Boolean(this=True)

    if incremental.on_cursor_value_missing == "include":
        is_null = sge.Is(this=column_ref.copy(), expression=sge.Null())
        condition = sge.Or(this=condition, expression=is_null)
    elif incremental.on_cursor_value_missing in ("exclude", "raise"):
        # "raise" can't raise mid-query in SQL pushdown, so we do explicit IS NOT NULL
        # and warn users
        is_not_null = sge.Not(this=sge.Is(this=column_ref.copy(), expression=sge.Null()))
        condition = sge.And(this=condition, expression=is_not_null)
    else:
        raise ValueError(
            "Incremental `on_cursor_value_missing="
            f"{incremental.on_cursor_value_missing!r}` is not supported by "
            "`Relation.incremental()`. Expected one of: 'include', 'exclude', 'raise'."
        )

    condition.meta[_INCREMENTAL_META_KEY] = True
    return condition


def _has_incremental_marker(expression: sge.Expression) -> bool:
    """True when any node in `expression` carries the incremental meta marker."""
    for node in expression.walk():
        if node.meta.get(_INCREMENTAL_META_KEY):
            return True
    return False


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
        stacklevel=3,
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
