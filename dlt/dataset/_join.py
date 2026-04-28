from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING, Optional, Sequence, Union

import sqlglot
import sqlglot.expressions as sge

from dlt.common.typing import TypedDict
from dlt.common.schema import Schema, utils as schema_utils
from dlt.common.schema.typing import TTableReference, TTableSchemaColumns
from dlt.common.libs.sqlglot import TSqlGlotDialect

if TYPE_CHECKING:
    from dlt.dataset.relation import Relation, TJoinType

_INTERMEDIATE_JOIN_ALIAS_PREFIX = "_dlt_int_t"


class _JoinRef(TypedDict):
    """A resolved join step from currently attached table to a target table."""

    target_table: str
    on_pairs: tuple[tuple[str, str], ...]
    """(existing_side_col, new_side_col) pairs for the ON clause."""


class _JoinParams(TypedDict):
    target: sge.Expression
    on: tuple[tuple[str, str], ...]
    left_qualifier: str
    right_qualifier: str


def _to_join_ref(ref: TTableReference, from_table: str) -> _JoinRef:
    if "table" not in ref or ref["table"] is None or "referenced_table" not in ref:
        raise ValueError(
            f"Malformed table reference for join: {ref} - missing 'table' or 'referenced_table'"
        )
    columns = ref.get("columns", [])
    referenced_columns = ref.get("referenced_columns", [])
    if not columns or not referenced_columns or len(columns) != len(referenced_columns):
        raise ValueError(
            f"Malformed table reference for join: {ref} - 'columns' or 'referenced_columns' are"
            " empty"
        )

    if from_table == ref["table"]:
        return _JoinRef(
            target_table=ref["referenced_table"],
            on_pairs=tuple(zip(columns, referenced_columns)),
        )

    if from_table == ref["referenced_table"]:
        return _JoinRef(
            target_table=ref["table"],
            on_pairs=tuple(zip(referenced_columns, columns)),
        )

    raise ValueError(
        f"Malformed table reference for join: {ref} - table `{from_table}` is not connected"
    )


def _resolve_parent_reference_chain(schema: Schema, left: str, right: str) -> list[_JoinRef]:
    """Resolve ordered join steps between ancestor/descendant tables only."""

    upward_chain_from_left = [
        TTableReference(**ref)
        for ref in schema_utils.get_all_parent_references_to_root(schema.tables, left)
        if "table" in ref and "referenced_table" in ref
    ]
    upward_chain_from_right = [
        TTableReference(**ref)
        for ref in schema_utils.get_all_parent_references_to_root(schema.tables, right)
        if "table" in ref and "referenced_table" in ref
    ]

    # Case 1: right is an ancestor of left (walk up from left to right)
    current_left = left
    steps_to_ancestor: list[_JoinRef] = []
    for ref in upward_chain_from_left:
        step = _to_join_ref(ref, current_left)
        steps_to_ancestor.append(step)
        current_left = step["target_table"]
        if current_left == right:
            return steps_to_ancestor

    # Case 2: left is an ancestor of right (walk down from left to right)
    ancestor_index = next(
        (
            index
            for index, ref in enumerate(upward_chain_from_right)
            if ref["referenced_table"] == left
        ),
        None,
    )
    if ancestor_index is not None:
        current = left
        steps_from_ancestor: list[_JoinRef] = []
        upward_segment_to_ancestor = upward_chain_from_right[: ancestor_index + 1]
        for ref in reversed(upward_segment_to_ancestor):
            step = _to_join_ref(ref, current)
            steps_from_ancestor.append(step)
            current = step["target_table"]
        if current == right:
            return steps_from_ancestor

    raise ValueError(f"Unable to resolve reference chain between {left} and {right}")


def _resolve_reference_chain(schema: Schema, left: str, right: str) -> list[_JoinRef]:
    """Resolve ordered join steps between two tables."""
    if left == right:
        raise ValueError(f"Cannot join a table to itself: {left}")

    # Check direct references first
    for ref in schema.references:
        if (ref.get("table") == left and ref.get("referenced_table") == right) or (
            ref.get("table") == right and ref.get("referenced_table") == left
        ):
            return [_to_join_ref(TTableReference(**ref), left)]

    # Fall back to parent-child reference chain
    return _resolve_parent_reference_chain(schema, left, right)


def _build_join_condition_from_pairs(
    column_pairs: Sequence[tuple[str, str]],
    *,
    left_alias: str,
    right_alias: str,
) -> sge.Expression:
    """Build join ON condition from explicit column pairs."""
    if not column_pairs:
        raise ValueError("Cannot build join condition from empty column pairs")

    conditions: list[sge.Expression] = []

    for left_col, right_col in column_pairs:
        condition = sge.EQ(
            this=sge.Column(
                this=sge.to_identifier(left_col, quoted=True),
                table=sge.to_identifier(left_alias, quoted=False),
            ),
            expression=sge.Column(
                this=sge.to_identifier(right_col, quoted=True),
                table=sge.to_identifier(right_alias, quoted=False),
            ),
        )
        conditions.append(condition)
    if len(conditions) == 1:
        return conditions[0]
    return reduce(lambda x, y: sge.And(this=x, expression=y), conditions)


def _extract_table_qualifier(table_expr: sge.Expression) -> Optional[tuple[str, str]]:
    if not isinstance(table_expr, sge.Table):
        return None

    table_identifier = table_expr.args.get("this")
    if isinstance(table_identifier, sge.Identifier):
        table_name = table_identifier.name
    elif isinstance(table_identifier, str):
        table_name = table_identifier
    else:
        return None

    alias_expr = table_expr.args.get("alias")
    if isinstance(alias_expr, sge.TableAlias):
        alias_identifier = alias_expr.this
        if isinstance(alias_identifier, sge.Identifier):
            return table_name, alias_identifier.name
        if isinstance(alias_identifier, str):
            return table_name, alias_identifier

    return table_name, table_name


def _extract_joined_table_aliases(query: sge.Query) -> dict[str, str]:
    alias_map: dict[str, str] = {}
    # sqlglot >= 28 renamed `from` to `from_` internally
    from_expr = query.args.get("from_") or query.args.get("from")
    if not isinstance(from_expr, sge.From) or not isinstance(from_expr.this, sge.Table):
        return alias_map

    tables: list[sge.Table] = [from_expr.this]
    for join in query.args.get("joins") or []:
        if isinstance(join.this, sge.Table):
            tables.append(join.this)

    for table in tables:
        table_qualifier = _extract_table_qualifier(table)
        if not table_qualifier:
            continue
        table_name, qualifier = table_qualifier
        alias_map[table_name] = qualifier

    return alias_map


def _next_generated_alias_index(qualifier_map: dict[str, str]) -> int:
    next_index = 1
    for qualifier in qualifier_map.values():
        if qualifier.startswith(_INTERMEDIATE_JOIN_ALIAS_PREFIX):
            alias_index = qualifier[len(_INTERMEDIATE_JOIN_ALIAS_PREFIX) :]
            if alias_index.isdigit():
                next_index = max(next_index, int(alias_index) + 1)
    return next_index


def _discover_join_params(
    expression: sge.Query,
    *,
    schema: Schema,
    left_table: str,
    right_table: str,
) -> tuple[list[_JoinParams], str]:
    """Discover join params from the schema reference chain."""
    # Full reference chain from `left_table` to `right_table`.
    refs = _resolve_reference_chain(schema, left_table, right_table)

    qualifier_map = _extract_joined_table_aliases(expression)
    if left_table not in qualifier_map:
        raise ValueError("Join query has no base table to resolve references.")

    attach_qualifier = qualifier_map[left_table]

    # Skip join steps whose target table is already present in the query.
    pending = [ref for ref in refs if ref["target_table"] not in qualifier_map]

    # Attach new joins to the most recent qualifier already present on the chain.
    for ref in refs:
        if ref["target_table"] in qualifier_map:
            attach_qualifier = qualifier_map[ref["target_table"]]

    start_index = _next_generated_alias_index(qualifier_map)
    # last pending target is the target table (right) and shouldn't get aliased later
    last_pending_target = pending[-1]["target_table"] if pending else None

    joins: list[_JoinParams] = []
    for ref in pending:
        target_table = ref["target_table"]
        right_qualifier = target_table
        target_expr = sge.Table(this=sge.to_identifier(target_table, quoted=True))

        if target_table != last_pending_target:
            generated_alias = f"{_INTERMEDIATE_JOIN_ALIAS_PREFIX}{start_index}"
            target_expr = sge.Table(
                this=sge.to_identifier(target_table, quoted=True),
                alias=sge.TableAlias(this=sge.to_identifier(generated_alias, quoted=False)),
            )
            right_qualifier = generated_alias
            start_index += 1

        joins.append(
            _JoinParams(
                target=target_expr,
                on=ref["on_pairs"],
                left_qualifier=attach_qualifier,
                right_qualifier=right_qualifier,
            )
        )
        qualifier_map[target_table] = right_qualifier
        attach_qualifier = right_qualifier

    target_qualifier = qualifier_map[right_table]
    return joins, target_qualifier


def _apply_join_projection(
    query: sge.Select,
    *,
    left_table: str,
    target_columns: TTableSchemaColumns,
    target_qualifier: str,
    projection_prefix: str,
    allow_existing_target_projection: bool,
) -> None:
    """Apply join projection contract onto ``query``.

    Preserves the left-side projection and appends only columns from the
    joined target as ``{projection_prefix}__{column}`` aliases.

    ``allow_existing_target_projection`` is used for idempotent re-joins: when a
    join call contributes no new join edges, all target-prefixed columns may already
    exist in the left projection and should be accepted as a no-op instead of raising
    a collision error.
    """
    # Unbound columns must refer to the origin table so bind them to it
    origin_identifier = sge.to_identifier(left_table, quoted=False)
    normalized_left_expressions: list[sge.Expression] = []
    for expr in query.selects:
        if isinstance(expr, sge.Star):
            normalized_left_expressions.append(sge.Column(table=origin_identifier, this=sge.Star()))
        elif isinstance(expr, sge.Column) and expr.args.get("table") is None:
            expr_copy = expr.copy()
            expr_copy.set("table", origin_identifier)
            normalized_left_expressions.append(expr_copy)
        else:
            normalized_left_expressions.append(expr)

    existing_projection_column_names = {
        expr.output_name
        for expr in normalized_left_expressions
        if expr.output_name not in {"", "*"}
    }

    target_output_names = {
        f"{projection_prefix}__{column_name}" for column_name in target_columns.keys()
    }
    duplicate_output_names = target_output_names & existing_projection_column_names
    if duplicate_output_names:
        if duplicate_output_names == target_output_names and allow_existing_target_projection:
            # no-op: all target columns are already projected (on duplicate join call for example)
            return
        duplicate_names_list = ", ".join(sorted(duplicate_output_names))
        raise ValueError(
            "Join projection output names conflict with existing columns: "
            f"{duplicate_names_list}. Choose a different `alias` for `join(...)`."
        )

    appended_target_columns: list[sge.Expression] = []
    for column_name in target_columns.keys():
        output_name = f"{projection_prefix}__{column_name}"
        appended_target_columns.append(
            sge.Alias(
                this=sge.Column(
                    table=sge.to_identifier(target_qualifier, quoted=False),
                    this=sge.to_identifier(column_name, quoted=True),
                ),
                alias=sge.to_identifier(output_name, quoted=True),
            )
        )

    query.set("expressions", [*normalized_left_expressions, *appended_target_columns])


def _apply_join(
    expression: sge.Query,
    *,
    schema: Schema,
    left_table: str,
    right_table: str,
    projection_prefix: str,
    kind: TJoinType = "inner",
) -> sge.Select:
    """Apply schema-driven join(s) to ``expression`` and return the new query."""
    if left_table not in schema.tables:
        raise ValueError(f"Table `{left_table}` not found in dataset schema")
    if right_table not in schema.tables:
        raise ValueError(f"Table `{right_table}` not found in dataset schema")

    query = expression.copy()
    if not isinstance(query, sge.Select):
        raise ValueError(f"Join query `{query}` must be an SQL SELECT statement.")

    join_params, target_qualifier = _discover_join_params(
        query,
        schema=schema,
        left_table=left_table,
        right_table=right_table,
    )

    for join_param in join_params:
        join_expr = sge.Join(
            this=join_param["target"],
            kind=kind.upper(),
        ).on(
            _build_join_condition_from_pairs(
                join_param["on"],
                left_alias=join_param["left_qualifier"],
                right_alias=join_param["right_qualifier"],
            )
        )
        query = query.join(join_expr)

    _apply_join_projection(
        query,
        left_table=left_table,
        target_columns=schema.tables[right_table]["columns"],
        target_qualifier=target_qualifier,
        projection_prefix=projection_prefix,
        allow_existing_target_projection=not join_params,
    )
    return query


def _rewrite_on_qualifiers(
    on_expr: sge.Expression,
    target_table: str,
    internal_alias: str,
) -> sge.Expression:
    """Rewrite column qualifiers in the ON expression that reference the target table.

    The user writes ``on="users.id = orders.user_id"`` using logical table names.
    Once the target is aliased internally, those references must point to the alias
    so the SQL engine can resolve them.
    """
    on_expr = on_expr.copy()
    for col in on_expr.find_all(sge.Column):
        table_node = col.args.get("table")
        if isinstance(table_node, sge.Identifier) and table_node.name == target_table:
            table_node.set("this", internal_alias)
    return on_expr


def _apply_explicit_join(
    expression: sge.Query,
    *,
    target: Optional["Relation"] = None,
    target_table: str,
    target_dataset_name: Optional[str],
    target_columns: TTableSchemaColumns,
    on: Union[str, sge.Expression],
    projection_prefix: str,
    kind: "TJoinType",
    destination_dialect: TSqlGlotDialect,
) -> sge.Select:
    """Apply an explicit-ON join to ``expression`` and return the new query.

    Args:
        expression: Left-side query to join onto.
        target: Right-hand Relation object (if transformed/subquery), or None for
            string / base-table targets.
        target_table: Bare table name for schema lookups and projection.
        target_dataset_name: Foreign dataset qualifier, or None for local.
        target_columns: Columns from the right-hand side for projection.
        on: Join condition as a SQL string or sqlglot expression.
        projection_prefix: Prefix for appended column aliases.
        kind: SQL join type.
        destination_dialect: Dialect for parsing string ON expressions.
    """
    query = expression.copy()
    if not isinstance(query, sge.Select):
        raise ValueError(f"Join query `{query}` must be an SQL SELECT statement.")

    internal_alias = f"_dlt_jt_{projection_prefix}"

    # build target expression
    target_expr: sge.Expression
    if target is not None:
        # transformed Relation -> subquery (preserves WHERE, SELECT, etc.)
        target_expr = sge.Subquery(
            this=target.sqlglot_expression,
            alias=sge.TableAlias(this=sge.to_identifier(internal_alias, quoted=False)),
        )
    else:
        # base-table target (Relation with _table_name, or str)
        table_node_args: dict[str, sge.Expression] = {
            "this": sge.to_identifier(target_table, quoted=True),
            "alias": sge.TableAlias(this=sge.to_identifier(internal_alias, quoted=False)),
        }
        if target_dataset_name:
            table_node_args["db"] = sge.to_identifier(target_dataset_name, quoted=False)
        target_expr = sge.Table(**table_node_args)

    if isinstance(on, str):
        on_expr = sqlglot.parse_one(on, dialect=destination_dialect)
    else:
        on_expr = on

    on_expr = _rewrite_on_qualifiers(on_expr, target_table, internal_alias)

    join_expr = sge.Join(this=target_expr, kind=kind.upper()).on(on_expr)
    query = query.join(join_expr)

    from_expr = query.args.get("from_") or query.args.get("from")
    if not isinstance(from_expr, sge.From) or not isinstance(from_expr.this, sge.Table):
        raise ValueError(
            "Cannot apply explicit join: left-side query must have a base table "
            "in its FROM clause (not a subquery or derived table)."
        )
    left_table = from_expr.this.this.name

    _apply_join_projection(
        query,
        left_table=left_table,
        target_columns=target_columns,
        target_qualifier=internal_alias,
        projection_prefix=projection_prefix,
        allow_existing_target_projection=False,
    )
    return query
