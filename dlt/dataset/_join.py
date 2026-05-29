from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING, Any, Optional, Sequence, Set, TypeVar, Union

import sqlglot
import sqlglot.expressions as sge

from dlt.common.typing import TypedDict
from dlt.common.schema import Schema, utils as schema_utils
from dlt.common.schema.typing import TTableReference, TTableSchemaColumns
from dlt.common.libs.sqlglot import TSqlGlotDialect

if TYPE_CHECKING:
    from dlt.dataset.relation import Relation, TJoinType

_INTERMEDIATE_JOIN_ALIAS_PREFIX = "_dlt_int_t"
_EXPLICIT_JOIN_ALIAS_PREFIX = "_dlt_jt_"

_TExpr = TypeVar("_TExpr", bound=sge.Expression)


def _is_internal_join_alias(qualifier: str) -> bool:
    return qualifier.startswith((_INTERMEDIATE_JOIN_ALIAS_PREFIX, _EXPLICIT_JOIN_ALIAS_PREFIX))


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


def _identifier_name(node: Any) -> Optional[str]:
    """Return the string name of an sqlglot identifier-or-string node."""
    if isinstance(node, sge.Identifier):
        return node.name
    if isinstance(node, str):
        return node
    return None


def _subquery_alias_name(subquery: sge.Subquery) -> Optional[str]:
    """Return the alias name of a subquery, or `None`."""
    alias_expr = subquery.args.get("alias")
    if not isinstance(alias_expr, sge.TableAlias):
        return None
    return _identifier_name(alias_expr.this)


def _extract_table_qualifier(table_expr: sge.Expression) -> Optional[tuple[str, str]]:
    if not isinstance(table_expr, sge.Table):
        return None

    table_name = _identifier_name(table_expr.args.get("this"))
    if table_name is None:
        return None

    alias_expr = table_expr.args.get("alias")
    if isinstance(alias_expr, sge.TableAlias):
        alias_name = _identifier_name(alias_expr.this)
        if alias_name is not None:
            return table_name, alias_name

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


def _normalize_left_projection(
    query: sge.Select, left_source_qualifier: str
) -> list[sge.Expression]:
    """Qualify the left-side projection so an added JOIN cannot leak right-side columns."""
    origin_identifier = sge.to_identifier(left_source_qualifier, quoted=False)
    normalized: list[sge.Expression] = []
    for expr in query.selects:
        if isinstance(expr, sge.Star):
            normalized.append(sge.Column(table=origin_identifier.copy(), this=sge.Star()))
        elif isinstance(expr, sge.Column) and expr.args.get("table") is None:
            expr_copy = expr.copy()
            expr_copy.set("table", origin_identifier.copy())
            normalized.append(expr_copy)
        else:
            normalized.append(expr)
    return normalized


def _apply_join_projection(
    query: sge.Select,
    *,
    left_source_qualifier: str,
    target_columns: TTableSchemaColumns,
    target_qualifier: str,
    projection_prefix: str,
    allow_existing_target_projection: bool,
) -> None:
    """Apply join projection contract onto `query`.

    Preserves the left-side projection and appends only columns from the
    joined target as `{projection_prefix}__{column}` aliases.

    `allow_existing_target_projection` is used for idempotent re-joins: when a
    join call contributes no new join edges, all target-prefixed columns may already
    exist in the left projection and should be accepted as a no-op instead of raising
    a collision error.
    """
    normalized_left_expressions = _normalize_left_projection(query, left_source_qualifier)

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
    project: bool = True,
) -> sge.Select:
    """Apply schema-driven join(s) to `expression` and return the new query."""
    # `project=False` adds the JOIN without touching the SELECT list — for join targets whose
    # columns are referenced in WHERE/ON predicates but should not appear in the output
    if left_table not in schema.tables:
        raise ValueError(f"Table `{left_table}` not found in dataset schema")
    if right_table not in schema.tables:
        raise ValueError(f"Table `{right_table}` not found in dataset schema")

    query = expression.copy()
    if not isinstance(query, sge.Select):
        raise ValueError(f"Join query `{query}` must be an SQL SELECT statement.")

    # qualify its bare WHERE/ORDER BY columns so they survive a later join
    # that introduces a same-named column
    if not query.args.get("joins"):
        _qualify_unscoped_predicate_columns(query, _left_source_qualifier(query) or left_table)

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

    left_source_qualifier = _left_source_qualifier(query) or left_table

    if project:
        _apply_join_projection(
            query,
            left_source_qualifier=left_source_qualifier,
            target_columns=schema.get_table_columns(right_table),
            target_qualifier=target_qualifier,
            projection_prefix=projection_prefix,
            allow_existing_target_projection=not join_params,
        )
    else:
        # filter-only join: qualify the left projection so a bare `*` does not
        # expand across the joined table and leak right-side columns at runtime.
        query.set("expressions", _normalize_left_projection(query, left_source_qualifier))
    return query


def _qualify_physical_tables_with_dataset(expression: _TExpr, dataset_name: str) -> _TExpr:
    """Bind every physical table reference in ``expression`` to ``dataset_name``."""
    expression = expression.copy()
    cte_names = {cte.alias_or_name for cte in expression.find_all(sge.CTE)}
    db_identifier = sge.to_identifier(dataset_name, quoted=False)
    for table in expression.find_all(sge.Table):
        if table.name in cte_names:
            continue
        if table.args.get("db"):
            continue
        table.set("db", db_identifier.copy())
    return expression


def _left_source_qualifier(query: sge.Query) -> Optional[str]:
    """Return the qualifier used to reference the FROM source (alias or table name)."""
    from_expr = query.args.get("from_") or query.args.get("from")
    if not isinstance(from_expr, sge.From):
        return None
    from_this = from_expr.this
    if isinstance(from_this, sge.Table):
        result = _extract_table_qualifier(from_this)
        return result[1] if result else None
    if isinstance(from_this, sge.Subquery):
        return _subquery_alias_name(from_this)
    return None


def _existing_source_qualifier_map(query: sge.Query) -> dict[str, str]:
    """Map each existing join input's user-facing qualifier to its SQL qualifier."""
    qualifier_map: dict[str, str] = {}
    sources: list[sge.Expression] = []

    from_expr = query.args.get("from_") or query.args.get("from")
    if isinstance(from_expr, sge.From) and from_expr.this is not None:
        sources.append(from_expr.this)

    for join in query.args.get("joins") or []:
        if join.this is not None:
            sources.append(join.this)

    for source in sources:
        if isinstance(source, sge.Table):
            result = _extract_table_qualifier(source)
            if not result:
                continue
            table_name, sql_qualifier = result
            if sql_qualifier == table_name or _is_internal_join_alias(sql_qualifier):
                # unaliased or internally aliased: the user references it by table name
                qualifier_map[table_name] = sql_qualifier
            else:
                # an explicit alias replaces the table name as the usable qualifier
                qualifier_map[sql_qualifier] = sql_qualifier
        elif isinstance(source, sge.Subquery):
            sql_qualifier = _subquery_alias_name(source)
            if sql_qualifier is None:
                continue
            source_qualifier = _left_source_qualifier(source.this) or sql_qualifier
            qualifier_map[source_qualifier] = sql_qualifier

    return qualifier_map


def _bind_on_predicate(
    on_expr: sge.Expression,
    *,
    existing_qualifier_map: dict[str, str],
    new_right_qualifiers: Set[str],
    new_right_alias: str,
) -> sge.Expression:
    """Rewrite column qualifiers in ``on_expr`` to the SQL qualifiers of the join inputs."""
    on_expr = on_expr.copy()
    for col in on_expr.find_all(sge.Column):
        table_node = col.args.get("table")
        if not isinstance(table_node, sge.Identifier):
            continue
        qualifier = table_node.name
        in_existing = qualifier in existing_qualifier_map
        in_new = qualifier in new_right_qualifiers
        if in_existing and in_new:
            raise ValueError(
                f"Ambiguous qualifier `{qualifier}` in join `on` expression: it matches both "
                "the left and right side of the join. Alias one side (e.g. via `query(...)` "
                "or the join `alias`) so each `on` qualifier is unambiguous."
            )
        if in_new:
            col.set("table", sge.to_identifier(new_right_alias, quoted=False))
        elif in_existing:
            col.set("table", sge.to_identifier(existing_qualifier_map[qualifier], quoted=False))
    return on_expr


def _qualify_unscoped_predicate_columns(query: sge.Select, source_qualifier: str) -> None:
    """Bind unqualified columns in pre-join WHERE/ORDER BY clauses to the single source."""
    qualifier_identifier = sge.to_identifier(source_qualifier, quoted=False)
    for clause_key in ("where", "order"):
        clause = query.args.get(clause_key)
        if clause is None:
            continue
        for col in clause.find_all(sge.Column):
            if col.args.get("table") is None and col.parent_select is query:
                col.set("table", qualifier_identifier.copy())


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
    left_dataset_name: str,
) -> sge.Select:
    """Apply an explicit-ON join to ``expression`` and return the new query.

    Args:
        expression: Left-side query to join onto.
        target: Right-hand Relation object (if transformed/subquery), or None for
            string / base-table targets.
        target_table: Bare table name for schema lookups and projection.
        target_dataset_name: Dataset name for the right-hand side.
        target_columns: Columns from the right-hand side for projection.
        on: Join condition as a SQL string or sqlglot expression.
        projection_prefix: Prefix for appended column aliases.
        kind: SQL join type.
        destination_dialect: Dialect for parsing string ON expressions.
        left_dataset_name: Dataset name for the left-hand side.
    """
    query = expression.copy()
    if not isinstance(query, sge.Select):
        raise ValueError(f"Join query `{query}` must be an SQL SELECT statement.")

    # bind LHS physical tables to the LHS dataset before composing the join.
    # otherwise, adding the RHS dataset to the resolver makes bare LHS tables
    # ambiguous
    query = _qualify_physical_tables_with_dataset(query, left_dataset_name)

    from_expr = query.args.get("from_") or query.args.get("from")
    if not isinstance(from_expr, sge.From) or not isinstance(from_expr.this, sge.Table):
        raise ValueError(
            "Cannot apply explicit join: left-side query must have a base table "
            "in its FROM clause (not a subquery or derived table)."
        )
    left_source_qualifier = _left_source_qualifier(query) or from_expr.this.name

    if not query.args.get("joins"):
        _qualify_unscoped_predicate_columns(query, left_source_qualifier)

    internal_alias = f"{_EXPLICIT_JOIN_ALIAS_PREFIX}{projection_prefix}"

    target_expr: sge.Expression
    if target is not None and target._query is not None:
        # transformed Relation: embed as subquery
        rhs_inner = target.sqlglot_expression
        if target_dataset_name:
            rhs_inner = _qualify_physical_tables_with_dataset(rhs_inner, target_dataset_name)
        target_expr = sge.Subquery(
            this=rhs_inner,
            alias=sge.TableAlias(this=sge.to_identifier(internal_alias, quoted=False)),
        )
    else:
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

    on_expr = _bind_on_predicate(
        on_expr,
        existing_qualifier_map=_existing_source_qualifier_map(query),
        new_right_qualifiers={target_table, projection_prefix},
        new_right_alias=internal_alias,
    )

    join_expr = sge.Join(this=target_expr, kind=kind.upper()).on(on_expr)
    query = query.join(join_expr)

    _apply_join_projection(
        query,
        left_source_qualifier=left_source_qualifier,
        target_columns=target_columns,
        target_qualifier=internal_alias,
        projection_prefix=projection_prefix,
        allow_existing_target_projection=False,
    )
    return query
