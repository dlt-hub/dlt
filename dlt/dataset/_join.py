from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING, Any, Iterable, NamedTuple, Optional, Sequence, Set, Union

import sqlglot
import sqlglot.expressions as sge
from sqlglot.errors import ParseError, TokenError

from dlt.common.typing import TypedDict
from dlt.common.schema import Schema, utils as schema_utils
from dlt.common.schema.typing import TTableReference, TTableSchemaColumns
from dlt.common.libs.sqlglot import TSqlGlotDialect

if TYPE_CHECKING:
    from dlt.dataset.relation import TJoinType

_INTERMEDIATE_JOIN_ALIAS_PREFIX = "_dlt_int_t"


class _JoinTarget(NamedTuple):
    """Resolved right-hand side of a `Relation.join()`."""

    dataset_name: str
    table_name: str
    columns: TTableSchemaColumns
    schemas: Sequence[Schema]
    subquery: Optional[sge.Query] = None
    """RHS query embedded as a derived table for transformed relations; `None` for base tables."""
    physical_dataset_name: Optional[str] = None
    """Physical (normalized) dataset name; set for foreign targets."""


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
        raise ValueError(
            f"Cannot join table `{left}` to itself via schema references. Use an explicit "
            "`on=` predicate and alias one side (e.g. via `query('SELECT * FROM ... AS alias')`) "
            "to self-join."
        )

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


def _from_source(query: sge.Query) -> Optional[sge.Expression]:
    """Return the FROM source expression (table or subquery), or `None`."""
    # sqlglot >= 28 renamed `from` to `from_` internally
    from_expr = query.args.get("from_") or query.args.get("from")
    if not isinstance(from_expr, sge.From):
        return None
    source: Optional[sge.Expression] = from_expr.this
    return source


def _source_qualifier(source: Optional[sge.Expression]) -> Optional[str]:
    """Return the SQL qualifier (alias or table name) of a FROM/JOIN source."""
    if isinstance(source, sge.Table):
        result = _extract_table_qualifier(source)
        return result[1] if result else None
    if isinstance(source, sge.Subquery):
        return _subquery_alias_name(source)
    return None


def _extract_joined_table_aliases(
    query: sge.Query, dataset_name: Optional[str] = None
) -> dict[str, str]:
    alias_map: dict[str, str] = {}
    tables: list[sge.Table] = []
    from_this = _from_source(query)
    if isinstance(from_this, sge.Table):
        tables.append(from_this)
    for join in query.args.get("joins") or []:
        if isinstance(join.this, sge.Table):
            tables.append(join.this)

    for table in tables:
        if dataset_name is not None and table.db and table.db != dataset_name:
            continue
        table_qualifier = _extract_table_qualifier(table)
        if not table_qualifier:
            continue
        table_name, qualifier = table_qualifier
        alias_map[table_name] = qualifier

    return alias_map


def _next_generated_alias_index(qualifiers: Iterable[str]) -> int:
    next_index = 1
    for qualifier in qualifiers:
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
    dataset_name: Optional[str] = None,
) -> tuple[list[_JoinParams], str]:
    """Discover join params from the schema reference chain."""
    # Full reference chain from `left_table` to `right_table`.
    refs = _resolve_reference_chain(schema, left_table, right_table)

    qualifier_map = _extract_joined_table_aliases(expression, dataset_name)
    if left_table not in qualifier_map:
        # the left base table may be embedded in a derived table; join via its alias
        if (
            not isinstance(_from_source(expression), sge.Subquery)
            or (left_qualifier := _left_source_qualifier(expression)) is None
        ):
            raise ValueError("Join query has no base table to resolve references.")
        qualifier_map[left_table] = left_qualifier

    attach_qualifier = qualifier_map[left_table]

    # Skip join steps whose target table is already present in the query.
    pending = [ref for ref in refs if ref["target_table"] not in qualifier_map]

    # Attach new joins to the most recent qualifier already present on the chain.
    for ref in refs:
        if ref["target_table"] in qualifier_map:
            attach_qualifier = qualifier_map[ref["target_table"]]

    used_qualifiers = _collect_source_qualifiers(expression)
    start_index = _next_generated_alias_index(used_qualifiers)
    last_pending_target = pending[-1]["target_table"] if pending else None

    joins: list[_JoinParams] = []
    for ref in pending:
        target_table = ref["target_table"]
        right_qualifier = target_table
        target_expr = sge.Table(this=sge.to_identifier(target_table, quoted=True))

        if target_table != last_pending_target or target_table in used_qualifiers:
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
        used_qualifiers.add(right_qualifier)
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
        else:
            expr_copy = expr.copy()
            # an unqualified column turns ambiguous once the JOIN adds a same-named column
            for col in expr_copy.find_all(sge.Column):
                if col.args.get("table") is None and col.parent_select is None:
                    col.set("table", origin_identifier.copy())
            normalized.append(expr_copy)
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


def _copy_as_select(expression: sge.Query) -> sge.Select:
    """Copy `expression` and assert it is a SELECT so a join can be applied."""
    query = expression.copy()
    if not isinstance(query, sge.Select):
        raise ValueError(f"Join query `{query}` must be an SQL SELECT statement.")
    return query


def _apply_join(
    expression: sge.Query,
    *,
    schema: Schema,
    left_table: str,
    right_table: str,
    projection_prefix: str,
    kind: TJoinType = "inner",
    project: bool = True,
    dataset_name: Optional[str] = None,
) -> sge.Select:
    """Apply schema-driven join(s) to `expression` and return the new query."""
    if left_table not in schema.tables:
        raise ValueError(f"Table `{left_table}` not found in dataset schema")
    if right_table not in schema.tables:
        raise ValueError(f"Table `{right_table}` not found in dataset schema")

    query = _copy_as_select(expression)

    left_source_qualifier = _left_source_qualifier(query) or left_table
    query = _seal_left_side(query, left_source_qualifier, kind)
    _qualify_unscoped_predicate_columns(query, left_source_qualifier)

    join_params, target_qualifier = _discover_join_params(
        query,
        schema=schema,
        left_table=left_table,
        right_table=right_table,
        dataset_name=dataset_name,
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
        query.set("expressions", _normalize_left_projection(query, left_source_qualifier))
    return query


def _qualify_unscoped_tables_with_dataset(expression: sge.Expression, dataset_name: str) -> None:
    """Set the logical `dataset_name` qualifier on table references that lack one.

    Skips CTE references; physical (normalized) dataset resolution happens later in `bind_query`.
    """
    cte_names = {cte.alias_or_name for cte in expression.find_all(sge.CTE)}
    db_identifier = sge.to_identifier(dataset_name, quoted=False)
    for table in expression.find_all(sge.Table):
        if table.name in cte_names:
            continue
        if table.args.get("db"):
            continue
        table.set("db", db_identifier.copy())


def _left_source_qualifier(query: sge.Query) -> Optional[str]:
    """Return the qualifier used to reference the FROM source (alias or table name)."""
    return _source_qualifier(_from_source(query))


def _collect_source_qualifiers(query: sge.Query) -> Set[str]:
    """Collect the SQL qualifiers (aliases or table names) of every FROM/JOIN source."""
    sources = [_from_source(query), *(join.this for join in query.args.get("joins") or [])]
    return {qualifier for source in sources if (qualifier := _source_qualifier(source)) is not None}


def _is_flat_select(query: sge.Select) -> bool:
    if any(
        query.args.get(key) for key in ("group", "having", "qualify", "distinct", "limit", "offset")
    ):
        return False
    return not any(sel.find(sge.AggFunc, sge.Window) for sel in query.selects)


def _qualify_unscoped_predicate_columns(query: sge.Select, source_qualifier: str) -> None:
    """Bind unqualified WHERE/ORDER BY columns to the single source.

    ORDER BY references to select output aliases stay bare; `bind_query` resolves them.
    """
    if query.args.get("joins"):
        return
    qualifier_identifier = sge.to_identifier(source_qualifier, quoted=False)
    output_aliases = {sel.output_name for sel in query.selects if isinstance(sel, sge.Alias)}
    for clause_key in ("where", "order"):
        clause = query.args.get(clause_key)
        if clause is None:
            continue
        for col in clause.find_all(sge.Column):
            if col.args.get("table") is not None or col.parent_select is not query:
                continue
            # ORDER BY resolves output aliases first; WHERE is pre-projection and sees columns
            if clause_key == "order" and col.name in output_aliases:
                continue
            col.set("table", qualifier_identifier.copy())


def _aliased_subquery(query: sge.Query, qualifier: str) -> sge.Subquery:
    """Wrap `query` as a derived table exposed under `qualifier`."""
    return sge.Subquery(
        this=query,
        alias=sge.TableAlias(this=sge.to_identifier(qualifier, quoted=False)),
    )


def _wrap_as_derived_table(query: sge.Select, qualifier: str) -> sge.Select:
    """Re-select all of `query`'s columns from it embedded as a derived table."""
    return (
        sge.Select()
        .select(sge.Column(table=sge.to_identifier(qualifier), this=sge.Star()))
        .from_(_aliased_subquery(query, qualifier))
    )


def _seal_left_side(query: sge.Select, left_source_qualifier: str, kind: TJoinType) -> sge.Select:
    """Seal the left side in a derived table when its rows must be fixed before the join.

    A non-flat left side (LIMIT/OFFSET/DISTINCT/GROUP/aggregate), or a WHERE that must precede a
    RIGHT/FULL join, would otherwise leak past the join and change which rows survive.
    """
    where_must_apply_before_join = kind in ("right", "full") and query.args.get("where") is not None
    if not _is_flat_select(query) or where_must_apply_before_join:
        return _wrap_as_derived_table(query, left_source_qualifier)
    return query


def _apply_explicit_join(
    expression: sge.Query,
    target: _JoinTarget,
    *,
    on: Union[str, sge.Expression],
    projection_prefix: str,
    kind: TJoinType,
    destination_dialect: TSqlGlotDialect,
    left_dataset_name: str,
) -> sge.Select:
    """Apply an explicit-ON join to `expression` and return the new query.

    Args:
        expression: Left-side query to join onto.
        target: Resolved right-hand side of the join.
        on: Join condition as a SQL string or sqlglot expression.
        projection_prefix: Prefix for appended column aliases.
        kind: SQL join type.
        destination_dialect: Dialect for parsing string ON expressions.
        left_dataset_name: Dataset name for the left-hand side.
    """
    query = _copy_as_select(expression)
    _qualify_unscoped_tables_with_dataset(query, left_dataset_name)

    from_this = _from_source(query)
    left_source_qualifier = _source_qualifier(from_this)
    if left_source_qualifier is None:
        raise ValueError(
            "Cannot apply explicit join: left-side query must have a named source "
            "in its FROM clause (a base table or an aliased derived table)."
        )

    query = _seal_left_side(query, left_source_qualifier, kind)

    _qualify_unscoped_predicate_columns(query, left_source_qualifier)

    target_qualifier = target.table_name
    if target_qualifier in _collect_source_qualifiers(query):
        raise ValueError(
            f"Join target qualifier `{target_qualifier}` already names a source in the query. "
            "Alias one side (e.g. via `query('SELECT * FROM ... AS alias')`) so each `on` "
            "qualifier is unambiguous."
        )

    target_expr: sge.Expression
    if target.subquery is not None:
        # transformed relation: embed its query as a subquery
        rhs_inner = target.subquery.copy()
        _qualify_unscoped_tables_with_dataset(rhs_inner, target.dataset_name)
        target_expr = _aliased_subquery(rhs_inner, target_qualifier)
    else:
        target_expr = sge.Table(
            this=sge.to_identifier(target.table_name, quoted=True),
            db=sge.to_identifier(target.dataset_name, quoted=False),
        )

    if isinstance(on, str):
        try:
            on_expr = sqlglot.parse_one(on, dialect=destination_dialect)
        except (ParseError, TokenError) as e:
            raise ValueError(f"Cannot parse `on` join condition `{on}`: {e}") from e
    else:
        on_expr = on
    if not isinstance(on_expr, sge.Condition):
        raise ValueError(
            f"`on` join condition `{on_expr.sql(destination_dialect)}` must be an SQL boolean"
            " expression (e.g. `left.col = right.col`)."
        )

    join_expr = sge.Join(this=target_expr, kind=kind.upper()).on(on_expr)
    query = query.join(join_expr)

    _apply_join_projection(
        query,
        left_source_qualifier=left_source_qualifier,
        target_columns=target.columns,
        target_qualifier=target_qualifier,
        projection_prefix=projection_prefix,
        allow_existing_target_projection=False,
    )
    return query
