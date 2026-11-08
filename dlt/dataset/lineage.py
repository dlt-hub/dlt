import difflib
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple, cast

import sqlglot.expressions as sge

from sqlglot.errors import OptimizeError, SchemaError
from sqlglot.schema import Schema as SQLGlotSchema, ensure_schema
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.qualify_columns import Resolver
from sqlglot.optimizer.scope import Scope, traverse_scope

import dlt
from dlt.common.libs.sqlglot import (
    to_sqlglot_type,
    from_sqlglot_type,
    set_metadata,
    get_metadata,
    TSqlGlotDialect,
)
from dlt.common.schema.typing import (
    TTableSchemaColumns,
    TColumnSchema,
)
from dlt.common.typing import TypedDict

from dlt.dataset.exceptions import (
    LineageFailedException,
    TLineageFailureReason,
    TLineageSource,
    join_names,
)


class TLineageDiagnosis(TypedDict, total=False):
    """Cause of a lineage failure, passed as keyword arguments to `LineageFailedException`"""

    reason: TLineageFailureReason
    column: str
    qualifier: str
    sources: Dict[str, TLineageSource]
    suggestions: List[str]
    datasets: List[str]


def _qualify_dialect(dialect: TSqlGlotDialect) -> Optional[str]:
    """Dialect that keeps the case of dlt normalized identifiers when qualifying."""
    return f"{dialect}, normalization_strategy=case_sensitive" if dialect else None


def create_sqlglot_schema(
    schema_map: Mapping[str, Sequence[dlt.Schema]],
    dialect: TSqlGlotDialect,
) -> SQLGlotSchema:
    """Create an SQLGlot schema from multiple dlt schemas grouped by dataset name.

    Each key in `schema_map` becomes a top-level qualifier (SQL schema /
    catalog) that scopes all tables underneath it. Tables from multiple dlt
    schemas that share a dataset name are merged via `Schema.unify_schemas`;
    the first schema in each sequence is treated as the default and wins on
    column-level collisions.

    Args:
        schema_map: Mapping of dataset_name to a list of dlt schemas. The dataset name
            is used as the qualifying namespace in the generated SQLGlot
            schema.
        dialect: SQLGlot dialect for the target destination.
    """
    nested_schema: Dict[str, Dict[str, Any]] = {}

    for dataset_name, schemas in schema_map.items():
        if len(schemas) > 1:
            unified_schema = schemas[0].unify_schemas(list(schemas[1:]))
        elif schemas:
            unified_schema = schemas[0]
        else:
            continue

        sqlglot_tables: Dict[str, Any] = {}

        for table_name in unified_schema.tables.keys():
            column_mapping = {}

            for column_name, column in unified_schema.get_table_columns(
                table_name, include_incomplete=False
            ).items():
                sqlglot_type = to_sqlglot_type(
                    dlt_type=column["data_type"],
                    nullable=column.get("nullable"),
                    precision=column.get("precision"),
                    scale=column.get("scale"),
                    timezone=column.get("timezone"),
                )
                column_mapping[column_name] = set_metadata(sqlglot_type, column)

            if column_mapping:
                sqlglot_tables[table_name] = column_mapping

        nested_schema[dataset_name] = sqlglot_tables

    # keep case-sensitive so star-expansion doesn't re-fold already-normalized identifiers
    return ensure_schema(nested_schema, dialect=_qualify_dialect(dialect), normalize=False)


def _lenient_qualify(
    expression: sge.Query, sqlglot_schema: SQLGlotSchema, dialect: TSqlGlotDialect
) -> sge.Query:
    """Qualifies a copy of `expression` keeping the columns that sqlglot cannot resolve."""
    work = expression.copy()
    work.meta["case_sensitive"] = True
    return cast(
        sge.Query,
        qualify(
            work,
            schema=sqlglot_schema,
            dialect=_qualify_dialect(dialect),
            quote_identifiers=False,
            expand_stars=True,
            validate_qualify_columns=False,
            allow_partial_qualification=True,
        ),
    )


def _describe_sources(
    scope: Scope, sqlglot_schema: SQLGlotSchema, user_aliases: Set[str]
) -> Dict[str, TLineageSource]:
    """Labels the sources of `scope` and lists their columns, `None` when the schema does not know them."""
    resolver = Resolver(scope, sqlglot_schema, infer_schema=False)
    sources: Dict[str, TLineageSource] = {}
    for alias, source in scope.sources.items():
        if isinstance(source, sge.Table):
            label = f"table `{sge.table_name(source)}`"
            if alias != source.name:
                label += f" (alias `{alias}`)"
        elif source.is_cte:
            label = f"CTE `{alias}`"
        elif alias in user_aliases:
            label = f"subquery `{alias}`"
        else:
            # sqlglot names unaliased subqueries, the name is not in the query text
            label = "unaliased subquery"
        try:
            columns = list(resolver.get_source_columns(alias))
        except (OptimizeError, SchemaError):
            columns = []
        sources[alias] = (label, None if not columns or "*" in columns else columns)
    return sources


def _find_source(
    scope: Optional[Scope], qualifier: str, by_scope: Dict[int, Dict[str, TLineageSource]]
) -> Optional[TLineageSource]:
    """Looks up `qualifier` in `scope` and, for correlated subqueries, in the enclosing scopes."""
    while scope is not None:
        sources = by_scope.get(id(scope), {})
        if qualifier in sources:
            return sources[qualifier]
        if not scope.can_be_correlated:
            return None
        scope = scope.parent
    return None


def _diagnose_column(
    column: sge.Column,
    scope: Scope,
    sources: Dict[str, TLineageSource],
    by_scope: Dict[int, Dict[str, TLineageSource]],
) -> Optional[TLineageDiagnosis]:
    name = column.name
    if column.table:
        source = _find_source(scope, column.table, by_scope)
        if source is None:
            return TLineageDiagnosis(
                reason="unknown_qualifier",
                column=name,
                qualifier=column.table,
                sources=sources,
                suggestions=difflib.get_close_matches(column.table, list(sources)),
            )
        # sqlglot accepts qualified columns of tables it does not know
        if source[1] is None or name in source[1]:
            return None
        return TLineageDiagnosis(
            reason="unknown_column",
            column=name,
            qualifier=column.table,
            sources={column.table: source, **sources},
            suggestions=difflib.get_close_matches(name, source[1]),
        )
    owners = {alias: source for alias, source in sources.items() if source[1] and name in source[1]}
    if len(owners) > 1:
        return TLineageDiagnosis(reason="ambiguous_column", column=name, sources=owners)
    if owners:
        return None
    if any(columns is None for _, columns in sources.values()):
        return TLineageDiagnosis(reason="unknown_table", column=name, sources=sources)
    known = sorted({c for _, columns in sources.values() for c in columns or []})
    return TLineageDiagnosis(
        reason="column_not_found",
        column=name,
        sources=sources,
        suggestions=difflib.get_close_matches(name, known),
    )


def _diagnose_ambiguous_table(
    expression: sge.Query, sqlglot_schema: SQLGlotSchema
) -> Optional[TLineageDiagnosis]:
    """Finds an unqualified table that exists in more than one dataset of `sqlglot_schema`."""
    mapping: Dict[str, Dict[str, Any]] = getattr(sqlglot_schema, "mapping", {})
    cte_names = {cte.alias_or_name for cte in expression.find_all(sge.CTE)}
    for table in expression.find_all(sge.Table):
        if table.db or table.name in cte_names:
            continue
        datasets = [ds for ds, tables in mapping.items() if table.name in tables]
        if len(datasets) > 1:
            return TLineageDiagnosis(
                reason="ambiguous_table",
                qualifier=table.name,
                sources={table.name: (f"table `{table.name}`", None)},
                datasets=datasets,
            )
    return None


def describe_lineage_failure(
    expression: sge.Query,
    sqlglot_schema: SQLGlotSchema,
    dialect: TSqlGlotDialect,
    error: Optional[Exception] = None,
) -> TLineageDiagnosis:
    """Names the column, table or `*` that stops `expression` from resolving, `other` when it cannot."""
    # `error` is what `qualify` raised, `None` diagnoses a `*` that stayed unexpanded
    if isinstance(error, SchemaError):
        ambiguous = _diagnose_ambiguous_table(expression, sqlglot_schema)
        if ambiguous:
            return ambiguous
    try:
        work = _lenient_qualify(expression, sqlglot_schema, dialect)
    except (OptimizeError, SchemaError):
        return TLineageDiagnosis(reason="other")
    user_aliases = {node.alias for node in expression.find_all(sge.Subquery) if node.alias}
    scopes = [
        (scope, _describe_sources(scope, sqlglot_schema, user_aliases))
        for scope in traverse_scope(work)
        if isinstance(scope.expression, sge.Select)
    ]
    if not scopes:
        return TLineageDiagnosis(reason="other")
    by_scope = {id(scope): sources for scope, sources in scopes}
    if error is None:
        unknown = {
            alias: source
            for _, sources in scopes
            for alias, source in sources.items()
            if source[1] is None
        }
        return TLineageDiagnosis(reason="unresolved_star", sources=unknown or scopes[-1][1])
    for scope, sources in scopes:
        for column in scope.columns:
            diagnosis = _diagnose_column(column, scope, sources, by_scope)
            if diagnosis:
                return diagnosis
    return TLineageDiagnosis(reason="other")


def describe_unknown_tables(
    expression: sge.Query, schema_map: Mapping[str, Sequence[dlt.Schema]], default_dataset: str
) -> List[str]:
    """Explains the tables of `expression` that the dlt schemas in `schema_map` cannot type."""
    notes: List[str] = []
    cte_names = {cte.alias_or_name for cte in expression.find_all(sge.CTE)}
    for table in expression.find_all(sge.Table):
        if table.name in cte_names:
            continue
        dataset_name = table.db or default_dataset
        schemas = schema_map.get(dataset_name)
        if schemas is None:
            notes.append(
                f"Dataset `{dataset_name}` of table `{table.name}` is not a dataset of this"
                f" relation, known datasets: {join_names(list(schema_map))}."
            )
        elif not any(table.name in schema.tables for schema in schemas):
            known = sorted(
                {name for s in schemas for name in s.data_table_names(include_incomplete=True)}
            )
            close = difflib.get_close_matches(table.name, known)
            if close:
                hint = " Did you mean: " + ", ".join(f"`{c}`" for c in close) + "?"
            else:
                hint = f" Known tables: {join_names(known)}."
            notes.append(
                f"Table `{table.name}` is not in the schema of dataset `{dataset_name}`.{hint}"
            )
        elif not any(s.get_table_columns(table.name) for s in schemas if table.name in s.tables):
            notes.append(
                f"Table `{table.name}` exists in dataset `{dataset_name}` but none of its columns"
                " has a data type so lineage cannot see it. Load data into it or declare `columns`"
                " with data types."
            )
    return notes


def compute_columns_schema(
    expression: sge.Expression,
    sqlglot_schema: SQLGlotSchema,
    dialect: TSqlGlotDialect,
    allow_partial: bool = False,
) -> Tuple[TTableSchemaColumns, sge.Query]:
    """Compute the dlt columns schema of the output of an SQL SELECT query. No case-folding or
    quoting is performed on the query.

    Columns that come from tables unknown to `sqlglot_schema`, or whose type sqlglot cannot derive,
    have no `data_type`.

    Args:
        expression (sge.Expression): Parsed SQL query, with identifiers in the dlt schema
            namespace. It is not modified.
        sqlglot_schema (SQLGlotSchema): Schema of the tables the query reads, as created by
            `create_sqlglot_schema`. Provides column types and dlt column hints.
        dialect (TSqlGlotDialect): SQLGlot dialect used to qualify the query.
        allow_partial (bool): If True, columns that cannot be resolved have no `data_type` and a
            `*` over a table unknown to `sqlglot_schema` is skipped.

    Returns:
        Tuple[TTableSchemaColumns, sge.Query]: The dlt columns schema of the query output, keyed
            by output column name, and the qualified query with every column tied to its table,
            stars expanded and every projection aliased.

    Raises:
        LineageFailedException: If the query is not a SELECT or, unless `allow_partial` is set,
            a column cannot be resolved or a `*` selects from a table unknown to `sqlglot_schema`.
    """
    if not isinstance(expression, sge.Query):
        raise LineageFailedException(
            "Parsed SQL query is not a SELECT statement. Received SQL expression of type"
            f" {type(expression).__name__}.",
            reason="not_select",
            query=expression.sql(dialect=dialect),
            dialect=dialect,
        )

    # make sure we don't modify the original expression
    select_expression = expression.copy()
    # prevent normalization
    select_expression.meta["case_sensitive"] = True

    try:
        select_expression = cast(
            sge.Query,
            qualify(
                select_expression,
                schema=sqlglot_schema,
                dialect=_qualify_dialect(dialect),
                quote_identifiers=False,
                expand_stars=True,
                # keeps columns that cannot be tied to a table instead of raising
                validate_qualify_columns=not allow_partial,
                # tolerates `t.unknown` on a known table like an unqualified unknown column
                allow_partial_qualification=allow_partial,
            ),
        )
    except (OptimizeError, SchemaError) as e:
        raise LineageFailedException(
            str(e),
            query=expression.sql(dialect=dialect),
            dialect=dialect,
            **describe_lineage_failure(expression, sqlglot_schema, dialect, error=e),
        ) from e

    select_expression = annotate_types(select_expression, schema=sqlglot_schema)

    dlt_table_schema: dict[str, TColumnSchema] = {}
    for col in select_expression.selects:
        if col.output_name == "*":
            if allow_partial:
                continue
            raise LineageFailedException(
                "SELECT statement includes a `*` selection that can't be resolved."
                f" Column:\n\t{col}",
                query=expression.sql(dialect=dialect),
                dialect=dialect,
                **describe_lineage_failure(expression, sqlglot_schema, dialect),
            )

        data_type_hints = from_sqlglot_type(sqlglot_type=col.type)
        additional_hints = get_metadata(sqlglot_type=col.type)
        # get original name for queries that aliased the source dlt column
        propagated_name = additional_hints.pop("name", None)
        # NOTE dictionary unpacking order matters; unpacking `data_type_hints` last ensures precedence.
        dlt_table_schema[col.output_name] = {
            "name": col.output_name,
            **additional_hints,
            **data_type_hints,
        }
        if propagated_name and col.output_name != propagated_name:
            dlt_table_schema[col.output_name]["x-original-name"] = propagated_name  # type: ignore[typeddict-unknown-key]

    return dlt_table_schema, select_expression
