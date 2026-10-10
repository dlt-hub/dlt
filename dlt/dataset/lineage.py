from typing import Any, Dict, Mapping, Sequence, Tuple, cast

import sqlglot.expressions as sge

from sqlglot.errors import OptimizeError, SchemaError
from sqlglot.schema import Schema as SQLGlotSchema, ensure_schema
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify import qualify

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

from dlt.dataset.exceptions import LineageFailedException


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
    return ensure_schema(
        nested_schema,
        dialect=f"{dialect}, normalization_strategy=case_sensitive" if dialect else dialect,
        normalize=False,
    )


def compute_columns_schema(
    expression: sge.Expression,
    sqlglot_schema: SQLGlotSchema,
    dialect: TSqlGlotDialect,
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

    Returns:
        Tuple[TTableSchemaColumns, sge.Query]: The dlt columns schema of the query output, keyed
            by output column name, and the qualified query with every column tied to its table,
            stars expanded and every projection aliased.

    Raises:
        LineageFailedException: If the query is not a SELECT, a column cannot be resolved or a `*`
            selects from a table unknown to `sqlglot_schema`.
    """
    if not isinstance(expression, sge.Query):
        raise LineageFailedException(
            "Parsed SQL query is not a SELECT statement. Received SQL expression of type"
            f" {expression.type}.",
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
                dialect=f"{dialect}, normalization_strategy=case_sensitive" if dialect else None,
                quote_identifiers=False,
                expand_stars=True,
            ),
        )
    except (OptimizeError, SchemaError) as e:
        raise LineageFailedException(
            f"Failed to resolve SQL query against the schema received: {e}"
        ) from e

    select_expression = annotate_types(select_expression, schema=sqlglot_schema)

    dlt_table_schema: dict[str, TColumnSchema] = {}
    for col in select_expression.selects:
        if col.output_name == "*":
            raise LineageFailedException(
                "SELECT statement includes a `*` selection that can't be resolved. Modify the"
                " query to select columns explicitly or limit `*` to known tables (e.g., `SELECT"
                f" known_table.*`).\nColumn:\n\t{col}",
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
