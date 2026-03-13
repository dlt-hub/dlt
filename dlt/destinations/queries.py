from functools import partial
from typing import Any, List

import sqlglot.expressions as sge
from sqlglot.schema import Schema as SQLGlotSchema

from dlt.common.destination.capabilities import TCasefoldIdentifier
from dlt.common.libs.sqlglot import bind_query
from dlt.destinations.sql_client import SqlClientBase


def _normalize_query(
    qualified_query: sge.Query,
    sqlglot_schema: SQLGlotSchema,
    *,
    sql_client: SqlClientBase[Any],
    casefold_identifier: TCasefoldIdentifier,
) -> sge.Query:
    """Backward compatibility wrapper for bind_query.

    TODO: remove after next dlthub release
    """
    return bind_query(
        qualified_query,
        sqlglot_schema,
        expand_table_name=partial(
            sql_client.make_qualified_table_name_path, quote=False, casefold=False
        ),
        casefold_identifier=casefold_identifier,
    )


def build_row_counts_expr(
    table_name: str,
    quoted_identifiers: bool = True,
    dlt_load_id_col: str = None,
    load_id: str = None,
) -> sge.Select:
    """Builds a SQL expression to count rows in a table, optionally filtering by a load ID."""

    if bool(load_id) ^ bool(dlt_load_id_col):
        raise ValueError("Both `load_id` and `dlt_load_id_col` must be provided together.")

    table_expr = sge.Table(this=sge.to_identifier(table_name, quoted=quoted_identifiers))

    select_expr = sge.Select(
        expressions=[
            sge.Alias(
                this=sge.Literal.string(table_name),
                alias=sge.to_identifier("table_name"),
            ),
            sge.Alias(
                this=sge.Count(this=sge.Star()),
                alias=sge.to_identifier("row_count"),
            ),
        ]
    ).from_(table_expr)

    if load_id:
        where_condition = sge.EQ(
            this=sge.to_identifier(dlt_load_id_col, quoted=quoted_identifiers),
            expression=sge.Literal.string(load_id),
        )
        select_expr = select_expr.where(where_condition)

    return select_expr


def build_select_expr(
    table_name: str,
    selected_columns: List[str] = None,
    quoted_identifiers: bool = True,
) -> sge.Select:
    """Builds a SQL expression to select all or selected rows from a table."""

    if selected_columns is None:
        selected_columns = ["*"]

    table_expr = sge.Table(this=sge.to_identifier(table_name, quoted=quoted_identifiers))

    columns_expr = [
        (
            sge.Star()
            if col == "*"
            else sge.Column(this=sge.to_identifier(col, quoted=quoted_identifiers))
        )
        for col in selected_columns
    ]

    select_expr = sge.Select(expressions=columns_expr).from_(table_expr)

    return select_expr
