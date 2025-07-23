from typing import Dict, Any, List

import sqlglot
import sqlglot.expressions as sge
from sqlglot.schema import Schema as SQLGlotSchema

from dlt.destinations.sql_client import SqlClientBase


def normalize_query(
    sqlglot_schema: SQLGlotSchema,
    qualified_query: sge.Query,
    sql_client: SqlClientBase[Any],
) -> sge.Query:
    """Normalizes a qualified query compliant with the dlt schema into the namespace of the source dataset"""

    # this function modifies the incoming query
    qualified_query = qualified_query.copy()

    # do we need to case-fold identifiers?
    caps = sql_client.capabilities
    is_casefolding = caps.casefold_identifier is not str

    # preserve "column" names in original selects which are done in dlt schema namespace
    orig_selects: Dict[int, str] = None
    if is_casefolding:
        orig_selects = {}
        for i, proj in enumerate(qualified_query.selects):
            orig_selects[i] = proj.name or proj.args["alias"].name

    # case fold all identifiers and quote
    for node in qualified_query.walk():
        if isinstance(node, sge.Table):
            # expand named of known tables. this is currently clickhouse things where
            # we use dataset.table in queries but render those as dataset___table
            if sqlglot_schema.column_names(node):
                expanded_path = sql_client.make_qualified_table_name_path(
                    node.name, quote=False, casefold=False
                )
                # set the table name
                if node.name != expanded_path[-1]:
                    node.this.set("this", expanded_path[-1])
                # set the dataset/schema name
                if node.db != expanded_path[-2]:
                    node.set("db", sqlglot.to_identifier(expanded_path[-2], quoted=False))
                # set the catalog name
                if len(expanded_path) == 3:
                    if node.db != expanded_path[0]:
                        node.set("catalog", sqlglot.to_identifier(expanded_path[0], quoted=False))
        # quote and case-fold identifiers, TODO: maybe we could be more intelligent, but then we need to unquote ibis
        if isinstance(node, sge.Identifier):
            if is_casefolding:
                node.set("this", caps.casefold_identifier(node.this))
            node.set("quoted", True)

    # add aliases to output selects to stay compatible with dlt schema after the query
    if orig_selects:
        for i, orig in orig_selects.items():
            case_folded_orig = caps.casefold_identifier(orig)
            if case_folded_orig != orig:
                # somehow we need to alias just top select in UNION (tested on Snowflake)
                sel_expr = qualified_query.selects[i]
                qualified_query.selects[i] = sge.alias_(sel_expr, orig, quoted=True)

    return qualified_query


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
