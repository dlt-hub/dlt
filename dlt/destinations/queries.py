from typing import Dict, Any, List, Optional, overload, Union, Match

import re

import sqlglot
import sqlglot.expressions as sge
from sqlglot.schema import Schema as SQLGlotSchema

from dlt.destinations.sql_client import SqlClientBase
from dlt.common.typing import TypedDict


@overload
def normalize_query(
    sqlglot_schema: SQLGlotSchema,
    qualified_query: sge.Query,
    sql_client: SqlClientBase[Any],
) -> sge.Query: ...


@overload
def normalize_query(
    sqlglot_schema: SQLGlotSchema,
    qualified_query: sge.Insert,
    sql_client: SqlClientBase[Any],
) -> sge.Insert: ...


def normalize_query(
    sqlglot_schema: SQLGlotSchema,
    qualified_query: Union[sge.Query, sge.Insert],
    sql_client: SqlClientBase[Any],
) -> Union[sge.Query, sge.Insert]:
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


def build_insert_expr(
    table_name: str,
    columns: List[str],
    quoted_identifiers: bool = True,
) -> sge.Insert:
    """Builds a SQL Insert expression with placeholders."""
    table_expr = sge.Table(this=sge.to_identifier(table_name, quoted=quoted_identifiers))
    columns_expr = [
        sge.Column(this=sge.to_identifier(col, quoted=quoted_identifiers)) for col in columns
    ]
    placeholders_expr = sge.Values(
        expressions=[sge.Tuple(expressions=[sge.Placeholder() for _ in enumerate(columns)])]
    )

    insert_expr = sge.Insert(this=table_expr, columns=columns_expr, expression=placeholders_expr)

    return insert_expr


def build_stored_state_expr(
    pipeline_name: str,
    state_table_name: str,
    state_table_cols: List[str],
    loads_table_name: str,
    c_load_id: str,
    c_dlt_load_id: str,
    c_pipeline_name: str,
    c_status: str,
) -> sge.Select:
    """Builds a SQL expression for querying the stored state."""
    state_table_expr = sge.Table(
        this=sge.to_identifier(state_table_name, quoted=True),
        alias=sge.to_identifier("s", quoted=False),
    )

    loads_table_expr = sge.Table(
        this=sge.to_identifier(loads_table_name, quoted=True),
        alias=sge.to_identifier("l", quoted=False),
    )

    join_condition = sge.EQ(
        this=sge.Column(
            this=sge.to_identifier(c_load_id, quoted=True),
            table=sge.to_identifier("l", quoted=False),
        ),
        expression=sge.Column(
            this=sge.to_identifier(c_dlt_load_id, quoted=True),
            table=sge.to_identifier("s", quoted=False),
        ),
    )

    where_condition = sge.and_(
        sge.EQ(
            this=sge.Column(this=sge.to_identifier(c_pipeline_name, quoted=True)),
            expression=sge.Literal.string(pipeline_name),
        ),
        sge.EQ(
            this=sge.Column(
                this=sge.to_identifier(c_status, quoted=True),
                table=sge.to_identifier("l", quoted=False),
            ),
            expression=sge.Literal.number(0),
        ),
    )

    select_expr = sge.Select(
        expressions=[
            sge.Column(this=sge.to_identifier(col, quoted=True)) for col in state_table_cols
        ]
    )

    order_expr = sge.Ordered(
        this=sge.Column(this=sge.to_identifier(c_load_id, quoted=True)),
        desc=True,
    )

    select_expr = (
        select_expr.from_(state_table_expr)
        .join(loads_table_expr, on=join_condition)
        .where(where_condition)
        .order_by(order_expr)
        .limit(1)
    )
    return select_expr


@overload
def build_stored_schema_expr(
    table_name: str,
    version_table_schema_columns: List[str],
    *,
    schema_name: str,
    c_inserted_at: str,
    c_schema_name: str,
) -> sge.Select: ...


@overload
def build_stored_schema_expr(
    table_name: str,
    version_table_schema_columns: List[str],
    *,
    version_hash: str,
    c_version_hash: str,
) -> sge.Select: ...


def build_stored_schema_expr(
    table_name: str,
    version_table_schema_columns: List[str],
    schema_name: Optional[str] = None,
    c_inserted_at: Optional[str] = None,
    c_schema_name: Optional[str] = None,
    version_hash: Optional[str] = None,
    c_version_hash: Optional[str] = None,
) -> sge.Select:
    """Builds a SQL expression for querying the stored schema based on the version hash if provided."""
    select_expr = build_select_expr(
        table_name=table_name,
        selected_columns=version_table_schema_columns,
        quoted_identifiers=True,
    )

    if version_hash:
        where_condition = sge.EQ(
            this=sge.Column(this=sge.to_identifier(c_version_hash, quoted=True)),
            expression=sge.Literal.string(version_hash),
        )

        select_expr = select_expr.where(where_condition).limit(1)
        return select_expr

    order_expr = sge.Ordered(
        this=sge.Column(this=sge.to_identifier(c_inserted_at, quoted=True)),
        desc=True,
    )

    where_condition = None
    if schema_name:
        where_condition = sge.EQ(
            this=sge.Column(this=sge.to_identifier(c_schema_name, quoted=True)),
            expression=sge.Literal.string(schema_name),
        )

    select_expr = select_expr.where(where_condition).order_by(order_expr)

    return select_expr


def replace_placeholders(query: str, dialect: str) -> str:
    """Replaces (?, ?, ?, ...), as well as ({?: }, {?: }, {?: }, ...) placeholders with (%s, %s, %s, ...)."""

    # Sqlglot creates "{?: }"" placeholders for clickhouse, otherwise just "?""
    placeholder_pattern = r"\{\?:\s*\}" if dialect == "clickhouse" else r"\?"

    # Only match tuples, not stray ?’s elsewhere
    base_pattern = rf"\(\s*{placeholder_pattern}\s*(,\s*{placeholder_pattern}\s*)*\)"

    def _convert_to_percents(match: Match[str]) -> str:
        n = len(re.findall(placeholder_pattern, match.group()))
        return "(" + ", ".join(["%s"] * n) + ")"

    query = re.sub(base_pattern, _convert_to_percents, query)
    return query
