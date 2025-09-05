from typing import cast

import duckdb
import pytest
import sqlglot
from sqlglot import exp as sge
from sqlglot.schema import MappingSchema as SQLGlotSchema

import dlt
from dlt.common.schema.typing import C_DLT_LOAD_ID
from dlt.dataset.lineage import compute_columns_schema
from dlt.destinations.queries import build_row_counts_expr, build_select_expr, _normalize_query
from dlt.destinations.impl.duckdb.configuration import DuckDbClientConfiguration


def test_basic() -> None:
    stmt = build_row_counts_expr("my_table", quoted_identifiers=True)
    expected = (
        """SELECT 'my_table' AS table_name, """ """COUNT(*) AS row_count """ """FROM "my_table\""""
    )
    assert stmt.sql() == expected

    stmt = build_row_counts_expr("my_table", quoted_identifiers=False)
    expected = "SELECT 'my_table' AS table_name, COUNT(*) AS row_count FROM my_table"
    assert stmt.sql() == expected


def test_with_load_id_filter():
    with pytest.raises(ValueError) as py_exc:
        _ = build_row_counts_expr(
            table_name="my_table",
            dlt_load_id_col=C_DLT_LOAD_ID,
        )
    assert "Both `load_id` and `dlt_load_id_col` must be provided together." in py_exc.value.args

    stmt = build_row_counts_expr(
        table_name="my_table", dlt_load_id_col=C_DLT_LOAD_ID, load_id="abcd-123"
    )
    expected = (
        "SELECT 'my_table' AS table_name, "
        "COUNT(*) AS row_count "
        'FROM "my_table" '
        "WHERE \"_dlt_load_id\" = 'abcd-123'"
    )
    assert stmt.sql() == expected


def test_select_star():
    stmt = build_select_expr("events", ["*"])
    expected = 'SELECT * FROM "events"'
    assert stmt.sql() == expected

    stmt = build_select_expr("events")
    assert stmt.sql() == expected


def test_selected_columns():
    stmt = build_select_expr(
        table_name="events",
        selected_columns=["event_id", "created_at"],
        quoted_identifiers=True,
    )
    expected = 'SELECT "event_id", "created_at" FROM "events"'
    assert stmt.sql() == expected
    stmt = build_select_expr(
        table_name="events",
        selected_columns=["event_id", "created_at"],
        quoted_identifiers=False,
    )
    expected = "SELECT event_id, created_at FROM events"
    assert stmt.sql() == expected


def test_qualified_query():
    sqlglot_schema = SQLGlotSchema(
        {"dataset_name": {"items": {"id": str}, "double_items": {"double_id": str, "id": str}}}
    )
    query_expr = sqlglot.parse_one("""
SELECT
    i.id AS id,
    di.double_id AS double_id
FROM dataset_name.items AS i
JOIN dataset_name.double_items as di
ON (i.id = di.id)
WHERE i.id < 20
ORDER BY i.id ASC
""")

    expected_qualified_query = (
        "SELECT i.id AS id, di.double_id AS double_id FROM dataset_name.items AS i JOIN"
        " dataset_name.double_items AS di ON (i.id = di.id) WHERE i.id < 20 ORDER BY i.id ASC"
    )

    _, qualified_query_expr = compute_columns_schema(
        expression=query_expr,
        sqlglot_schema=sqlglot_schema,
        dialect="duckdb",
    )
    qualified_query = qualified_query_expr.sql()

    assert qualified_query == expected_qualified_query


def test_normalize_query():
    sqlglot_schema = SQLGlotSchema(
        {"dataset_name": {"items": {"id": str}, "double_items": {"double_id": str, "id": str}}}
    )
    qualified_query_expr = sqlglot.parse_one("""
SELECT
    i.id AS id,
    di.double_id AS double_id
FROM dataset_name.items AS i
JOIN dataset_name.double_items as di
ON (i.id = di.id)
WHERE i.id < 20
ORDER BY i.id ASC
""")

    expected_normalized_query = (
        'SELECT "i"."id" AS "id", "di"."double_id" AS "double_id" FROM "dataset_name"."items" AS'
        ' "i" JOIN "dataset_name"."double_items" AS "di" ON ("i"."id" = "di"."id") WHERE'
        ' "i"."id" < 20 ORDER BY "i"."id" ASC'
    )

    con = duckdb.connect(":memory:")
    duckdb_dest = dlt.destinations.duckdb(con)
    duckdb_destination_client = duckdb_dest.client(
        dlt.Schema("foobar"), DuckDbClientConfiguration()._bind_dataset_name("dataset_name")
    )

    with duckdb_destination_client.sql_client as sql_client:
        normalized_query_expr = _normalize_query(
            qualified_query=cast(sge.Query, qualified_query_expr),
            sqlglot_schema=sqlglot_schema,
            sql_client=sql_client,
            casefold_identifier=sql_client.capabilities.casefold_identifier,
        )
        normalized_query = normalized_query_expr.sql()

    assert normalized_query == expected_normalized_query
