from typing import Any, Iterator, List, Optional, cast

import duckdb
import pytest
import sqlglot
from sqlglot import exp as sge
from sqlglot.schema import MappingSchema as SQLGlotSchema

import dlt
from dlt.common.schema.typing import C_DLT_LOAD_ID
from dlt.dataset.lineage import compute_columns_schema
from dlt.destinations.queries import (
    build_row_counts_expr,
    build_select_expr,
    bind_query,
    make_expand_table_name,
)
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.impl.duckdb.configuration import DuckDbClientConfiguration


@pytest.fixture
def duckdb_sql_client() -> Iterator[SqlClientBase[Any]]:
    """In-memory duckdb sql client bound to `dataset_name`."""
    con = duckdb.connect(":memory:")
    destination_client = dlt.destinations.duckdb(con).client(
        dlt.Schema("foobar"), DuckDbClientConfiguration()._bind_dataset_name("dataset_name")
    )
    with destination_client.sql_client as sql_client:
        yield sql_client


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


def test_normalize_query(duckdb_sql_client: SqlClientBase[Any]) -> None:
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

    normalized_query_expr = bind_query(
        qualified_query=cast(sge.Query, qualified_query_expr),
        sqlglot_schema=sqlglot_schema,
        expand_table_name=make_expand_table_name(duckdb_sql_client),
        casefold_identifier=duckdb_sql_client.capabilities.casefold_identifier,
    )

    assert normalized_query_expr.sql() == expected_normalized_query


def test_expand_table_name_with_legacy_path_signature(
    duckdb_sql_client: SqlClientBase[Any],
) -> None:
    """Sql clients overriding `make_qualified_table_name_path` without the `dataset_name`
    parameter keep working for tables without a dataset qualifier."""

    class _LegacyPathClient:
        def make_qualified_table_name_path(
            self, table_name: Optional[str], quote: bool = True, casefold: bool = True
        ) -> List[str]:
            return duckdb_sql_client.make_qualified_table_name_path(
                table_name, quote=quote, casefold=casefold
            )

    expand = make_expand_table_name(cast(SqlClientBase[Any], _LegacyPathClient()))

    assert expand("items", None) == duckdb_sql_client.make_qualified_table_name_path(
        "items", quote=False, casefold=False
    )
    # a dataset qualifier requires the `dataset_name` parameter on the override
    with pytest.raises(TypeError, match="dataset_name"):
        expand("items", "other_dataset")
