import random
from datetime import datetime, timedelta
from typing import cast, Union

import duckdb
import pytest
import sqlglot
import sqlglot.optimizer
from sqlglot import exp as sge
from sqlglot.diff import diff as sqlglot_diff
from sqlglot.schema import MappingSchema as SQLGlotSchema

import dlt
from dlt.common.schema.typing import C_DLT_LOAD_ID, TTableReference
from dlt.dataset.lineage import compute_columns_schema
from dlt.dataset.queries import build_row_counts_expr, build_select_expr, _normalize_query
from dlt.destinations.impl.duckdb.configuration import DuckDbClientConfiguration


def queries_are_equivalent(
    query1: Union[str, sge.Query],
    query2: Union[str, sge.Query],
) -> bool:
    query1 = sqlglot.maybe_parse(query1)
    query2 = sqlglot.maybe_parse(query2)

    query1_optimized = sqlglot.optimizer.optimize(query1)
    query2_optimized = sqlglot.optimizer.optimize(query2)

    edits = sqlglot_diff(query1_optimized, query2_optimized, delta_only=True)
    return edits == []


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


# def test_join_on_child_parent_relation():
#     @dlt.resource(primary_key="id", columns={"name": {"x-annotation-pii": True}})  # type: ignore[typeddict-unknown-key]
#     def purchases():
#         yield from [
#             {
#                 "id": 1,
#                 "name": "simon",
#                 "city": "berlin",
#                 "items": [
#                     {"name": "item1", "price": 10},
#                     {"name": "item2", "price": 20},
#                 ],
#             },
#             {
#                 "id": 2,
#                 "name": "violet",
#                 "city": "montreal",
#                 "items": [
#                     {"name": "item3", "price": 30},
#                     {"name": "item1", "price": 10},
#                 ],
#             },
#             {
#                 "id": 3,
#                 "name": "tammo",
#                 "city": "new york",
#                 "items": [
#                     {"name": "item2", "price": 20},
#                     {"name": "item3", "price": 30},
#                 ],
#             },
#         ]

#     expected_columns = (
#         "_dlt_load_id",  # from parent table
#         "_dlt_id",  # from parent table
#         "_dlt_list_idx",  # from child table
#         "items___dlt_id",  # `_dlt_id` from child table; equivalent to `(_dlt_id, _dlt_list_idx)`
#         "purchases__id",
#         "purchases__name",
#         "purchases__city",
#         "items__name",
#         "items__price",
#     )
#     expected_sql_query = """
# SELECT
#     "purchases"."_dlt_load_id" AS "_dlt_load_id",
#     "purchases"."_dlt_id" AS "_dlt_id",
#     "purchases__items"."_dlt_list_idx" AS "_dlt_list_idx",
#     "purchases__items"."_dlt_id" AS "items___dlt_id",
#     "purchases"."id" AS "purchases__id",
#     "purchases"."name" AS "purchases__name",
#     "purchases"."city" AS "purchases__city",
#     "purchases__items"."name" AS "items__name",
#     "purchases__items"."price" AS "items__price"
# FROM "parent_child_relation_dataset"."purchases" AS "purchases"
# LEFT JOIN "parent_child_relation_dataset"."purchases__items" AS "purchases__items"
# ON "purchases"."_dlt_id" = "purchases__items"."_dlt_parent_id"
# """

#     pipeline = dlt.pipeline("parent_child_relation", destination="duckdb")
#     pipeline.run([purchases])

#     schema = pipeline.default_schema
#     assert "purchases" in schema.tables
#     assert "purchases__items" in schema.tables
#     assert schema.tables["purchases"].get("references") is None

#     dataset = pipeline.dataset()
#     purchases_rel = dataset.table("purchases")

#     joined_rel = purchases_rel.join_child("purchases__items")
#     df = joined_rel.df()
#     sql_query = joined_rel.to_sql()

#     assert all(col in expected_columns for col in joined_rel.schema["columns"].keys())
#     assert all(col in expected_columns for col in df.columns)
#     assert queries_are_equivalent(sql_query, expected_sql_query)


def test_join_on_references():
    references = [
        TTableReference(
            columns=["customer_id"],
            referenced_table="customers",
            referenced_columns=["id"],
        )
    ]

    @dlt.resource(primary_key="id", columns={"name": {"x-annotation-pii": True}})  # type: ignore[typeddict-unknown-key]
    def customers():
        """Load customer data from three cities from a simple python list."""
        yield from [
            {"id": 1, "name": "simon", "city": "berlin"},
            {"id": 2, "name": "violet", "city": "montreal"},
            {"id": 3, "name": "tammo", "city": "new york"},
            {"id": 4, "name": "dave", "city": "berlin"},
            {"id": 5, "name": "andrea", "city": "montreal"},
            {"id": 6, "name": "marcin", "city": "new york"},
            {"id": 7, "name": "sarah", "city": "berlin"},
            {"id": 8, "name": "miguel", "city": "new york"},
            {"id": 9, "name": "yuki", "city": "montreal"},
            {"id": 10, "name": "olivia", "city": "berlin"},
            {"id": 11, "name": "raj", "city": "montreal"},
            {"id": 12, "name": "sofia", "city": "new york"},
            {"id": 13, "name": "chen", "city": "berlin"},
        ]

    @dlt.resource(
        primary_key="id",
        references=references,
    )
    def purchases():
        """Generate 100 seeded random purchases between Mon. Oct 1 and Sun. Oct 14, 2018."""
        random.seed(42)
        start_date = datetime(2018, 10, 1)
        customers_ids = list(range(1, 14))  # 13 customers
        inventory_ids = list(range(1, 7))  # 6 inventory items

        yield from [
            {
                "id": i + 1,
                "customer_id": random.choice(customers_ids),
                "inventory_id": random.choice(inventory_ids),
                "quantity": random.randint(1, 5),
                "date": (start_date + timedelta(days=random.randint(0, 13))).strftime("%Y-%m-%d"),
            }
            for i in range(20)
        ]

    expected_columns = (
        "purchases___dlt_id",
        "customers___dlt_id",
        "purchases__id",
        "purchases__customer_id",
        "purchases__inventory_id",
        "purchases__quantity",
        "purchases__date",
        "customers__id",
        "customers__name",
        "customers__city",
    )
    expected_sql_query = """\
SELECT
    "purchases"."_dlt_id" AS "purchases___dlt_id",
    "customers"."_dlt_id" AS "customers___dlt_id",
    "purchases"."id" AS "purchases__id",
    "purchases"."customer_id" AS "purchases__customer_id",
    "purchases"."inventory_id" AS "purchases__inventory_id",
    "purchases"."quantity" AS "purchases__quantity",
    "purchases"."date" AS "purchases__date",
    "customers"."id" AS "customers__id",
    "customers"."name" AS "customers__name",
    "customers"."city" AS "customers__city"
FROM "fruit_with_ref_dataset"."purchases" AS "purchases"
INNER JOIN "fruit_with_ref_dataset"."customers" AS "customers"
ON "purchases"."customer_id" = "customers"."id"
"""

    pipeline = dlt.pipeline("fruit_with_ref", destination="duckdb")
    pipeline.run([customers, purchases])

    schema = pipeline.default_schema
    assert schema.tables["purchases"].get("references") == references
    assert schema.tables["customers"].get("references") is None

    dataset = pipeline.dataset()

    purchases_rel = dataset.table("purchases")
    customers_rel = dataset.table("customers")

    joined_rel = purchases_rel.join(customers_rel)
    df = joined_rel.df()
    sql_query = joined_rel.to_sql()

    assert all(col in expected_columns for col in joined_rel.schema["columns"].keys())
    assert all(col in expected_columns for col in df.columns)
    assert queries_are_equivalent(sql_query, expected_sql_query)
