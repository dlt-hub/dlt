import pytest

import sqlglot

from dlt import Schema
from dlt.sources._single_file_templates.fruitshop_pipeline import (
    fruitshop as fruitshop_source,
)
from dlt.transformations import lineage
from tests.utils import autouse_test_storage

from dlt.destinations import duckdb


@pytest.fixture
def example_schema(autouse_test_storage) -> Schema:
    s = Schema("d1")
    s.update_table(
        {
            "name": "customers",
            "columns": {
                "id": {"data_type": "bigint", "name": "id"},
                "name": {  #  type: ignore[typeddict-unknown-key]
                    "data_type": "text",
                    "name": "name",
                    "x-pii": True,
                },
                "email": {"data_type": "text", "name": "email", "nullable": True},
            },
        }
    )
    s.update_table(
        {
            "name": "orders",
            "columns": {
                "id": {"data_type": "bigint", "name": "id"},
                "customer_id": {"data_type": "bigint", "name": "customer_id"},
                "amount": {"data_type": "double", "name": "amount"},
            },
        }
    )
    return s


def test_various_queries(example_schema: Schema):
    # setup
    # TODO: run for all supported destinations
    destination = duckdb(credentials=":memory:")
    dialect = destination.capabilities().sqlglot_dialect
    DATASET_NAME = "d1"
    sqlglot_schema = lineage.create_sqlglot_schema(
        DATASET_NAME,
        example_schema,
        dialect,
        destination.capabilities().get_type_mapper(),
    )

    # test star select
    sql_query = "SELECT * FROM customers"
    assert lineage.compute_columns_schema(sql_query, sqlglot_schema, "duckdb") == {
        "id": {"name": "id", "data_type": "bigint"},
        "name": {"name": "name", "data_type": "text", "x-pii": True},
        "email": {"name": "email", "data_type": "text", "nullable": True},
    }

    # test select with fully qualified table and column names
    sql_query = "SELECT d1.customers.id, d1.customers.name, d1.customers.email FROM d1.customers"
    assert lineage.compute_columns_schema(sql_query, sqlglot_schema, "duckdb") == {
        "id": {"name": "id", "data_type": "bigint"},
        "name": {"name": "name", "data_type": "text", "x-pii": True},
        "email": {"name": "email", "data_type": "text", "nullable": True},
    }

    # test select with casting and avg
    sql_query = "SELECT AVG(id) as mean_id, name, email, CAST(LEN(name) as DOUBLE) FROM customers"
    assert lineage.compute_columns_schema(sql_query, sqlglot_schema, "duckdb") == {
        "mean_id": {"name": "mean_id", "data_type": "double"},
        "name": {"name": "name", "data_type": "text", "x-pii": True},
        "email": {"name": "email", "data_type": "text", "nullable": True},
        "_col_3": {"name": "_col_3", "data_type": "double"},  # anonymous columns
    }

    # test concat
    sql_query = "SELECT CONCAT(name, email) as concat FROM customers"
    assert lineage.compute_columns_schema(sql_query, sqlglot_schema, "duckdb") == {
        "concat": {"name": "concat", "data_type": "text"},
    }

    # test join
    sql_query = (
        "SELECT customers.name, orders.amount FROM customers JOIN orders ON customers.id ="
        " orders.customer_id"
    )
    assert lineage.compute_columns_schema(sql_query, sqlglot_schema, "duckdb") == {
        "name": {"name": "name", "data_type": "text", "x-pii": True},
        "amount": {"name": "amount", "data_type": "double"},
    }

    # test topk
    sql_query = """
        SELECT * FROM ( SELECT amount, COUNT(*) AS "count" FROM
        orders GROUP BY 1 ) AS "t1" ORDER BY t1.count DESC
        LIMIT 10
    """
    assert lineage.compute_columns_schema(sql_query, sqlglot_schema, "duckdb") == {
        "count": {"name": "count", "data_type": "bigint"},
        "amount": {"name": "amount", "data_type": "double"},
    }

    # test select unknown column
    sql_query = "SELECT unknown_column FROM customers"
    with pytest.raises(sqlglot.errors.OptimizeError):
        lineage.compute_columns_schema(sql_query, sqlglot_schema, "duckdb")
