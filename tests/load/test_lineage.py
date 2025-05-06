import pytest

import sqlglot

from dlt import Schema
from dlt.sources._single_file_templates.fruitshop_pipeline import (
    fruitshop as fruitshop_source,
)
from dlt.transformations import lineage
from tests.utils import autouse_test_storage

from dlt.destinations import duckdb
from tests.load.utils import destinations_configs, DestinationTestConfiguration
from dlt.transformations.exceptions import LineageFailedException


# TODO: add all data types for one table
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


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        # TODO: see if we can get sqlalchemy, snowflake to work natively
        default_sql_configs=True,
        local_filesystem_configs=True,
        exclude=["sqlalchemy", "snowflake", "clickhouse"],
    ),
    ids=lambda x: x.name,
)
def test_various_queries(destination_config: DestinationTestConfiguration, example_schema: Schema):
    # setup
    destination = destination_config.destination_factory(dataset_name="d1")
    caps = destination.client(example_schema).sql_client.capabilities
    dialect = caps.sqlglot_dialect
    sqlglot_schema = lineage.create_sqlglot_schema(
        destination.client(example_schema).sql_client,
        example_schema,
        dialect,
        caps,
    )

    # TODO: investigate if we can fix this, this is a side effect of going via duckdb dialect
    # in snowflake bigint is the same as decimal(19,0)
    id_result_type = "bigint" if ("snowflake" not in destination_config.name) else "decimal"

    # test star select
    sql_query = "SELECT * FROM customers"
    assert lineage.compute_columns_schema(sql_query, sqlglot_schema, dialect) == {
        "id": {"name": "id", "data_type": id_result_type},
        "name": {"name": "name", "data_type": "text", "x-pii": True},
        "email": {"name": "email", "data_type": "text", "nullable": True},
    }

    # test select with fully qualified table and column names
    sql_query = "SELECT ID, d1.customers.name, d1.customers.email FROM d1.customers"
    assert lineage.compute_columns_schema(sql_query, sqlglot_schema, dialect) == {
        "id": {"name": "id", "data_type": id_result_type},
        "name": {"name": "name", "data_type": "text", "x-pii": True},
        "email": {"name": "email", "data_type": "text", "nullable": True},
    }

    # test select with casting and avg
    sql_query = "SELECT AVG(id) as mean_id, name, email, CAST(LEN(name) as DOUBLE) FROM customers"
    assert lineage.compute_columns_schema(sql_query, sqlglot_schema, dialect) == {
        "mean_id": {"name": "mean_id", "data_type": "double"},
        "name": {"name": "name", "data_type": "text", "x-pii": True},
        "email": {"name": "email", "data_type": "text", "nullable": True},
        "_col_3": {"name": "_col_3", "data_type": "double"},  # anonymous columns
    }

    # test concat
    sql_query = "SELECT CONCAT(name, email) as concat FROM customers"
    assert lineage.compute_columns_schema(sql_query, sqlglot_schema, dialect) == {
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
    with pytest.raises(LineageFailedException) as exc:
        lineage.compute_columns_schema(sql_query, sqlglot_schema, "duckdb")
    assert "Failed to resolve SQL query against the schema received" in str(exc.value)

    # test window function with over and join
    # TODO: total amount data type is not inferred
    sql_query = """
        SELECT c.name, SUM(o.amount) OVER (PARTITION BY c.name) as total_amount
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
    """
    assert lineage.compute_columns_schema(sql_query, sqlglot_schema, "duckdb") == {
        "name": {"name": "name", "data_type": "text", "x-pii": True},
        "total_amount": {"name": "total_amount"},  # , "data_type": "double"},
    }

    # test WITH clause
    sql_query = """
        WITH customer_orders AS (
            SELECT customer_id, SUM(amount) as total_amount
            FROM orders
            GROUP BY customer_id
        )
        SELECT customer_id, total_amount FROM customer_orders
    """
    assert lineage.compute_columns_schema(sql_query, sqlglot_schema, "duckdb") == {
        "customer_id": {"name": "customer_id", "data_type": "bigint"},
        "total_amount": {"name": "total_amount", "data_type": "double"},
    }
