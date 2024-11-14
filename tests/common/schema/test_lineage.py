"""Test schema lineage with sqlglot"""
import pytest
from dlt.common.schema.lineage import get_result_column_names_from_select, get_result_origins
from sqlglot.lineage import lineage


# TODO: if we are working with schemas and catalogs, we need to implement this here too
EXAMPLE_SCHEMA = {
    "orders": {
        "order_id": "INTEGER",
        "customer_id": "INTEGER",
        "total": "FLOAT",
    },
    "customers": {
        "customer_id": "INTEGER",
        "name": "STRING",
    },
    "items": {
        "item_id": "INTEGER",
        "name": "STRING",
        "price": "FLOAT",
    },
    "order_items": {
        "order_id": "INTEGER",
        "item_id": "INTEGER",
        "quantity": "INTEGER",
        "spot_price": "FLOAT",
    },
}


def test_result_column_names_from_select():
    # simple select, no schema
    sql = "SELECT total FROM orders"
    column_names = get_result_column_names_from_select(sql)
    assert column_names == ["total"]

    # star select, no schema, will not know the column names
    sql = "SELECT * FROM orders"
    column_names = get_result_column_names_from_select(sql)
    assert column_names == ["*"]

    # simple select, with schema
    sql = "SELECT total, order_id FROM orders"
    column_names = get_result_column_names_from_select(sql, EXAMPLE_SCHEMA)
    assert column_names == ["total", "order_id"]

    # star select, with schema, will know the column names
    sql = "SELECT * FROM orders"
    column_names = get_result_column_names_from_select(sql, EXAMPLE_SCHEMA)
    assert column_names == ["order_id", "customer_id", "total"]

    # select unknown column (works, could be not known column)
    sql = "SELECT unknown FROM orders"
    column_names = get_result_column_names_from_select(sql, EXAMPLE_SCHEMA)
    assert column_names == ["unknown"]

    # star select from joined tables
    sql = "SELECT * FROM orders JOIN customers ON orders.customer_id = customers.customer_id"
    column_names = get_result_column_names_from_select(sql, EXAMPLE_SCHEMA)
    assert column_names == ["order_id", "customer_id", "total", "customer_id", "name"]

    # nested star select
    sql = """
    SELECT *
    FROM (SELECT order_id, customer_id FROM orders)
    """
    column_names = get_result_column_names_from_select(sql, EXAMPLE_SCHEMA)
    assert column_names == ["order_id", "customer_id"]

    # select with alias
    sql = """
    SELECT * FROM (SELECT name AS renamed_name FROM customers)
    """
    column_names = get_result_column_names_from_select(sql, EXAMPLE_SCHEMA)
    assert column_names == ["renamed_name"]

    # aggregate select
    sql = """
    SELECT SUM(total) as total_sum FROM orders
    """
    column_names = get_result_column_names_from_select(sql, EXAMPLE_SCHEMA)
    assert column_names == ["total_sum"]

    # triple nested subquery with join
    sql = """
    SELECT * FROM (SELECT * FROM (SELECT order_id, spot_price FROM orders o JOIN order_items i ON o.order_id = i.order_id)) LIMIT 5
    """
    column_names = get_result_column_names_from_select(sql, EXAMPLE_SCHEMA)
    assert column_names == ["order_id", "spot_price"]

    # test simple alias (this fails, as nested subquery it will work)
    sql = """
    SELECT order_id AS alias_order_id FROM orders
    """
    column_names = get_result_column_names_from_select(sql, EXAMPLE_SCHEMA)
    assert column_names == ["alias_order_id"]

    # group by aggregate
    sql = """
    SELECT customer_id, SUM(total) as sum FROM orders GROUP BY customer_id
    """
    column_names = get_result_column_names_from_select(sql, EXAMPLE_SCHEMA)
    assert column_names == ["customer_id", "sum"]


def test_result_origins():
    # simple select no schema
    sql = "SELECT total FROM orders"
    origins = get_result_origins(sql)
    assert origins == [("total", ("orders", "total"))]

    # star select no schema, will not know what comes from where
    sql = "SELECT * FROM orders JOIN customers"
    origins = get_result_origins(sql)
    assert origins == []

    # join no schema, will still know columns from tables if given in statement
    sql = "SELECT o.order_id, c.name FROM orders o JOIN customers c"
    origins = get_result_origins(sql)
    assert origins == [("order_id", ("orders", "order_id")), ("name", ("customers", "name"))]

    # simple select with schema
    sql = "SELECT total, customer_id FROM orders"
    origins = get_result_origins(sql, EXAMPLE_SCHEMA)
    assert origins == [("total", ("orders", "total")), ("customer_id", ("orders", "customer_id"))]

    # star select
    sql = "SELECT * FROM orders"
    origins = get_result_origins(sql, EXAMPLE_SCHEMA)
    assert origins == [
        ("order_id", ("orders", "order_id")),
        ("customer_id", ("orders", "customer_id")),
        ("total", ("orders", "total")),
    ]

    # join
    sql = (
        "SELECT o.order_id, c.name FROM orders o JOIN customers c ON o.customer_id = c.customer_id"
    )
    origins = get_result_origins(sql, EXAMPLE_SCHEMA)
    assert origins == [
        ("order_id", ("orders", "order_id")),
        ("name", ("customers", "name")),
    ]

    # triple nested subquery with join and rename
    sql = """
    SELECT * FROM (SELECT * FROM (SELECT o.order_id, i.spot_price as price FROM orders o JOIN order_items i ON o.order_id = i.order_id)) LIMIT 5
    """
    column_names = get_result_origins(sql, EXAMPLE_SCHEMA)
    assert column_names == [
        ("order_id", ("orders", "order_id")),
        ("price", ("order_items", "spot_price")),
    ]

    # group by with aggregate
    sql = """
    SELECT customer_id, SUM(total) as sum FROM orders GROUP BY customer_id
    """
    origins = get_result_origins(sql, EXAMPLE_SCHEMA)
    assert origins == [("customer_id", ("orders", "customer_id")), ("sum", ("orders", "total"))]

    # select unknown column
    sql = "SELECT unknown FROM orders"
    origins = get_result_origins(sql, EXAMPLE_SCHEMA)
    assert origins == [("unknown", (None, None))]

    # concatenate two columns, for now it selects the first column type?
    sql = "SELECT (total || ' ' || customer_id) as concat FROM orders"
    origins = get_result_origins(sql, EXAMPLE_SCHEMA, dialect="duckdb")
    assert origins ==  [('concat', ('orders', 'total'))]

    # where clause
    sql = "SELECT * FROM orders WHERE total > 100"
    origins = get_result_origins(sql, EXAMPLE_SCHEMA)
    assert origins == [('order_id', ('orders', 'order_id')), ('customer_id', ('orders', 'customer_id')), ('total', ('orders', 'total'))]
