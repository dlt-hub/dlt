"""Test schema lineage with sqlglot"""


def test_lineage_with_sqlglot():
    from sqlglot.lineage import lineage
    import sqlglot

    # Define the SQL query with a join and column renaming
    sql_query = """
    SELECT * FROM 
    (
    SELECT c.name, of.amount AS total_amount, of.more
    FROM orders of
    JOIN customers c ON o.customer_id = c.customer_id
    )
    """

    # Specify the column to trace lineage for
    column_name = "total_amount"

    # Define the schema for the tables
    schema = {
        "orders": {
            "order_id": "INTEGER",
            "customer_id": "INTEGER",
            "amount": "FLOAT",
            "more": "FLOAT",
        },
        "customers": {"customer_id": "INTEGER", "name": "STRING"},
    }

    # Generate the lineage graph
    lineage_graph = lineage(column=column_name, sql=sql_query, schema=schema)
    print(lineage_graph)

    table_name = None
    column_name = None
    for node in lineage_graph.walk():
        # Check if the node has `table` and `column` attributes, which would indicate an origin column
        if type(node.expression) == sqlglot.expressions.Table:
            table_name = node.expression.name

        if type(node.expression) == sqlglot.expressions.Alias:
            print(type(node.expression.this))
            column_name = node.expression.this.this.this

    print("Origin Columns:", table_name, column_name)

    assert False
