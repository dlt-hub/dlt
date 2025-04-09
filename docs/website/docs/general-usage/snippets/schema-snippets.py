import dlt
from dlt.common import Decimal


def nested_hints_primary_key() -> None:
    # @@@DLT_SNIPPET_START nested_hints_primary_key
    @dlt.resource(
        primary_key="id",
        write_disposition="merge",
        nested_hints={
            "purchases": dlt.mark.make_nested_hints(
                # column hint is optional - makes sure that customer_id is a first column in the table
                columns=[{"name": "customer_id", "data_type": "bigint"}],
                primary_key=["customer_id", "id"],
                write_disposition="merge",
                references=[
                    {
                        "referenced_table": "customers",
                        "columns": ["customer_id"],
                        "referenced_columns": ["id"],
                    }
                ],
            )
        },
    )
    def customers():
        """Load customer data from a simple python list."""
        yield [
            {
                "id": 1,
                "name": "simon",
                "city": "berlin",
                "purchases": [{"id": 1, "name": "apple", "price": Decimal("1.50")}],
            },
            {
                "id": 2,
                "name": "violet",
                "city": "london",
                "purchases": [{"id": 1, "name": "banana", "price": Decimal("1.70")}],
            },
            {
                "id": 3,
                "name": "tammo",
                "city": "new york",
                "purchases": [{"id": 1, "name": "pear", "price": Decimal("2.50")}],
            },
        ]

    def _pushdown_customer_id(row):
        id_ = row["id"]
        for purchase in row["purchases"]:
            purchase["customer_id"] = id_
        return row

    p = dlt.pipeline(
        pipeline_name="test_nested_hints_primary_key", destination="duckdb", dataset_name="local"
    )
    p.run(customers().add_map(_pushdown_customer_id))
    # load same data again to prove that merge works
    p.run(customers().add_map(_pushdown_customer_id))
    # check counts
    row_count = p.dataset().row_counts().fetchall()
    assert row_count == [("customers", 3), ("customers__purchases", 3)]
    # @@@DLT_SNIPPET_END nested_hints_primary_key
