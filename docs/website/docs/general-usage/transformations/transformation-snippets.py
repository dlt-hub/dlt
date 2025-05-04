import pytest
import dlt

from decimal import Decimal
import os
from typing import Any


from tests.pipeline.utils import load_table_counts


@pytest.fixture(scope="function")
def fruitshop_pipeline() -> dlt.Pipeline:
    """Set up a fruitshop fixture dataset for transformations examples"""

    # @@@DLT_SNIPPET_START quick_start_example

    from dlt.sources._single_file_templates.fruitshop_pipeline import (
        fruitshop as fruitshop_source,
    )

    fruitshop_pipeline = dlt.pipeline("fruitshop", destination="duckdb")
    fruitshop_pipeline.run(fruitshop_source())
    # @@@DLT_SNIPPET_END quick_start_example

    return fruitshop_pipeline


def basic_transformation_snippet(fruitshop_pipeline: dlt.Pipeline):
    # @@@DLT_SNIPPET_START basic_transformation

    @dlt.transformation()
    def copied_customers(dataset: dlt.Dataset) -> Any:
        # Ibis expression: sort by name and keep first 5 rows
        customers_table = dataset["customers"]
        return customers_table.order_by("name").limit(5)

    # Same pipeline & same dataset
    fruitshop_pipeline.run(copied_customers(fruitshop_pipeline.dataset()))

    # show rowcounts again, we now have a new table in the schema and the destination
    print(fruitshop_pipeline.dataset().row_counts().df())
    # @@@DLT_SNIPPET_END basic_transformation

    # copied customers now also exist
    assert load_table_counts(fruitshop_pipeline, "copied_customers") == {"copied_customers": 5}


def orders_per_user_snippet(fruitshop_pipeline: dlt.Pipeline):
    # @@@DLT_SNIPPET_START orders_per_user
    from dlt.common.destination.dataset import (
        TReadableRelation,
    )

    @dlt.transformation(name="orders_per_user", write_disposition="merge")
    def orders_per_user(dataset: dlt.Dataset) -> TReadableRelation:
        purchases = dataset["purchases"]
        return purchases.group_by(purchases.customer_id).aggregate(order_count=purchases.id.count())

    # @@@DLT_SNIPPET_END orders_per_user
    fruitshop_pipeline.run(orders_per_user(fruitshop_pipeline.dataset()))
    assert load_table_counts(fruitshop_pipeline, "orders_per_user") == {"orders_per_user": 2}


def loading_to_other_datasets_snippet(fruitshop_pipeline: dlt.Pipeline):
    # @@@DLT_SNIPPET_START loading_to_other_datasets
    import dlt

    @dlt.transformation()
    def copied_customers(dataset: dlt.Dataset):
        customers_table = dataset["customers"]
        return customers_table.order_by("name").limit(5)

    # Same duckdb instance, different dataset
    dest_p = dlt.pipeline("fruitshop", destination="duckdb", dataset_name="copied_dataset")
    dest_p.run(copied_customers(fruitshop_pipeline.dataset()))

    # Different engine (Postgres → DuckDB)
    duck_p = dlt.pipeline("fruitshop", destination="postgres")
    duck_p.run(copied_customers(fruitshop_pipeline.dataset()))
    # @@@DLT_SNIPPET_END loading_to_other_datasets


def multiple_transformations_snippet(fruitshop_pipeline: dlt.Pipeline):
    # @@@DLT_SNIPPET_START multiple_transformations
    import dlt

    @dlt.source
    def my_transformations(dataset: dlt.Dataset):
        @dlt.transformation(write_disposition="append")
        def enriched_purchases(dataset: dlt.Dataset):
            purchases = dataset["purchases"]
            customers = dataset["customers"]
            return purchases.join(customers, purchases.customer_id == customers.id)

        @dlt.transformation(write_disposition="replace")
        def total_items_sold(dataset: dlt.Dataset):
            purchases = dataset["purchases"]
            return purchases.aggregate(total_qty=purchases.quantity.sum())

        return enriched_purchases(dataset), total_items_sold(dataset)

    fruitshop_pipeline.run(my_transformations(fruitshop_pipeline.dataset()))
    # @@@DLT_SNIPPET_END multiple_transformations
    assert load_table_counts(fruitshop_pipeline, "enriched_purchases", "total_items_sold") == {
        "enriched_purchases": 3,
        "total_items_sold": 1,
    }


def dataset_inspection_snippet(fruitshop_pipeline: dlt.Pipeline):
    # @@@DLT_SNIPPET_START dataset_inspection
    # Show row counts for every table
    print(fruitshop_pipeline.dataset().row_counts().df())
    # @@@DLT_SNIPPET_END dataset_inspection


def sql_queries_snippet(fruitshop_pipeline: dlt.Pipeline):
    # @@@DLT_SNIPPET_START sql_queries
    # Convert the transformation above that selected the first 5 customers to a sql query
    @dlt.transformation()
    def copied_customers(dataset: dlt.Dataset):
        customers_table = dataset("SELECT * FROM customers LIMIT 5 ORDER BY name")
        return customers_table

    # Joins are also possible of course
    @dlt.transformation()
    def enriched_purchases(dataset: dlt.Dataset):
        enriched_purchases = dataset(
            "SELECT customers.name, purchases.quantity FROM purchases JOIN customers ON"
            " purchases.customer_id = customers.id"
        )
        return enriched_purchases

    # @@@DLT_SNIPPET_END sql_queries

    fruitshop_pipeline.run(
        [
            enriched_purchases(fruitshop_pipeline.dataset()),
            copied_customers(fruitshop_pipeline.dataset()),
        ]
    )
    assert load_table_counts(fruitshop_pipeline, "copied_customers", "enriched_purchases") == {
        "copied_customers": 5,
        "enriched_purchases": 3,
    }
