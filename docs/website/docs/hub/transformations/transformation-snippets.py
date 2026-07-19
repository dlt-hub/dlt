import pytest
from typing import Any, Iterator

import dlt
from tests.pipeline.utils import load_table_counts

pytest.importorskip("dlthub")


@pytest.fixture(scope="function")
def fruitshop_pipeline() -> dlt.Pipeline:
    """Set up a fruitshop fixture dataset for transformations examples"""

    # @@@DLT_SNIPPET_START quick_start_example

    import dlt
    from dlt.destinations import duckdb
    from dlt._workspace._templates._single_file_templates.fruitshop_pipeline import (
        fruitshop as fruitshop_source,
    )

    fruitshop_pipeline = dlt.pipeline(
        "fruitshop", destination=duckdb("./test_duck.duckdb"), dev_mode=True
    )
    fruitshop_pipeline.run(fruitshop_source())
    # @@@DLT_SNIPPET_END quick_start_example

    return fruitshop_pipeline


def basic_transformation_snippet(fruitshop_pipeline: dlt.Pipeline) -> None:
    # @@@DLT_SNIPPET_START basic_transformation
    from typing import Any

    @dlt.hub.transformation
    def copied_customers(dataset: dlt.Dataset) -> Any:
        customers_table = dataset["customers"]
        yield customers_table.order_by("name").limit(5)

    # Same pipeline & same dataset
    fruitshop_pipeline.run(copied_customers(fruitshop_pipeline.dataset()))

    # show rowcounts again, we now have a new table in the schema and the destination
    print(fruitshop_pipeline.dataset().row_counts().df())
    # @@@DLT_SNIPPET_END basic_transformation

    # copied customers now also exist
    assert load_table_counts(fruitshop_pipeline, "copied_customers") == {
        "copied_customers": 5
    }


def orders_per_user_snippet(fruitshop_pipeline: dlt.Pipeline) -> None:
    # @@@DLT_SNIPPET_START orders_per_user

    @dlt.hub.transformation(name="orders_per_user", write_disposition="merge")
    def orders_per_user(dataset: dlt.Dataset) -> Any:
        purchases = dataset.table("purchases").to_ibis()
        yield purchases.group_by(purchases.customer_id).aggregate(
            order_count=purchases.id.count()
        )

    # @@@DLT_SNIPPET_END orders_per_user
    fruitshop_pipeline.run(orders_per_user(fruitshop_pipeline.dataset()))
    assert load_table_counts(fruitshop_pipeline, "orders_per_user") == {
        "orders_per_user": 13
    }


def loading_to_other_datasets_snippet(fruitshop_pipeline: dlt.Pipeline) -> None:
    # @@@DLT_SNIPPET_START loading_to_other_datasets
    import dlt
    from dlt.destinations import duckdb

    @dlt.hub.transformation
    def copied_customers(dataset: dlt.Dataset) -> Any:
        customers_table = dataset["customers"]
        yield customers_table.order_by("name").limit(5)

    # Same duckdb instance, different dataset
    dest_p = dlt.pipeline(
        "fruitshop_dataset",
        destination=duckdb("./test_duck.duckdb"),
        dataset_name="copied_dataset",
        dev_mode=True,
    )
    dest_p.run(copied_customers(fruitshop_pipeline.dataset()))
    # @@@DLT_SNIPPET_END loading_to_other_datasets

    # @@@DLT_SNIPPET_START loading_to_other_datasets_other_engine
    # Different engine (Postgres → DuckDB)
    duck_p = dlt.pipeline("fruitshop_warehouse", destination="postgres")
    duck_p.run(copied_customers(fruitshop_pipeline.dataset()))
    # @@@DLT_SNIPPET_END loading_to_other_datasets_other_engine


def multiple_transformations_snippet(fruitshop_pipeline: dlt.Pipeline) -> None:
    # @@@DLT_SNIPPET_START multiple_transformations
    import dlt

    @dlt.source
    def my_transformations(dataset: dlt.Dataset) -> Any:
        @dlt.hub.transformation(write_disposition="append")
        def enriched_purchases(dataset: dlt.Dataset) -> Any:
            purchases = dataset.table("purchases").to_ibis()
            customers = dataset.table("customers").to_ibis()
            yield purchases.join(customers, purchases.customer_id == customers.id)

        @dlt.hub.transformation(write_disposition="replace")
        def total_items_sold(dataset: dlt.Dataset) -> Any:
            purchases = dataset.table("purchases").to_ibis()
            yield purchases.aggregate(total_qty=purchases.quantity.sum())

        return enriched_purchases(dataset), total_items_sold(dataset)

    fruitshop_pipeline.run(my_transformations(fruitshop_pipeline.dataset()))
    # @@@DLT_SNIPPET_END multiple_transformations
    assert load_table_counts(
        fruitshop_pipeline, "enriched_purchases", "total_items_sold"
    ) == {
        "enriched_purchases": 100,
        "total_items_sold": 1,
    }


def multiple_transformation_instructions_snippet(
    fruitshop_pipeline: dlt.Pipeline,
) -> None:
    # @@@DLT_SNIPPET_START multiple_transformation_instructions
    import dlt

    # this (probably nonsensical) transformation will create a union of the customers and purchases tables
    @dlt.hub.transformation(write_disposition="append")
    def union_of_tables(dataset: dlt.Dataset) -> Any:
        yield dataset.table("purchases")
        yield dataset.table("customers")

    # @@@DLT_SNIPPET_END multiple_transformation_instructions


def supply_hints_snippet(fruitshop_pipeline: dlt.Pipeline) -> None:
    # @@@DLT_SNIPPET_START supply_hints
    import dlt

    # change precision and scale of the price column
    @dlt.hub.transformation(
        write_disposition="append", columns={"price": {"precision": 10, "scale": 2}}
    )
    def precision_change(dataset: dlt.Dataset) -> Any:
        yield dataset.inventory

    # @@@DLT_SNIPPET_END supply_hints


def dataset_inspection_snippet(fruitshop_pipeline: dlt.Pipeline) -> None:
    # @@@DLT_SNIPPET_START dataset_inspection
    # Show row counts for every table
    print(fruitshop_pipeline.dataset().row_counts().df())
    # @@@DLT_SNIPPET_END dataset_inspection


def sql_queries_snippet(fruitshop_pipeline: dlt.Pipeline) -> None:
    # @@@DLT_SNIPPET_START sql_queries
    # @@@DLT_SNIPPET_START sql_queries_short
    # Convert the transformation above that selected the first 5 customers to a sql query
    @dlt.hub.transformation
    def copied_customers(dataset: dlt.Dataset) -> Any:
        customers_table = dataset(
            """
            SELECT *
            FROM customers
            ORDER BY name
            LIMIT 5
        """
        )
        yield customers_table

    # @@@DLT_SNIPPET_END sql_queries_short

    # Joins and other more complex queries are also possible
    @dlt.hub.transformation
    def enriched_purchases(dataset: dlt.Dataset) -> Any:
        enriched_purchases = dataset(
            """
            SELECT customers.name, purchases.quantity
            FROM purchases
            JOIN customers
                ON purchases.customer_id = customers.id
            """
        )
        yield enriched_purchases

    # You can even use a different dialect than the one used by the destination by supplying the dialect parameter
    # dlt will compile the query to the right destination dialect
    @dlt.hub.transformation
    def enriched_purchases_postgres(dataset: dlt.Dataset) -> Any:
        enriched_purchases = dataset(
            """
            SELECT customers.name, purchases.quantity
            FROM purchases
            JOIN customers
                ON purchases.customer_id = customers.id
            """,
            query_dialect="duckdb",
        )
        yield enriched_purchases

    # @@@DLT_SNIPPET_END sql_queries

    fruitshop_pipeline.run(
        [
            enriched_purchases(fruitshop_pipeline.dataset()),
            copied_customers(fruitshop_pipeline.dataset()),
        ]
    )
    assert load_table_counts(
        fruitshop_pipeline, "copied_customers", "enriched_purchases"
    ) == {
        "copied_customers": 5,
        "enriched_purchases": 100,
    }


def arrow_dataframe_operations_snippet(fruitshop_pipeline: dlt.Pipeline) -> None:
    # @@@DLT_SNIPPET_START arrow_dataframe_operations

    @dlt.hub.transformation
    def copied_customers(dataset: dlt.Dataset) -> Any:
        # get full customers table as arrow table
        customers = dataset.table("customers").arrow()

        # Sort the table by 'name'
        sorted_customers = customers.sort_by([("name", "ascending")])

        # Take first 5 rows
        yield sorted_customers.slice(0, 5)

    # Example tables (replace with your actual data)
    @dlt.hub.transformation
    def enriched_purchases(dataset: dlt.Dataset) -> Any:
        # get both fully tables as dataframes
        purchases = dataset.table("purchases").df()
        customers = dataset.table("customers").df()

        # Merge (JOIN) the DataFrames
        result = purchases.merge(customers, left_on="customer_id", right_on="id")

        # Select only the desired columns
        yield result[["name", "quantity"]]

    # @@@DLT_SNIPPET_END arrow_dataframe_operations

    # Perform the join
    fruitshop_pipeline.run(
        [
            enriched_purchases(fruitshop_pipeline.dataset()),
            copied_customers(fruitshop_pipeline.dataset()),
        ]
    )
    assert load_table_counts(
        fruitshop_pipeline, "copied_customers", "enriched_purchases"
    ) == {
        "copied_customers": 5,
        "enriched_purchases": 100,
    }


def computed_schema_snippet(fruitshop_pipeline: dlt.Pipeline) -> None:
    # @@@DLT_SNIPPET_START computed_schema
    # Show the computed schema before the transformation is executed
    dataset = fruitshop_pipeline.dataset()
    purchases = dataset.table("purchases").to_ibis()
    customers = dataset.table("customers").to_ibis()
    enriched_purchases = purchases.join(
        customers, purchases.customer_id == customers.id
    )
    print(dataset(enriched_purchases).columns)
    # @@@DLT_SNIPPET_END computed_schema


def column_level_lineage_snippet(fruitshop_pipeline: dlt.Pipeline) -> None:
    # @@@DLT_SNIPPET_START column_level_lineage
    @dlt.hub.transformation
    def enriched_purchases(dataset: dlt.Dataset) -> Any:
        enriched_purchases = dataset(
            """
            SELECT customers.name, purchases.quantity
            FROM purchases
            JOIN customers
                ON purchases.customer_id = customers.id
            """
        )
        yield enriched_purchases

    # Let's run the transformation and see that the name column in the NEW table is also marked as PII
    fruitshop_pipeline.run(enriched_purchases(fruitshop_pipeline.dataset()))
    assert (
        fruitshop_pipeline.dataset().schema.tables["enriched_purchases"]["columns"][
            "name"
        ][
            "x-annotation-pii"  # type: ignore
        ]
        is True
    )
    # @@@DLT_SNIPPET_END column_level_lineage


def in_transit_transformations_snippet() -> None:
    # @@@DLT_SNIPPET_START in_transit_transformations multiple transformations in a sou
    from dlt.sources.rest_api import (
        rest_api_source,
    )

    # loads some data from our example api at https://jaffle-shop.scalevector.ai/docs
    source = rest_api_source(
        {
            "client": {
                "base_url": "https://jaffle-shop.scalevector.ai/api/v1",
            },
            "resources": [
                "stores",
                {
                    "name": "orders",
                    "endpoint": {
                        "path": "orders",
                        "params": {
                            "start_date": "2017-01-01",
                            "end_date": "2017-01-31",
                        },
                    },
                },
            ],
        }
    )

    # load to a local DuckDB instance
    transit_pipeline = dlt.pipeline(
        "jaffle_shop", destination="duckdb", dataset_name="in_transit"
    )
    transit_pipeline.run(source)

    # load aggregated data to a warehouse destination
    @dlt.hub.transformation
    def orders_per_store(dataset: dlt.Dataset) -> Any:
        orders = dataset.table("orders").to_ibis()
        stores = dataset.table("stores").to_ibis()
        yield (
            orders.join(stores, orders.store_id == stores.id)
            .group_by(stores.name)
            .aggregate(order_count=orders.id.count())
        )

    # load aggregated data to a warehouse destination
    warehouse_pipeline = dlt.pipeline(
        "jaffle_warehouse",
        destination="postgres",
        dataset_name="warehouse",
        dev_mode=True,
    )
    warehouse_pipeline.run(orders_per_store(transit_pipeline.dataset()))
    # @@@DLT_SNIPPET_END in_transit_transformations

    assert load_table_counts(warehouse_pipeline, "orders_per_store") == {
        "orders_per_store": 1
    }


def incremental_stateful_cursor_snippet() -> None:
    # @@@DLT_SNIPPET_START incremental_stateful_cursor
    from typing import Any, Iterator, List

    import dlt
    from dlt.common.pendulum import pendulum
    from dlt.destinations import duckdb

    @dlt.resource(name="orders", primary_key="id", write_disposition="append")
    def orders(rows: List[Any]) -> Iterator[Any]:
        yield rows

    pipeline = dlt.pipeline(
        "orders_stateful",
        destination=duckdb("./orders_stateful.duckdb"),
        dev_mode=True,
    )
    pipeline.run(
        orders(
            [
                {
                    "id": 1,
                    "amount": 100,
                    "created_at": pendulum.datetime(2026, 1, 1, tz="UTC"),
                },
                {
                    "id": 2,
                    "amount": 200,
                    "created_at": pendulum.datetime(2026, 1, 2, tz="UTC"),
                },
                {
                    "id": 3,
                    "amount": 300,
                    "created_at": pendulum.datetime(2026, 1, 3, tz="UTC"),
                },
            ]
        )
    )

    # @@@DLT_SNIPPET_START incremental_stateful_cursor_definition
    @dlt.hub.transformation(
        write_disposition="append",
        primary_key="id",
        incremental=dlt.sources.incremental(
            "created_at",
            initial_value=pendulum.datetime(2000, 1, 1, tz="UTC"),
            range_start="open",
        ),
    )
    def recent_orders(dataset: dlt.Dataset) -> Any:
        yield dataset.table("orders")

    # @@@DLT_SNIPPET_END incremental_stateful_cursor_definition

    pipeline.run(recent_orders(pipeline.dataset()))

    # More orders arrive.
    pipeline.run(
        orders(
            [
                {
                    "id": 4,
                    "amount": 400,
                    "created_at": pendulum.datetime(2026, 1, 4, tz="UTC"),
                },
                {
                    "id": 5,
                    "amount": 500,
                    "created_at": pendulum.datetime(2026, 1, 5, tz="UTC"),
                },
            ]
        )
    )

    # Second run: only the new rows land
    pipeline.run(recent_orders(pipeline.dataset()))
    # @@@DLT_SNIPPET_END incremental_stateful_cursor

    ids = sorted(row[0] for row in pipeline.dataset().table("recent_orders").fetchall())
    assert ids == [1, 2, 3, 4, 5]


def incremental_load_time_cursor_snippet() -> None:
    # @@@DLT_SNIPPET_START incremental_load_time_cursor
    from typing import Any, Iterator, List

    import dlt
    from dlt.common.pendulum import pendulum
    from dlt.destinations import duckdb

    @dlt.resource(name="orders", primary_key="id", write_disposition="append")
    def orders(rows: List[Any]) -> Iterator[Any]:
        yield rows

    pipeline = dlt.pipeline(
        "orders_by_load",
        destination=duckdb("./orders_by_load.duckdb"),
        dev_mode=True,
    )
    pipeline.run(orders([{"id": 1}, {"id": 2}, {"id": 3}]))

    # @@@DLT_SNIPPET_START incremental_load_time_cursor_definition
    @dlt.hub.transformation(write_disposition="append")
    def orders_by_load(
        dataset: dlt.Dataset,
        loaded_at: dlt.sources.incremental[pendulum.DateTime] = dlt.sources.incremental(
            "_dlt_loads.inserted_at",
            initial_value=pendulum.datetime(2000, 1, 1, tz="UTC"),
            range_start="open",
        ),
    ) -> Any:
        yield dataset.table("orders").incremental(loaded_at)

    # @@@DLT_SNIPPET_END incremental_load_time_cursor_definition

    pipeline.run(orders_by_load(pipeline.dataset()))

    pipeline.run(orders([{"id": 4}, {"id": 5}]))

    pipeline.run(orders_by_load(pipeline.dataset()))
    # @@@DLT_SNIPPET_END incremental_load_time_cursor

    ids = sorted(
        row[0] for row in pipeline.dataset().table("orders_by_load").fetchall()
    )
    assert ids == [1, 2, 3, 4, 5]


def incremental_scheduler_window_snippet() -> None:
    # @@@DLT_SNIPPET_START incremental_scheduler_window
    import os
    from typing import Any, Iterator, List

    import dlt
    from dlt.common.pendulum import pendulum
    from dlt.destinations import duckdb

    @dlt.resource(name="orders", primary_key="id", write_disposition="append")
    def orders(rows: List[Any]) -> Iterator[Any]:
        yield rows

    pipeline = dlt.pipeline(
        "orders_window",
        destination=duckdb("./orders_window.duckdb"),
        dev_mode=True,
    )
    pipeline.run(
        orders(
            [
                {"id": i, "created_at": pendulum.datetime(2026, 1, i, tz="UTC")}
                for i in range(1, 11)
            ]
        )
    )

    # @@@DLT_SNIPPET_START incremental_scheduler_window_definition
    @dlt.hub.transformation(write_disposition="replace")
    def orders_window(
        dataset: dlt.Dataset,
        window: dlt.sources.incremental[pendulum.DateTime] = dlt.sources.incremental(
            "created_at",
            initial_value=pendulum.datetime(2000, 1, 1, tz="UTC"),
            allow_external_schedulers=True,
            range_start="closed",
            range_end="open",
        ),
    ) -> Any:
        yield dataset.table("orders").incremental(window)

    # @@@DLT_SNIPPET_END incremental_scheduler_window_definition

    os.environ["DLT_INTERVAL_START"] = "2026-01-05T00:00:00+00:00"
    os.environ["DLT_INTERVAL_END"] = "2026-01-10T00:00:00+00:00"
    pipeline.run(orders_window(pipeline.dataset()))  # ids 5..9 land

    pipeline.run(orders_window(pipeline.dataset()))

    os.environ["DLT_INTERVAL_START"] = "2026-01-02T00:00:00+00:00"
    os.environ["DLT_INTERVAL_END"] = "2026-01-05T00:00:00+00:00"
    pipeline.run(orders_window(pipeline.dataset()))  # ids 2..4 land
    # @@@DLT_SNIPPET_END incremental_scheduler_window

    os.environ.pop("DLT_INTERVAL_START", None)
    os.environ.pop("DLT_INTERVAL_END", None)

    ids = sorted(row[0] for row in pipeline.dataset().table("orders_window").fetchall())
    assert ids == [2, 3, 4]
