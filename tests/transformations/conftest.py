from typing import Any

import pytest
from random import randrange, choice

import dlt

from dlt.destinations import duckdb


@pytest.fixture(scope="function")
def pipeline_1() -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="example_pipeline",
        destination="duckdb",
        dataset_name="example_dataset",
        dev_mode=True,
    )


@pytest.fixture(scope="function")
def dest_p() -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="example_pipeline",
        destination="duckdb",
        dataset_name="example_dataset",
        dev_mode=True,
    )


#
# Populated fruitshop pipeline, we could create a proper fixture for this for speed
# maybe jsonl filesystem source
#


@pytest.fixture(scope="function")
def fruit_p() -> dlt.Pipeline:
    raw_items = [
        {"id": 1, "name": "banana", "price": 10},
        {"id": 2, "name": "apple", "price": 25},
        {"id": 3, "name": "orange", "price": 17},
        {"id": 4, "name": "pear", "price": 35},
    ]

    store_locations_raw = [
        {"id": 1, "name": "mitte"},
        {"id": 2, "name": "wedding"},
        {"id": 3, "name": "kreuzberg"},
        {"id": 4, "name": "neukölln"},
    ]

    @dlt.resource(name="customers", primary_key="id", write_disposition="merge")
    def customers() -> Any:
        yield from [
            {"id": 1, "name": "andrea", "location_id": choice(store_locations_raw)["id"]},
            {"id": 2, "name": "violetta", "location_id": choice(store_locations_raw)["id"]},
            {"id": 3, "name": "marcin", "location_id": choice(store_locations_raw)["id"]},
            {
                "id": 4,
                "name": "dave",
                "location_id": choice(store_locations_raw)["id"],
                "addresses": [
                    {
                        "id": 1,
                        "city": "berlin",
                    },
                    {
                        "id": 2,
                        "city": "leipzig",
                    },
                ],
            },
        ]

    # apply a ppi hint
    customers.apply_hints(columns={"name": {"x-pii": True}})  # type: ignore

    @dlt.resource(name="items", primary_key="id", write_disposition="merge")
    def items() -> Any:
        yield from raw_items

    @dlt.resource(name="store_locations", primary_key="id", write_disposition="merge")
    def store_locations() -> Any:
        yield from store_locations_raw

    @dlt.resource(name="purchases", primary_key="id", write_disposition="merge")
    def purchases() -> Any:
        # we create 20 orders with a random amount of random items
        for order_id in range(1, 21):
            order_items = []
            total = 0
            for i in range(0, 3):
                item = choice(raw_items)
                total += item["price"]  # type: ignore
                order_items.append({"item_id": item["id"], "position": i})

            yield {
                "id": order_id,
                "customer_id": randrange(1, 5),
                "billing_customer_id": randrange(1, 5),
                "total": total,
                "items": order_items,
            }

    pipeline = dlt.pipeline(
        pipeline_name="fruitshop_pipeline",
        destination=duckdb(credentials="fruits.db"),
        dataset_name="fruitshop_dataset",
        dev_mode=True,
    )

    pipeline.run([customers(), items(), purchases(), store_locations()])
    return pipeline


@pytest.fixture(scope="function")
def private_fruit_p() -> dlt.Pipeline:
    """second pipeline that loads some stuff to the same duckdb file but other dataset"""

    @dlt.resource(name="customers_ages", primary_key="id", write_disposition="merge")
    def customers_ages() -> Any:
        yield from [
            {"id": 1, "age": 25},
            {"id": 2, "age": 30},
            {"id": 3, "age": 35},
            {"id": 4, "age": 40},
        ]

    customers_ages.apply_hints(columns={"age": {"x-pii-2": True}})  # type: ignore

    pipeline = dlt.pipeline(
        pipeline_name="private_fruitshop_pipeline",
        destination=duckdb(credentials="fruits.db"),
        dataset_name="fruitshop_dataset_private",
        dev_mode=True,
    )

    pipeline.run([customers_ages()])

    return pipeline


EXPECTED_FRUIT_ROW_COUNTS = {
    "customers": 4,
    "items": 4,
    "purchases": 20,
    "store_locations": 4,
    "customers__addresses": 2,
    "purchases__items": 60,
}
