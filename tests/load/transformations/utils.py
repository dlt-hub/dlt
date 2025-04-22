import dlt
from random import randrange, choice
from typing import Any

from dlt.common.destination.dataset import SupportsReadableDataset

from tests.load.utils import (
    FILE_BUCKET,
    destinations_configs,
    SFTP_BUCKET,
    MEMORY_BUCKET,
)


# helpers
def row_counts(dataset: SupportsReadableDataset[Any], tables: list[str] = None) -> dict[str, int]:
    counts = dataset.row_counts(table_names=tables).arrow().to_pydict()
    return {t: c for t, c in zip(counts["table_name"], counts["row_count"])}


transformation_configs = destinations_configs(
    default_sql_configs=True,
    all_buckets_filesystem_configs=True,
    table_format_filesystem_configs=True,
    exclude=[
        "athena",  # NOTE: athena iceberg will probably work, we need to implement the model files for it
        "sqlalchemy_sqlite-no-staging",  # NOTE: sqlalchemy has no uuid support
    ],
    bucket_exclude=[SFTP_BUCKET, MEMORY_BUCKET],
)


def load_fruit_dataset(p: dlt.Pipeline) -> None:
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

    p.run([customers(), items(), purchases(), store_locations()])


def load_private_fruit_dataset(p: dlt.Pipeline) -> None:
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
    p.run([customers_ages()])


EXPECTED_FRUIT_ROW_COUNTS = {
    "customers": 4,
    "items": 4,
    "purchases": 20,
    "store_locations": 4,
    "customers__addresses": 2,
    "purchases__items": 60,
}
