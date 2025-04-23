import dlt
import pytest
import os
from random import randrange, choice
from typing import Any, Dict, Tuple

from dlt.common.destination.dataset import SupportsReadableDataset

from tests.load.utils import (
    FILE_BUCKET,
    destinations_configs,
    SFTP_BUCKET,
    MEMORY_BUCKET,
    DestinationTestConfiguration,
)


# helpers
def row_counts(dataset: SupportsReadableDataset[Any], tables: list[str] = None) -> dict[str, int]:
    counts = dataset.row_counts(table_names=tables).arrow().to_pydict()
    return {t: c for t, c in zip(counts["table_name"], counts["row_count"])}


def get_job_types(p: dlt.Pipeline, stage: str = "loaded") -> Dict[str, Dict[str, Any]]:
    """
    gets a list of loaded jobs by type for each table
    this is useful for testing to check that the correct transformations were used
    """

    jobs = []
    if stage == "loaded":
        jobs = p.last_trace.last_load_info.load_packages[0].jobs["completed_jobs"]
    elif stage == "extracted":
        jobs = p.last_trace.last_extract_info.load_packages[0].jobs["new_jobs"]

    tables: Dict[str, Dict[str, Any]] = {}

    for j in jobs:
        file_format = j.job_file_info.file_format
        table_name = j.job_file_info.table_name
        if table_name.startswith("_dlt"):
            continue
        tables.setdefault(table_name, {})[file_format] = (
            tables.get(table_name, {}).get(file_format, 0) + 1
        )

    return tables


def transformation_configs(only_duckdb: bool = False):
    return destinations_configs(
        default_sql_configs=True,
        all_buckets_filesystem_configs=True,
        table_format_filesystem_configs=True,
        exclude=[
            "athena",  # NOTE: athena iceberg will probably work, we need to implement the model files for it
            "sqlalchemy_sqlite-no-staging",  # NOTE: sqlalchemy has no uuid support
            (  # NOTE: duckdb parquet does not need to be tested explicitely if we have the regular
                "duckdb-parquet-no-staging"
            ),
            "synapse",  # should probably work? no model file support at the moment
            "dremio",  # should probably work? no model file support at the moment
        ],
        bucket_exclude=[SFTP_BUCKET, MEMORY_BUCKET],
        subset=(
            [
                "duckdb",
            ]
            if only_duckdb
            else None
        ),
    )


def setup_transformation_pipelines(
    destination_config: DestinationTestConfiguration, transformation_type: str
) -> Tuple[dlt.Pipeline, dlt.Pipeline]:
    """Set up a source and a destination pipeline for transformation tests"""

    if destination_config.destination_type == "filesystem" and transformation_type == "sql":
        pytest.skip("Filesystem destination does not support sql transformations")

    # Disable unique indexing for postgres, otherwise there will be a not null constraint error
    # because we're copying from the same table, should be fixed in hints forwarding
    if destination_config.destination_type == "postgres":
        os.environ["DESTINATION__POSTGRES__CREATE_INDEXES"] = "false"

    # setup incoming data and transformed data pipelines
    fruit_p = destination_config.setup_pipeline(
        "fruit_pipeline", dataset_name="source", use_single_dataset=False, dev_mode=True
    )
    dest_p = destination_config.setup_pipeline(
        "fruit_pipeline",
        dataset_name="transformed",
        use_single_dataset=False,
        # for filesystem destination we load to a duckdb on python transformations
        destination="duckdb" if destination_config.destination_type == "filesystem" else None,
        dev_mode=True,
    )

    return fruit_p, dest_p


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
