from typing import Iterator, Dict, Union

import pytest

import dlt
from dlt.common.pendulum import pendulum
from dlt.common.utils import uniq_id
from dlt.extract import DltResource
from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_partition_by_date(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(f"bigquery_{uniq_id()}", full_refresh=True)

    @dlt.resource(
        write_disposition="merge",
        primary_key="my_date_column",
        columns={"my_date_column": {"data_type": "date", "partition": True, "nullable": False}},
    )
    def demo_resource() -> Iterator[Dict[str, Union[int, pendulum.DateTime]]]:
        for i in range(10):
            yield {"my_date_column": pendulum.from_timestamp(1700784000 + i * 50_000), "metric": i}

    @dlt.source(max_table_nesting=0)
    def demo_source() -> DltResource:
        return demo_resource

    pipeline.run(demo_source())


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_partition_by_integer(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(f"bigquery_{uniq_id()}", full_refresh=True)

    @dlt.resource(
        columns={"some_int": {"data_type": "bigint", "partition": True, "nullable": False}},
    )
    def demo_resource() -> Iterator[Dict[str, Union[int, pendulum.DateTime]]]:
        for i in range(10):
            yield {
                "my_date_column": pendulum.from_timestamp(1700784000 + i * 50_000),
                "some_int": i,
            }

    @dlt.source(max_table_nesting=0)
    def demo_source() -> DltResource:
        return demo_resource

    pipeline.run(demo_source())
