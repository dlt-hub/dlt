import pytest

import dlt
from dlt.common import Decimal
from dlt.destinations.impl.bigquery.bigquery_adapter import bigquery_adapter

from tests.pipeline.utils import assert_load_info
from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration
from tests.load.utils import delete_dataset


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["bigquery"]),
    ids=lambda x: x.name,
)
def test_bigquery_numeric_types(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline("test_bigquery_numeric_types")

    columns = [
        {"name": "col_big_numeric", "data_type": "decimal", "precision": 47, "scale": 9},
        {"name": "col_numeric", "data_type": "decimal", "precision": 38, "scale": 9},
    ]

    data = [
        {
            # Valid BIGNUMERIC and NUMERIC values
            "col_big_numeric": Decimal("12345678901234567890123456789012345678.123456789"),
            "col_numeric": Decimal("12345678901234567890123456789.123456789"),
        },
    ]

    info = pipeline.run(iter(data), table_name="big_numeric", columns=columns)  # type: ignore[arg-type]
    assert_load_info(info)

    with pipeline.sql_client() as client:
        with client.execute_query("SELECT col_big_numeric, col_numeric FROM big_numeric;") as q:
            row = q.fetchone()
            assert row[0] == data[0]["col_big_numeric"]
            assert row[1] == data[0]["col_numeric"]


def test_bigquery_streaming_insert():
    pipe = dlt.pipeline(destination="bigquery")
    pack = pipe.run([{"field1": 1, "field2": 2}], table_name="test_streaming_items")

    assert_load_info(pack)


def test_bigquery_adapter_streaming_insert():
    @dlt.resource
    def test_resource():
        yield {"field1": 1, "field2": 2}

    bigquery_adapter(test_resource, insert_api="streaming")

    pipe = dlt.pipeline(destination="bigquery")
    pack = pipe.run(test_resource, table_name="test_streaming_items")

    assert_load_info(pack)
