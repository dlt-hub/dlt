from typing import Iterator

import pytest

import dlt
from dlt.common.typing import TDataItem
from dlt.common.utils import uniq_id
from tests.load.utils import destinations_configs, DestinationTestConfiguration
from tests.pipeline.utils import load_table_counts


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, all_staging_configs=True, subset=["clickhouse"]),
    ids=lambda x: x.name,
)
def test_clickhouse_destination_append(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(f"clickhouse_{uniq_id()}", dev_mode=True)

    try:

        @dlt.resource(name="items", write_disposition="append")
        def items() -> Iterator[TDataItem]:
            yield {
                "id": 1,
                "name": "item",
                "sub_items": [
                    {"id": 101, "name": "sub item 101"},
                    {"id": 101, "name": "sub item 102"},
                ],
            }

        pipeline.run(
            items,
            **destination_config.run_kwargs,
            staging=destination_config.staging,
        )

        table_counts = load_table_counts(
            pipeline, *[t["name"] for t in pipeline.default_schema._schema_tables.values()]
        )
        assert table_counts["items"] == 1
        assert table_counts["items__sub_items"] == 2
        assert table_counts["_dlt_loads"] == 1

        # Load again with schema evolution.
        @dlt.resource(name="items", write_disposition="append")
        def items2() -> Iterator[TDataItem]:
            yield {
                "id": 1,
                "name": "item",
                "new_field": "hello",
                "sub_items": [
                    {
                        "id": 101,
                        "name": "sub item 101",
                        "other_new_field": "hello 101",
                    },
                    {
                        "id": 101,
                        "name": "sub item 102",
                        "other_new_field": "hello 102",
                    },
                ],
            }

        pipeline.run(items2, **destination_config.run_kwargs)
        table_counts = load_table_counts(
            pipeline, *[t["name"] for t in pipeline.default_schema._schema_tables.values()]
        )
        assert table_counts["items"] == 2
        assert table_counts["items__sub_items"] == 4
        assert table_counts["_dlt_loads"] == 2

    except Exception as e:
        raise e

    finally:
        with pipeline.sql_client() as client:
            client.drop_dataset()
