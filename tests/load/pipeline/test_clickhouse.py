from typing import Any, Iterator

import pytest

import dlt
from dlt.common.destination.reference import DestinationClientDwhConfiguration
from dlt.common.schema.schema import Schema
from dlt.common.typing import TDataItem
from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from tests.load.utils import destinations_configs, DestinationTestConfiguration
from tests.pipeline.utils import load_table_counts
from tests.utils import TEST_STORAGE_ROOT, assert_load_info

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


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


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["clickhouse"]),
    ids=lambda x: x.name,
)
def test_clickhouse_no_dataset_name(destination_config: DestinationTestConfiguration) -> None:
    # add staging to cover staging dataset name that must be present
    destination_config.staging = dlt.destinations.filesystem(TEST_STORAGE_ROOT)
    # create explicitly empty dataset
    # NOTE: we use empty string here but when creating pipeline directly you can just skip
    # the dataset_name argument
    pipeline = destination_config.setup_pipeline("no_dataset_name_items", dataset_name="")
    # run only on localhost because we have a common dataset for all tests that may run in parallel
    dest_client, staging_client = pipeline._get_destination_clients(Schema("test"))

    # make sure staging has dataset name which is pipeline default
    assert isinstance(staging_client.config, DestinationClientDwhConfiguration)
    assert staging_client.config.dataset_name == pipeline._make_dataset_name(None, pipeline.staging)

    assert isinstance(dest_client.config, DestinationClientDwhConfiguration)
    print(dest_client.config.dataset_name)
    assert dest_client.config.dataset_name == ""

    if dest_client.config.credentials.host != "localhost":  # type: ignore[attr-defined]
        pytest.skip("Empty dataset may be tested only on a localhost clickhouse")

    @dlt.resource(name="items", write_disposition="merge", primary_key="id")
    def items() -> Iterator[Any]:
        yield {
            "id": 1,
            "name": "item",
            "sub_items": [{"id": 101, "name": "sub item 101"}, {"id": 101, "name": "sub item 102"}],
        }

    info = pipeline.run([items], **destination_config.run_kwargs)
    assert_load_info(info)

    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema._schema_tables.values()]
    )
    assert table_counts["items"] == 1
    assert table_counts["items__sub_items"] == 2
    assert table_counts["_dlt_loads"] == 1

    # test drop storage, first create additional table and make sure it was not destroyed
    pipeline_2 = destination_config.setup_pipeline("no_dataset_name_events", dataset_name="")
    info = pipeline_2.run([items.with_name("events")], **destination_config.run_kwargs)
    assert_load_info(info)

    table_counts_2 = load_table_counts(
        pipeline_2, *[t["name"] for t in pipeline_2.default_schema._schema_tables.values()]
    )
    assert table_counts_2["events"] == 1
    assert table_counts_2["events__sub_items"] == 2
    assert table_counts_2["_dlt_loads"] == 2

    with pipeline.destination_client() as dest_client:
        assert dest_client.is_storage_initialized()
        dest_client.drop_storage()
        assert not dest_client.is_storage_initialized()

    # events are still there
    table_counts_2 = load_table_counts(
        pipeline_2, *[t for t in pipeline_2.default_schema.data_table_names()]
    )
    assert table_counts_2["events"] == 1
    assert table_counts_2["events__sub_items"] == 2

    # all other tables got dropped
    with pytest.raises(DatabaseUndefinedRelation):
        table_counts_2 = load_table_counts(
            pipeline_2, *[t for t in pipeline_2.default_schema.dlt_table_names()]
        )

    # load again
    info = pipeline_2.run([items.with_name("events")], **destination_config.run_kwargs)
    assert_load_info(info)

    table_counts_2 = load_table_counts(
        pipeline_2, *[t["name"] for t in pipeline_2.default_schema._schema_tables.values()]
    )
    # merge
    assert table_counts_2["events"] == 1
    assert table_counts_2["events__sub_items"] == 2
    # table got dropped so we have 1 load
    assert table_counts_2["_dlt_loads"] == 1
