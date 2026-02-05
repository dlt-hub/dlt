from typing import Any, Iterator

import pytest

import dlt
from dlt.common.destination.client import DestinationClientDwhConfiguration
from dlt.common.schema.schema import Schema
from dlt.common.typing import TDataItem
from dlt.common.utils import custom_environ, uniq_id
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.pipeline.exceptions import PipelineStepFailed
from tests.load.utils import destinations_configs, DestinationTestConfiguration, AWS_BUCKET
from tests.utils import get_test_storage_root
from tests.pipeline.utils import assert_load_info, load_table_counts


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
    destination_config.staging = dlt.destinations.filesystem(get_test_storage_root())
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


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_staging_configs=True,
        subset=["clickhouse"],
        bucket_subset=[AWS_BUCKET],
        with_file_format="parquet",
    ),
    ids=lambda x: x.name,
)
def test_clickhouse_s3_extra_credentials_produces_valid_sql(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Verify that s3_extra_credentials generates syntactically valid SQL for ClickHouse.

    Uses a fake role_arn with a real S3 bucket and staging config. ClickHouse Cloud
    parses the extra_credentials() clause and attempts to assume the role, resulting
    in a 403/access error from S3 — proving the SQL syntax is valid.

    Skipped on OSS ClickHouse (localhost) where extra_credentials is not supported.
    """
    # extra_credentials is a ClickHouse Cloud-only feature, skip on localhost (OSS)
    pipeline = destination_config.setup_pipeline(f"ch_extra_creds_{uniq_id()}", dev_mode=True)
    with pipeline.destination_client() as dest_client:
        if dest_client.config.credentials.host == "localhost":  # type: ignore[attr-defined]
            pytest.skip("extra_credentials is only supported on ClickHouse Cloud")

    # set a fake role_arn via env var so it's picked up during credential resolution
    fake_role = "arn:aws:iam::000000000000:role/fake-test-role"
    with custom_environ(
        {"DESTINATION__CREDENTIALS__S3_EXTRA_CREDENTIALS": f'{{"role_arn": "{fake_role}"}}'}
    ):
        pipeline = destination_config.setup_pipeline(f"ch_extra_creds_{uniq_id()}", dev_mode=True)

        @dlt.resource(name="test_items", write_disposition="append")
        def test_items() -> Iterator[TDataItem]:
            yield {"id": 1, "value": "test"}

        # the load must fail because the role_arn is fake, but the SQL must be parseable
        with pytest.raises(PipelineStepFailed) as exc_info:
            pipeline.run(
                test_items,
                **destination_config.run_kwargs,
                staging=destination_config.staging,
            )

    # ClickHouse parsed extra_credentials() and tried to read from S3 with the fake role.
    # A 403 / storage read error proves the SQL was accepted; a syntax error would not
    # reach the storage layer at all.
    error_msg = str(exc_info.value)
    assert (
        "403" in error_msg or "ReadFromObjectStorage" in error_msg
    ), f"Expected S3 access error (403) but got: {error_msg}"
