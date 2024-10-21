from typing import Any, cast

import pytest
import dlt
import os

from dlt import Pipeline
from dlt.common import Decimal
from dlt.common.utils import uniq_id

from typing import List
from functools import reduce

from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    GCS_BUCKET,
    SFTP_BUCKET,
    MEMORY_BUCKET,
)
from dlt.destinations import filesystem
from tests.utils import TEST_STORAGE_ROOT
from dlt.common.destination.reference import TDestinationReferenceArg
from dlt.destinations.dataset import ReadableDBAPIDataset


def _run_dataset_checks(
    pipeline: Pipeline,
    destination_config: DestinationTestConfiguration,
    table_format: Any = None,
    alternate_access_pipeline: Pipeline = None,
) -> None:
    destination_type = pipeline.destination_client().config.destination_type

    skip_df_chunk_size_check = False
    expected_columns = ["id", "decimal", "other_decimal", "_dlt_load_id", "_dlt_id"]
    if destination_type == "bigquery":
        chunk_size = 50
        total_records = 80
    elif destination_type == "mssql":
        chunk_size = 700
        total_records = 1000
    else:
        chunk_size = 2048
        total_records = 3000

    # on filesystem one chunk is one file and not the default vector size
    if destination_type == "filesystem":
        skip_df_chunk_size_check = True

    # we always expect 2 chunks based on the above setup
    expected_chunk_counts = [chunk_size, total_records - chunk_size]

    @dlt.source()
    def source():
        @dlt.resource(
            table_format=table_format,
            write_disposition="replace",
            columns={
                "id": {"data_type": "bigint"},
                # we add a decimal with precision to see wether the hints are preserved
                "decimal": {"data_type": "decimal", "precision": 10, "scale": 3},
                "other_decimal": {"data_type": "decimal", "precision": 12, "scale": 3},
            },
        )
        def items():
            yield from [
                {
                    "id": i,
                    "children": [{"id": i + 100}, {"id": i + 1000}],
                    "decimal": Decimal("10.433"),
                    "other_decimal": Decimal("10.433"),
                }
                for i in range(total_records)
            ]

        @dlt.resource(
            table_format=table_format,
            write_disposition="replace",
            columns={
                "id": {"data_type": "bigint"},
                "double_id": {"data_type": "bigint"},
            },
        )
        def double_items():
            yield from [
                {
                    "id": i,
                    "double_id": i * 2,
                }
                for i in range(total_records)
            ]

        return [items, double_items]

    # run source
    s = source()
    pipeline.run(s, loader_file_format=destination_config.file_format)

    if alternate_access_pipeline:
        pipeline.destination = alternate_access_pipeline.destination

    # access via key
    table_relationship = pipeline._dataset()["items"]

    # full frame
    df = table_relationship.df()
    assert len(df.index) == total_records

    #
    # check dataframes
    #

    # chunk
    df = table_relationship.df(chunk_size=chunk_size)
    if not skip_df_chunk_size_check:
        assert len(df.index) == chunk_size
    # lowercase results for the snowflake case
    assert set(df.columns.values) == set(expected_columns)

    # iterate all dataframes
    frames = list(table_relationship.iter_df(chunk_size=chunk_size))
    if not skip_df_chunk_size_check:
        assert [len(df.index) for df in frames] == expected_chunk_counts

    # check all items are present
    ids = reduce(lambda a, b: a + b, [f[expected_columns[0]].to_list() for f in frames])
    assert set(ids) == set(range(total_records))

    # access via prop
    table_relationship = pipeline._dataset().items

    #
    # check arrow tables
    #

    # full table
    table = table_relationship.arrow()
    assert table.num_rows == total_records

    # chunk
    table = table_relationship.arrow(chunk_size=chunk_size)
    assert set(table.column_names) == set(expected_columns)
    assert table.num_rows == chunk_size

    # check frame amount and items counts
    tables = list(table_relationship.iter_arrow(chunk_size=chunk_size))
    assert [t.num_rows for t in tables] == expected_chunk_counts

    # check all items are present
    ids = reduce(lambda a, b: a + b, [t.column(expected_columns[0]).to_pylist() for t in tables])
    assert set(ids) == set(range(total_records))

    # check fetch accessors
    table_relationship = pipeline._dataset().items

    # check accessing one item
    one = table_relationship.fetchone()
    assert one[0] in range(total_records)

    # check fetchall
    fall = table_relationship.fetchall()
    assert len(fall) == total_records
    assert {item[0] for item in fall} == set(range(total_records))

    # check fetchmany
    many = table_relationship.fetchmany(chunk_size)
    assert len(many) == chunk_size

    # check iterfetchmany
    chunks = list(table_relationship.iter_fetch(chunk_size=chunk_size))
    assert [len(chunk) for chunk in chunks] == expected_chunk_counts
    ids = reduce(lambda a, b: a + b, [[item[0] for item in chunk] for chunk in chunks])
    assert set(ids) == set(range(total_records))

    # check that hints are carried over to arrow table
    expected_decimal_precision = 10
    expected_decimal_precision_2 = 12
    if destination_config.destination_type == "bigquery":
        # bigquery does not allow precision configuration..
        expected_decimal_precision = 38
        expected_decimal_precision_2 = 38
    assert (
        table_relationship.arrow().schema.field("decimal").type.precision
        == expected_decimal_precision
    )
    assert (
        table_relationship.arrow().schema.field("other_decimal").type.precision
        == expected_decimal_precision_2
    )

    # simple check that query also works
    tname = pipeline.sql_client().make_qualified_table_name("items")
    query_relationship = pipeline._dataset()(f"select * from {tname} where id < 20")

    # we selected the first 20
    table = query_relationship.arrow()
    assert table.num_rows == 20

    # check join query
    tdname = pipeline.sql_client().make_qualified_table_name("double_items")
    query = (
        f"SELECT i.id, di.double_id FROM {tname} as i JOIN {tdname} as di ON (i.id = di.id) WHERE"
        " i.id < 20 ORDER BY i.id ASC"
    )
    join_relationship = pipeline._dataset()(query)
    table = join_relationship.fetchall()
    assert len(table) == 20
    assert list(table[0]) == [0, 0]
    assert list(table[5]) == [5, 10]
    assert list(table[10]) == [10, 20]

    # check loads table access
    loads_table = pipeline._dataset()[pipeline.default_schema.loads_table_name]
    loads_table.fetchall()

    destination_for_dataset: TDestinationReferenceArg = (
        alternate_access_pipeline.destination
        if alternate_access_pipeline
        else destination_config.destination_type
    )

    # check dataset factory
    dataset = dlt._dataset(destination=destination_for_dataset, dataset_name=pipeline.dataset_name)
    # verfiy that sql client and schema are lazy loaded
    assert not dataset._schema
    assert not dataset._sql_client
    table_relationship = dataset.items
    table = table_relationship.fetchall()
    assert len(table) == total_records

    # check that schema is loaded by name
    dataset = cast(
        ReadableDBAPIDataset,
        dlt._dataset(
            destination=destination_for_dataset,
            dataset_name=pipeline.dataset_name,
            schema=pipeline.default_schema_name,
        ),
    )
    assert dataset.schema.tables["items"]["write_disposition"] == "replace"

    # check that schema is not loaded when wrong name given
    dataset = cast(
        ReadableDBAPIDataset,
        dlt._dataset(
            destination=destination_for_dataset,
            dataset_name=pipeline.dataset_name,
            schema="wrong_schema_name",
        ),
    )
    assert "items" not in dataset.schema.tables
    assert dataset.schema.name == pipeline.dataset_name

    # check that schema is loaded if no schema name given
    dataset = cast(
        ReadableDBAPIDataset,
        dlt._dataset(
            destination=destination_for_dataset,
            dataset_name=pipeline.dataset_name,
        ),
    )
    assert dataset.schema.name == pipeline.default_schema_name
    assert dataset.schema.tables["items"]["write_disposition"] == "replace"

    # check that there is no error when creating dataset without schema table
    dataset = cast(
        ReadableDBAPIDataset,
        dlt._dataset(
            destination=destination_for_dataset,
            dataset_name="unknown_dataset",
        ),
    )
    assert dataset.schema.name == "unknown_dataset"
    assert "items" not in dataset.schema.tables

    # create a newer schema with different name and see wether this is loaded
    from dlt.common.schema import Schema
    from dlt.common.schema import utils

    other_schema = Schema("some_other_schema")
    other_schema.tables["other_table"] = utils.new_table("other_table")

    pipeline._inject_schema(other_schema)
    pipeline.default_schema_name = other_schema.name
    with pipeline.destination_client() as client:
        client.update_stored_schema()

    dataset = cast(
        ReadableDBAPIDataset,
        dlt._dataset(
            destination=destination_for_dataset,
            dataset_name=pipeline.dataset_name,
        ),
    )
    assert dataset.schema.name == "some_other_schema"
    assert "other_table" in dataset.schema.tables


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True),
    ids=lambda x: x.name,
)
def test_read_interfaces_sql(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(
        "read_pipeline", dataset_name="read_test", dev_mode=True
    )
    _run_dataset_checks(pipeline, destination_config)


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        local_filesystem_configs=True,
        all_buckets_filesystem_configs=True,
        bucket_exclude=[SFTP_BUCKET, MEMORY_BUCKET],
    ),  # TODO: make SFTP work
    ids=lambda x: x.name,
)
def test_read_interfaces_filesystem(destination_config: DestinationTestConfiguration) -> None:
    # we force multiple files per table, they may only hold 700 items
    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "700"

    if destination_config.file_format not in ["parquet", "jsonl"]:
        pytest.skip(
            f"Test only works for jsonl and parquet, given: {destination_config.file_format}"
        )

    pipeline = destination_config.setup_pipeline(
        "read_pipeline",
        dataset_name="read_test",
        dev_mode=True,
    )

    _run_dataset_checks(pipeline, destination_config)

    # for gcs buckets we additionally test the s3 compat layer
    if destination_config.bucket_url == GCS_BUCKET:
        gcp_bucket = filesystem(
            GCS_BUCKET.replace("gs://", "s3://"), destination_name="filesystem_s3_gcs_comp"
        )
        pipeline = destination_config.setup_pipeline(
            "read_pipeline", dataset_name="read_test", dev_mode=True, destination=gcp_bucket
        )
        _run_dataset_checks(pipeline, destination_config)


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format="delta",
        bucket_exclude=[SFTP_BUCKET, MEMORY_BUCKET],
    ),
    ids=lambda x: x.name,
)
def test_delta_tables(destination_config: DestinationTestConfiguration) -> None:
    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "700"

    pipeline = destination_config.setup_pipeline(
        "read_pipeline", dataset_name="read_test", dev_mode=True
    )

    # in case of gcs we use the s3 compat layer for reading
    # for writing we still need to use the gc authentication, as delta_rs seems to use
    # methods on the s3 interface that are not implemented by gcs
    access_pipeline = pipeline
    if destination_config.bucket_url == GCS_BUCKET:
        gcp_bucket = filesystem(
            GCS_BUCKET.replace("gs://", "s3://"), destination_name="filesystem_s3_gcs_comp"
        )
        access_pipeline = destination_config.setup_pipeline(
            "read_pipeline", dataset_name="read_test", destination=gcp_bucket
        )

    _run_dataset_checks(
        pipeline,
        destination_config,
        table_format="delta",
        alternate_access_pipeline=access_pipeline,
    )
