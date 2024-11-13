from typing import Any, cast

import pytest
import dlt
import os

from dlt import Pipeline
from dlt.common import Decimal

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
from dlt.destinations.dataset import ReadableDBAPIDataset, ReadableRelationUnknownColumnException
from tests.load.utils import drop_pipeline_data

EXPECTED_COLUMNS = ["id", "decimal", "other_decimal", "_dlt_load_id", "_dlt_id"]


def _total_records(p: Pipeline) -> int:
    """how many records to load for a given pipeline"""
    if p.destination.destination_type == "dlt.destinations.bigquery":
        return 80
    elif p.destination.destination_type == "dlt.destinations.mssql":
        return 1000
    return 3000


def _chunk_size(p: Pipeline) -> int:
    """chunk size for a given pipeline"""
    if p.destination.destination_type == "dlt.destinations.bigquery":
        return 50
    elif p.destination.destination_type == "dlt.destinations.mssql":
        return 700
    return 2048


def _expected_chunk_count(p: Pipeline) -> List[int]:
    return [_chunk_size(p), _total_records(p) - _chunk_size(p)]


@pytest.fixture(scope="session")
def populated_pipeline(request) -> Any:
    """fixture that returns a pipeline object populated with the example data"""
    destination_config = cast(DestinationTestConfiguration, request.param)

    if (
        destination_config.file_format not in ["parquet", "jsonl"]
        and destination_config.destination_type == "filesystem"
    ):
        pytest.skip(
            "Test only works for jsonl and parquet on filesystem destination, given:"
            f" {destination_config.file_format}"
        )

    pipeline = destination_config.setup_pipeline(
        "read_pipeline", dataset_name="read_test", dev_mode=True
    )
    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "700"
    total_records = _total_records(pipeline)

    @dlt.source()
    def source():
        @dlt.resource(
            table_format=destination_config.table_format,
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
            table_format=destination_config.table_format,
            write_disposition="replace",
            columns={
                "id": {"data_type": "bigint"},
                "double_id": {"data_type": "bigint"},
                "di_decimal": {"data_type": "decimal", "precision": 7, "scale": 3},
            },
        )
        def double_items():
            yield from [
                {
                    "id": i,
                    "double_id": i * 2,
                    "di_decimal": Decimal("10.433"),
                }
                for i in range(total_records)
            ]

        return [items, double_items]

    # run source
    s = source()
    pipeline.run(s, loader_file_format=destination_config.file_format)

    # in case of delta on gcs we use the s3 compat layer for reading
    # for writing we still need to use the gc authentication, as delta_rs seems to use
    # methods on the s3 interface that are not implemented by gcs
    if destination_config.bucket_url == GCS_BUCKET and destination_config.table_format == "delta":
        gcp_bucket = filesystem(
            GCS_BUCKET.replace("gs://", "s3://"), destination_name="filesystem_s3_gcs_comp"
        )
        access_pipeline = destination_config.setup_pipeline(
            "read_pipeline", dataset_name="read_test", destination=gcp_bucket
        )

        pipeline.destination = access_pipeline.destination

    # return pipeline to test
    yield pipeline

    # NOTE: we need to drop pipeline data here since we are keeping the pipelines around for the whole module
    drop_pipeline_data(pipeline)


# NOTE: we collect all destination configs centrally, this way the session based
# pipeline population per fixture setup will work and save a lot of time
configs = destinations_configs(
    default_sql_configs=True,
    all_buckets_filesystem_configs=True,
    table_format_filesystem_configs=True,
    bucket_exclude=[SFTP_BUCKET, MEMORY_BUCKET],
)


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_arrow_access(populated_pipeline: Pipeline) -> None:
    table_relationship = populated_pipeline._dataset().items
    total_records = _total_records(populated_pipeline)
    chunk_size = _chunk_size(populated_pipeline)
    expected_chunk_counts = _expected_chunk_count(populated_pipeline)

    # full table
    table = table_relationship.arrow()
    assert table.num_rows == total_records

    # chunk
    table = table_relationship.arrow(chunk_size=chunk_size)
    assert set(table.column_names) == set(EXPECTED_COLUMNS)
    assert table.num_rows == chunk_size

    # check frame amount and items counts
    tables = list(table_relationship.iter_arrow(chunk_size=chunk_size))
    assert [t.num_rows for t in tables] == expected_chunk_counts

    # check all items are present
    ids = reduce(lambda a, b: a + b, [t.column(EXPECTED_COLUMNS[0]).to_pylist() for t in tables])
    assert set(ids) == set(range(total_records))


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_dataframe_access(populated_pipeline: Pipeline) -> None:
    # access via key
    table_relationship = populated_pipeline._dataset()["items"]
    total_records = _total_records(populated_pipeline)
    chunk_size = _chunk_size(populated_pipeline)
    expected_chunk_counts = _expected_chunk_count(populated_pipeline)
    skip_df_chunk_size_check = (
        populated_pipeline.destination.destination_type == "dlt.destinations.filesystem"
    )

    # full frame
    df = table_relationship.df()
    assert len(df.index) == total_records

    # chunk
    df = table_relationship.df(chunk_size=chunk_size)
    if not skip_df_chunk_size_check:
        assert len(df.index) == chunk_size

    # lowercase results for the snowflake case
    assert set(df.columns.values) == set(EXPECTED_COLUMNS)

    # iterate all dataframes
    frames = list(table_relationship.iter_df(chunk_size=chunk_size))
    if not skip_df_chunk_size_check:
        assert [len(df.index) for df in frames] == expected_chunk_counts

    # check all items are present
    ids = reduce(lambda a, b: a + b, [f[EXPECTED_COLUMNS[0]].to_list() for f in frames])
    assert set(ids) == set(range(total_records))


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_db_cursor_access(populated_pipeline: Pipeline) -> None:
    # check fetch accessors
    table_relationship = populated_pipeline._dataset().items
    total_records = _total_records(populated_pipeline)
    chunk_size = _chunk_size(populated_pipeline)
    expected_chunk_counts = _expected_chunk_count(populated_pipeline)

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


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_hint_preservation(populated_pipeline: Pipeline) -> None:
    table_relationship = populated_pipeline._dataset().items
    # check that hints are carried over to arrow table
    expected_decimal_precision = 10
    expected_decimal_precision_2 = 12
    if populated_pipeline.destination.destination_type == "dlt.destinations.bigquery":
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


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_loads_table_access(populated_pipeline: Pipeline) -> None:
    # check loads table access, we should have one entry
    loads_table = populated_pipeline._dataset()[populated_pipeline.default_schema.loads_table_name]
    assert len(loads_table.fetchall()) == 1


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_sql_queries(populated_pipeline: Pipeline) -> None:
    # simple check that query also works
    tname = populated_pipeline.sql_client().make_qualified_table_name("items")
    query_relationship = populated_pipeline._dataset()(f"select * from {tname} where id < 20")

    # we selected the first 20
    table = query_relationship.arrow()
    assert table.num_rows == 20

    # check join query
    tdname = populated_pipeline.sql_client().make_qualified_table_name("double_items")
    query = (
        f"SELECT i.id, di.double_id FROM {tname} as i JOIN {tdname} as di ON (i.id = di.id) WHERE"
        " i.id < 20 ORDER BY i.id ASC"
    )
    join_relationship = populated_pipeline._dataset()(query)
    table = join_relationship.fetchall()
    assert len(table) == 20
    assert list(table[0]) == [0, 0]
    assert list(table[5]) == [5, 10]
    assert list(table[10]) == [10, 20]


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_limit_and_head(populated_pipeline: Pipeline) -> None:
    table_relationship = populated_pipeline._dataset().items

    assert len(table_relationship.head().fetchall()) == 5
    assert len(table_relationship.limit(24).fetchall()) == 24

    assert len(table_relationship.head().df().index) == 5
    assert len(table_relationship.limit(24).df().index) == 24

    assert table_relationship.head().arrow().num_rows == 5
    assert table_relationship.limit(24).arrow().num_rows == 24


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_column_selection(populated_pipeline: Pipeline) -> None:
    table_relationship = populated_pipeline._dataset().items

    columns = ["_dlt_load_id", "other_decimal"]
    data_frame = table_relationship.select(*columns).head().df()
    assert [v.lower() for v in data_frame.columns.values] == columns
    assert len(data_frame.index) == 5

    columns = ["decimal", "other_decimal"]
    arrow_table = table_relationship[columns].head().arrow()
    assert arrow_table.column_names == columns
    assert arrow_table.num_rows == 5

    # hints should also be preserved via computed reduced schema
    expected_decimal_precision = 10
    expected_decimal_precision_2 = 12
    if populated_pipeline.destination.destination_type == "dlt.destinations.bigquery":
        # bigquery does not allow precision configuration..
        expected_decimal_precision = 38
        expected_decimal_precision_2 = 38
    assert arrow_table.schema.field("decimal").type.precision == expected_decimal_precision
    assert arrow_table.schema.field("other_decimal").type.precision == expected_decimal_precision_2

    with pytest.raises(ReadableRelationUnknownColumnException):
        arrow_table = table_relationship.select("unknown_column").head().arrow()


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_standalone_dataset(populated_pipeline: Pipeline) -> None:
    total_records = _total_records(populated_pipeline)

    # check dataset factory
    dataset = dlt._dataset(
        destination=populated_pipeline.destination, dataset_name=populated_pipeline.dataset_name
    )
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
            destination=populated_pipeline.destination,
            dataset_name=populated_pipeline.dataset_name,
            schema=populated_pipeline.default_schema_name,
        ),
    )
    assert dataset.schema.tables["items"]["write_disposition"] == "replace"

    # check that schema is not loaded when wrong name given
    dataset = cast(
        ReadableDBAPIDataset,
        dlt._dataset(
            destination=populated_pipeline.destination,
            dataset_name=populated_pipeline.dataset_name,
            schema="wrong_schema_name",
        ),
    )
    assert "items" not in dataset.schema.tables
    assert dataset.schema.name == populated_pipeline.dataset_name

    # check that schema is loaded if no schema name given
    dataset = cast(
        ReadableDBAPIDataset,
        dlt._dataset(
            destination=populated_pipeline.destination,
            dataset_name=populated_pipeline.dataset_name,
        ),
    )
    assert dataset.schema.name == populated_pipeline.default_schema_name
    assert dataset.schema.tables["items"]["write_disposition"] == "replace"

    # check that there is no error when creating dataset without schema table
    dataset = cast(
        ReadableDBAPIDataset,
        dlt._dataset(
            destination=populated_pipeline.destination,
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

    populated_pipeline._inject_schema(other_schema)
    populated_pipeline.default_schema_name = other_schema.name
    with populated_pipeline.destination_client() as client:
        client.update_stored_schema()

    dataset = cast(
        ReadableDBAPIDataset,
        dlt._dataset(
            destination=populated_pipeline.destination,
            dataset_name=populated_pipeline.dataset_name,
        ),
    )
    assert dataset.schema.name == "some_other_schema"
    assert "other_table" in dataset.schema.tables


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_ibis_expression_relation(populated_pipeline: Pipeline) -> None:
    # NOTE: we could generalize this with a context for certain deps

    # install ibis, we do not need any backends
    import subprocess

    subprocess.check_call(["pip", "install", "ibis-framework"])

    from dlt.destinations.ibis_dataset import dataset as create_ibis_dataset

    dataset = create_ibis_dataset(
        populated_pipeline.destination,
        populated_pipeline.dataset_name,
        populated_pipeline.default_schema_name,
    )
    total_records = _total_records(populated_pipeline)

    items_table = dataset.table("items")
    double_items_table = dataset.table("double_items")

    # check full table access
    df = items_table.df()
    assert len(df.index) == total_records

    df = double_items_table.df()
    assert len(df.index) == total_records

    # check limit
    df = items_table.limit(5).df()
    assert len(df.index) == 5

    # check chained expression with join, column selection, order by and limit
    joined_table = (
        items_table.join(double_items_table, items_table.id == double_items_table.id)[
            ["id", "double_id"]
        ]
        .order_by("id")
        .limit(20)
    )
    table = joined_table.fetchall()
    assert len(table) == 20
    assert list(table[0]) == [0, 0]
    assert list(table[5]) == [5, 10]
    assert list(table[10]) == [10, 20]

    # check aggregate of first 20 items
    agg_table = items_table.order_by("id").limit(20).aggregate(sum_id=items_table.id.sum())
    assert agg_table.fetchone()[0] == reduce(lambda a, b: a + b, range(20))

    # # NOTE: here we test that dlt column type resolution still works
    # # hints should also be preserved via computed reduced schema
    # expected_decimal_precision = 10
    # expected_decimal_precision_2 = 12
    # expected_decimal_precision_di = 7
    # if populated_pipeline.destination.destination_type == "dlt.destinations.bigquery":
    #     # bigquery does not allow precision configuration..
    #     expected_decimal_precision = 38
    #     expected_decimal_precision_2 = 38
    #     expected_decimal_precision_di = 38

    # joined_table = items_table.join(double_items_table, items_table.id == double_items_table.id)[
    #     ["decimal", "other_decimal", "di_decimal"]
    # ].rename(decimal_renamed="di_decimal").limit(20)
    # table = joined_table.arrow()
    # print(joined_table.compute_columns_schema(force=True))
    # assert table.schema.field("decimal").type.precision == expected_decimal_precision
    # assert table.schema.field("other_decimal").type.precision == expected_decimal_precision_2
    # assert table.schema.field("di_decimal").type.precision == expected_decimal_precision_di
