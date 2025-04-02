import os
import re
from functools import reduce
from typing import TYPE_CHECKING, Any, cast, Tuple, List

import pytest

import dlt
from dlt import Pipeline
from dlt.common import Decimal
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import C_DLT_LOAD_ID
from dlt.common.storages.exceptions import SchemaNotFoundError
from dlt.common.storages.file_storage import FileStorage
from dlt.destinations import filesystem
from dlt.destinations.dataset import dataset as _dataset
from dlt.destinations.dataset.ibis_relation import ReadableIbisRelation
from dlt.destinations.dataset.dataset import ReadableDBAPIDataset
from dlt.destinations.dataset.exceptions import ReadableRelationUnknownColumnException

from tests.utils import TEST_STORAGE_ROOT, clean_test_storage
from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    drop_pipeline_data,
    GCS_BUCKET,
    SFTP_BUCKET,
    MEMORY_BUCKET,
)

if TYPE_CHECKING:
    from dlt.common.libs import pandas as pd
else:
    pd = Any


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


# this also disables autouse_test_storage on function level which destroys some tests here
@pytest.fixture(scope="session")
def autouse_test_storage() -> FileStorage:
    return clean_test_storage()


@pytest.fixture(scope="session")
def populated_pipeline(request, autouse_test_storage) -> Any:
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

    @dlt.source(root_key=True)
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
    # create a second schema in the pipeline
    # NOTE: that generates additional load package and then another one for the state
    # NOTE: "aleph" schema is now the newest schema in the dataset and we assume that later in the tests
    # TODO: we need some kind of idea for multi-schema datasets
    pipeline.run([1, 2, 3], table_name="digits", schema=Schema("aleph"))

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


# TODO move `destination_config` to it's own fixture
# TODO move source function to top-level
@pytest.fixture(scope="session")
def pipeline_with_multiple_loads(request, autouse_test_storage) -> Any:
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

    @dlt.source(root_key=True)
    def source(expected_load_id):
        @dlt.resource(
            table_format=destination_config.table_format,
            write_disposition="replace",
            columns={
                "id": {"data_type": "bigint"},
                "expected_load_id": {"data_type": "bigint"},
                # we add a decimal with precision to see wether the hints are preserved
                "decimal": {"data_type": "decimal", "precision": 10, "scale": 3},
                "other_decimal": {"data_type": "decimal", "precision": 12, "scale": 3},
            },
        )
        def items():
            for i in range(total_records):
                yield {
                    "id": i,
                    "expected_load_id": expected_load_id,
                    "children": [{"id": i + 100}, {"id": i + 1000}],
                    "decimal": Decimal("10.433"),
                    "other_decimal": Decimal("10.433"),
                }

        @dlt.resource(
            table_format=destination_config.table_format,
            write_disposition="replace",
            columns={
                "id": {"data_type": "bigint"},
                "expected_load_id": {"data_type": "bigint"},
                "double_id": {"data_type": "bigint"},
                "di_decimal": {"data_type": "decimal", "precision": 7, "scale": 3},
            },
        )
        def double_items():
            for i in range(total_records):
                if expected_load_id ==1 and i > (total_records / 2):
                    raise RuntimeError("This mocks a runtime error that leads to a failed job.")

                yield {                 
                    "id": i,
                    "expected_load_id": expected_load_id,
                    "double_id": i * 2,
                    "di_decimal": Decimal("10.433"),
                }

        return [items, double_items]

    # run source
    pipeline.run(
        source(0), loader_file_format=destination_config.file_format, write_disposition="append"
    )
    # execute a pipeline run and mock a failed execution
    try:
        pipeline.run(
            source(1), loader_file_format=destination_config.file_format, write_disposition="append"
        )
    except Exception:
        pass
    pipeline.run(
        source(2), loader_file_format=destination_config.file_format, write_disposition="append"
    )

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
def test_explicit_dataset_type_selection(populated_pipeline: Pipeline):
    from dlt.destinations.dataset.dataset import ReadableDBAPIRelation
    from dlt.destinations.dataset.ibis_relation import ReadableIbisRelation

    assert isinstance(
        populated_pipeline.dataset(dataset_type="default").items, ReadableDBAPIRelation
    )
    assert isinstance(populated_pipeline.dataset(dataset_type="ibis").items, ReadableIbisRelation)


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_arrow_access(populated_pipeline: Pipeline) -> None:
    table_relationship = populated_pipeline.dataset().items
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
    table_relationship = populated_pipeline.dataset()["items"]
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
    table_relationship = populated_pipeline.dataset().items
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
    table_relationship = populated_pipeline.dataset(dataset_type="default").items
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
    # check loads table access, we should have 3 entires
    # - first source (default schema)
    # - additional schema (digits)
    # - state update send in separate package to default schema
    loads_table = populated_pipeline.dataset()[populated_pipeline.default_schema.loads_table_name]
    assert len(loads_table.fetchall()) == 3


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_row_counts(populated_pipeline: Pipeline) -> None:
    total_records = _total_records(populated_pipeline)

    dataset = populated_pipeline.dataset()
    # default is all data tables
    assert set(dataset.row_counts().df().itertuples(index=False, name=None)) == {
        (
            "items",
            total_records,
        ),
        (
            "double_items",
            total_records,
        ),
        (
            "items__children",
            total_records * 2,
        ),
    }
    # get only one data table
    assert set(
        dataset.row_counts(table_names=["items"]).df().itertuples(index=False, name=None)
    ) == {
        (
            "items",
            total_records,
        ),
    }
    # get all dlt tables
    assert set(
        dataset.row_counts(dlt_tables=True, data_tables=False)
        .df()
        .itertuples(index=False, name=None)
    ) == {
        (
            "_dlt_version",
            2,
        ),
        (
            "_dlt_loads",
            3,
        ),
        (
            "_dlt_pipeline_state",
            2,
        ),
    }
    # get them all
    assert set(dataset.row_counts(dlt_tables=True).df().itertuples(index=False, name=None)) == {
        (
            "_dlt_version",
            2,
        ),
        (
            "_dlt_loads",
            3,
        ),
        (
            "_dlt_pipeline_state",
            2,
        ),
        (
            "items",
            total_records,
        ),
        (
            "double_items",
            total_records,
        ),
        (
            "items__children",
            total_records * 2,
        ),
    }

# TODO remove per-destination parameterization for many tests in this file.
# If they focus on ReadableDBAPIDataset and don't load data, they're unlikely to need parameterization
@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    [c for c in configs if c[0][0].destination_type == "duckdb"],
    indirect=True,
    ids=lambda x: x.name,
)
def test_filter(populated_pipeline: Pipeline) -> None:
    mock_load_ids = ["foo1", "bar2"]
    dataset = populated_pipeline.dataset()
    dataset_filtered_at_init = populated_pipeline.dataset(load_ids=mock_load_ids)

    assert dataset._load_ids == set()
    assert dataset_filtered_at_init._load_ids == set(mock_load_ids)
    assert type(dataset) is type(dataset_filtered_at_init)

    dataset_filtered_later = dataset.filter(mock_load_ids)

    # ensure `dataset` wasn't mutated
    assert dataset._load_ids == set()
    assert dataset_filtered_later._load_ids == set(mock_load_ids)
    assert type(dataset) is type(dataset_filtered_later)  

    # chain filters
    other_load_ids = ["baz3"]
    dataset_filtered_twice = dataset.filter(mock_load_ids).filter(other_load_ids)
    assert dataset_filtered_twice._load_ids == set(mock_load_ids + other_load_ids)


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "pipeline_with_multiple_loads",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_list_load_ids(pipeline_with_multiple_loads: Pipeline) -> None:
    successful_load_ids = pipeline_with_multiple_loads.list_completed_load_packages()
    # test includes 2 success and 1 failed load
    assert len(successful_load_ids) == 2
    dataset: ReadableDBAPIDataset = pipeline_with_multiple_loads.dataset()

    retrieved_load_ids = dataset.list_load_ids()
    # only accepts keyword arguments
    with pytest.raises(TypeError):
        dataset.list_load_ids(1)

    assert isinstance(retrieved_load_ids, tuple)
    assert all(isinstance(load_id, str) for load_id in retrieved_load_ids)
    assert set(retrieved_load_ids) == set(successful_load_ids)
    assert retrieved_load_ids == tuple(sorted(successful_load_ids, reverse=True))

    # check status kwarg
    # status=0 is currently "success" and should match status=None when there's no failure
    assert dataset.list_load_ids(status=0) == retrieved_load_ids
    assert len(dataset.list_load_ids(status=0)) == len(successful_load_ids)
    # status=1 is currently an invalid value and should never match rows
    assert len(dataset.list_load_ids(status=1)) == 0
    assert len(dataset.list_load_ids(status=[0])) == len(successful_load_ids)
    assert len(dataset.list_load_ids(status=[0, 1])) == len(successful_load_ids)
    assert len(dataset.list_load_ids(status=[1, 2])) == 0

    # check limit kwarg
    assert dataset.list_load_ids(limit=0) == tuple()
    assert len(dataset.list_load_ids(limit=2)) == 2
    assert len(dataset.list_load_ids(limit=5)) == len(successful_load_ids)
    # sorting should happen before limit; i.e., limit=1 returns the max value
    assert dataset.list_load_ids(limit=1)[0] == max(successful_load_ids)


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "pipeline_with_multiple_loads",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_latest_load_id(pipeline_with_multiple_loads: Pipeline) -> None:
    successful_load_ids = pipeline_with_multiple_loads.list_completed_load_packages()
    # test includes 2 success and 1 failed load
    assert len(successful_load_ids) == 2
    dataset: ReadableDBAPIDataset = pipeline_with_multiple_loads.dataset()

    latest_load_id = dataset.latest_load_id()
    # only accepts keyword arguments
    with pytest.raises(TypeError):
        dataset.list_load_ids(1)

    assert isinstance(latest_load_id, str)
    assert latest_load_id == max(successful_load_ids)

    # check status kwarg
    # status=0 is currently "success" and should match status=None when there's no failure
    assert dataset.latest_load_id(status=0) == latest_load_id
    # status=1 is currently an invalid value and should never match rows
    assert dataset.latest_load_id(status=1) is None
    assert dataset.latest_load_id(status=1) is None
    assert dataset.latest_load_id(status=[0]) == latest_load_id
    assert dataset.latest_load_id(status=[0, 1]) == latest_load_id
    assert dataset.latest_load_id(status=[1, 2]) is None


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
    query_relationship = populated_pipeline.dataset()(f"select * from {tname} where id < 20")

    # we selected the first 20
    table = query_relationship.arrow()
    assert table.num_rows == 20

    # check join query
    tdname = populated_pipeline.sql_client().make_qualified_table_name("double_items")
    query = (
        f"SELECT i.id, di.double_id FROM {tname} as i JOIN {tdname} as di ON (i.id = di.id) WHERE"
        " i.id < 20 ORDER BY i.id ASC"
    )
    join_relationship = populated_pipeline.dataset()(query)
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
    table_relationship = populated_pipeline.dataset().items

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
    table_relationship = populated_pipeline.dataset(dataset_type="default").items
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
def test_schema_arg(populated_pipeline: Pipeline) -> None:
    """Simple test to ensure schemas may be selected via schema arg"""

    # if there is no arg, the default schema is used
    dataset = populated_pipeline.dataset()
    assert dataset.schema.name == populated_pipeline.default_schema_name
    assert "items" in dataset.schema.tables

    # if setting a different schema, it must be present in pipeline
    with pytest.raises(SchemaNotFoundError):
        populated_pipeline.dataset(schema="unknown_schema")

    # explicit schema object is OK
    dataset = populated_pipeline.dataset(schema=Schema("unknown_schema"))
    assert dataset.schema.name == "unknown_schema"
    assert "items" not in dataset.schema.tables

    # providing the schema name of the right schema will load it
    dataset = populated_pipeline.dataset(schema="aleph")
    assert dataset.schema.name == "aleph"
    assert "digits" in dataset.schema.tables
    dataset.digits.fetchall()


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "pipeline_with_multiple_loads",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("dataset_type", ("ibis", ))#"default"))
def test_dataset_methods_inherit_dataset_filter(pipeline_with_multiple_loads: Pipeline, dataset_type: str) -> None:
    """ReadableDBAPIDataset.__call__() creates a ReadableDBAPIRelation with a pre-generated
    SQL query. This allows dataset methods (e.g., `.row_counts()`) to inherit the filter.
    """
    successful_load_ids = pipeline_with_multiple_loads.list_completed_load_packages()
    # test includes 2 success and 1 failed load
    assert len(successful_load_ids) == 2
    selected_load_id = successful_load_ids[0]
    dataset: ReadableDBAPIDataset = pipeline_with_multiple_loads.dataset(dataset_type=dataset_type)
    dataset_filtered = dataset.filter([selected_load_id])

    # .list_load_ids()
    assert set(dataset.list_load_ids()) == set(successful_load_ids)
    assert set(dataset_filtered.list_load_ids()) ==  set([selected_load_id])

    # .latest_load_id()
    assert dataset_filtered.latest_load_id() == selected_load_id != dataset.latest_load_id()
    
    # .row_counts()
    rows_per_table = {table_name: row_count for table_name, row_count in dataset.row_counts().fetchall()}
    rows_per_table_filtered = {table_name: row_count for table_name, row_count in dataset_filtered.row_counts().fetchall()}
    assert rows_per_table == rows_per_table_filtered


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "pipeline_with_multiple_loads",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("dataset_type", ("ibis", "default"))
def test_filter_root_table(pipeline_with_multiple_loads: Pipeline, dataset_type: str) -> None:
    successful_load_ids = pipeline_with_multiple_loads.list_completed_load_packages()
    # test includes 2 success and 1 failed load
    assert len(successful_load_ids) == 2
    selected_load_id = successful_load_ids[0]
    dataset: ReadableDBAPIDataset = pipeline_with_multiple_loads.dataset(dataset_type=dataset_type)
    dataset_filtered = dataset.filter([selected_load_id])
    normalized_load_id_col = dataset.schema.naming.normalize_table_identifier(C_DLT_LOAD_ID)

    items_df: pd.DataFrame = dataset.items.df()
    items_df_filtered: pd.DataFrame = dataset_filtered.items.df()

    assert items_df.shape[0] > items_df_filtered.shape[0]
    assert set(successful_load_ids) != set([selected_load_id])
    assert set(items_df[normalized_load_id_col].unique()) == set(successful_load_ids)
    assert set(items_df_filtered[normalized_load_id_col].unique()) == set([selected_load_id])


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "pipeline_with_multiple_loads",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("dataset_type", ("ibis", "default"))
def test_filter_non_root_table_with_row_key(pipeline_with_multiple_loads: Pipeline, dataset_type: str) -> None:
    successful_load_ids = pipeline_with_multiple_loads.list_completed_load_packages()
    # test includes 2 success and 1 failed load
    assert len(successful_load_ids) == 2
    selected_load_id = successful_load_ids[0]
    dataset: ReadableDBAPIDataset = pipeline_with_multiple_loads.dataset(dataset_type=dataset_type)
    dataset_filtered = dataset.filter([selected_load_id])
    normalized_load_id_col = dataset.schema.naming.normalize_table_identifier(C_DLT_LOAD_ID)

    nested_df: pd.DataFrame = dataset.items__children.df()

    if dataset_type == "ibis":
        nested_df_filtered: pd.DataFrame = dataset_filtered.items__children.df()
        # by default, non-root tables don't have a `_dlt_load_id` column
        assert normalized_load_id_col not in nested_df.columns
        assert normalized_load_id_col in nested_df_filtered.columns
        assert nested_df.shape[0] > nested_df_filtered.shape[0]
        assert set(successful_load_ids) != set([selected_load_id])
        assert set(nested_df_filtered[normalized_load_id_col].unique()) == set([selected_load_id])
    else:
        with pytest.raises(RuntimeError):
            nested_df_filtered: pd.DataFrame = dataset_filtered.items__children.df()


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
    import ibis  # type: ignore

    # now we should get the more powerful ibis relation
    dataset = populated_pipeline.dataset()
    total_records = _total_records(populated_pipeline)

    items_table = dataset["items"]
    double_items_table = dataset["double_items"]

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

    # check filtering
    filtered_table = items_table.filter(items_table.id < 10)
    assert len(filtered_table.fetchall()) == 10

    if populated_pipeline.destination.destination_type != "dlt.destinations.duckdb":
        return

    # we check a bunch of expressions without executing them to see that they produce correct sql
    # also we return the keys of the disovered schema columns
    def sql_from_expr(expr: Any) -> Tuple[str, List[str]]:
        query = str(expr.query()).replace(populated_pipeline.dataset_name, "dataset")
        columns = list(expr.columns_schema.keys()) if expr.columns_schema else None
        return re.sub(r"\s+", " ", query), columns

    # test all functions discussed here: https://ibis-project.org/tutorials/ibis-for-sql-users
    ALL_COLUMNS = ["id", "decimal", "other_decimal", "_dlt_load_id", "_dlt_id"]

    # selecting two columns
    assert sql_from_expr(items_table.select("id", "decimal")) == (
        'SELECT "t0"."id", "t0"."decimal" FROM "dataset"."items" AS "t0"',
        ["id", "decimal"],
    )

    # selecting all columns
    assert sql_from_expr(items_table) == ('SELECT * FROM "dataset"."items"', ALL_COLUMNS)

    # selecting two other columns via item getter
    assert sql_from_expr(items_table["id", "decimal"]) == (
        'SELECT "t0"."id", "t0"."decimal" FROM "dataset"."items" AS "t0"',
        ["id", "decimal"],
    )

    # adding a new columns
    new_col = (items_table.id * 2).name("new_col")
    assert sql_from_expr(items_table.select("id", "decimal", new_col)) == (
        (
            'SELECT "t0"."id", "t0"."decimal", "t0"."id" * 2 AS "new_col" FROM'
            ' "dataset"."items" AS "t0"'
        ),
        None,
    )

    # mutating table (add a new column computed from existing columns)
    assert sql_from_expr(
        items_table.mutate(double_id=items_table.id * 2).select("id", "double_id")
    ) == (
        'SELECT "t0"."id", "t0"."id" * 2 AS "double_id" FROM "dataset"."items" AS "t0"',
        None,
    )

    # mutating table add new static column
    assert sql_from_expr(
        items_table.mutate(new_col=ibis.literal("static_value")).select("id", "new_col")
    ) == ('SELECT "t0"."id", \'static_value\' AS "new_col" FROM "dataset"."items" AS "t0"', None)

    # check filtering (preserves all columns)
    assert sql_from_expr(items_table.filter(items_table.id < 10)) == (
        'SELECT * FROM "dataset"."items" AS "t0" WHERE "t0"."id" < 10',
        ALL_COLUMNS,
    )

    # filtering and selecting a single column
    assert sql_from_expr(items_table.filter(items_table.id < 10).select("id")) == (
        'SELECT "t0"."id" FROM "dataset"."items" AS "t0" WHERE "t0"."id" < 10',
        ["id"],
    )

    # check filter "and" condition
    assert sql_from_expr(items_table.filter(items_table.id < 10).filter(items_table.id > 5)) == (
        'SELECT * FROM "dataset"."items" AS "t0" WHERE "t0"."id" < 10 AND "t0"."id" > 5',
        ALL_COLUMNS,
    )

    # check filter "or" condition
    assert sql_from_expr(items_table.filter((items_table.id < 10) | (items_table.id > 5))) == (
        'SELECT * FROM "dataset"."items" AS "t0" WHERE ( "t0"."id" < 10 ) OR ( "t0"."id" > 5 )',
        ALL_COLUMNS,
    )

    # check group by and aggregate
    assert sql_from_expr(
        items_table.group_by("id")
        .having(items_table.count() >= 1000)
        .aggregate(sum_id=items_table.id.sum())
    ) == (
        (
            'SELECT "t1"."id", "t1"."sum_id" FROM ( SELECT "t0"."id", SUM("t0"."id") AS "sum_id",'
            ' COUNT(*) AS "CountStar(items)" FROM "dataset"."items" AS "t0" GROUP BY 1 ) AS "t1"'
            ' WHERE "t1"."CountStar(items)" >= 1000'
        ),
        None,
    )

    # sorting and ordering
    assert sql_from_expr(items_table.order_by("id", "decimal").limit(10)) == (
        (
            'SELECT * FROM "dataset"."items" AS "t0" ORDER BY "t0"."id" ASC, "t0"."decimal" ASC'
            " LIMIT 10"
        ),
        ALL_COLUMNS,
    )

    # sort desc and asc
    assert sql_from_expr(items_table.order_by(ibis.desc("id"), ibis.asc("decimal")).limit(10)) == (
        (
            'SELECT * FROM "dataset"."items" AS "t0" ORDER BY "t0"."id" DESC, "t0"."decimal" ASC'
            " LIMIT 10"
        ),
        ALL_COLUMNS,
    )

    # offset and limit
    assert sql_from_expr(items_table.order_by("id").limit(10, offset=5)) == (
        'SELECT * FROM "dataset"."items" AS "t0" ORDER BY "t0"."id" ASC LIMIT 10 OFFSET 5',
        ALL_COLUMNS,
    )

    # join
    assert sql_from_expr(
        items_table.join(double_items_table, items_table.id == double_items_table.id)[
            ["id", "double_id"]
        ]
    ) == (
        (
            'SELECT "t2"."id", "t3"."double_id" FROM "dataset"."items" AS "t2" INNER JOIN'
            ' "dataset"."double_items" AS "t3" ON "t2"."id" = "t3"."id"'
        ),
        None,
    )

    # subqueries
    assert sql_from_expr(
        items_table.filter(items_table.decimal.isin(double_items_table.di_decimal))
    ) == (
        (
            'SELECT * FROM "dataset"."items" AS "t0" WHERE "t0"."decimal" IN ( SELECT'
            ' "t1"."di_decimal" FROM "dataset"."double_items" AS "t1" )'
        ),
        ALL_COLUMNS,
    )

    # topk
    assert sql_from_expr(items_table.decimal.topk(10)) == (
        (
            'SELECT * FROM ( SELECT "t0"."decimal", COUNT(*) AS "CountStar(items)" FROM'
            ' "dataset"."items" AS "t0" GROUP BY 1 ) AS "t1" ORDER BY "t1"."CountStar(items)" DESC'
            " LIMIT 10"
        ),
        None,
    )


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_ibis_dataset_access(populated_pipeline: Pipeline) -> None:
    # NOTE: we could generalize this with a context for certain deps

    # make sure the not implemented error is raised if the ibis backend can't be created
    try:
        ibis_connection = populated_pipeline.dataset().ibis()
    except NotImplementedError:
        pytest.raises(NotImplementedError)
        return
    except Exception as e:
        pytest.fail(f"Unexpected error rasied: {e}")

    total_records = _total_records(populated_pipeline)

    map_i = lambda x: x
    if populated_pipeline.destination.destination_type == "dlt.destinations.snowflake":
        map_i = lambda x: x.upper()

    dataset_name = map_i(populated_pipeline.dataset_name)
    table_like_statement = None
    table_name_prefix = ""
    additional_tables = []

    # clickhouse has no datasets, but table prefixes and a sentinel table
    if populated_pipeline.destination.destination_type == "dlt.destinations.clickhouse":
        table_like_statement = dataset_name + "."
        table_name_prefix = dataset_name + "___"
        dataset_name = None
        additional_tables += ["dlt_sentinel_table"]

    # filesystem uses duckdb and views to map know tables. for other ibis will list
    # all available tables so both schemas tables are visible
    if populated_pipeline.destination.destination_type != "dlt.destinations.filesystem":
        # from aleph schema
        additional_tables += ["digits"]

    add_table_prefix = lambda x: table_name_prefix + x

    # just do a basic check to see wether ibis can connect
    assert set(ibis_connection.list_tables(database=dataset_name, like=table_like_statement)) == {
        add_table_prefix(map_i(x))
        for x in (
            [
                "_dlt_loads",
                "_dlt_pipeline_state",
                "_dlt_version",
                "double_items",
                "items",
                "items__children",
            ]
            + additional_tables
        )
    }

    items_table = ibis_connection.table(add_table_prefix(map_i("items")), database=dataset_name)
    assert items_table.count().to_pandas() == total_records
    ibis_connection.disconnect()


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    configs,
    indirect=True,
    ids=lambda x: x.name,
)
def test_ibis_column_selection(populated_pipeline: Pipeline) -> None:
    import ibis.expr.types as ir  # type: ignore

    rel = populated_pipeline.dataset(dataset_type="ibis").items

    table_expr = [rel[["id"]], rel["id", "decimal"], rel[["id", "decimal"]]]
    for t in table_expr:
        assert isinstance(t, ReadableIbisRelation)
        assert isinstance(t._ibis_object, ir.Table)

    col_expr = [rel.id, rel["id"]]
    for c in col_expr:
        assert isinstance(c, ReadableIbisRelation)
        assert isinstance(c._ibis_object, ir.Column)


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
    dataset = cast(
        ReadableDBAPIDataset,
        _dataset(
            destination=populated_pipeline.destination,
            dataset_name=populated_pipeline.dataset_name,
            # use name otherwise aleph schema is loaded
            schema=populated_pipeline.default_schema_name,
        ),
    )
    # verfiy that sql client and schema are lazy loaded
    assert not dataset._schema
    assert not dataset._sql_client
    table_relationship = dataset.items
    table = table_relationship.fetchall()
    assert len(table) == total_records
    assert dataset.schema.tables["items"]["write_disposition"] == "replace"

    # check that schema is not loaded when wrong name given
    dataset = cast(
        ReadableDBAPIDataset,
        _dataset(
            destination=populated_pipeline.destination,
            dataset_name=populated_pipeline.dataset_name,
            schema="wrong_schema_name",
        ),
    )
    assert "items" not in dataset.schema.tables
    assert dataset.schema.name == "wrong_schema_name"

    # check that schema is loaded if no schema name given
    dataset = cast(
        ReadableDBAPIDataset,
        _dataset(
            destination=populated_pipeline.destination,
            dataset_name=populated_pipeline.dataset_name,
        ),
    )
    # aleph is a secondary schema in the pipeline but because it was stored second
    # will be retrieved by default
    assert dataset.schema.name == "aleph"
    assert dataset.schema.tables["digits"]["write_disposition"] == "append"

    # check that there is no error when creating dataset without schema table
    dataset = cast(
        ReadableDBAPIDataset,
        _dataset(
            destination=populated_pipeline.destination,
            dataset_name="unknown_dataset",
        ),
    )
    assert dataset.schema.name == "unknown_dataset"
    assert "items" not in dataset.schema.tables

    # NOTE: this breaks the following test, it will need to be fixed somehow
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
        _dataset(
            destination=populated_pipeline.destination,
            dataset_name=populated_pipeline.dataset_name,
        ),
    )
    assert dataset.schema.name == "some_other_schema"
    assert "other_table" in dataset.schema.tables
