import csv
import os
import posixpath
from pathlib import Path
from typing import Any, Callable, List, Dict, cast
from importlib.metadata import version as pkg_version
from packaging.version import Version

from pytest_mock import MockerFixture
import dlt
import pytest

from dlt.common import json
from dlt.common import pendulum
from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.common.utils import uniq_id
from dlt.common.schema.typing import TWriteDisposition
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.destinations import filesystem
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
from dlt.destinations.impl.filesystem.typing import TExtraPlaceholders
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.load.exceptions import LoadClientJobRetry

from tests.cases import arrow_table_all_data_types, table_update_and_row, assert_all_data_types_row
from tests.common.utils import load_json_case
from tests.utils import ALL_TEST_DATA_ITEM_FORMATS, TestDataItemFormat, skip_if_not_active
from dlt.destinations.path_utils import create_path
from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    MEMORY_BUCKET,
    FILE_BUCKET,
    AZ_BUCKET,
)

from tests.pipeline.utils import load_table_counts, assert_load_info, load_tables_to_dicts


skip_if_not_active("filesystem")


def test_pipeline_merge_write_disposition(default_buckets_env: str) -> None:
    """Run pipeline twice with merge write disposition
    Regardless wether primary key is set or not, filesystem appends
    """

    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "True"

    pipeline = dlt.pipeline(
        pipeline_name="test_" + uniq_id(),
        destination="filesystem",
        dataset_name="test_" + uniq_id(),
    )

    @dlt.resource(primary_key="id")
    def some_data():
        yield [{"id": 1}, {"id": 2}, {"id": 3}]

    @dlt.resource
    def other_data():
        yield [1, 2, 3, 4, 5]

    @dlt.source
    def some_source():
        return [some_data(), other_data()]

    pipeline.run(some_source(), write_disposition="merge")
    assert load_table_counts(pipeline, "some_data", "other_data") == {
        "some_data": 3,
        "other_data": 5,
    }

    # second load shows that merge always appends on filesystem
    pipeline.run(some_source(), write_disposition="merge")
    assert load_table_counts(pipeline, "some_data", "other_data") == {
        "some_data": 6,
        "other_data": 10,
    }

    # Force replace, back to initial values
    pipeline.run(some_source(), write_disposition="replace")
    assert load_table_counts(pipeline, "some_data", "other_data") == {
        "some_data": 3,
        "other_data": 5,
    }


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_pipeline_csv_filesystem_destination(item_type: TestDataItemFormat) -> None:
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "True"
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    # store locally
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "_storage"

    pipeline = dlt.pipeline(
        pipeline_name="parquet_test_" + uniq_id(),
        destination="filesystem",
        dataset_name="parquet_test_" + uniq_id(),
    )

    item, rows, _ = arrow_table_all_data_types(item_type, include_json=False, include_time=True)
    info = pipeline.run(item, table_name="table", loader_file_format="csv")
    job = info.load_packages[0].jobs["completed_jobs"][0].file_path
    assert job.endswith("csv")
    with open(job, "r", encoding="utf-8", newline="") as f:
        csv_rows = list(csv.DictReader(f, dialect=csv.unix_dialect))
        # header + 3 data rows
        assert len(csv_rows) == 3


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_csv_options(item_type: TestDataItemFormat) -> None:
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "True"
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    # set delimiter and disable headers
    os.environ["NORMALIZE__DATA_WRITER__DELIMITER"] = "|"
    os.environ["NORMALIZE__DATA_WRITER__INCLUDE_HEADER"] = "False"
    # store locally
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "_storage"
    pipeline = dlt.pipeline(
        pipeline_name="parquet_test_" + uniq_id(),
        destination="filesystem",
        dataset_name="parquet_test_" + uniq_id(),
    )

    item, rows, _ = arrow_table_all_data_types(item_type, include_json=False, include_time=True)
    info = pipeline.run(item, table_name="table", loader_file_format="csv")
    job = info.load_packages[0].jobs["completed_jobs"][0].file_path
    assert job.endswith("csv")
    with open(job, "r", encoding="utf-8", newline="") as f:
        csv_rows = list(csv.reader(f, dialect=csv.unix_dialect, delimiter="|"))
        # no header
        assert len(csv_rows) == 3
    # object csv adds dlt columns
    dlt_columns = 2 if item_type == "object" else 0
    assert len(rows[0]) + dlt_columns == len(csv_rows[0])


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_csv_quoting_style(item_type: TestDataItemFormat) -> None:
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "True"
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    # set quotes to all
    os.environ["NORMALIZE__DATA_WRITER__QUOTING"] = "quote_all"
    os.environ["NORMALIZE__DATA_WRITER__INCLUDE_HEADER"] = "False"
    # store locally
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "_storage"
    pipeline = dlt.pipeline(
        pipeline_name="parquet_test_" + uniq_id(),
        destination="filesystem",
        dataset_name="parquet_test_" + uniq_id(),
    )

    item, _, _ = arrow_table_all_data_types(item_type, include_json=False, include_time=True)
    info = pipeline.run(item, table_name="table", loader_file_format="csv")
    job = info.load_packages[0].jobs["completed_jobs"][0].file_path
    assert job.endswith("csv")
    with open(job, "r", encoding="utf-8", newline="") as f:
        # we skip headers and every line of data has 3 physical lines (due to string value in arrow_table_all_data_types)
        for line in f:
            line += f.readline()
            line += f.readline()
            # all elements are quoted
            for elem in line.strip().split(","):
                # NULL values are not quoted on arrow writer
                assert (
                    elem.startswith('"')
                    and elem.endswith('"')
                    or (len(elem) == 0 and item_type != "object")
                )


def test_pipeline_parquet_filesystem_destination() -> None:
    import pyarrow.parquet as pq  # Module is evaluated by other tests

    # store locally
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "_storage"
    pipeline = dlt.pipeline(
        pipeline_name="parquet_test_" + uniq_id(),
        destination="filesystem",
        dataset_name="parquet_test_" + uniq_id(),
    )

    @dlt.resource(primary_key="id")
    def some_data():
        yield [{"id": 1}, {"id": 2}, {"id": 3}]

    @dlt.resource
    def other_data():
        yield [1, 2, 3, 4, 5]

    @dlt.source
    def some_source():
        return [some_data(), other_data()]

    info = pipeline.run(some_source(), loader_file_format="parquet")
    package_info = pipeline.get_load_package_info(info.loads_ids[0])
    assert package_info.state == "loaded"

    # all three jobs succeeded
    assert len(package_info.jobs["failed_jobs"]) == 0
    assert len(package_info.jobs["completed_jobs"]) == 3

    client: FilesystemClient = pipeline.destination_client()  # type: ignore[assignment]
    some_data_glob = posixpath.join(client.dataset_path, "some_data/*")
    other_data_glob = posixpath.join(client.dataset_path, "other_data/*")

    some_data_files = client.fs_client.glob(some_data_glob)
    other_data_files = client.fs_client.glob(other_data_glob)

    assert len(some_data_files) == 1
    assert len(other_data_files) == 1

    with open(some_data_files[0], "rb") as f:
        table = pq.read_table(f)
        assert table.column("id").to_pylist() == [1, 2, 3]

    with open(other_data_files[0], "rb") as f:
        table = pq.read_table(f)
        assert table.column("value").to_pylist() == [1, 2, 3, 4, 5]


def test_delta_table_pyarrow_version_check() -> None:
    """Tests pyarrow version checking for `delta` table format.

    DependencyVersionException should be raised if pyarrow<17.0.0.
    """
    # test intentionally does not use destination_configs(), because that
    # function automatically marks `delta` table format configs as
    # `needspyarrow17`, which should not happen for this test to run in an
    # environment where pyarrow<17.0.0

    assert Version(pkg_version("pyarrow")) < Version("17.0.0"), "test assumes `pyarrow<17.0.0`"

    @dlt.resource(table_format="delta")
    def foo():
        yield {"foo": 1, "bar": 2}

    pipeline = dlt.pipeline(destination=filesystem(FILE_BUCKET))

    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run(foo())
    assert isinstance(pip_ex.value.__context__, LoadClientJobRetry)
    assert (
        "`pyarrow>=17.0.0` is needed for `delta` table format on `filesystem` destination"
        in pip_ex.value.__context__.retry_message
    )


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format="delta",
        bucket_exclude=(MEMORY_BUCKET),
    ),
    ids=lambda x: x.name,
)
def test_delta_table_core(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Tests core functionality for `delta` table format.

    Tests all data types, all filesystems.
    Tests `append` and `replace` write dispositions (`merge` is tested elsewhere).
    """

    from dlt.common.libs.deltalake import get_delta_tables

    # create resource that yields rows with all data types
    column_schemas, row = table_update_and_row()

    @dlt.resource(columns=column_schemas, table_format="delta")
    def data_types():
        nonlocal row
        yield [row] * 10

    pipeline = destination_config.setup_pipeline("fs_pipe", dev_mode=True)

    # run pipeline, this should create Delta table
    info = pipeline.run(data_types())
    assert_load_info(info)

    # `delta` table format should use `parquet` file format
    completed_jobs = info.load_packages[0].jobs["completed_jobs"]
    data_types_jobs = [
        job for job in completed_jobs if job.job_file_info.table_name == "data_types"
    ]
    assert all([job.file_path.endswith((".parquet", ".reference")) for job in data_types_jobs])

    # 10 rows should be loaded to the Delta table and the content of the first
    # row should match expected values
    rows = load_tables_to_dicts(pipeline, "data_types", exclude_system_cols=True)["data_types"]
    assert len(rows) == 10
    assert_all_data_types_row(rows[0], schema=column_schemas)

    # make sure remote_url is in metrics
    metrics = info.metrics[info.loads_ids[0]][0]
    # TODO: only final copy job has remote_url. not the initial (empty) job for particular files
    # we could implement an empty job for delta that generates correct remote_url
    remote_url = list(metrics["job_metrics"].values())[-1].remote_url
    assert remote_url.endswith("data_types")
    bucket_url = destination_config.bucket_url
    if FilesystemConfiguration.is_local_path(bucket_url):
        bucket_url = FilesystemConfiguration.make_file_url(bucket_url)
    assert remote_url.startswith(bucket_url)

    # another run should append rows to the table
    info = pipeline.run(data_types())
    assert_load_info(info)
    rows = load_tables_to_dicts(pipeline, "data_types", exclude_system_cols=True)["data_types"]
    assert len(rows) == 20

    # ensure "replace" write disposition is handled
    # should do logical replace, increasing the table version
    info = pipeline.run(data_types(), write_disposition="replace")
    assert_load_info(info)
    assert get_delta_tables(pipeline, "data_types")["data_types"].version() == 2
    rows = load_tables_to_dicts(pipeline, "data_types", exclude_system_cols=True)["data_types"]
    assert len(rows) == 10


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format="delta",
        bucket_subset=(FILE_BUCKET),
    ),
    ids=lambda x: x.name,
)
def test_delta_table_does_not_contain_job_files(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Asserts Parquet job files do not end up in Delta table."""

    pipeline = destination_config.setup_pipeline("fs_pipe", dev_mode=True)

    @dlt.resource(table_format="delta")
    def delta_table():
        yield [{"foo": 1}]

    # create Delta table
    info = pipeline.run(delta_table())
    assert_load_info(info)

    # get Parquet jobs
    completed_jobs = info.load_packages[0].jobs["completed_jobs"]
    parquet_jobs = [
        job
        for job in completed_jobs
        if job.job_file_info.table_name == "delta_table" and job.file_path.endswith(".parquet")
    ]
    assert len(parquet_jobs) == 1

    # get Parquet files in Delta table folder
    with pipeline.destination_client() as client:
        assert isinstance(client, FilesystemClient)
        table_dir = client.get_table_dir("delta_table")
        parquet_files = [f for f in client.fs_client.ls(table_dir) if f.endswith(".parquet")]
    assert len(parquet_files) == 1

    # Parquet file should not be the job file
    file_id = parquet_jobs[0].job_file_info.file_id
    assert file_id not in parquet_files[0]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format="delta",
        bucket_subset=(FILE_BUCKET),
    ),
    ids=lambda x: x.name,
)
def test_delta_table_multiple_files(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Tests loading multiple files into a Delta table.

    Files should be loaded into the Delta table in a single commit.
    """

    from dlt.common.libs.deltalake import get_delta_tables

    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "2"  # force multiple files

    @dlt.resource(table_format="delta")
    def delta_table():
        yield [{"foo": True}] * 10

    pipeline = destination_config.setup_pipeline("fs_pipe", dev_mode=True)

    info = pipeline.run(delta_table())
    assert_load_info(info)

    # multiple Parquet files should have been created
    completed_jobs = info.load_packages[0].jobs["completed_jobs"]
    delta_table_parquet_jobs = [
        job
        for job in completed_jobs
        if job.job_file_info.table_name == "delta_table"
        and job.job_file_info.file_format == "parquet"
    ]
    assert len(delta_table_parquet_jobs) == 5  # 10 records, max 2 per file

    # all 10 records should have been loaded into a Delta table in a single commit
    assert get_delta_tables(pipeline, "delta_table")["delta_table"].version() == 0
    rows = load_tables_to_dicts(pipeline, "delta_table", exclude_system_cols=True)["delta_table"]
    assert len(rows) == 10


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format="delta",
        bucket_subset=(FILE_BUCKET),
    ),
    ids=lambda x: x.name,
)
def test_delta_table_child_tables(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Tests child table handling for `delta` table format."""

    @dlt.resource(table_format="delta")
    def nested_table():
        yield [
            {
                "foo": 1,
                "child": [{"bar": True, "grandchild": [1, 2]}, {"bar": True, "grandchild": [1]}],
            },
            {
                "foo": 2,
                "child": [
                    {"bar": False, "grandchild": [1, 3]},
                ],
            },
        ]

    pipeline = destination_config.setup_pipeline("fs_pipe", dev_mode=True)

    info = pipeline.run(nested_table())
    assert_load_info(info)
    rows_dict = load_tables_to_dicts(
        pipeline,
        "nested_table",
        "nested_table__child",
        "nested_table__child__grandchild",
        exclude_system_cols=True,
    )
    # assert row counts
    assert len(rows_dict["nested_table"]) == 2
    assert len(rows_dict["nested_table__child"]) == 3
    assert len(rows_dict["nested_table__child__grandchild"]) == 5
    # assert column names
    assert rows_dict["nested_table"][0].keys() == {"foo"}
    assert rows_dict["nested_table__child"][0].keys() == {"bar"}
    assert rows_dict["nested_table__child__grandchild"][0].keys() == {"value"}

    # test write disposition handling with child tables
    info = pipeline.run(nested_table())
    assert_load_info(info)
    rows_dict = load_tables_to_dicts(
        pipeline,
        "nested_table",
        "nested_table__child",
        "nested_table__child__grandchild",
        exclude_system_cols=True,
    )
    assert len(rows_dict["nested_table"]) == 2 * 2
    assert len(rows_dict["nested_table__child"]) == 3 * 2
    assert len(rows_dict["nested_table__child__grandchild"]) == 5 * 2

    info = pipeline.run(nested_table(), write_disposition="replace")
    assert_load_info(info)
    rows_dict = load_tables_to_dicts(
        pipeline,
        "nested_table",
        "nested_table__child",
        "nested_table__child__grandchild",
        exclude_system_cols=True,
    )
    assert len(rows_dict["nested_table"]) == 2
    assert len(rows_dict["nested_table__child"]) == 3
    assert len(rows_dict["nested_table__child__grandchild"]) == 5

    # now drop children and grandchildren, use merge write disposition to create and pass full table chain
    # also for tables that do not have jobs
    info = pipeline.run(
        [{"foo": 3}] * 10000,
        table_name="nested_table",
        primary_key="foo",
        write_disposition="merge",
    )
    assert_load_info(info)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format="delta",
        bucket_subset=(FILE_BUCKET),
    ),
    ids=lambda x: x.name,
)
def test_delta_table_partitioning(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Tests partitioning for `delta` table format."""

    from dlt.common.libs.deltalake import get_delta_tables
    from tests.pipeline.utils import users_materialize_table_schema

    pipeline = destination_config.setup_pipeline("fs_pipe", dev_mode=True)

    # zero partition columns
    @dlt.resource(table_format="delta")
    def zero_part():
        yield {"foo": 1, "bar": 1}

    info = pipeline.run(zero_part())
    assert_load_info(info)
    dt = get_delta_tables(pipeline, "zero_part")["zero_part"]
    assert dt.metadata().partition_columns == []
    assert load_table_counts(pipeline, "zero_part")["zero_part"] == 1

    # one partition column
    @dlt.resource(table_format="delta", columns={"c1": {"partition": True}})
    def one_part():
        yield [
            {"c1": "foo", "c2": 1},
            {"c1": "foo", "c2": 2},
            {"c1": "bar", "c2": 3},
            {"c1": "baz", "c2": 4},
        ]

    info = pipeline.run(one_part())
    assert_load_info(info)
    dt = get_delta_tables(pipeline, "one_part")["one_part"]
    assert dt.metadata().partition_columns == ["c1"]
    assert load_table_counts(pipeline, "one_part")["one_part"] == 4

    # two partition columns
    @dlt.resource(
        table_format="delta", columns={"c1": {"partition": True}, "c2": {"partition": True}}
    )
    def two_part():
        yield [
            {"c1": "foo", "c2": 1, "c3": True},
            {"c1": "foo", "c2": 2, "c3": True},
            {"c1": "bar", "c2": 1, "c3": True},
            {"c1": "baz", "c2": 1, "c3": True},
        ]

    info = pipeline.run(two_part())
    assert_load_info(info)
    dt = get_delta_tables(pipeline, "two_part")["two_part"]
    assert dt.metadata().partition_columns == ["c1", "c2"]
    assert load_table_counts(pipeline, "two_part")["two_part"] == 4

    # test partitioning with empty source
    users_materialize_table_schema.apply_hints(
        table_format="delta",
        columns={"id": {"partition": True}},
    )
    info = pipeline.run(users_materialize_table_schema())
    assert_load_info(info)
    dt = get_delta_tables(pipeline, "users")["users"]
    assert dt.metadata().partition_columns == ["id"]
    assert load_table_counts(pipeline, "users")["users"] == 0

    # changing partitioning after initial table creation is not supported
    zero_part.apply_hints(columns={"foo": {"partition": True}})
    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run(zero_part())
    assert isinstance(pip_ex.value.__context__, LoadClientJobRetry)
    assert "partitioning" in pip_ex.value.__context__.retry_message
    dt = get_delta_tables(pipeline, "zero_part")["zero_part"]
    assert dt.metadata().partition_columns == []


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format="delta",
        bucket_subset=(FILE_BUCKET),
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "write_disposition",
    (
        "append",
        "replace",
        pytest.param({"disposition": "merge", "strategy": "upsert"}, id="upsert"),
    ),
)
def test_delta_table_schema_evolution(
    destination_config: DestinationTestConfiguration,
    write_disposition: TWriteDisposition,
) -> None:
    """Tests schema evolution (adding new columns) for `delta` table format."""
    from dlt.common.libs.deltalake import get_delta_tables, ensure_delta_compatible_arrow_data
    from dlt.common.libs.pyarrow import pyarrow

    @dlt.resource(
        write_disposition=write_disposition,
        primary_key="pk",
        table_format="delta",
    )
    def delta_table(data):
        yield data

    pipeline = destination_config.setup_pipeline("fs_pipe", dev_mode=True)

    # create Arrow table with one column, one row
    pk_field = pyarrow.field("pk", pyarrow.int64(), nullable=False)
    schema = pyarrow.schema([pk_field])
    arrow_table = pyarrow.Table.from_pydict({"pk": [1]}, schema=schema)
    assert arrow_table.shape == (1, 1)

    # initial load
    info = pipeline.run(delta_table(arrow_table))
    assert_load_info(info)
    dt = get_delta_tables(pipeline, "delta_table")["delta_table"]
    expected = ensure_delta_compatible_arrow_data(arrow_table)
    actual = dt.to_pyarrow_table()
    assert actual.equals(expected)

    # create Arrow table with many columns, two rows
    arrow_table = arrow_table_all_data_types(
        "arrow-table",
        include_decimal_default_precision=True,
        include_decimal_arrow_max_precision=True,
        include_not_normalized_name=False,
        include_null=False,
        num_rows=2,
    )[0]
    arrow_table = arrow_table.add_column(0, pk_field, [[1, 2]])

    # second load — this should evolve the schema (i.e. add the new columns)
    info = pipeline.run(delta_table(arrow_table))
    assert_load_info(info)
    dt = get_delta_tables(pipeline, "delta_table")["delta_table"]
    actual = dt.to_pyarrow_table()
    expected = ensure_delta_compatible_arrow_data(arrow_table)
    if write_disposition == "append":
        # just check shape and schema for `append`, because table comparison is
        # more involved than with the other dispositions
        assert actual.num_rows == 3
        actual.schema.equals(expected.schema)
    else:
        assert actual.sort_by("pk").equals(expected.sort_by("pk"))

    # create empty Arrow table with additional column
    arrow_table = arrow_table.append_column(
        pyarrow.field("another_new_column", pyarrow.string()),
        [["foo", "foo"]],
    )
    empty_arrow_table = arrow_table.schema.empty_table()

    # load 3 — this should evolve the schema without changing data
    info = pipeline.run(delta_table(empty_arrow_table))
    assert_load_info(info)
    dt = get_delta_tables(pipeline, "delta_table")["delta_table"]
    actual = dt.to_pyarrow_table()
    expected_schema = ensure_delta_compatible_arrow_data(arrow_table).schema
    assert actual.schema.equals(expected_schema)
    expected_num_rows = 3 if write_disposition == "append" else 2
    assert actual.num_rows == expected_num_rows
    # new column should have NULLs only
    assert (
        actual.column("another_new_column").combine_chunks().to_pylist()
        == [None] * expected_num_rows
    )


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format="delta",
        bucket_subset=(FILE_BUCKET, AZ_BUCKET),
    ),
    ids=lambda x: x.name,
)
def test_delta_table_empty_source(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Tests empty source handling for `delta` table format.

    Tests both empty Arrow table and `dlt.mark.materialize_table_schema()`.
    """
    from dlt.common.libs.deltalake import ensure_delta_compatible_arrow_data, get_delta_tables
    from tests.pipeline.utils import users_materialize_table_schema

    @dlt.resource(table_format="delta")
    def delta_table(data):
        yield data

    # create empty Arrow table with schema
    arrow_table = arrow_table_all_data_types(
        "arrow-table",
        include_decimal_default_precision=True,
        include_decimal_arrow_max_precision=True,
        include_not_normalized_name=False,
        include_null=False,
        num_rows=2,
    )[0]
    empty_arrow_table = arrow_table.schema.empty_table()
    assert empty_arrow_table.num_rows == 0  # it's empty
    assert empty_arrow_table.schema.equals(arrow_table.schema)  # it has a schema

    pipeline = destination_config.setup_pipeline("fs_pipe", dev_mode=True)

    # run 1: empty Arrow table with schema
    # this should create empty Delta table with same schema as Arrow table
    info = pipeline.run(delta_table(empty_arrow_table))
    assert_load_info(info)
    dt = get_delta_tables(pipeline, "delta_table")["delta_table"]
    assert dt.version() == 0
    dt_arrow_table = dt.to_pyarrow_table()
    assert dt_arrow_table.shape == (0, empty_arrow_table.num_columns)
    assert dt_arrow_table.schema.equals(
        ensure_delta_compatible_arrow_data(empty_arrow_table).schema
    )

    # run 2: non-empty Arrow table with same schema as run 1
    # this should load records into Delta table
    info = pipeline.run(delta_table(arrow_table))
    assert_load_info(info)
    dt = get_delta_tables(pipeline, "delta_table")["delta_table"]
    assert dt.version() == 1
    dt_arrow_table = dt.to_pyarrow_table()
    assert dt_arrow_table.shape == (2, empty_arrow_table.num_columns)
    assert dt_arrow_table.schema.equals(
        ensure_delta_compatible_arrow_data(empty_arrow_table).schema
    )

    # now run the empty frame again
    info = pipeline.run(delta_table(empty_arrow_table))
    assert_load_info(info)

    # use materialized list
    # NOTE: this will create an empty parquet file with a schema takes from dlt schema.
    # the original parquet file had a nested (struct) type in `json` field that is now
    # in the delta table schema. the empty parquet file lost this information and had
    # string type (converted from dlt `json`)
    info = pipeline.run([dlt.mark.materialize_table_schema()], table_name="delta_table")
    assert_load_info(info)

    # test `dlt.mark.materialize_table_schema()`
    users_materialize_table_schema.apply_hints(table_format="delta")
    info = pipeline.run(users_materialize_table_schema(), loader_file_format="parquet")
    assert_load_info(info)
    dt = get_delta_tables(pipeline, "users")["users"]
    assert dt.version() == 0
    dt_arrow_table = dt.to_pyarrow_table()
    assert dt_arrow_table.num_rows == 0
    assert "id", "name" == dt_arrow_table.schema.names[:2]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format="delta",
        bucket_subset=(FILE_BUCKET),
    ),
    ids=lambda x: x.name,
)
def test_delta_table_mixed_source(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Tests file format handling in mixed source.

    One resource uses `delta` table format, the other doesn't.
    """

    @dlt.resource(table_format="delta")
    def delta_table():
        yield [{"foo": True}]

    @dlt.resource()
    def non_delta_table():
        yield [1, 2, 3]

    @dlt.source
    def s():
        return [delta_table(), non_delta_table()]

    pipeline = destination_config.setup_pipeline("fs_pipe", dev_mode=True)

    info = pipeline.run(s(), loader_file_format="jsonl")  # set file format at pipeline level
    assert_load_info(info)
    completed_jobs = info.load_packages[0].jobs["completed_jobs"]

    # `jsonl` file format should be overridden for `delta_table` resource
    # because it's not supported for `delta` table format
    delta_table_jobs = [
        job for job in completed_jobs if job.job_file_info.table_name == "delta_table"
    ]
    assert all([job.file_path.endswith((".parquet", ".reference")) for job in delta_table_jobs])

    # `jsonl` file format should be respected for `non_delta_table` resource
    non_delta_table_job = [
        job for job in completed_jobs if job.job_file_info.table_name == "non_delta_table"
    ][0]
    assert non_delta_table_job.file_path.endswith(".jsonl")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format="delta",
        bucket_subset=(FILE_BUCKET),
    ),
    ids=lambda x: x.name,
)
def test_delta_table_dynamic_dispatch(
    destination_config: DestinationTestConfiguration,
) -> None:
    @dlt.resource(primary_key="id", table_name=lambda i: i["type"], table_format="delta")
    def github_events():
        with open(
            "tests/normalize/cases/github.events.load_page_1_duck.json", "r", encoding="utf-8"
        ) as f:
            yield json.load(f)

    pipeline = destination_config.setup_pipeline("fs_pipe", dev_mode=True)

    info = pipeline.run(github_events())
    assert_load_info(info)
    completed_jobs = info.load_packages[0].jobs["completed_jobs"]
    # 20 event types, two jobs per table (.parquet and .reference), 1 job for _dlt_pipeline_state
    assert len(completed_jobs) == 2 * 20 + 1


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format="delta",
        bucket_subset=(FILE_BUCKET, AZ_BUCKET),
    ),
    ids=lambda x: x.name,
)
def test_delta_table_get_delta_tables_helper(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Tests `get_delta_tables` helper function."""
    from dlt.common.libs.deltalake import DeltaTable, get_delta_tables

    @dlt.resource(table_format="delta")
    def foo_delta():
        yield [{"foo": 1}, {"foo": 2}]

    @dlt.resource(table_format="delta")
    def bar_delta():
        yield [{"bar": 1}]

    @dlt.resource
    def baz_not_delta():
        yield [{"baz": 1}]

    pipeline = destination_config.setup_pipeline("fs_pipe", dev_mode=True)

    info = pipeline.run(foo_delta())
    assert_load_info(info)
    delta_tables = get_delta_tables(pipeline)
    assert delta_tables.keys() == {"foo_delta"}
    assert isinstance(delta_tables["foo_delta"], DeltaTable)
    assert delta_tables["foo_delta"].to_pyarrow_table().num_rows == 2

    info = pipeline.run([foo_delta(), bar_delta(), baz_not_delta()])
    assert_load_info(info)
    delta_tables = get_delta_tables(pipeline)
    assert delta_tables.keys() == {"foo_delta", "bar_delta"}
    assert delta_tables["bar_delta"].to_pyarrow_table().num_rows == 1
    assert get_delta_tables(pipeline, "foo_delta").keys() == {"foo_delta"}
    assert get_delta_tables(pipeline, "bar_delta").keys() == {"bar_delta"}
    assert get_delta_tables(pipeline, "foo_delta", "bar_delta").keys() == {"foo_delta", "bar_delta"}

    # test with child table
    @dlt.resource(table_format="delta")
    def parent_delta():
        yield [{"foo": 1, "child": [1, 2, 3]}]

    info = pipeline.run(parent_delta())
    assert_load_info(info)
    delta_tables = get_delta_tables(pipeline)
    assert "parent_delta__child" in delta_tables.keys()
    assert delta_tables["parent_delta__child"].to_pyarrow_table().num_rows == 3

    # test invalid input
    with pytest.raises(ValueError):
        get_delta_tables(pipeline, "baz_not_delta")

    with pytest.raises(ValueError):
        get_delta_tables(pipeline, "non_existing_table")

    # test unknown schema
    with pytest.raises(FileNotFoundError):
        get_delta_tables(pipeline, "non_existing_table", schema_name="aux_2")

    # load to a new schema and under new name
    aux_schema = dlt.Schema("aux_2")
    # NOTE: you cannot have a file with name
    info = pipeline.run(parent_delta().with_name("aux_delta"), schema=aux_schema)
    # also state in seprate package
    assert_load_info(info, expected_load_packages=2)
    delta_tables = get_delta_tables(pipeline, schema_name="aux_2")
    assert "aux_delta__child" in delta_tables.keys()
    get_delta_tables(pipeline, "aux_delta", schema_name="aux_2")
    with pytest.raises(ValueError):
        get_delta_tables(pipeline, "aux_delta")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format="delta",
        bucket_subset=(FILE_BUCKET,),
    ),
    ids=lambda x: x.name,
)
def test_parquet_to_delta_upgrade(destination_config: DestinationTestConfiguration):
    # change the resource to start creating delta tables
    from dlt.common.libs.deltalake import get_delta_tables

    @dlt.resource()
    def foo():
        yield [{"foo": 1}, {"foo": 2}]

    pipeline = destination_config.setup_pipeline("fs_pipe")

    info = pipeline.run(foo())
    assert_load_info(info)
    delta_tables = get_delta_tables(pipeline)
    assert set(delta_tables.keys()) == set()

    # drop the pipeline
    pipeline.deactivate()

    # redefine the resource

    @dlt.resource(table_format="delta")  # type: ignore
    def foo():
        yield [{"foo": 1}, {"foo": 2}]

    pipeline = destination_config.setup_pipeline("fs_pipe")

    info = pipeline.run(foo())
    assert_load_info(info)
    delta_tables = get_delta_tables(pipeline)
    assert set(delta_tables.keys()) == {"foo"}

    # optimize all delta tables to make sure storage is there
    for table in delta_tables.values():
        table.vacuum()


TEST_LAYOUTS = (
    "{schema_name}/{table_name}/{load_id}.{file_id}.{ext}",
    "{schema_name}.{table_name}.{load_id}.{file_id}.{ext}",
    "{table_name}88{load_id}-u-{file_id}.{ext}",
    "{table_name}/{curr_date}/{load_id}.{file_id}.{ext}{timestamp}",
    "{table_name}/{YYYY}-{MM}-{DD}/{load_id}.{file_id}.{ext}",
    "{table_name}/{YYYY}-{MMM}-{D}/{load_id}.{file_id}.{ext}",
    "{table_name}/{DD}/{HH}/{m}/{load_id}.{file_id}.{ext}",
    "{table_name}/{D}/{HH}/{mm}/{load_id}.{file_id}.{ext}",
    "{table_name}/{timestamp}/{load_id}.{file_id}.{ext}",
    "{table_name}/{timestamp_ms}/{load_id}.{file_id}.{ext}",
    "{table_name}/{load_package_timestamp}/{d}/{load_id}.{file_id}.{ext}",
    "{table_name}/{load_package_timestamp_ms}/{d}/{load_id}.{file_id}.{ext}",
    (
        "{table_name}/{YYYY}/{YY}/{Y}/{MMMM}/{MMM}/{MM}/{M}/{DD}/{D}/"
        "{HH}/{H}/{ddd}/{dd}/{d}/{ss}/{s}/{Q}/{timestamp}/{curr_date}/{load_id}.{file_id}.{ext}"
    ),
    (
        "{table_name}/{YYYY}/{YY}/{Y}/{MMMM}/{MMM}/{MM}/{M}/{DD}/{D}/"
        "{SSSS}/{SSS}/{SS}/{S}/{Q}/{timestamp}/{curr_date}/{load_id}.{file_id}.{ext}"
    ),
)


@pytest.mark.parametrize("layout", TEST_LAYOUTS)
def test_filesystem_destination_extended_layout_placeholders(
    layout: str, default_buckets_env: str, mocker: MockerFixture
) -> None:
    data = load_json_case("simple_row")
    call_count = 0

    def counter(value: Any) -> Callable[..., Any]:
        def count(*args, **kwargs) -> Any:
            nonlocal call_count
            call_count += 1
            return value

        return count

    extra_placeholders: TExtraPlaceholders = {
        "who": "marcin",
        "action": "says",
        "what": "no potato",
        "func": counter("lifting"),
        "woot": "woot-woot",
        "hiphip": counter("Hurraaaa"),
    }
    now = pendulum.now()
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "_storage"
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "TRUE"

    # the reason why we are patching pendulum.from_timestamp is that
    # we are checking if the load package under a given path exists
    # so we have to mock this out because there will difference in
    # calculated timestamps thus will make the test flaky due to
    # small differences in timestamp calculations
    mocker.patch("pendulum.from_timestamp", return_value=now)
    fs_destination = filesystem(
        layout=layout,
        extra_placeholders=extra_placeholders,
        current_datetime=counter(now),
    )
    pipeline = dlt.pipeline(
        pipeline_name="test_extended_layouts",
        destination=fs_destination,
    )
    load_info = pipeline.run(
        [
            dlt.resource(data, name="table_1"),
            dlt.resource(data * 2, name="table_2"),
            dlt.resource(data * 3, name="table_3"),
        ],
        write_disposition="append",
    )
    client = pipeline.destination_client()

    expected_files = set()
    known_files = set()
    for basedir, _dirs, files in client.fs_client.walk(client.dataset_path):  # type: ignore[attr-defined]
        # strip out special tables
        if "_dlt" in basedir:
            continue

        for file in files:
            if ".jsonl" in file:
                expected_files.add(Path(posixpath.join(basedir, file)))

    for load_package in load_info.load_packages:
        for load_info in load_package.jobs["completed_jobs"]:  # type: ignore[assignment]
            job_info = ParsedLoadJobFileName.parse(load_info.file_path)  # type: ignore[attr-defined]
            # state file gets loaded a differentn way
            if job_info.table_name == "_dlt_pipeline_state":
                continue
            path = create_path(
                layout,
                file_name=job_info.file_name(),
                schema_name="test_extended_layouts",
                load_id=load_package.load_id,
                current_datetime=now,
                load_package_timestamp=load_info.created_at.to_iso8601_string(),  # type: ignore[attr-defined]
                extra_placeholders=extra_placeholders,
            )
            full_path = posixpath.join(client.dataset_path, path)  # type: ignore[attr-defined]
            assert client.fs_client.exists(full_path)  # type: ignore[attr-defined]
            if ".jsonl" in full_path:
                known_files.add(Path(full_path))

    assert expected_files == known_files
    assert known_files
    # 6 is because simple_row contains two rows
    # and in this test scenario we have 3 callbacks
    assert call_count >= 6

    # check that table separation works for every path
    # we cannot test when ext is not the last value
    if ".{ext}{timestamp}" not in layout:
        assert load_table_counts(pipeline, "table_1", "table_2", "table_3") == {
            "table_1": 2,
            "table_2": 4,
            "table_3": 6,
        }
    pipeline._fs_client().truncate_tables(["table_1", "table_3"])
    if ".{ext}{timestamp}" not in layout:
        assert load_table_counts(pipeline, "table_1", "table_2", "table_3") == {"table_2": 4}


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_buckets_filesystem_configs=True),
    ids=lambda x: x.name,
)
def test_state_files(destination_config: DestinationTestConfiguration) -> None:
    def _collect_files(p) -> List[str]:
        client = p.destination_client()
        found = []
        for basedir, _dirs, files in client.fs_client.walk(client.dataset_path):
            for file in files:
                found.append(os.path.join(basedir, file).replace(client.dataset_path, ""))
        return found

    def _collect_table_counts(p, *items: str) -> Dict[str, int]:
        expected_items = set(items).intersection({"items", "items2", "items3"})
        print(expected_items)
        return load_table_counts(
            p, *expected_items, "_dlt_loads", "_dlt_version", "_dlt_pipeline_state"
        )

    # generate 4 loads from 2 pipelines, store load ids
    dataset_name = "layout_test_" + uniq_id()
    p1 = destination_config.setup_pipeline("p1", dataset_name=dataset_name)
    p2 = destination_config.setup_pipeline("p2", dataset_name=dataset_name)
    c1 = cast(FilesystemClient, p1.destination_client())
    c2 = cast(FilesystemClient, p2.destination_client())

    # first two loads
    p1.run([1, 2, 3], table_name="items").loads_ids[0]
    load_id_2_1 = p2.run([4, 5, 6], table_name="items").loads_ids[0]
    assert _collect_table_counts(p1, "items") == {
        "items": 6,
        "_dlt_loads": 2,
        "_dlt_pipeline_state": 2,
        "_dlt_version": 2,
    }
    sc1_old = c1.get_stored_schema(c1.schema.name)
    sc2_old = c2.get_stored_schema(c2.schema.name)
    s1_old = c1.get_stored_state("p1")
    s2_old = c1.get_stored_state("p2")

    created_files = _collect_files(p1)
    # 4 init files, 2 item files, 2 load files, 2 state files, 2 version files
    assert len(created_files) == 12

    # second two loads
    @dlt.resource(table_name="items2")
    def some_data():
        dlt.current.resource_state()["state"] = {"some": "state"}
        yield from [1, 2, 3]

    load_id_1_2 = p1.run(some_data(), table_name="items2").loads_ids[
        0
    ]  # force state and migration bump here
    p2.run([4, 5, 6], table_name="items").loads_ids[0]  # no migration here

    # 4 loads for 2 pipelines, one schema and state change on p2 changes so 3 versions and 3 states
    assert _collect_table_counts(p1, "items", "items2") == {
        "items": 9,
        "items2": 3,
        "_dlt_loads": 4,
        "_dlt_pipeline_state": 3,
        "_dlt_version": 3,
    }

    # test accessors for state
    s1 = c1.get_stored_state("p1")
    s2 = c1.get_stored_state("p2")
    assert s1._dlt_load_id == load_id_1_2  # second load
    assert s2._dlt_load_id == load_id_2_1  # first load
    assert s1_old.version != s1.version
    assert s2_old.version == s2.version

    # test accessors for schema
    sc1 = c1.get_stored_schema(c1.schema.name)
    sc2 = c2.get_stored_schema(c2.schema.name)
    assert sc1.version_hash != sc1_old.version_hash
    assert sc2.version_hash == sc2_old.version_hash
    assert sc1.version_hash != sc2.version_hash

    assert not c1.get_stored_schema_by_hash("blah")
    assert c2.get_stored_schema_by_hash(sc1_old.version_hash)

    created_files = _collect_files(p1)
    # 4 init files, 4 item files, 4 load files, 3 state files, 3 version files
    assert len(created_files) == 18

    # drop it
    p1.destination_client().drop_storage()
    created_files = _collect_files(p1)
    assert len(created_files) == 0


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_buckets_filesystem_configs=True),
    ids=lambda x: x.name,
)
def test_knows_dataset_state(destination_config: DestinationTestConfiguration) -> None:
    # check if pipeline knows initializisation state of dataset
    p1 = destination_config.setup_pipeline("p1", dataset_name="layout_test")
    assert not p1.destination_client().is_storage_initialized()
    p1.run([1, 2, 3], table_name="items")
    assert p1.destination_client().is_storage_initialized()
    p1.destination_client().drop_storage()
    assert not p1.destination_client().is_storage_initialized()


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_buckets_filesystem_configs=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("restore", [True, False])
@pytest.mark.parametrize(
    "layout",
    [
        "{table_name}/{load_id}.{file_id}.{ext}",
        "{schema_name}/other_folder/{table_name}-{load_id}.{file_id}.{ext}",
        "{table_name}/{load_package_timestamp}/{d}/{load_id}.{file_id}.{ext}",
    ],
)  # we need a layout where the table has its own folder and one where it does not
def test_state_with_simple_incremental(
    destination_config: DestinationTestConfiguration,
    restore: bool,
    layout: str,
) -> None:
    os.environ["RESTORE_FROM_DESTINATION"] = str(restore)
    os.environ["DESTINATION__FILESYSTEM__LAYOUT"] = layout

    p = destination_config.setup_pipeline("p1", dataset_name="incremental_test")

    @dlt.resource(name="items")
    def my_resource(prim_key=dlt.sources.incremental("id")):
        yield from [
            {"id": 1},
            {"id": 2},
        ]

    @dlt.resource(name="items")
    def my_resource_inc(prim_key=dlt.sources.incremental("id")):
        yield from [
            {"id": 1},
            {"id": 2},
            {"id": 3},
            {"id": 4},
        ]

    p.run(my_resource)
    p._wipe_working_folder()

    # check incremental
    p = destination_config.setup_pipeline("p1", dataset_name="incremental_test")
    p.run(my_resource_inc)
    assert load_table_counts(p, "items") == {"items": 4 if restore else 6}


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_buckets_filesystem_configs=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "layout",
    [
        "{table_name}/{load_id}.{file_id}.{ext}",
        "{schema_name}/other_folder/{table_name}-{load_id}.{file_id}.{ext}",
    ],
)  # we need a layout where the table has its own folder and one where it does not
def test_client_methods(
    destination_config: DestinationTestConfiguration,
    layout: str,
) -> None:
    p = destination_config.setup_pipeline("access", dataset_name="incremental_test")
    os.environ["DESTINATION__FILESYSTEM__LAYOUT"] = layout

    @dlt.resource()
    def table_1():
        yield [1, 2, 3, 4, 5]

    @dlt.resource()
    def table_2():
        yield [1, 2, 3, 4, 5, 6, 7]

    @dlt.resource()
    def table_3():
        yield [1, 2, 3, 4, 5, 6, 7, 8]

    # 3 files for t_1, 2 files for t_2
    p.run([table_1(), table_2()])
    p.run([table_1(), table_2()])
    p.run([table_1()])

    fs_client = p._fs_client()
    t1_files = fs_client.list_table_files("table_1")
    t2_files = fs_client.list_table_files("table_2")
    assert len(t1_files) == 3
    assert len(t2_files) == 2

    assert load_table_counts(p, "table_1", "table_2") == {"table_1": 15, "table_2": 14}

    # verify that files are in the same folder on the second layout
    folder = fs_client.get_table_dir("table_1")
    file_count = len(fs_client.fs_client.ls(folder))
    if "{table_name}/" in layout:
        print(fs_client.fs_client.ls(folder))
        assert file_count == 3
    else:
        assert file_count == 5

    # check opening of file
    values = []
    for line in fs_client.read_text(t1_files[0], encoding="utf-8").split("\n"):
        if line:
            values.append(json.loads(line)["value"])
    assert values == [1, 2, 3, 4, 5]

    # check binary read
    assert fs_client.read_bytes(t1_files[0]) == str.encode(
        fs_client.read_text(t1_files[0], encoding="utf-8")
    )

    # check truncate
    fs_client.truncate_tables(["table_1"])
    assert load_table_counts(p, "table_1", "table_2") == {"table_2": 14}

    # load again
    p.run([table_1(), table_2(), table_3()])
    assert load_table_counts(p, "table_1", "table_2", "table_3") == {
        "table_1": 5,
        "table_2": 21,
        "table_3": 8,
    }

    # test truncate multiple
    fs_client.truncate_tables(["table_1", "table_3"])
    assert load_table_counts(p, "table_1", "table_2", "table_3") == {"table_2": 21}


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_buckets_filesystem_configs=True),
    ids=lambda x: x.name,
)
def test_cleanup_states_by_load_id(destination_config: DestinationTestConfiguration) -> None:
    """
    Test the pipeline state cleanup functionality by verifying that old state files are removed based on `load_id` when multiple loads are executed.

    Specifically, the oldest state file (corresponding to the first `load_id`) should be deleted.

    This test checks that when running a pipeline with a resource that produces incremental data, older state files are cleared according to the `max_state_files` setting.

    Steps:
    1. Set `max_state_files` to 2, allowing only two newest state files to be kept.
    2. Run the pipeline three times.
    3. Verify that the state file from the first load is no longer present in the state table.
    """

    dataset_name = f"{destination_config.destination_name}{uniq_id()}"
    p = destination_config.setup_pipeline("p1", dataset_name=dataset_name)

    @dlt.resource(name="items", primary_key="id")
    def r1(_=dlt.sources.incremental("id")):
        yield from [{"id": 0}]

    @dlt.resource(name="items", primary_key="id")
    def r2(_=dlt.sources.incremental("id")):
        yield from [{"id": 0}, {"id": 1}]

    @dlt.resource(name="items", primary_key="id")
    def r3(_=dlt.sources.incremental("id")):
        yield from [{"id": 0}, {"id": 1}, {"id": 2}]

    os.environ["DESTINATION__FILESYSTEM__MAX_STATE_FILES"] = str(2)

    info = p.run(r1)
    first_load_id = info.loads_ids[0]

    info = p.run(r2)
    second_load_id = [load_id for load_id in info.loads_ids if load_id != first_load_id][0]

    info = p.run(r3)
    third_load_id = [
        load_id
        for load_id in info.loads_ids
        if load_id != first_load_id and load_id != second_load_id
    ][0]

    client: FilesystemClient = p.destination_client()  # type: ignore
    state_table_files = list(client._list_dlt_table_files(client.schema.state_table_name, "p1"))

    assert not any(fileparts[1] == first_load_id for _, fileparts in state_table_files)
    assert any(fileparts[1] == second_load_id for _, fileparts in state_table_files)
    assert any(fileparts[1] == third_load_id for _, fileparts in state_table_files)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_buckets_filesystem_configs=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("max_state_files", [-1, 0, 1, 3])
def test_cleanup_states(
    destination_config: DestinationTestConfiguration, max_state_files: int
) -> None:
    """
    Test the behavior of pipeline state cleanup based on different max_state_files configurations.

    Steps:
    1. Run the pipeline five times with max_state_files set to -1, 0, 1, and 3.
    2. Verify that state files are cleaned or retained according to the max_state_files setting:
        - Negative or zero values disable cleanup.
        - Positive values trigger cleanup, keeping only the specified number of state files.
    """
    os.environ["DESTINATION__FILESYSTEM__MAX_STATE_FILES"] = str(max_state_files)

    dataset_name = f"{destination_config.destination_name}{uniq_id()}"
    p = destination_config.setup_pipeline("p1", dataset_name=dataset_name)

    @dlt.resource(name="items", primary_key="id")
    def r1(_=dlt.sources.incremental("id")):
        yield from [{"id": 0}]

    @dlt.resource(name="items", primary_key="id")
    def r2(_=dlt.sources.incremental("id")):
        yield from [{"id": 0}, {"id": 1}]

    @dlt.resource(name="items", primary_key="id")
    def r3(_=dlt.sources.incremental("id")):
        yield from [{"id": 0}, {"id": 1}, {"id": 2}]

    @dlt.resource(name="items", primary_key="id")
    def r4(_=dlt.sources.incremental("id")):
        yield from [{"id": 0}, {"id": 1}, {"id": 2}, {"id": 3}]

    @dlt.resource(name="items", primary_key="id")
    def r5(_=dlt.sources.incremental("id")):
        yield from [{"id": 0}, {"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}]

    # run pipeline
    run_count = 5

    p.run(r1)
    p.run(r2)
    p.run(r3)
    p.run(r4)
    p.run(r5)

    client: FilesystemClient = p.destination_client()  # type: ignore
    state_table_files = list(client._list_dlt_table_files(client.schema.state_table_name, "p1"))

    if max_state_files == -1 or max_state_files == 0:
        assert len(state_table_files) == run_count
    else:
        assert len(state_table_files) == max_state_files


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_buckets_filesystem_configs=True),
    ids=lambda x: x.name,
)
def test_cleanup_states_shared_dataset(destination_config: DestinationTestConfiguration) -> None:
    """
    Test that two pipelines sharing the same bucket_url and dataset_name can independently
    clean their _dlt_pipeline_state files with different max_state_files configurations.

    Steps:
    1. Run pipeline p1 five times with max_state_files set to 5.
    2. Run pipeline p2 five times with max_state_files set to 2.
    3. Verify that each pipeline only deletes its own state files and does not affect the other.
    """
    dataset_name = f"{destination_config.destination_name}{uniq_id()}"

    p1 = destination_config.setup_pipeline("p1", dataset_name=dataset_name)
    p2 = destination_config.setup_pipeline("p2", dataset_name=dataset_name)

    @dlt.resource(name="items", primary_key="id")
    def r1(_=dlt.sources.incremental("id")):
        yield from [{"id": 0}]

    @dlt.resource(name="items", primary_key="id")
    def r2(_=dlt.sources.incremental("id")):
        yield from [{"id": 0}, {"id": 1}]

    @dlt.resource(name="items", primary_key="id")
    def r3(_=dlt.sources.incremental("id")):
        yield from [{"id": 0}, {"id": 1}, {"id": 2}]

    @dlt.resource(name="items", primary_key="id")
    def r4(_=dlt.sources.incremental("id")):
        yield from [{"id": 0}, {"id": 1}, {"id": 2}, {"id": 3}]

    @dlt.resource(name="items", primary_key="id")
    def r5(_=dlt.sources.incremental("id")):
        yield from [{"id": 0}, {"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}]

    os.environ["DESTINATION__FILESYSTEM__MAX_STATE_FILES"] = str(5)
    p1.run(r1)
    p1.run(r2)
    p1.run(r3)
    p1.run(r4)
    p1.run(r5)

    os.environ["DESTINATION__FILESYSTEM__MAX_STATE_FILES"] = str(2)
    p2.run(r1)
    p2.run(r2)
    p2.run(r3)
    p2.run(r4)
    p2.run(r5)

    p1_client: FilesystemClient = p1.destination_client()  # type: ignore
    p1_state_files = list(p1_client._list_dlt_table_files(p1_client.schema.state_table_name, "p1"))

    p2_client: FilesystemClient = p2.destination_client()  # type: ignore
    p2_state_files = list(p2_client._list_dlt_table_files(p2_client.schema.state_table_name, "p2"))

    assert len(p1_state_files) == 5

    assert len(p2_state_files) == 2
