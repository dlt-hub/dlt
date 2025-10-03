import csv
import os
import posixpath
from pathlib import Path
from typing import Any, Callable, List, Dict, cast, Tuple
from importlib.metadata import version as pkg_version
from packaging.version import Version

from pytest_mock import MockerFixture
import dlt
import pytest

from dlt.common import json
from dlt.common import pendulum
from dlt.common.storages.configuration import (
    FilesystemConfiguration,
    FilesystemConfigurationWithLocalFiles,
)
from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.common.utils import uniq_id
from dlt.common.schema.typing import TWriteDisposition, TTableFormat
from dlt.common.configuration.exceptions import ConfigurationValueError

from dlt.destinations import filesystem
from dlt.destinations.impl.duckdb.exceptions import IcebergViewException
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
from dlt.destinations.impl.filesystem.typing import TExtraPlaceholders
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.load.exceptions import LoadClientJobRetry

from tests.cases import arrow_table_all_data_types, table_update_and_row, assert_all_data_types_row
from tests.load.utils import (
    ABFS_BUCKET,
    AWS_BUCKET,
    destinations_configs,
    DestinationTestConfiguration,
    MEMORY_BUCKET,
    FILE_BUCKET,
    AZ_BUCKET,
    SFTP_BUCKET,
)
from tests.pipeline.utils import (
    load_table_counts,
    assert_load_info,
    load_tables_to_dicts,
    users_materialize_table_schema,
)


def get_expected_actual(
    pipeline: dlt.Pipeline,
    table_name: str,
    table_format: TTableFormat,
    arrow_table: "pyarrow.Table",  # type: ignore[name-defined] # noqa: F821
) -> Tuple["pyarrow.Table", "pyarrow.Table"]:  # type: ignore[name-defined] # noqa: F821
    from dlt.common.libs.pyarrow import pyarrow, cast_arrow_schema_types

    if table_format == "delta":
        from dlt.common.libs.deltalake import (
            get_delta_tables,
            ensure_delta_compatible_arrow_data,
        )

        dt = get_delta_tables(pipeline, table_name)[table_name]
        expected = ensure_delta_compatible_arrow_data(arrow_table)
        actual = dt.to_pyarrow_table()
    elif table_format == "iceberg":
        from dlt.common.libs.pyiceberg import (
            get_iceberg_tables,
            ensure_iceberg_compatible_arrow_data,
        )

        it = get_iceberg_tables(pipeline, table_name)[table_name]
        expected = ensure_iceberg_compatible_arrow_data(arrow_table)
        actual = it.scan().to_arrow()

        # work around pyiceberg bug https://github.com/apache/iceberg-python/issues/1128
        schema = cast_arrow_schema_types(
            actual.schema,
            {
                pyarrow.types.is_large_string: pyarrow.string(),
                pyarrow.types.is_large_binary: pyarrow.binary(),
            },
        )
        actual = actual.cast(schema)
    return (expected, actual)


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_table_format_core(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Tests core functionality for `delta` and `iceberg` table formats.

    Tests all data types, all filesystems.
    Tests `append` and `replace` write dispositions (`merge` is tested elsewhere).
    """
    if destination_config.table_format == "delta":
        from dlt.common.libs.deltalake import get_delta_tables

    # create resource that yields rows with all data types
    column_schemas, row = table_update_and_row()

    @dlt.resource(columns=column_schemas, table_format=destination_config.table_format)
    def data_types():
        nonlocal row
        yield [row] * 10

    pipeline = destination_config.setup_pipeline("fs_pipe", dev_mode=True)

    # run pipeline, this should create table
    info = pipeline.run(data_types())
    assert_load_info(info)

    # table formats should use `parquet` file format
    completed_jobs = info.load_packages[0].jobs["completed_jobs"]
    data_types_jobs = [
        job for job in completed_jobs if job.job_file_info.table_name == "data_types"
    ]
    assert all([job.file_path.endswith((".parquet", ".reference")) for job in data_types_jobs])

    # 10 rows should be loaded to the table and the content of the first
    # row should match expected values
    rows = load_tables_to_dicts(pipeline, "data_types", exclude_system_cols=True)["data_types"]
    assert len(rows) == 10
    with pipeline._maybe_destination_capabilities() as caps:
        assert_all_data_types_row(caps, rows[0], schema=column_schemas)

    # make sure remote_url is in metrics
    metrics = info.metrics[info.loads_ids[0]][0]
    # TODO: only final copy job has remote_url. not the initial (empty) job for particular files
    # we could implement an empty job for delta that generates correct remote_url
    remote_url = list(metrics["job_metrics"].values())[-1].remote_url
    assert remote_url.endswith(("data_types", "_dlt_pipeline_state", ""))
    bucket_url = destination_config.bucket_url
    if FilesystemConfiguration.is_local_path(bucket_url):
        bucket_url = FilesystemConfigurationWithLocalFiles.make_file_url(bucket_url)
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
    if destination_config.table_format == "delta":
        assert get_delta_tables(pipeline, "data_types")["data_types"].version() == 2
    rows = load_tables_to_dicts(pipeline, "data_types", exclude_system_cols=True)["data_types"]
    assert len(rows) == 10


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_local_configs=True,
        subset=("filesystem",),
    ),
    ids=lambda x: x.name,
)
def test_preferred_table_format_caps(
    destination_config: DestinationTestConfiguration,
) -> None:
    # generate destination with modified caps
    dest_ = destination_config.destination_factory(
        preferred_table_format=destination_config.table_format
    )
    # make table format a default
    assert dest_.caps_params["preferred_table_format"] == destination_config.table_format

    pipeline = destination_config.setup_pipeline(
        "test_preferred_table_format_caps", destination=dest_
    )
    caps = pipeline._get_destination_capabilities()
    assert caps.preferred_table_format == destination_config.table_format

    # make sure right table format got created, note that we do not set the table format explicitly
    pipeline.run(
        [1, 2, 3], table_name="table_format", write_disposition="merge", primary_key="value"
    )
    if destination_config.table_format == "delta":
        from dlt.common.libs.deltalake import get_delta_tables

        delta_tables = get_delta_tables(pipeline, "table_format")
        delta_tables["table_format"].history()

    elif destination_config.table_format == "iceberg":
        from dlt.common.libs.pyiceberg import get_iceberg_tables

        iceberg_tables = get_iceberg_tables(pipeline, "table_format")
        iceberg_tables["table_format"].history()

    # get data
    print(pipeline.dataset().table_format.df())

    # now use native table to override preferred table format
    pipeline.run([1, 2, 3], table_name="native_format", table_format="native")

    fs_client = pipeline._fs_client()
    assert os.path.splitext(fs_client.list_table_files("native_format")[0])[1] == ".jsonl"


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_local_configs=True,
        # job orchestration is same across table formats—no need to test all formats
        with_table_format="delta",
        # job orchestration is particular to filesystem open tables implementation
        subset=("filesystem",),
    ),
    ids=lambda x: x.name,
)
def test_table_format_does_not_contain_job_files(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Asserts Parquet job files do not end up in table."""

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
        table_format_local_configs=True,
        # job orchestration is same across table formats—no need to test all formats
        with_table_format="delta",
        # job orchestration is particular to filesystem open tables implementation
        subset=("filesystem",),
    ),
    ids=lambda x: x.name,
)
def test_table_format_multiple_files(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Tests loading multiple files into a table.

    Files should be loaded into the table in a single commit.
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
        table_format_local_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_table_format_child_tables(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Tests child table handling for `delta` and `iceberg` table formats."""

    @dlt.resource(table_format=destination_config.table_format)
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
        [{"foo": i} for i in range(3, 10003)],
        table_name="nested_table",
        primary_key="foo",
        write_disposition="merge",
    )
    assert_load_info(info)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_local_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_table_format_partitioning(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Tests partitioning for `delta` and `iceberg` table formats."""

    from dlt.common.libs.pyarrow import pyarrow as pa

    def assert_partition_columns(
        table_name: str, table_format: TTableFormat, expected_partition_columns: List[str]
    ) -> None:
        if table_format == "delta":
            from dlt.common.libs.deltalake import get_delta_tables

            dt = get_delta_tables(pipeline, table_name)[table_name]
            actual_partition_columns = dt.metadata().partition_columns
        elif table_format == "iceberg":
            from dlt.common.libs.pyiceberg import get_iceberg_tables

            it = get_iceberg_tables(pipeline, table_name)[table_name]
            actual_partition_columns = [f.name for f in it.metadata.specs_struct().fields]
        assert actual_partition_columns == expected_partition_columns

    pipeline = destination_config.setup_pipeline("fs_pipe", dev_mode=True)

    # zero partition columns
    @dlt.resource(table_format=destination_config.table_format)
    def zero_part():
        yield {"foo": 1, "bar": 1}

    info = pipeline.run(zero_part())
    assert_load_info(info)
    assert_partition_columns("zero_part", destination_config.table_format, [])
    assert load_table_counts(pipeline, "zero_part")["zero_part"] == 1

    # one partition column
    @dlt.resource(table_format=destination_config.table_format, columns={"c1": {"partition": True}})
    def one_part():
        yield [
            {"c1": "foo", "c2": 1},
            {"c1": "foo", "c2": 2},
            {"c1": "bar", "c2": 3},
            {"c1": "baz", "c2": 4},
        ]

    info = pipeline.run(one_part())
    assert_load_info(info)
    assert_partition_columns("one_part", destination_config.table_format, ["c1"])
    assert load_table_counts(pipeline, "one_part")["one_part"] == 4

    # two partition columns
    @dlt.resource(
        table_format=destination_config.table_format,
        columns={"c1": {"partition": True}, "c2": {"partition": True}},
    )
    def two_part():
        yield [
            {"c1": "foo", "c2": 1, "c3": True},
            {"c1": "foo", "c2": 2, "c3": True},
            {"c1": "bar", "c2": 1, "c3": True},
            {"c1": "baz", "c2": 1, "c3": True},
        ]

    # Copy of two_part that yields a pandas DataFrame with 0 rows but same schema
    @dlt.resource(
        table_format=destination_config.table_format,
        columns={"c1": {"partition": True}, "c2": {"partition": True}},
    )
    def two_part_empty():
        schema = pa.schema(
            [pa.field("c1", pa.string()), pa.field("c2", pa.int64()), pa.field("c3", pa.bool_())]
        )
        table = pa.Table.from_arrays(
            [
                pa.array([], type=pa.string()),
                pa.array([], type=pa.int64()),
                pa.array([], type=pa.bool_()),
            ],
            schema=schema,
        )
        yield table

    info = pipeline.run(two_part())
    assert_load_info(info)
    assert_partition_columns("two_part", destination_config.table_format, ["c1", "c2"])
    assert load_table_counts(pipeline, "two_part")["two_part"] == 4

    # test partitioning with 0 rows
    info = pipeline.run(two_part_empty())
    assert_load_info(info)
    assert_partition_columns("two_part_empty", destination_config.table_format, ["c1", "c2"])
    try:
        assert load_table_counts(pipeline, "two_part_empty")["two_part_empty"] == 0
        # filesystem destination will use direct file access to do counts so duckdb view problem on iceberg does not happen
        assert destination_config.destination_type == "filesystem"
    except IcebergViewException:
        # currently duckdb does not allow to create views on empty Iceberg tables
        # assert destination_config.destination_type != "filesystem"
        pass

    # test partitioning with empty source
    users_source = users_materialize_table_schema()
    users_source.apply_hints(
        table_format=destination_config.table_format,
        columns={"id": {"partition": True}},
    )
    info = pipeline.run(users_source)
    assert_load_info(info)
    assert_partition_columns("users", destination_config.table_format, ["id"])
    try:
        assert load_table_counts(pipeline, "users")["users"] == 0
        assert destination_config.destination_type == "filesystem"
    except IcebergViewException:
        pass
        # assert destination_config.destination_type != "filesystem"

    # changing partitioning after initial table creation is not supported
    zero_part.apply_hints(columns={"foo": {"partition": True}})
    if destination_config.table_format == "delta":
        # Delta raises error when trying to change partitioning
        with pytest.raises(PipelineStepFailed) as pip_ex:
            pipeline.run(zero_part())
        assert isinstance(pip_ex.value.__context__, LoadClientJobRetry)
        assert "partitioning" in pip_ex.value.__context__.retry_message
    elif destination_config.table_format == "iceberg":
        # while Iceberg supports partition evolution, we don't apply it
        pipeline.run(zero_part())
    assert_partition_columns("zero_part", destination_config.table_format, [])


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_local_configs=True,
        with_table_format="delta",
    ),
    ids=lambda x: x.name,
)
def test_delta_table_partitioning_arrow_load_id(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Tests partitioning on load id column added by Arrow normalizer.

    Case needs special handling because of bug in delta-rs:
    https://github.com/delta-io/delta-rs/issues/2969
    """
    from dlt.common.libs.pyarrow import pyarrow
    from dlt.common.libs.deltalake import get_delta_tables

    os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID"] = "true"

    pipeline = destination_config.setup_pipeline("fs_pipe", dev_mode=True)

    # append write disposition
    info = pipeline.run(
        pyarrow.table({"foo": [1]}),
        table_name="delta_table",
        columns={"_dlt_load_id": {"partition": True}},
        table_format="delta",
    )
    assert_load_info(info)
    dt = get_delta_tables(pipeline, "delta_table")["delta_table"]
    assert dt.metadata().partition_columns == ["_dlt_load_id"]
    assert load_table_counts(pipeline, "delta_table")["delta_table"] == 1

    # merge write disposition
    info = pipeline.run(
        pyarrow.table({"foo": [1, 2]}),
        table_name="delta_table",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        columns={"_dlt_load_id": {"partition": True}},
        primary_key="foo",
        table_format="delta",
    )
    assert_load_info(info)
    dt = get_delta_tables(pipeline, "delta_table")["delta_table"]
    assert dt.metadata().partition_columns == ["_dlt_load_id"]
    assert load_table_counts(pipeline, "delta_table")["delta_table"] == 2


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_local_configs=True,
        supports_merge=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "write_disposition",
    (
        "append",
        "replace",
        "merge",
    ),
)
def test_table_format_schema_evolution(
    destination_config: DestinationTestConfiguration,
    write_disposition: TWriteDisposition,
) -> None:
    """Tests schema evolution (adding new columns) for `delta` and `iceberg` table formats."""

    from dlt.common.libs.pyarrow import pyarrow

    @dlt.resource(
        write_disposition=write_disposition,
        primary_key="pk",
        table_format=destination_config.table_format,
    )
    def evolving_table(data):
        yield data

    pipeline = destination_config.setup_pipeline("fs_pipe", dev_mode=True)

    # create Arrow table with one column, one row
    pk_field = pyarrow.field("pk", pyarrow.int64(), nullable=False)
    schema = pyarrow.schema([pk_field])
    arrow_table = pyarrow.Table.from_pydict({"pk": [1]}, schema=schema)
    assert arrow_table.shape == (1, 1)

    # initial load
    info = pipeline.run(evolving_table(arrow_table))
    assert_load_info(info)
    expected, actual = get_expected_actual(
        pipeline, "evolving_table", destination_config.table_format, arrow_table
    )
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
    info = pipeline.run(evolving_table(arrow_table))
    assert_load_info(info)
    expected, actual = get_expected_actual(
        pipeline, "evolving_table", destination_config.table_format, arrow_table
    )
    if write_disposition == "append":
        # just check shape and schema for `append`, because table comparison is
        # more involved than with the other dispositions
        assert actual.num_rows == 3
        assert actual.schema.equals(expected.schema)
    else:
        assert actual.schema.equals(expected.schema)
        assert actual.sort_by("pk").equals(expected.sort_by("pk"))

    # create empty Arrow table with additional column
    # NOTE: delta cannot do upsert when nested type contains mappings
    nested_type = pyarrow.struct(
        [
            pyarrow.field("list_field", pyarrow.list_(pyarrow.int32())),
            pyarrow.field(
                "struct_field",
                pyarrow.struct(
                    [
                        pyarrow.field("nested_int", pyarrow.int64()),
                        pyarrow.field("nested_string", pyarrow.string()),
                    ]
                ),
            ),
            # pyarrow.field("map_field", pyarrow.map_(pyarrow.string(), pyarrow.float64())),
            # pyarrow.field("dict_field", pyarrow.dictionary(pyarrow.int8(), pyarrow.string()))
        ]
    )

    num_rows = arrow_table.num_rows
    example_data = []

    for _ in range(num_rows):
        example_data.append(
            {
                "list_field": [1, 2, 3],
                "struct_field": {"nested_int": 42, "nested_string": "hello"},
                # "map_field": [("key1", 1.1), ("key2", 2.2)],
                # "dict_field": "example"
            }
        )

    arrow_table = arrow_table.append_column(
        pyarrow.field("another_new_column", nested_type),
        pyarrow.array(example_data, type=nested_type),
    )
    empty_arrow_table = arrow_table.schema.empty_table()

    # load 3 — this should evolve the schema without changing data
    info = pipeline.run(evolving_table(empty_arrow_table))
    assert_load_info(info)
    expected, actual = get_expected_actual(
        pipeline, "evolving_table", destination_config.table_format, arrow_table
    )
    # NOTE: pyiceberg mangles nested types. ie. instead of string we see large_string
    # TODO: handle when we implement nested types fully
    if destination_config.table_format == "delta":
        assert actual.schema.equals(expected.schema)

    if write_disposition == "append":
        expected_num_rows = 3
    elif write_disposition == "replace":
        expected_num_rows = 0
    elif write_disposition == "merge":
        expected_num_rows = 2
    assert actual.num_rows == expected_num_rows
    # new column should have NULLs only
    assert (
        actual.column("another_new_column").combine_chunks().to_pylist()
        == [None] * expected_num_rows
    )

    # load 4 - load nested types
    info = pipeline.run(evolving_table(arrow_table))

    if write_disposition == "merge":
        # PKs will overwrite two new rows
        expected_num_rows += 0
    else:
        expected_num_rows += 2

    # get data via sql_client from may different parquet files, including nested types
    # duckdb can't do that if schema inference is skipped so it is enabled by default
    assert pipeline.dataset().evolving_table.arrow().num_rows == expected_num_rows


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format=("delta", "iceberg"),
        bucket_subset=(FILE_BUCKET, ABFS_BUCKET),
    ),
    ids=lambda x: x.name,
)
def test_table_format_empty_source(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Tests empty source handling for `delta` and `iceberg` table formats.

    Tests both empty Arrow table and `dlt.mark.materialize_table_schema()`.
    """
    from tests.pipeline.utils import users_materialize_table_schema

    def get_table_version(  # type: ignore[return]
        pipeline: dlt.Pipeline,
        table_name: str,
        table_format: TTableFormat,
    ) -> int:
        if table_format == "delta":
            from dlt.common.libs.deltalake import get_delta_tables

            dt = get_delta_tables(pipeline, table_name)[table_name]
            return dt.version()
        elif table_format == "iceberg":
            from dlt.common.libs.pyiceberg import get_iceberg_tables

            it = get_iceberg_tables(pipeline, table_name)[table_name]
            return it.last_sequence_number - 1  # subtract 1 to match `delta`

    @dlt.resource(table_format=destination_config.table_format)
    def a_table(data):
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
    info = pipeline.run(a_table(empty_arrow_table))
    assert_load_info(info)
    assert get_table_version(pipeline, "a_table", destination_config.table_format) == 0
    expected, actual = get_expected_actual(
        pipeline, "a_table", destination_config.table_format, empty_arrow_table
    )
    assert actual.shape == (0, expected.num_columns)
    assert actual.schema.equals(expected.schema)

    # run 2: non-empty Arrow table with same schema as run 1
    # this should load records into Delta table
    info = pipeline.run(a_table(arrow_table))
    assert_load_info(info)
    assert get_table_version(pipeline, "a_table", destination_config.table_format) == 1
    expected, actual = get_expected_actual(
        pipeline, "a_table", destination_config.table_format, empty_arrow_table
    )
    assert actual.shape == (2, expected.num_columns)
    assert actual.schema.equals(expected.schema)

    # now run the empty frame again
    info = pipeline.run(a_table(empty_arrow_table))
    assert_load_info(info)

    # use materialized list
    # NOTE: this will create an empty parquet file with a schema takes from dlt schema.
    # the original parquet file had a nested (struct) type in `json` field that is now
    # in the delta table schema. the empty parquet file lost this information and had
    # string type (converted from dlt `json`)
    # TODO: implement nested types so this test will actually pass
    # with pytest.raises(PipelineStepFailed):
    # both pyiceberg and delta-rs complain on coercion error when evolving schema
    pipeline.run([dlt.mark.materialize_table_schema()], table_name="a_table")
    # pipeline.drop_pending_packages()

    # test `dlt.mark.materialize_table_schema()`
    users_materialize_table_schema.apply_hints(table_format=destination_config.table_format)
    info = pipeline.run(users_materialize_table_schema(), loader_file_format="parquet")
    assert_load_info(info)
    assert get_table_version(pipeline, "users", destination_config.table_format) == 0
    _, actual = get_expected_actual(
        pipeline, "users", destination_config.table_format, empty_arrow_table
    )
    assert actual.num_rows == 0
    assert "id", "name" == actual.schema.names[:2]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_local_configs=True,
        # job orchestration is same across table formats—no need to test all formats
        with_table_format="delta",
    ),
    ids=lambda x: x.name,
)
def test_table_format_mixed_source(
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
        table_format_local_configs=True,
        # job orchestration is same across table formats—no need to test all formats
        with_table_format="delta",
    ),
    ids=lambda x: x.name,
)
def test_table_format_dynamic_dispatch(
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
        with_table_format=("delta", "iceberg"),
        bucket_subset=(FILE_BUCKET, AWS_BUCKET),
    ),
    ids=lambda x: x.name,
)
def test_table_format_get_tables_helper(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Tests `get_delta_tables` / `get_iceberg_tables` helper functions."""
    get_tables: Any
    if destination_config.table_format == "delta":
        from dlt.common.libs.deltalake import DeltaTable, get_delta_tables

        get_tables = get_delta_tables
        get_num_rows = lambda table: table.to_pyarrow_table().num_rows
    elif destination_config.table_format == "iceberg":
        from dlt.common.libs.pyiceberg import IcebergTable, get_iceberg_tables

        get_tables = get_iceberg_tables
        get_num_rows = lambda table: table.scan().to_arrow().num_rows

    @dlt.resource(table_format=destination_config.table_format)
    def foo_table_format():
        yield [{"foo": 1}, {"foo": 2}]

    @dlt.resource(table_format=destination_config.table_format)
    def bar_table_format():
        yield [{"bar": 1}]

    @dlt.resource
    def baz_not_table_format():
        yield [{"baz": 1}]

    pipeline = destination_config.setup_pipeline("fs_pipe", dev_mode=True)

    info = pipeline.run(foo_table_format())
    assert_load_info(info)
    tables = get_tables(pipeline)
    assert tables.keys() == {"foo_table_format"}
    if destination_config.table_format == "delta":
        assert isinstance(tables["foo_table_format"], DeltaTable)
    elif destination_config.table_format == "iceberg":
        assert isinstance(tables["foo_table_format"], IcebergTable)
    assert get_num_rows(tables["foo_table_format"]) == 2

    info = pipeline.run([foo_table_format(), bar_table_format(), baz_not_table_format()])
    assert_load_info(info)
    tables = get_tables(pipeline)

    # filesystem based open tables support all table formats at the same time so we could make
    # baz a different type
    if destination_config.destination_type == "filesystem":
        assert tables.keys() == {"foo_table_format", "bar_table_format"}
    else:
        assert tables.keys() == {"foo_table_format", "bar_table_format", "baz_not_table_format"}
    assert get_num_rows(tables["bar_table_format"]) == 1
    assert get_tables(pipeline, "foo_table_format").keys() == {"foo_table_format"}
    assert get_tables(pipeline, "bar_table_format").keys() == {"bar_table_format"}
    assert get_tables(pipeline, "foo_table_format", "bar_table_format").keys() == {
        "foo_table_format",
        "bar_table_format",
    }

    # test with child table
    @dlt.resource(table_format=destination_config.table_format)
    def parent_table_format():
        yield [{"foo": 1, "child": [1, 2, 3]}]

    info = pipeline.run(parent_table_format())
    assert_load_info(info)
    tables = get_tables(pipeline)
    assert "parent_table_format__child" in tables.keys()
    assert get_num_rows(tables["parent_table_format__child"]) == 3

    # test invalid input
    if destination_config.destination_type == "filesystem":
        with pytest.raises(ValueError):
            get_tables(pipeline, "baz_not_table_format")

    with pytest.raises(ValueError):
        get_tables(pipeline, "non_existing_table")

    # test unknown schema
    with pytest.raises(FileNotFoundError):
        get_tables(pipeline, "non_existing_table", schema_name="aux_2")

    # load to a new schema and under new name
    aux_schema = dlt.Schema("aux_2")
    # NOTE: you cannot have a file with name
    info = pipeline.run(parent_table_format().with_name("aux_table"), schema=aux_schema)
    # also state in separate package
    assert_load_info(info, expected_load_packages=2)
    tables = get_tables(pipeline, schema_name="aux_2")
    assert "aux_table__child" in tables.keys()
    get_tables(pipeline, "aux_table", schema_name="aux_2")
    with pytest.raises(ValueError):
        get_tables(pipeline, "aux_table")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_local_configs=True,
        with_table_format="delta",
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
