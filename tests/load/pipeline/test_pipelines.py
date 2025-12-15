from copy import deepcopy
import gzip
import os
from typing import Any, Iterator, List, cast, Tuple
from pathlib import Path
import pytest

import dlt
from dlt.common import json, sleep
from dlt.common.exceptions import TerminalValueError
from dlt.common.pipeline import SupportsPipeline
from dlt.common.destination import Destination
from dlt.common.destination.client import WithStagingDataset
from dlt.common.destination.exceptions import UnknownDestinationModule
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import VERSION_TABLE_NAME, REPLACE_STRATEGIES, TLoaderReplaceStrategy
from dlt.common.schema.utils import new_table
from dlt.common.schema import TTableSchema
from dlt.common.typing import TDataItem, TDataItems
from dlt.common.utils import uniq_id

from dlt.destinations.exceptions import DestinationUndefinedEntity
from dlt.destinations.job_client_impl import SqlJobClientBase
from dlt.extract.exceptions import ResourceNameMissing
from dlt.pipeline.exceptions import (
    CannotRestorePipelineException,
    PipelineConfigMissing,
    PipelineStepFailed,
)

from tests.cases import TABLE_ROW_ALL_DATA_TYPES_DATETIMES, table_update_and_row
from tests.utils import TEST_STORAGE_ROOT, data_to_item_format
from tests.pipeline.utils import (
    assert_table_counts,
    assert_load_info,
    assert_query_column,
    assert_table_column,
    load_table_counts,
    select_data,
)
from tests.load.utils import (
    assert_all_data_types_row,
    delete_dataset,
    destinations_configs,
    DestinationTestConfiguration,
    FILE_BUCKET,
)
from tests.load.pipeline.utils import simple_nested_pipeline, skip_if_unsupported_replace_strategy


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("use_single_dataset", [True, False])
def test_default_pipeline_names(
    use_single_dataset: bool, destination_config: DestinationTestConfiguration
) -> None:
    destination_config.setup()
    p = dlt.pipeline()
    p.config.use_single_dataset = use_single_dataset
    # this is a name of executing test harness or blank pipeline on windows
    possible_names = ["dlt_pytest", "dlt_pipeline"]
    possible_dataset_names = ["dlt_pytest_dataset", "dlt_pipeline_dataset"]
    assert p.pipeline_name in possible_names
    assert p.pipelines_dir == os.path.abspath(os.path.join(TEST_STORAGE_ROOT, ".dlt", "pipelines"))
    assert p.dataset_name is None
    assert p.destination is None
    assert p.default_schema_name is None

    data = ["a", "b", "c"]
    with pytest.raises(PipelineStepFailed) as step_ex:
        p.extract(data)
    assert step_ex.value.step == "extract"
    assert isinstance(step_ex.value.exception, ResourceNameMissing)

    def data_fun() -> Iterator[Any]:
        yield data

    # this will create default schema
    p.extract(
        data_fun,
        table_format=destination_config.table_format,
        loader_file_format=destination_config.file_format,
    )
    # _pipeline suffix removed when creating default schema name
    assert p.default_schema_name in ["dlt_pytest", "dlt", "dlt_jb_pytest_runner"]

    # this will create additional schema
    p.extract(
        data_fun(),
        schema=dlt.Schema("names"),
        table_format=destination_config.table_format,
        loader_file_format=destination_config.file_format,
    )
    assert p.default_schema_name in ["dlt_pytest", "dlt", "dlt_jb_pytest_runner"]
    assert "names" in p.schemas.keys()

    with pytest.raises(PipelineConfigMissing):
        p.normalize()

    # mock the correct destinations (never do that in normal code)
    with p.managed_state():
        p._set_destinations(
            destination=destination_config.destination_factory(),
            staging=(
                Destination.from_reference(destination_config.staging)
                if destination_config.staging
                else None
            ),
        )
        p._set_dataset_name(None)

        if p.destination.spec().needs_dataset_name():  # type: ignore
            # sets dataset names for destinations that require it
            assert p.dataset_name in possible_dataset_names
            # never do that in production code
            p.dataset_name = None
            # set no dataset name -> if destination does not support it we revert to default
            p._set_dataset_name(None)
            assert p.dataset_name in possible_dataset_names
        else:
            # does not need dataset
            assert p.dataset_name is None
    # the last package contains just the state (we added a new schema)
    last_load_id = p.list_extracted_load_packages()[-1]
    state_package = p.get_load_package_info(last_load_id)
    assert len(state_package.jobs["new_jobs"]) == 1
    assert state_package.schema_name == p.default_schema_name
    p.normalize()
    info = p.load(dataset_name="default_names_ds_" + uniq_id())
    print(p.dataset_name)
    assert info.pipeline is p
    # two packages in two different schemas were loaded
    assert len(info.loads_ids) == 3

    # if loaded to single data, double the data was loaded to a single table because the schemas overlapped
    if use_single_dataset:
        assert_table_column(p, "data_fun", data * 2, info=info)
    else:
        # loaded to separate data sets
        assert_table_column(p, "data_fun", data, info=info)
        assert_table_column(p, "data_fun", data, schema_name="names", info=info)


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
        subset=("filesystem", "redshift", "snowflake"),
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("use_single_dataset", [True, False])
@pytest.mark.parametrize(
    "naming_convention",
    [
        "duck_case",
        "snake_case",
        "sql_cs_v1",
    ],
)
def test_default_schema_name(
    destination_config: DestinationTestConfiguration,
    use_single_dataset: bool,
    naming_convention: str,
) -> None:
    os.environ["SCHEMA__NAMING"] = naming_convention
    destination_config.setup()
    dataset_name = "dataset_" + uniq_id()
    data = [
        {"id": idx, "CamelInfo": uniq_id(), "GEN_ERIC": alpha}
        for idx, alpha in [(0, "A"), (0, "B"), (0, "C")]
    ]

    p = destination_config.setup_pipeline(
        "test_default_schema_name",
        dataset_name=dataset_name,
        pipelines_dir=TEST_STORAGE_ROOT,
    )

    p.config.use_single_dataset = use_single_dataset
    p.extract(
        data,
        table_name="test",
        schema=Schema("default"),
        table_format=destination_config.table_format,
        loader_file_format=destination_config.file_format,
    )
    p.normalize()
    info = p.load()
    print(info)

    # try to restore pipeline
    r_p = dlt.attach("test_default_schema_name", TEST_STORAGE_ROOT)
    schema = r_p.default_schema
    # schema name not normalized
    assert schema.name == "default"

    # check if dlt ables have exactly the required schemas
    # TODO: uncomment to check dlt tables schemas
    # assert (
    #     r_p.default_schema.tables[PIPELINE_STATE_TABLE_NAME]["columns"]
    #     == pipeline_state_table()["columns"]
    # )

    # assert_table(p, "test", data, info=info)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        table_format_local_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_attach_pipeline(destination_config: DestinationTestConfiguration) -> None:
    # load data and then restore the pipeline and see if data is still there
    data = ["a", "b", "c"]

    @dlt.resource(name="data_table")
    def _data():
        for d in data:
            yield d

    destination_config.setup()
    info = dlt.run(
        _data(),
        destination=destination_config.destination_factory(),
        staging=destination_config.staging,
        dataset_name="specific" + uniq_id(),
        **destination_config.run_kwargs,
    )

    with pytest.raises(CannotRestorePipelineException):
        dlt.attach("unknown")

    # restore default pipeline
    p = dlt.attach()

    # other instance
    assert info.pipeline is not p
    # same pipe
    old_p: SupportsPipeline = info.pipeline
    assert p.pipeline_name == old_p.pipeline_name
    assert p.working_dir == old_p.working_dir
    # secret will be the same: if not explicitly provided it is derived from pipeline name
    assert p.pipeline_salt == old_p.pipeline_salt
    assert p.default_schema_name == p.default_schema_name

    # query data
    # we add the destination so sqlglot may resolve the correct dialect for dataset access in the assert
    assert_table_column(p, "data_table", data, info=info)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_attach_edgecases(destination_config: DestinationTestConfiguration) -> None:
    """
    Tests various attach edge cases and automated syncing. Only needs to be done for one destination (duckdb)
    """

    EXAMPLE_DATA = ["a", "b", "c"]

    @dlt.resource
    def data_table():
        for d in EXAMPLE_DATA:
            yield d

    p = destination_config.setup_pipeline("test_attach_edgecases")
    p.run(data_table())
    state_version_hash = p.state["_state_version"]
    dataset_name = p.dataset_name

    # re-attaching works
    p = dlt.attach("test_attach_edgecases")
    assert_table_column(p, "data_table", EXAMPLE_DATA)
    assert p._pipeline_storage.has_folder("")
    assert p.state["_state_version"] == state_version_hash

    # re-attaching only with name will fail
    p._wipe_working_folder()
    with pytest.raises(CannotRestorePipelineException) as exc_info:
        dlt.attach("test_attach_edgecases")
    assert "no destination was provided to restore from" in str(exc_info.value)
    # no working folder left behing
    assert not p._pipeline_storage.has_folder("")

    # re-attaching with destination but incorrect dataset name will fail
    p._wipe_working_folder()
    with pytest.raises(CannotRestorePipelineException) as exc_info:
        dlt.attach("test_attach_edgecases", destination="duckdb", dataset_name="incorrect")
    # no working folder left behing
    assert "provided destination and dataset do not contain state for this pipeline" in str(
        exc_info.value
    )
    assert not p._pipeline_storage.has_folder("")

    # re-attaching with destination and dataset name will work
    p = dlt.attach("test_attach_edgecases", destination="duckdb", dataset_name=dataset_name)
    assert_table_column(p, "data_table", EXAMPLE_DATA)
    assert p.state["_state_version"] == state_version_hash
    # working folder is here again
    assert p._pipeline_storage.has_folder("")

    p._wipe_working_folder()

    # re-attaching with env vars will work
    os.environ["PIPELINES__TEST_ATTACH_EDGECASES__DESTINATION_TYPE"] = "duckdb"
    os.environ["PIPELINES__TEST_ATTACH_EDGECASES__DATASET_NAME"] = dataset_name
    p = dlt.attach("test_attach_edgecases")
    assert_table_column(p, "data_table", EXAMPLE_DATA)
    assert p.state["_state_version"] == state_version_hash
    # working folder is here again
    assert p._pipeline_storage.has_folder("")


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
def test_skip_sync_schema_for_tables_without_columns(
    destination_config: DestinationTestConfiguration,
) -> None:
    # load data and then restore the pipeline and see if data is still there
    data = ["a", "b", "c"]

    @dlt.resource(name="data_table")
    def _data():
        for d in data:
            yield d

    p = destination_config.setup_pipeline("test_skip_sync_schema_for_tables", dev_mode=True)
    p.extract(_data, table_format=destination_config.table_format)
    schema = p.default_schema
    assert "data_table" in schema.tables
    assert schema.tables["data_table"]["columns"] == {}

    p.sync_schema()

    with p._get_destination_clients(schema)[0] as job_client:
        # there's some data at all
        exists, _ = job_client.get_storage_table(VERSION_TABLE_NAME)  # type: ignore[attr-defined]
        assert exists is True

        # such tables are not created but silently ignored
        exists, _ = job_client.get_storage_table("data_table")  # type: ignore[attr-defined]
        assert not exists


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        all_buckets_filesystem_configs=True,
        table_format_filesystem_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_run_dev_mode(destination_config: DestinationTestConfiguration) -> None:
    data = ["a", ["a", "b", "c"], ["a", "b", "c"]]
    destination_config.setup()

    def d():
        yield data

    @dlt.source(name="nested")
    def _data():
        return dlt.resource(d(), name="lists", write_disposition="replace")

    p = dlt.pipeline(dev_mode=True)
    info = p.run(
        _data(),
        destination=destination_config.destination_factory(),
        staging=destination_config.staging,
        dataset_name="iteration" + uniq_id(),
        **destination_config.run_kwargs,
    )
    assert info.dataset_name == p.dataset_name
    assert info.dataset_name.endswith(p._pipeline_instance_id)
    # print(p.default_schema.to_pretty_yaml())
    # print(info)

    # restore the pipeline
    p = dlt.attach()
    # restored pipeline should be never put in full refresh
    assert p.dev_mode is False
    # assert parent table (easy), None First (db order)
    assert_table_column(p, "lists", [None, None, "a"], info=info)
    # child tables contain nested lists
    data_list = cast(List[str], data[1]) + cast(List[str], data[2])
    assert_table_column(p, "lists__value", sorted(data_list))


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, table_format_filesystem_configs=True),
    ids=lambda x: x.name,
)
def test_evolve_schema(destination_config: DestinationTestConfiguration) -> None:
    dataset_name = "d" + uniq_id()
    row = {
        "id": "level0",
        "f": [{"id": "level1", "l": ["a", "b", "c"], "v": 120, "o": [{"a": 1}, {"a": 2}]}],
    }

    @dlt.source(name="parallel")
    def source(top_elements: int):
        @dlt.defer
        def get_item(no: int) -> TDataItem:
            # the test will not last 10 seconds but 2 (there are 5 working threads by default)
            sleep(1)
            data = deepcopy(row)
            data["id"] = "level" + str(no)
            return data

        @dlt.resource(
            columns={
                "id": {
                    "name": "id",
                    "nullable": False,
                    "data_type": "text",
                    "unique": True,
                    "sort": True,
                }
            }
        )
        def simple_rows():
            for no in range(top_elements):
                # yield deferred items resolved in threads
                yield get_item(no)

        @dlt.resource(
            table_name="simple_rows",
            columns={"new_column": {"nullable": True, "data_type": "decimal"}},
        )
        def extended_rows():
            for no in range(top_elements):
                # yield deferred items resolved in threads
                yield get_item(no + 100)

        return simple_rows(), extended_rows(), dlt.resource(["a", "b", "c"], name="simple")

    import_schema_path = os.path.join(TEST_STORAGE_ROOT, "schemas", "import")
    export_schema_path = os.path.join(TEST_STORAGE_ROOT, "schemas", "export")
    p = destination_config.setup_pipeline(
        "my_pipeline", import_schema_path=import_schema_path, export_schema_path=export_schema_path
    )

    p.extract(
        source(10).with_resources("simple_rows"),
        table_format=destination_config.table_format,
        loader_file_format=destination_config.file_format,
    )
    # print(p.default_schema.to_pretty_yaml())
    p.normalize()
    info = p.load(dataset_name=dataset_name)
    # test __str__
    print(info)
    # test fingerprint in load
    assert info.destination_fingerprint == p.destination_client().config.fingerprint()
    # print(p.default_schema.to_pretty_yaml())
    schema = p.default_schema
    version_history = [schema.stored_version_hash]
    assert "simple_rows" in schema.tables
    assert "simple" not in schema.tables
    assert "new_column" not in schema.get_table("simple_rows")["columns"]

    # lets violate unique constraint on postgres, redshift and BQ ignore unique indexes
    if destination_config.destination_type == "postgres":
        # let it complete even with PK violation (which is a teminal error)
        os.environ["RAISE_ON_FAILED_JOBS"] = "false"
        assert p.dataset_name == dataset_name
        err_info = p.run(
            source(1).with_resources("simple_rows"),
            **destination_config.run_kwargs,
        )
        version_history.append(p.default_schema.stored_version_hash)
        # print(err_info)
        # we have failed jobs
        assert len(err_info.load_packages[0].jobs["failed_jobs"]) == 1

    # update schema
    # - new column in "simple_rows" table
    # - new "simple" table
    info_ext = dlt.run(
        source(10).with_resources("extended_rows", "simple"), **destination_config.run_kwargs
    )
    print(info_ext)
    # print(p.default_schema.to_pretty_yaml())
    schema = p.default_schema
    version_history.append(schema.stored_version_hash)
    assert "simple_rows" in schema.tables
    assert "simple" in schema.tables
    assert "new_column" in schema.get_table("simple_rows")["columns"]
    assert "extended_rows" not in schema.tables

    # TODO: test export and import schema
    # test data
    id_data = sorted(
        ["level" + str(n) for n in range(10)] + ["level" + str(n) for n in range(100, 110)]
    )
    with p.sql_client() as client:
        simple_rows_table = client.make_qualified_table_name("simple_rows")
        dlt_loads_table = client.make_qualified_table_name("_dlt_loads")
    assert_query_column(p, f"SELECT * FROM {simple_rows_table} ORDER BY id", id_data)
    assert_query_column(
        p,
        f"SELECT schema_version_hash FROM {dlt_loads_table} ORDER BY inserted_at",
        version_history,
    )


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        all_buckets_filesystem_configs=True,
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("disable_compression", [True, False])
def test_pipeline_data_writer_compression(
    disable_compression: bool, destination_config: DestinationTestConfiguration
) -> None:
    # Ensure pipeline works without compression
    data = ["a", "b", "c"]
    dataset_name = "compression_data_" + uniq_id()
    destination_config.disable_compression = disable_compression

    p = destination_config.setup_pipeline("compression_test", dataset_name=dataset_name)
    p.extract(
        dlt.resource(data, name="data"),
        table_format=destination_config.table_format,
        loader_file_format=destination_config.file_format,
    )
    s = p._get_normalize_storage()
    # check that files are not compressed if compression is disabled
    for name in s.list_files_to_normalize_sorted():
        full_path = s.extracted_packages.storage.make_full_path(name)
        if disable_compression:
            with pytest.raises(gzip.BadGzipFile):
                with gzip.open(full_path, "rb") as f:
                    f.read()
        else:
            # wont' decode zip file as utf
            with pytest.raises(UnicodeDecodeError):
                with open(full_path, "rt", encoding="utf-8") as f:
                    f.readline()

    p.normalize()

    info = p.load()
    assert_table_column(p, "data", data, info=info)


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
def test_source_max_nesting(destination_config: DestinationTestConfiguration) -> None:
    destination_config.setup()

    nested_part = {"l": [1, 2, 3], "c": {"a": 1, "b": 12.3}}

    @dlt.source(name="nested", max_table_nesting=0)
    def nested_data():
        return dlt.resource([{"idx": 1, "cn": nested_part}], name="nested_cn")

    dlt.run(
        nested_data(),
        destination=destination_config.destination_factory(),
        staging=destination_config.staging,
        dataset_name="ds_" + uniq_id(),
        **destination_config.run_kwargs,
    )
    rows = select_data(dlt.pipeline(), "SELECT cn FROM nested_cn")
    assert len(rows) == 1
    cn_val = rows[0][0]
    if isinstance(cn_val, str):
        cn_val = json.loads(cn_val)
    assert cn_val == nested_part


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        default_staging_configs=True,
        with_file_format="parquet",
        local_filesystem_configs=True,
        table_format_local_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_parquet_loading(destination_config: DestinationTestConfiguration) -> None:
    """Run pipeline twice with merge write disposition
    Resource with primary key falls back to append. Resource without keys falls back to replace.
    """
    pipeline = destination_config.setup_pipeline(
        "parquet_test_" + uniq_id(), dataset_name="parquet_test_" + uniq_id()
    )

    @dlt.resource(primary_key="id")
    def some_data():
        yield [{"id": 1}, {"id": 2}, {"id": 3}]

    @dlt.resource(write_disposition="replace")
    def other_data():
        yield [1, 2, 3, 4, 5]

    datetime_data = deepcopy(TABLE_ROW_ALL_DATA_TYPES_DATETIMES)
    columns_schema, _ = table_update_and_row()

    # parquet on bigquery and clickhouse does not support JSON but we still want to run the test
    if destination_config.destination_type in ["bigquery"]:
        columns_schema["col9_null"]["data_type"] = columns_schema["col9"]["data_type"] = "text"

    # drop TIME from databases not supporting it via parquet
    if destination_config.destination_type in [
        "redshift",
        "athena",
        "synapse",
        "databricks",
        "clickhouse",
    ]:
        datetime_data.pop("col11")
        datetime_data.pop("col11_null")
        datetime_data.pop("col11_precision")
        columns_schema.pop("col11")
        columns_schema.pop("col11_null")
        columns_schema.pop("col11_precision")

    if destination_config.destination_type in ("redshift", "dremio"):
        datetime_data.pop("col7_precision")
        columns_schema.pop("col7_precision")
    if destination_config.destination_type == "filesystem" and not destination_config.table_format:
        # duckdb view will be crated over parquet file
        # parquet file contains decimal256 (wei) which gets converted to float64 and we lose
        # precision, drop column to avoid test
        datetime_data.pop("col8")
        columns_schema.pop("col8")

    # apply the exact columns definitions so we process nested and wei types correctly!
    @dlt.resource(
        table_name="data_types",
        primary_key="col1",
        write_disposition="merge",
        columns=columns_schema,
    )
    def my_resource():
        nonlocal datetime_data

        start_idx = cast(int, datetime_data["col1"])
        for idx, item in enumerate([datetime_data] * 10):
            item = deepcopy(item)
            item["col1"] = start_idx + idx
            yield item

    @dlt.source(max_table_nesting=0)
    def some_source():
        return [some_data(), other_data(), my_resource()]

    info = pipeline.run(some_source(), **destination_config.run_kwargs)
    package_info = pipeline.get_load_package_info(info.loads_ids[0])
    # print(package_info.asstr(verbosity=2))
    assert package_info.state == "loaded"
    # all three jobs succeeded
    assert len(package_info.jobs["failed_jobs"]) == 0
    # 3 tables + 1 state + 4 reference jobs if staging or table format
    expected_completed_jobs = 4 + 4 if pipeline.staging else 4
    # add sql merge job
    if destination_config.supports_merge:
        expected_completed_jobs += 1
        # add iceberg copy jobs
        if destination_config.table_format in ("iceberg", "delta"):
            expected_completed_jobs += 2  # if destination_config.supports_merge else 4
    else:
        if destination_config.table_format:
            expected_completed_jobs += 3  # reference jobs for all tables but not state
    assert len(package_info.jobs["completed_jobs"]) == expected_completed_jobs

    with pipeline.sql_client() as sql_client:
        qual_name = sql_client.make_qualified_table_name
        assert [
            row[0]
            for row in sql_client.execute_sql(f"SELECT * FROM {qual_name('other_data')} ORDER BY 1")
        ] == [1, 2, 3, 4, 5]
        assert [
            row[0]
            for row in sql_client.execute_sql(f"SELECT * FROM {qual_name('some_data')} ORDER BY 1")
        ] == [1, 2, 3]
        db_rows = sql_client.execute_sql(f"SELECT * FROM {qual_name('data_types')} ORDER BY 1")
        assert len(db_rows) == 10
        db_row = list(db_rows[0])
        # "snowflake" and "bigquery" do not parse JSON form parquet string so double parse
        assert_all_data_types_row(
            sql_client.capabilities,
            db_row,
            schema=columns_schema,
            parse_json_strings=destination_config.destination_type
            in ["snowflake", "bigquery", "redshift"],
            allow_string_binary=destination_config.destination_type == "clickhouse",
        )


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, table_format_local_configs=True),
    ids=lambda x: x.name,
)
def test_dataset_name_change(destination_config: DestinationTestConfiguration) -> None:
    destination_config.setup()
    # standard name
    ds_1_name = "iteration" + uniq_id()
    # will go to snake case
    ds_2_name = "IteRation" + uniq_id()
    # illegal name that will be later normalized
    ds_3_name = "1it/era 👍 tion__" + uniq_id()
    p, s = simple_nested_pipeline(destination_config, dataset_name=ds_1_name, dev_mode=False)
    try:
        info = p.run(s(), **destination_config.run_kwargs)
        assert_load_info(info)
        assert info.dataset_name == ds_1_name
        ds_1_counts = load_table_counts(p, "lists", "lists__value")
        # run to another dataset
        info = p.run(s(), dataset_name=ds_2_name, **destination_config.run_kwargs)
        assert_load_info(info)
        assert info.dataset_name.startswith("ite_ration")
        # save normalized dataset name to delete correctly later
        ds_2_name = info.dataset_name
        ds_2_counts = load_table_counts(p, "lists", "lists__value")
        assert ds_1_counts == ds_2_counts
        # set name and run to another dataset
        p.dataset_name = ds_3_name
        info = p.run(s(), **destination_config.run_kwargs)
        assert_load_info(info)
        assert info.dataset_name.startswith("_1it_era_tion_")
        ds_3_counts = load_table_counts(p, "lists", "lists__value")
        assert ds_1_counts == ds_3_counts

    finally:
        # we have to clean dataset ourselves
        with p.sql_client() as client:
            delete_dataset(client, ds_1_name)
            delete_dataset(client, ds_2_name)
            # delete_dataset(client, ds_3_name)  # will be deleted by the fixture


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_staging_configs=True, default_sql_configs=True, table_format_filesystem_configs=True
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("replace_strategy", REPLACE_STRATEGIES)
def test_pipeline_upfront_tables_two_loads(
    destination_config: DestinationTestConfiguration, replace_strategy: TLoaderReplaceStrategy
) -> None:
    skip_if_unsupported_replace_strategy(destination_config, replace_strategy)

    # use staging tables for replace
    os.environ["DESTINATION__REPLACE_STRATEGY"] = replace_strategy
    os.environ["TRUNCATE_STAGING_DATASET"] = "True"

    pipeline = destination_config.setup_pipeline(
        "test_pipeline_upfront_tables_two_loads",
        dataset_name="test_pipeline_upfront_tables_two_loads",
        dev_mode=True,
    )

    @dlt.source
    def two_tables():
        @dlt.resource(
            columns=[{"name": "id", "data_type": "bigint", "nullable": False, "primary_key": True}],
            write_disposition="merge",
            table_format=destination_config.table_format,
        )
        def table_1():
            yield {"id": 1}

        @dlt.resource(
            columns=[
                {
                    "name": "id",
                    "data_type": "bigint",
                    "nullable": False,
                    "unique": True,
                    "primary_key": True,
                }
            ],
            write_disposition="merge",
            table_format=destination_config.table_format,
        )
        def table_2():
            yield data_to_item_format("arrow-table", [{"id": 2}])

        @dlt.resource(
            columns=[{"name": "id", "data_type": "bigint", "nullable": False}],
            write_disposition="replace",
            table_format=destination_config.table_format,
        )
        def table_3(make_data=False):
            if not make_data:
                return
            yield {"id": 3}

        return table_1, table_2, table_3

    # discover schema
    schema = two_tables().discover_schema()
    # print(schema.to_pretty_yaml())

    # now we use this schema but load just one resource
    source = two_tables()
    # push state, table 3 not created
    load_info_1 = pipeline.run(source.table_3, schema=schema, **destination_config.run_kwargs)
    assert_load_info(load_info_1)
    with pytest.raises(DestinationUndefinedEntity):
        load_table_counts(pipeline, "table_3")
    assert "x-normalizer" not in pipeline.default_schema.tables["table_3"]
    assert (
        pipeline.default_schema.tables["_dlt_pipeline_state"]["x-normalizer"]["seen-data"] is True
    )

    # load with one empty job, table 3 not created
    load_info = pipeline.run(source.table_3, **destination_config.run_kwargs)
    assert_load_info(load_info, expected_load_packages=0)
    with pytest.raises(DestinationUndefinedEntity):
        load_table_counts(pipeline, "table_3")
    # print(pipeline.default_schema.to_pretty_yaml())

    load_info_2 = pipeline.run([source.table_1, source.table_3], **destination_config.run_kwargs)
    assert_load_info(load_info_2)
    # 1 record in table 1
    assert pipeline.last_trace.last_normalize_info.row_counts["table_1"] == 1
    assert "table_3" not in pipeline.last_trace.last_normalize_info.row_counts
    assert "table_2" not in pipeline.last_trace.last_normalize_info.row_counts
    # only table_1 got created
    assert load_table_counts(pipeline, "table_1") == {"table_1": 1}
    with pytest.raises(DestinationUndefinedEntity):
        load_table_counts(pipeline, "table_2")
    with pytest.raises(DestinationUndefinedEntity):
        load_table_counts(pipeline, "table_3")

    # v4 = pipeline.default_schema.to_pretty_yaml()
    # print(v4)

    # now load the second one. for arrow format the schema will not update because
    # in that case normalizer does not add dlt specific fields, changes are not detected
    # and schema is not updated because the hash didn't change
    # also we make the replace resource to load its 1 record
    load_info_3 = pipeline.run(
        [source.table_3(make_data=True), source.table_2],
        **destination_config.run_kwargs,
    )
    assert_load_info(load_info_3)
    assert_table_counts(pipeline, {"table_1": 1, "table_2": 1, "table_3": 1})
    # v5 = pipeline.default_schema.to_pretty_yaml()
    # print(v5)

    # check if seen data is market correctly
    assert pipeline.default_schema.tables["table_3"]["x-normalizer"]["seen-data"] is True
    assert pipeline.default_schema.tables["table_2"]["x-normalizer"]["seen-data"] is True
    assert pipeline.default_schema.tables["table_1"]["x-normalizer"]["seen-data"] is True

    job_client, _ = pipeline._get_destination_clients(schema)

    if destination_config.staging and isinstance(job_client, WithStagingDataset):
        for i in range(1, 4):
            with pipeline.sql_client() as client:
                table_name = f"table_{i}"

                if job_client.should_load_data_to_staging_dataset(table_name):
                    with client.with_staging_dataset():
                        tab_name = client.make_qualified_table_name(table_name)
                        with client.execute_query(f"SELECT * FROM {tab_name}") as cur:
                            assert len(cur.fetchall()) == 0


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, exclude=["sqlalchemy"]),
    ids=lambda x: x.name,
)
def test_query_all_info_tables_fallback(destination_config: DestinationTestConfiguration) -> None:
    os.environ["INFO_TABLES_QUERY_THRESHOLD"] = "0"
    pipeline = destination_config.setup_pipeline(
        "parquet_test_" + uniq_id(), dataset_name="parquet_test_" + uniq_id()
    )
    info = pipeline.run([1, 2, 3], table_name="digits_1", **destination_config.run_kwargs)
    assert_load_info(info)
    # create empty table
    client: SqlJobClientBase
    # we must add it to schema
    pipeline.default_schema._schema_tables["existing_table"] = new_table("existing_table")
    with pipeline.destination_client() as client:  # type: ignore[assignment]
        sql = client._get_table_update_sql(
            "existing_table", [{"name": "_id", "data_type": "bigint"}], False
        )
        client.sql_client.execute_many(sql)
    # remove it from schema
    del pipeline.default_schema._schema_tables["existing_table"]
    # store another table
    info = pipeline.run([1, 2, 3], table_name="digits_2", **destination_config.run_kwargs)
    assert_table_counts(pipeline, {"digits_1": 3, "digits_2": 3})


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        # with_file_format=["insert_values", "jsonl", "parquet"],
    ),
    ids=lambda x: x.name,
)
def test_load_non_utc_timestamps(destination_config: DestinationTestConfiguration) -> None:
    """Checks if dates are stored properly and timezones are not mangled"""
    from datetime import timedelta, datetime, timezone
    from dlt.common import pendulum

    start_dt = datetime.now()

    @dlt.resource(
        standalone=True, primary_key="Hour", columns=[{"name": "naive_dt", "timezone": False}]
    )
    def date_data(
        max_hours: int = 2,
    ):
        data = [
            {
                "naive_dt": start_dt + timedelta(hours=hour),
                "utc_dt": pendulum.instance(start_dt + timedelta(hours=hour)),
                # tz="Europe/Berlin", timezone(offset=timedelta(hours=-8)
                "berlin_dt": pendulum.instance(
                    start_dt + timedelta(hours=hour), tz="Europe/Berlin"
                ),
                "hour": hour,
            }
            for hour in range(0, max_hours)
        ]
        yield data

    pipeline = destination_config.setup_pipeline(
        "test_load_non_utc_timestamps",
        dataset_name="test_load_non_utc_timestamps",
        dev_mode=True,
    )
    info = pipeline.run(date_data())
    assert_load_info(info)
    date_table = pipeline.default_schema.tables["date_data"]
    assert date_table["columns"]["naive_dt"]["timezone"] is False
    with pipeline.dataset() as dataset:
        # read with fetch
        object_data = dataset["date_data"].fetchall()
        print(object_data)
        # read with arrow
        arrow_data = dataset["date_data"].arrow()
        print(arrow_data)
    # table_name = pipeline.sql_client().make_qualified_table_name("some_data")
    # print(select_data(pipeline, f"SELECT * FROM {table_name}"))


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb", "postgres", "snowflake"]),
    ids=lambda x: x.name,
)
def test_dest_column_invalid_timestamp_precision(
    destination_config: DestinationTestConfiguration,
) -> None:
    invalid_precision = 10

    @dlt.resource(
        columns={
            "event_tstamp": {
                "data_type": "timestamp",
                "precision": invalid_precision,
                "timezone": False,
            }
        },
        primary_key="event_id",
    )
    def events():
        yield [{"event_id": 1, "event_tstamp": "2024-07-30T10:00:00.123+00:00"}]

    pipeline = destination_config.setup_pipeline("p" + uniq_id())

    with pytest.raises((TerminalValueError, PipelineStepFailed)):
        pipeline.run(events(), **destination_config.run_kwargs)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb", "snowflake", "postgres"]),
    ids=lambda x: x.name,
)
def test_dest_column_hint_timezone(destination_config: DestinationTestConfiguration) -> None:
    destination = destination_config.destination_type

    input_data = [
        {"event_id": 1, "event_tstamp": "2024-07-30T10:00:00.123+00:00"},
        {"event_id": 2, "event_tstamp": "2024-07-31T10:00:00.123456+02:00"},
        {"event_id": 3, "event_tstamp": "2024-07-29T10:00:00.123456"},
    ]

    output_values = [
        "2024-07-30T10:00:00.123000",
        "2024-07-31T08:00:00.123456",
        "2024-07-29T10:00:00.123456",
    ]

    output_map = {
        "postgres": {
            "tables": {
                "events_timezone_off": {
                    "timestamp_type": "timestamp without time zone",
                    "timestamp_values": output_values,
                },
                "events_timezone_on": {
                    "timestamp_type": "timestamp with time zone",
                    "timestamp_values": output_values,
                },
                "events_timezone_unset": {
                    "timestamp_type": "timestamp with time zone",
                    "timestamp_values": output_values,
                },
            },
            "query_data_type": (
                "SELECT data_type FROM information_schema.columns WHERE table_schema ='experiments'"
                " AND table_name = '%s' AND column_name = 'event_tstamp'"
            ),
        },
        "snowflake": {
            "tables": {
                "EVENTS_TIMEZONE_OFF": {
                    "timestamp_type": "TIMESTAMP_NTZ",
                    "timestamp_values": output_values,
                },
                "EVENTS_TIMEZONE_ON": {
                    "timestamp_type": "TIMESTAMP_TZ",
                    "timestamp_values": output_values,
                },
                "EVENTS_TIMEZONE_UNSET": {
                    "timestamp_type": "TIMESTAMP_TZ",
                    "timestamp_values": output_values,
                },
            },
            "query_data_type": (
                "SELECT data_type FROM information_schema.columns WHERE table_schema ='EXPERIMENTS'"
                " AND table_name = '%s' AND column_name = 'EVENT_TSTAMP'"
            ),
        },
        "duckdb": {
            "tables": {
                "events_timezone_off": {
                    "timestamp_type": "TIMESTAMP",
                    "timestamp_values": output_values,
                },
                "events_timezone_on": {
                    "timestamp_type": "TIMESTAMP WITH TIME ZONE",
                    "timestamp_values": output_values,
                },
                "events_timezone_unset": {
                    "timestamp_type": "TIMESTAMP WITH TIME ZONE",
                    "timestamp_values": output_values,
                },
            },
            "query_data_type": (
                "SELECT data_type FROM information_schema.columns WHERE table_schema ='experiments'"
                " AND table_name = '%s' AND column_name = 'event_tstamp'"
            ),
        },
    }

    # table: events_timezone_off
    @dlt.resource(
        columns={"event_tstamp": {"data_type": "timestamp", "timezone": False}},
        primary_key="event_id",
    )
    def events_timezone_off():
        yield input_data

    # table: events_timezone_on
    @dlt.resource(
        columns={"event_tstamp": {"data_type": "timestamp", "timezone": True}},
        primary_key="event_id",
    )
    def events_timezone_on():
        yield input_data

    # table: events_timezone_unset
    @dlt.resource(
        primary_key="event_id",
    )
    def events_timezone_unset():
        yield input_data

    destination_config.disable_compression = True
    pipeline = destination_config.setup_pipeline(
        f"{destination}_" + uniq_id(), dataset_name="experiments"
    )

    pipeline.run(
        [events_timezone_off(), events_timezone_on(), events_timezone_unset()],
        **destination_config.run_kwargs,
    )

    with pipeline.sql_client() as client:
        for t in output_map[destination]["tables"].keys():  # type: ignore
            # check data type
            column_info = client.execute_sql(output_map[destination]["query_data_type"] % t)
            assert column_info[0][0] == output_map[destination]["tables"][t]["timestamp_type"]  # type: ignore
            # check timestamp data
            rows = client.execute_sql(f"SELECT event_tstamp FROM {t} ORDER BY event_id")

            actual = [r[0].strftime("%Y-%m-%dT%H:%M:%S.%f") for r in rows]
            expected = output_map[destination]["tables"][t]["timestamp_values"]  # type: ignore
            assert actual == expected


def test_pipeline_with_named_destination() -> None:
    # 1. Destination type should be resolved from config (tests/.dlt/config.toml)
    pipeline = dlt.pipeline(destination="custom_name")
    assert pipeline.destination.destination_type == "dlt.destinations.duckdb"
    assert pipeline.destination.destination_name == "custom_name"

    @dlt.resource
    def test_data():
        yield [{"id": 1, "name": "test"}]

    info = pipeline.run(test_data())
    assert_load_info(info)

    # 2. Should raise UnknownDestinationModule when shorthand type resolution fails (no config)
    with pytest.raises(UnknownDestinationModule) as py_exc:
        dlt.pipeline(destination="another_custom_name")
    assert py_exc.value.named_dest_attempted is True
    assert not py_exc.value.destination_type
    assert "no destination type was configured" in str(py_exc.value)

    # 3. Should raise UnknownDestinationModule for invalid full module reference without falling back to shorthand type resolution
    with pytest.raises(UnknownDestinationModule) as py_exc:
        dlt.pipeline(destination="dlt.destinations.unknown")
    assert not py_exc.value.named_dest_attempted
    assert not py_exc.value.destination_type


def test_pipeline_with_named_destination_via_factory_initializer() -> None:
    # 1. Destination type should be resolved from config (tests/.dlt/config.toml) when not explicitly provided
    pipeline = dlt.pipeline(destination=dlt.destination("custom_name"))
    assert pipeline.destination.destination_type == "dlt.destinations.duckdb"
    assert pipeline.destination.destination_name == "custom_name"

    # 2. Explicit destination_type should override config settings
    pipeline = dlt.pipeline(
        destination=dlt.destination("custom_name", destination_type="filesystem")
    )
    assert pipeline.destination.destination_type == "dlt.destinations.filesystem"
    assert pipeline.destination.destination_name == "custom_name"

    # 3. Should fallback to shorthand type resolution when destination_type is not provided via explicit param or config
    pipeline = dlt.pipeline(destination=dlt.destination("duckdb"))
    assert pipeline.destination.destination_type == "dlt.destinations.duckdb"
    assert pipeline.destination.destination_name == "duckdb"

    # 4. Should raise UnknownDestinationModule when shorthand type resolution fails (no explicit param, no config)
    with pytest.raises(UnknownDestinationModule) as py_exc:
        dlt.pipeline(destination=dlt.destination("another_custom_name"))
    assert py_exc.value.named_dest_attempted is True
    assert not py_exc.value.destination_type

    # 5. Should resolve full module reference when destination_type is not provided via explicit param or config
    pipeline = dlt.pipeline(destination=dlt.destination("dlt.destinations.duckdb"))
    assert pipeline.destination.destination_type == "dlt.destinations.duckdb"
    assert pipeline.destination.destination_name == "duckdb"

    # 6. Should raise UnknownDestinationModule for invalid full module reference without falling back to shorthand type resolution
    with pytest.raises(UnknownDestinationModule) as py_exc:
        dlt.pipeline(destination=dlt.destination("dlt.destinations.unknown"))
    assert not py_exc.value.named_dest_attempted
    assert not py_exc.value.destination_type

    # 7. Should accept credentials
    pipeline = dlt.pipeline(
        destination=dlt.destination(
            "custom_name",
            credentials="duckdb:///random_duck_db.duckdb",
        )
    )
    assert pipeline.destination.destination_type == "dlt.destinations.duckdb"
    assert pipeline.destination.config_params["credentials"] == "duckdb:///random_duck_db.duckdb"

    @dlt.resource
    def test_data():
        yield [{"id": 1, "name": "test"}]

    info = pipeline.run(test_data())
    assert_load_info(info)
    assert (Path(TEST_STORAGE_ROOT) / "random_duck_db.duckdb").exists()

    # 8. Should also accept additional destination parameters (such as bucket_url)
    pipeline = dlt.pipeline(
        destination=dlt.destination(
            "custom_name",
            destination_type="filesystem",
            bucket_url=FILE_BUCKET,
        )
    )
    assert pipeline.destination.destination_type == "dlt.destinations.filesystem"
    assert pipeline.destination.config_params["bucket_url"] == FILE_BUCKET

    info = pipeline.run(test_data())
    assert_load_info(info)
    assert (Path(TEST_STORAGE_ROOT) / FILE_BUCKET / pipeline.dataset_name / "test_data").exists()

    # 9. Should automatically infer destination type as 'dlt.destinations.destination' (custom destination implementation),
    # if destination_callable is provided
    calls: List[Tuple[TDataItems, TTableSchema]] = []

    def local_sink_func(items: TDataItems, table: TTableSchema, my_val=dlt.config.value, /) -> None:
        nonlocal calls
        assert my_val == "something"
        calls.append((items, table))

    os.environ["DESTINATION__MY_VAL"] = "something"

    p = dlt.pipeline(
        "sink_test",
        destination=dlt.destination("custom_name", destination_callable=local_sink_func),
    )
    assert p.destination.destination_name == "custom_name"
    assert p.destination.destination_type == "dlt.destinations.destination"
    p.run([1, 2, 3], table_name="items")
    assert len(calls) == 1
