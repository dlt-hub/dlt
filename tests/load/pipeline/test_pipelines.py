from copy import deepcopy
import gzip
import os
from typing import Any, Callable, Iterator, Tuple, List, cast
import pytest

import dlt

from dlt.common.pipeline import SupportsPipeline
from dlt.common import json, sleep
from dlt.common.destination import Destination
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import VERSION_TABLE_NAME
from dlt.common.typing import TDataItem
from dlt.common.utils import uniq_id
from dlt.extract.exceptions import ResourceNameMissing
from dlt.extract import DltSource
from dlt.pipeline.exceptions import (
    CannotRestorePipelineException,
    PipelineConfigMissing,
    PipelineStepFailed,
)
from dlt.common.schema.exceptions import CannotCoerceColumnException
from dlt.common.exceptions import DestinationHasFailedJobs

from tests.utils import TEST_STORAGE_ROOT, preserve_environ
from tests.pipeline.utils import assert_load_info
from tests.load.utils import (
    TABLE_ROW_ALL_DATA_TYPES,
    TABLE_UPDATE_COLUMNS_SCHEMA,
    assert_all_data_types_row,
    delete_dataset,
)
from tests.load.pipeline.utils import (
    drop_active_pipeline_data,
    assert_query_data,
    assert_table,
    load_table_counts,
    select_data,
)
from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, all_buckets_filesystem_configs=True),
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
    assert p.dataset_name in possible_dataset_names
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
    p.extract(data_fun)
    # _pipeline suffix removed when creating default schema name
    assert p.default_schema_name in ["dlt_pytest", "dlt"]

    # this will create additional schema
    p.extract(data_fun(), schema=dlt.Schema("names"))
    assert p.default_schema_name in ["dlt_pytest", "dlt"]
    assert "names" in p.schemas.keys()

    with pytest.raises(PipelineConfigMissing):
        p.normalize()

    # mock the correct destinations (never do that in normal code)
    with p.managed_state():
        p._set_destinations(
            destination=Destination.from_reference(destination_config.destination),
            staging=(
                Destination.from_reference(destination_config.staging)
                if destination_config.staging
                else None
            ),
        )
        # does not reset the dataset name
        assert p.dataset_name in possible_dataset_names
        # never do that in production code
        p.dataset_name = None
        # set no dataset name -> if destination does not support it we revert to default
        p._set_dataset_name(None)
        assert p.dataset_name in possible_dataset_names
    # the last package contains just the state (we added a new schema)
    last_load_id = p.list_extracted_load_packages()[-1]
    state_package = p.get_load_package_info(last_load_id)
    assert len(state_package.jobs["new_jobs"]) == 1
    assert state_package.schema_name == p.default_schema_name
    p.normalize()
    info = p.load(dataset_name="d" + uniq_id())
    print(p.dataset_name)
    assert info.pipeline is p
    # two packages in two different schemas were loaded
    assert len(info.loads_ids) == 3

    # if loaded to single data, double the data was loaded to a single table because the schemas overlapped
    if use_single_dataset:
        assert_table(p, "data_fun", sorted(data * 2), info=info)
    else:
        # loaded to separate data sets
        assert_table(p, "data_fun", data, info=info)
        assert_table(p, "data_fun", data, schema_name="names", info=info)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, all_buckets_filesystem_configs=True),
    ids=lambda x: x.name,
)
def test_default_schema_name(destination_config: DestinationTestConfiguration) -> None:
    destination_config.setup()
    dataset_name = "dataset_" + uniq_id()
    data = ["a", "b", "c"]

    p = dlt.pipeline(
        "test_default_schema_name",
        TEST_STORAGE_ROOT,
        destination=destination_config.destination,
        staging=destination_config.staging,
        dataset_name=dataset_name,
    )
    p.extract(data, table_name="test", schema=Schema("default"))
    p.normalize()
    info = p.load()

    # try to restore pipeline
    r_p = dlt.attach("test_default_schema_name", TEST_STORAGE_ROOT)
    schema = r_p.default_schema
    assert schema.name == "default"

    assert_table(p, "test", data, info=info)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, all_buckets_filesystem_configs=True),
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
        destination=destination_config.destination,
        staging=destination_config.staging,
        dataset_name="specific" + uniq_id(),
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
    assert_table(p, "data_table", data, info=info)


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

    p = destination_config.setup_pipeline("test_skip_sync_schema_for_tables", full_refresh=True)
    p.extract(_data)
    schema = p.default_schema
    assert "data_table" in schema.tables
    assert schema.tables["data_table"]["columns"] == {}

    p.sync_schema()

    with p._sql_job_client(schema) as job_client:
        # there's some data at all
        exists, _ = job_client.get_storage_table(VERSION_TABLE_NAME)
        assert exists is True

        # such tables are not created but silently ignored
        exists, _ = job_client.get_storage_table("data_table")
        assert not exists


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, all_buckets_filesystem_configs=True),
    ids=lambda x: x.name,
)
def test_run_full_refresh(destination_config: DestinationTestConfiguration) -> None:
    data = ["a", ["a", "b", "c"], ["a", "b", "c"]]
    destination_config.setup()

    def d():
        yield data

    @dlt.source(name="nested")
    def _data():
        return dlt.resource(d(), name="lists", write_disposition="replace")

    p = dlt.pipeline(full_refresh=True)
    info = p.run(
        _data(),
        destination=destination_config.destination,
        staging=destination_config.staging,
        dataset_name="iteration" + uniq_id(),
    )
    assert info.dataset_name == p.dataset_name
    assert info.dataset_name.endswith(p._pipeline_instance_id)
    # print(p.default_schema.to_pretty_yaml())
    # print(info)

    # restore the pipeline
    p = dlt.attach()
    # restored pipeline should be never put in full refresh
    assert p.full_refresh is False
    # assert parent table (easy), None First (db order)
    assert_table(p, "lists", [None, None, "a"], info=info)
    # child tables contain nested lists
    data_list = cast(List[str], data[1]) + cast(List[str], data[2])
    assert_table(p, "lists__value", sorted(data_list))


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
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

    p.extract(source(10).with_resources("simple_rows"))
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
    if destination_config.destination == "postgres":
        assert p.dataset_name == dataset_name
        err_info = p.run(source(1).with_resources("simple_rows"))
        version_history.append(p.default_schema.stored_version_hash)
        # print(err_info)
        # we have failed jobs
        assert len(err_info.load_packages[0].jobs["failed_jobs"]) == 1

    # update schema
    # - new column in "simple_rows" table
    # - new "simple" table
    info_ext = dlt.run(source(10).with_resources("extended_rows", "simple"))
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
    assert_query_data(p, f"SELECT * FROM {simple_rows_table} ORDER BY id", id_data)
    assert_query_data(
        p,
        f"SELECT schema_version_hash FROM {dlt_loads_table} ORDER BY inserted_at",
        version_history,
    )


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, all_buckets_filesystem_configs=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("disable_compression", [True, False])
def test_pipeline_data_writer_compression(
    disable_compression: bool, destination_config: DestinationTestConfiguration
) -> None:
    # Ensure pipeline works without compression
    data = ["a", "b", "c"]
    dataset_name = "compression_data_" + uniq_id()
    dlt.config["data_writer"] = {
        "disable_compression": disable_compression
    }  # not sure how else to set this
    p = destination_config.setup_pipeline("compression_test", dataset_name=dataset_name)
    p.extract(dlt.resource(data, name="data"))
    s = p._get_normalize_storage()
    # check that files are not compressed if compression is disabled
    if disable_compression:
        for f in s.list_files_to_normalize_sorted():
            with pytest.raises(gzip.BadGzipFile):
                gzip.open(s.extracted_packages.storage.make_full_path(f), "rb").read()
    p.normalize()
    info = p.load()
    assert_table(p, "data", data, info=info)


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
def test_source_max_nesting(destination_config: DestinationTestConfiguration) -> None:
    destination_config.setup()

    complex_part = {"l": [1, 2, 3], "c": {"a": 1, "b": 12.3}}

    @dlt.source(name="complex", max_table_nesting=0)
    def complex_data():
        return dlt.resource([{"idx": 1, "cn": complex_part}], name="complex_cn")

    info = dlt.run(
        complex_data(),
        destination=destination_config.destination,
        staging=destination_config.staging,
        dataset_name="ds_" + uniq_id(),
    )
    print(info)
    with dlt.pipeline().sql_client() as client:
        complex_cn_table = client.make_qualified_table_name("complex_cn")
    rows = select_data(dlt.pipeline(), f"SELECT cn FROM {complex_cn_table}")
    assert len(rows) == 1
    cn_val = rows[0][0]
    if isinstance(cn_val, str):
        cn_val = json.loads(cn_val)
    assert cn_val == complex_part


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
def test_dataset_name_change(destination_config: DestinationTestConfiguration) -> None:
    destination_config.setup()
    # standard name
    ds_1_name = "iteration" + uniq_id()
    # will go to snake case
    ds_2_name = "IteRation" + uniq_id()
    # illegal name that will be later normalized
    ds_3_name = "1it/era 👍 tion__" + uniq_id()
    p, s = simple_nested_pipeline(destination_config, dataset_name=ds_1_name, full_refresh=False)
    try:
        info = p.run(s())
        assert_load_info(info)
        assert info.dataset_name == ds_1_name
        ds_1_counts = load_table_counts(p, "lists", "lists__value")
        # run to another dataset
        info = p.run(s(), dataset_name=ds_2_name)
        assert_load_info(info)
        assert info.dataset_name.startswith("ite_ration")
        # save normalized dataset name to delete correctly later
        ds_2_name = info.dataset_name
        ds_2_counts = load_table_counts(p, "lists", "lists__value")
        assert ds_1_counts == ds_2_counts
        # set name and run to another dataset
        p.dataset_name = ds_3_name
        info = p.run(s())
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


# do not remove - it allows us to filter tests by destination
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres"]),
    ids=lambda x: x.name,
)
def test_pipeline_explicit_destination_credentials(
    destination_config: DestinationTestConfiguration,
) -> None:
    # explicit credentials resolved
    p = dlt.pipeline(
        destination=Destination.from_reference("postgres", destination_name="mydest"),
        credentials="postgresql://loader:loader@localhost:7777/dlt_data",
    )
    c = p._get_destination_clients(Schema("s"), p._get_destination_client_initial_config())[0]
    assert c.config.credentials.port == 7777  # type: ignore[attr-defined]

    # TODO: may want to clear the env completely and ignore/mock config files somehow to avoid side effects
    # explicit credentials resolved ignoring the config providers
    os.environ["DESTINATION__MYDEST__CREDENTIALS__HOST"] = "HOST"
    p = dlt.pipeline(
        destination=Destination.from_reference("postgres", destination_name="mydest"),
        credentials="postgresql://loader:loader@localhost:5432/dlt_data",
    )
    c = p._get_destination_clients(Schema("s"), p._get_destination_client_initial_config())[0]
    assert c.config.credentials.host == "localhost"  # type: ignore[attr-defined]

    # explicit partial credentials will use config providers
    os.environ["DESTINATION__MYDEST__CREDENTIALS__USERNAME"] = "UN"
    os.environ["DESTINATION__MYDEST__CREDENTIALS__PASSWORD"] = "PW"
    p = dlt.pipeline(
        destination=Destination.from_reference("postgres", destination_name="mydest"),
        credentials="postgresql://localhost:5432/dlt_data",
    )
    c = p._get_destination_clients(Schema("s"), p._get_destination_client_initial_config())[0]
    assert c.config.credentials.username == "UN"  # type: ignore[attr-defined]
    # host is also overridden
    assert c.config.credentials.host == "HOST"  # type: ignore[attr-defined]

    # instance of credentials will be simply passed
    # c = RedshiftCredentials("postgresql://loader:loader@localhost/dlt_data")
    # assert c.is_resolved()
    # p = dlt.pipeline(destination="postgres", credentials=c)
    # inner_c = p._get_destination_clients(Schema("s"), p._get_destination_client_initial_config())[0]
    # assert inner_c is c


# do not remove - it allows us to filter tests by destination
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres"]),
    ids=lambda x: x.name,
)
def test_pipeline_with_sources_sharing_schema(
    destination_config: DestinationTestConfiguration,
) -> None:
    schema = Schema("shared")

    @dlt.source(schema=schema, max_table_nesting=1)
    def source_1():
        @dlt.resource(primary_key="user_id")
        def gen1():
            dlt.current.source_state()["source_1"] = True
            dlt.current.resource_state()["source_1"] = True
            yield {"id": "Y", "user_id": "user_y"}

        @dlt.resource(columns={"col": {"data_type": "bigint"}})
        def conflict():
            yield "conflict"

        return gen1, conflict

    @dlt.source(schema=schema, max_table_nesting=2)
    def source_2():
        @dlt.resource(primary_key="id")
        def gen1():
            dlt.current.source_state()["source_2"] = True
            dlt.current.resource_state()["source_2"] = True
            yield {"id": "X", "user_id": "user_X"}

        def gen2():
            yield from "CDE"

        @dlt.resource(columns={"col": {"data_type": "bool"}}, selected=False)
        def conflict():
            yield "conflict"

        return gen2, gen1, conflict

    # all selected tables with hints should be there
    discover_1 = source_1().discover_schema()
    assert "gen1" in discover_1.tables
    assert discover_1.tables["gen1"]["columns"]["user_id"]["primary_key"] is True
    assert "data_type" not in discover_1.tables["gen1"]["columns"]["user_id"]
    assert "conflict" in discover_1.tables
    assert discover_1.tables["conflict"]["columns"]["col"]["data_type"] == "bigint"

    discover_2 = source_2().discover_schema()
    assert "gen1" in discover_2.tables
    assert "gen2" in discover_2.tables
    # conflict deselected
    assert "conflict" not in discover_2.tables

    p = dlt.pipeline(pipeline_name="multi", destination="duckdb", full_refresh=True)
    p.extract([source_1(), source_2()])
    default_schema = p.default_schema
    gen1_table = default_schema.tables["gen1"]
    assert "user_id" in gen1_table["columns"]
    assert "id" in gen1_table["columns"]
    assert "conflict" in default_schema.tables
    assert "gen2" in default_schema.tables
    p.normalize()
    assert "gen2" in default_schema.tables
    p.load()
    table_names = [t["name"] for t in default_schema.data_tables()]
    counts = load_table_counts(p, *table_names)
    assert counts == {"gen1": 2, "gen2": 3, "conflict": 1}
    # both sources share the same state
    assert p.state["sources"] == {
        "shared": {
            "source_1": True,
            "resources": {"gen1": {"source_1": True, "source_2": True}},
            "source_2": True,
        }
    }
    drop_active_pipeline_data()

    # same pipeline but enable conflict
    p = dlt.pipeline(pipeline_name="multi", destination="duckdb", full_refresh=True)
    with pytest.raises(PipelineStepFailed) as py_ex:
        p.extract([source_1(), source_2().with_resources("conflict")])
    assert isinstance(py_ex.value.__context__, CannotCoerceColumnException)


# do not remove - it allows us to filter tests by destination
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres"]),
    ids=lambda x: x.name,
)
def test_many_pipelines_single_dataset(destination_config: DestinationTestConfiguration) -> None:
    schema = Schema("shared")

    @dlt.source(schema=schema, max_table_nesting=1)
    def source_1():
        @dlt.resource(primary_key="user_id")
        def gen1():
            dlt.current.source_state()["source_1"] = True
            dlt.current.resource_state()["source_1"] = True
            yield {"id": "Y", "user_id": "user_y"}

        return gen1

    @dlt.source(schema=schema, max_table_nesting=2)
    def source_2():
        @dlt.resource(primary_key="id")
        def gen1():
            dlt.current.source_state()["source_2"] = True
            dlt.current.resource_state()["source_2"] = True
            yield {"id": "X", "user_id": "user_X"}

        def gen2():
            yield from "CDE"

        return gen2, gen1

    # load source_1 to common dataset
    p = dlt.pipeline(
        pipeline_name="source_1_pipeline", destination="duckdb", dataset_name="shared_dataset"
    )
    p.run(source_1(), credentials="duckdb:///_storage/test_quack.duckdb")
    counts = load_table_counts(p, *p.default_schema.tables.keys())
    assert counts.items() >= {"gen1": 1, "_dlt_pipeline_state": 1, "_dlt_loads": 1}.items()
    p._wipe_working_folder()
    p.deactivate()

    p = dlt.pipeline(
        pipeline_name="source_2_pipeline", destination="duckdb", dataset_name="shared_dataset"
    )
    p.run(source_2(), credentials="duckdb:///_storage/test_quack.duckdb")
    # table_names = [t["name"] for t in p.default_schema.data_tables()]
    counts = load_table_counts(p, *p.default_schema.tables.keys())
    # gen1: one record comes from source_1, 1 record from source_2
    assert counts.items() >= {"gen1": 2, "_dlt_pipeline_state": 2, "_dlt_loads": 2}.items()
    # assert counts == {'gen1': 2, 'gen2': 3}
    p._wipe_working_folder()
    p.deactivate()

    # restore from destination, check state
    p = dlt.pipeline(
        pipeline_name="source_1_pipeline",
        destination="duckdb",
        dataset_name="shared_dataset",
        credentials="duckdb:///_storage/test_quack.duckdb",
    )
    p.sync_destination()
    # we have our separate state
    assert p.state["sources"]["shared"] == {
        "source_1": True,
        "resources": {"gen1": {"source_1": True}},
    }
    # but the schema was common so we have the earliest one
    assert "gen2" in p.default_schema.tables
    p._wipe_working_folder()
    p.deactivate()

    p = dlt.pipeline(
        pipeline_name="source_2_pipeline",
        destination="duckdb",
        dataset_name="shared_dataset",
        credentials="duckdb:///_storage/test_quack.duckdb",
    )
    p.sync_destination()
    # we have our separate state
    assert p.state["sources"]["shared"] == {
        "source_2": True,
        "resources": {"gen1": {"source_2": True}},
    }


# do not remove - it allows us to filter tests by destination
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["snowflake"]),
    ids=lambda x: x.name,
)
def test_snowflake_custom_stage(destination_config: DestinationTestConfiguration) -> None:
    """Using custom stage name instead of the table stage"""
    os.environ["DESTINATION__SNOWFLAKE__STAGE_NAME"] = "my_non_existing_stage"
    pipeline, data = simple_nested_pipeline(destination_config, f"custom_stage_{uniq_id()}", False)
    info = pipeline.run(data())
    with pytest.raises(DestinationHasFailedJobs) as f_jobs:
        info.raise_on_failed_jobs()
    assert "MY_NON_EXISTING_STAGE" in f_jobs.value.failed_jobs[0].failed_message

    drop_active_pipeline_data()

    # NOTE: this stage must be created in DLT_DATA database for this test to pass!
    # CREATE STAGE MY_CUSTOM_LOCAL_STAGE;
    # GRANT READ, WRITE ON STAGE DLT_DATA.PUBLIC.MY_CUSTOM_LOCAL_STAGE TO ROLE DLT_LOADER_ROLE;
    stage_name = "PUBLIC.MY_CUSTOM_LOCAL_STAGE"
    os.environ["DESTINATION__SNOWFLAKE__STAGE_NAME"] = stage_name
    pipeline, data = simple_nested_pipeline(destination_config, f"custom_stage_{uniq_id()}", False)
    info = pipeline.run(data())
    assert_load_info(info)

    load_id = info.loads_ids[0]

    # Get a list of the staged files and verify correct number of files in the "load_id" dir
    with pipeline.sql_client() as client:
        staged_files = client.execute_sql(f'LIST @{stage_name}/"{load_id}"')
        assert len(staged_files) == 3
        # check data of one table to ensure copy was done successfully
        tbl_name = client.make_qualified_table_name("lists")
        assert_query_data(pipeline, f"SELECT value FROM {tbl_name}", ["a", None, None])


# do not remove - it allows us to filter tests by destination
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["snowflake"]),
    ids=lambda x: x.name,
)
def test_snowflake_delete_file_after_copy(destination_config: DestinationTestConfiguration) -> None:
    """Using keep_staged_files = false option to remove staged files after copy"""
    os.environ["DESTINATION__SNOWFLAKE__KEEP_STAGED_FILES"] = "FALSE"

    pipeline, data = simple_nested_pipeline(
        destination_config, f"delete_staged_files_{uniq_id()}", False
    )

    info = pipeline.run(data())
    assert_load_info(info)

    load_id = info.loads_ids[0]

    with pipeline.sql_client() as client:
        # no files are left in table stage
        stage_name = client.make_qualified_table_name("%lists")
        staged_files = client.execute_sql(f'LIST @{stage_name}/"{load_id}"')
        assert len(staged_files) == 0

        # ensure copy was done
        tbl_name = client.make_qualified_table_name("lists")
        assert_query_data(pipeline, f"SELECT value FROM {tbl_name}", ["a", None, None])


# do not remove - it allows us to filter tests by destination
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, all_staging_configs=True, file_format="parquet"),
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

    data_types = deepcopy(TABLE_ROW_ALL_DATA_TYPES)
    column_schemas = deepcopy(TABLE_UPDATE_COLUMNS_SCHEMA)

    # parquet on bigquery does not support JSON but we still want to run the test
    if destination_config.destination == "bigquery":
        column_schemas["col9_null"]["data_type"] = column_schemas["col9"]["data_type"] = "text"

    # duckdb 0.9.1 does not support TIME other than 6
    if destination_config.destination in ["duckdb", "motherduck"]:
        column_schemas["col11_precision"]["precision"] = 0

    # drop TIME from databases not supporting it via parquet
    if destination_config.destination in ["redshift", "athena"]:
        data_types.pop("col11")
        data_types.pop("col11_null")
        data_types.pop("col11_precision")
        column_schemas.pop("col11")
        column_schemas.pop("col11_null")
        column_schemas.pop("col11_precision")

    if destination_config.destination == "redshift":
        data_types.pop("col7_precision")
        column_schemas.pop("col7_precision")

    # apply the exact columns definitions so we process complex and wei types correctly!
    @dlt.resource(table_name="data_types", write_disposition="merge", columns=column_schemas)
    def my_resource():
        nonlocal data_types
        yield [data_types] * 10

    @dlt.source(max_table_nesting=0)
    def some_source():
        return [some_data(), other_data(), my_resource()]

    info = pipeline.run(some_source(), loader_file_format="parquet")
    package_info = pipeline.get_load_package_info(info.loads_ids[0])
    # print(package_info.asstr(verbosity=2))
    assert package_info.state == "loaded"
    # all three jobs succeeded
    assert len(package_info.jobs["failed_jobs"]) == 0
    # 3 tables + 1 state + 4 reference jobs if staging
    expected_completed_jobs = 4 + 4 if destination_config.staging else 4
    # add sql merge job
    if destination_config.supports_merge:
        expected_completed_jobs += 1
    # add iceberg copy jobs
    if destination_config.force_iceberg:
        expected_completed_jobs += 4
    assert len(package_info.jobs["completed_jobs"]) == expected_completed_jobs

    with pipeline.sql_client() as sql_client:
        assert [
            row[0] for row in sql_client.execute_sql("SELECT * FROM other_data ORDER BY 1")
        ] == [1, 2, 3, 4, 5]
        assert [row[0] for row in sql_client.execute_sql("SELECT * FROM some_data ORDER BY 1")] == [
            1,
            2,
            3,
        ]
        db_rows = sql_client.execute_sql("SELECT * FROM data_types")
        assert len(db_rows) == 10
        db_row = list(db_rows[0])
        # "snowflake" and "bigquery" do not parse JSON form parquet string so double parse
        assert_all_data_types_row(
            db_row,
            schema=column_schemas,
            parse_complex_strings=destination_config.destination
            in ["snowflake", "bigquery", "redshift"],
            timestamp_precision=3 if destination_config.destination == "athena" else 6,
        )


def simple_nested_pipeline(
    destination_config: DestinationTestConfiguration, dataset_name: str, full_refresh: bool
) -> Tuple[dlt.Pipeline, Callable[[], DltSource]]:
    data = ["a", ["a", "b", "c"], ["a", "b", "c"]]

    def d():
        yield data

    @dlt.source(name="nested")
    def _data():
        return dlt.resource(d(), name="lists", write_disposition="append")

    p = dlt.pipeline(
        pipeline_name=f"pipeline_{dataset_name}",
        full_refresh=full_refresh,
        destination=destination_config.destination,
        staging=destination_config.staging,
        dataset_name=dataset_name,
    )
    return p, _data
