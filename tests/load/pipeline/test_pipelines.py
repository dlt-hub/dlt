from copy import deepcopy
import os
from typing import Any, Callable, Iterator, Tuple
import pytest
import itertools

import dlt

from dlt.common import json, sleep, logger
from dlt.common.destination.reference import DestinationReference
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import VERSION_TABLE_NAME
from dlt.common.typing import TDataItem
from dlt.common.utils import uniq_id
from dlt.extract.exceptions import ResourceNameMissing
from dlt.extract.source import DltSource
from dlt.pipeline.exceptions import CannotRestorePipelineException, PipelineConfigMissing, PipelineStepFailed

from tests.utils import ALL_DESTINATIONS, patch_home_dir, preserve_environ, autouse_test_storage, TEST_STORAGE_ROOT
# from tests.common.configuration.utils import environment
from tests.pipeline.utils import drop_dataset_from_env, assert_load_info
from tests.load.utils import delete_dataset
from tests.load.pipeline.utils import drop_pipeline, assert_query_data, assert_table, load_table_counts, select_data


@pytest.mark.parametrize('destination_name,use_single_dataset', itertools.product(ALL_DESTINATIONS, [True, False]))
def test_default_pipeline_names(destination_name: str, use_single_dataset: bool) -> None:
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
    assert p.default_schema_name in possible_names

    # this will create additional schema
    p.extract(data_fun(), schema=dlt.Schema("names"))
    assert p.default_schema_name in possible_names
    assert "names" in p.schemas.keys()

    with pytest.raises(PipelineConfigMissing):
        p.normalize()

    # mock the correct destinations (never do that in normal code)
    with p.managed_state():
        p.destination = DestinationReference.from_name(destination_name)
    p.normalize()
    info = p.load(dataset_name="d" + uniq_id())
    assert info.pipeline is p
    # two packages in two different schemas were loaded
    assert len(info.loads_ids) == 2

    # if loaded to single data, double the data was loaded to a single table because the schemas overlapped
    if use_single_dataset:
        assert_table(p, "data_fun", sorted(data * 2), info=info)
    else:
        # loaded to separate data sets
        assert_table(p, "data_fun", data, info=info)
        assert_table(p, "data_fun", data, schema_name="names", info=info)


def test_default_duckdb_dataset_name() -> None:
    # Check if dataset_name does not collide with pipeline_name
    data = ["a", "b", "c"]
    info = dlt.run(data, destination="duckdb", table_name="data")
    assert_table(info.pipeline, "data", data, info=info)


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_default_schema_name(destination_name: str) -> None:
    dataset_name = "dataset_" + uniq_id()
    data = ["a", "b", "c"]

    p = dlt.pipeline("test_default_schema_name", TEST_STORAGE_ROOT, destination=destination_name, dataset_name=dataset_name)
    p.extract(data, table_name="test", schema=Schema("default"))
    p.normalize()
    info = p.load()

    # try to restore pipeline
    r_p = dlt.attach("test_default_schema_name", TEST_STORAGE_ROOT)
    schema = r_p.default_schema
    assert schema.name == "default"

    assert_table(p, "test", data, info=info)


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_attach_pipeline(destination_name: str) -> None:
    # load data and then restore the pipeline and see if data is still there
    data = ["a", "b", "c"]

    @dlt.resource(name="data_table")
    def _data():
        for d in data:
            yield d

    info = dlt.run(_data(), destination=destination_name, dataset_name="specific" + uniq_id())

    with pytest.raises(CannotRestorePipelineException):
        dlt.attach("unknown")

    # restore default pipeline
    p = dlt.attach()
    # other instance
    assert info.pipeline is not p
    # same pipe
    old_p: dlt.Pipeline = info.pipeline
    assert p.pipeline_name == old_p.pipeline_name
    assert p.working_dir == old_p.working_dir
    # secret will be the same: if not explicitly provided it is derived from pipeline name
    assert p.pipeline_salt == old_p.pipeline_salt
    assert p.default_schema_name == p.default_schema_name

    # query data
    assert_table(p, "data_table", data, info=info)


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_skip_sync_schema_for_tables_without_columns(destination_name: str) -> None:

    # load data and then restore the pipeline and see if data is still there
    data = ["a", "b", "c"]

    @dlt.resource(name="data_table")
    def _data():
        for d in data:
            yield d

    p = dlt.pipeline(destination=destination_name, full_refresh=True)
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


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_run_full_refresh(destination_name: str) -> None:

    data = ["a", ["a", "b", "c"], ["a", "b", "c"]]

    def d():
        yield data

    @dlt.source(name="nested")
    def _data():
        return dlt.resource(d(), name="lists", write_disposition="replace")

    p = dlt.pipeline(full_refresh=True)
    info = p.run(_data(), destination=destination_name, dataset_name="iteration" + uniq_id())
    assert info.dataset_name == p.dataset_name
    assert info.dataset_name.endswith(p._pipeline_instance_id)
    # print(p.default_schema.to_pretty_yaml())
    # print(info)

    # restore the pipeline
    p = dlt.attach()
    # restored pipeline should be never put in full refresh
    assert p.full_refresh is False
    # assert parent table (easy), None first (db order)
    assert_table(p, "lists", [None, None, "a"], info=info)
    # child tables contain nested lists
    assert_table(p, "lists__value", sorted(data[1] + data[2]))


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_evolve_schema(destination_name: str) -> None:
    dataset_name = "d" + uniq_id()
    row = {
        "id": "level0",
        "f": [{
            "id": "level1",
            "l": ["a", "b", "c"],
            "v": 120,
            "o": [{"a": 1}, {"a": 2}]
        }]
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

        @dlt.resource(columns={"id": {"name": "id", "nullable": False, "data_type": "text", "unique": True, "sort": True}})
        def simple_rows():
            for no in range(top_elements):
                # yield deferred items resolved in threads
                yield get_item(no)

        @dlt.resource(table_name="simple_rows", columns={"new_column": {"nullable": True, "data_type": "decimal"}})
        def extended_rows():
            for no in range(top_elements):
                # yield deferred items resolved in threads
                yield get_item(no+100)

        return simple_rows(), extended_rows(), dlt.resource(["a", "b", "c"], name="simple")

    import_schema_path = os.path.join(TEST_STORAGE_ROOT, "schemas", "import")
    export_schema_path = os.path.join(TEST_STORAGE_ROOT, "schemas", "export")
    p = dlt.pipeline(destination=destination_name, import_schema_path=import_schema_path, export_schema_path=export_schema_path)
    p.extract(source(10).with_resources("simple_rows"))
    # print(p.default_schema.to_pretty_yaml())
    p.normalize()
    info = p.load(dataset_name=dataset_name)
    # test __str__
    print(info)
    # print(p.default_schema.to_pretty_yaml())
    schema = p.default_schema
    assert "simple_rows" in schema.tables
    assert "simple" not in schema.tables
    assert "new_column" not in schema.get_table("simple_rows")["columns"]

    # lets violate unique constraint on postgres, redshift and BQ ignore unique indexes
    if destination_name == "postgres":
        assert p.dataset_name == dataset_name
        err_info = p.run(source(1).with_resources("simple_rows"))
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
    assert "simple_rows" in schema.tables
    assert "simple" in schema.tables
    assert "new_column" in schema.get_table("simple_rows")["columns"]
    assert "extended_rows" not in schema.tables

    # TODO: test export and import schema
    # test data
    id_data = sorted(["level" + str(n) for n in range(10)] + ["level" + str(n) for n in range(100, 110)])
    assert_query_data(p, "SELECT * FROM simple_rows ORDER BY id", id_data)


# def test_evolve_schema_conflict() -> None:
#     pass


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_source_max_nesting(destination_name: str) -> None:

    complex_part = {
                    "l": [1, 2, 3],
                    "c": {
                        "a": 1,
                        "b": 12.3
                    }
                }

    @dlt.source(name="complex", max_table_nesting=0)
    def complex_data():
        return dlt.resource([
            {
                "idx": 1,
                "cn": complex_part
            }
        ], name="complex_cn")
    info = dlt.run(complex_data(), destination=destination_name, dataset_name="ds_" + uniq_id())
    print(info)
    rows = select_data(dlt.pipeline(), "SELECT cn FROM complex_cn")
    assert len(rows) == 1
    cn_val = rows[0][0]
    if isinstance(cn_val, str):
        cn_val = json.loads(cn_val)
    assert cn_val == complex_part


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_dataset_name_change(destination_name: str) -> None:
    ds_1_name = "iteration" + uniq_id()
    ds_2_name = "iteration" + uniq_id()
    ds_3_name = "iteration" + uniq_id()
    p, s = simple_nested_pipeline(destination_name, dataset_name=ds_1_name, full_refresh=False)
    try:
        info = p.run(s())
        assert_load_info(info)
        assert info.dataset_name == ds_1_name
        ds_1_counts = load_table_counts(p, "lists", "lists__value")
        # run to another dataset
        info = p.run(s(), dataset_name=ds_2_name)
        assert_load_info(info)
        assert info.dataset_name == ds_2_name
        ds_2_counts = load_table_counts(p, "lists", "lists__value")
        assert ds_1_counts == ds_2_counts
        # set name and run to another dataset
        p.dataset_name = ds_3_name
        info = p.run(s())
        assert_load_info(info)
        assert info.dataset_name == ds_3_name
        ds_3_counts = load_table_counts(p, "lists", "lists__value")
        assert ds_1_counts == ds_3_counts

    finally:
        # we have to clean dataset ourselves
        with p.sql_client() as client:
            delete_dataset(client, ds_1_name)
            delete_dataset(client, ds_2_name)
            # delete_dataset(client, ds_3_name)  # will be deleted by the fixture


@pytest.mark.parametrize('destination_name', ["postgres"])
def test_pipeline_explicit_destination_credentials(destination_name: str) -> None:

    # explicit credentials resolved
    p = dlt.pipeline(destination="postgres", credentials="postgresql://loader:loader@localhost:5432/dlt_data")
    c = p._get_destination_client(Schema("s"), p._get_destination_client_initial_config())
    assert c.config.credentials.host == "localhost"

    # explicit credentials resolved ignoring the config providers
    os.environ["CREDENTIALS__HOST"] = "HOST"
    p = dlt.pipeline(destination="postgres", credentials="postgresql://loader:loader@localhost:5432/dlt_data")
    c = p._get_destination_client(Schema("s"), p._get_destination_client_initial_config())
    assert c.config.credentials.host == "localhost"

    # explicit partial credentials will use config providers
    os.environ["CREDENTIALS__USERNAME"] = "UN"
    p = dlt.pipeline(destination="postgres", credentials="postgresql://localhost:5432/dlt_data")
    c = p._get_destination_client(Schema("s"), p._get_destination_client_initial_config())
    assert c.config.credentials.username == "UN"
    # host is also overridden
    assert c.config.credentials.host == "HOST"

    # instance of credentials will be simply passed
    # c = RedshiftCredentials("postgresql://loader:loader@localhost/dlt_data")
    # assert c.is_resolved()
    # p = dlt.pipeline(destination="postgres", credentials=c)
    # inner_c = p._get_destination_client(Schema("s"), p._get_destination_client_initial_config())
    # assert inner_c is c


def simple_nested_pipeline(destination_name: str, dataset_name: str, full_refresh: bool) -> Tuple[dlt.Pipeline, Callable[[], DltSource]]:
    data = ["a", ["a", "b", "c"], ["a", "b", "c"]]

    def d():
        yield data

    @dlt.source(name="nested")
    def _data():
        return dlt.resource(d(), name="lists", write_disposition="append")

    p = dlt.pipeline(full_refresh=full_refresh, destination=destination_name, dataset_name=dataset_name)
    return p, _data
