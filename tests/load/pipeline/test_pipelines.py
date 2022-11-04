from copy import deepcopy
import os
from unittest.mock import patch
from typing import Any, Iterator, List
import pytest
from os import environ

import dlt

from dlt.common.configuration.container import Container
from dlt.common.destination import DestinationReference
from dlt.common.pipeline import LoadInfo, PipelineContext
from dlt.common.schema.schema import Schema
from dlt.common.time import sleep
from dlt.common.typing import TDataItem
from dlt.common.utils import uniq_id
from dlt.extract.exceptions import ResourceNameMissing
from dlt.pipeline.exceptions import CannotRestorePipelineException, PipelineConfigMissing

from tests.utils import preserve_environ, autouse_test_storage, TEST_STORAGE_ROOT
# pipeline_module.get_default_working_dir = lambda s: os.path.join(TEST_STORAGE_ROOT, ".dlt", "pipelines")
from tests.common.configuration.utils import environment

ALL_CLIENT_TYPES = ["bigquery", "redshift", "postgres"]


@pytest.fixture(autouse=True)
def drop_dataset_from_env() -> None:
    if "DATASET_NAME" in environ:
        del environ["DATASET_NAME"]


@pytest.fixture(autouse=True)
def patch_working_dir() -> None:
    with patch("dlt.common.pipeline._get_home_dir") as _get_home_dir:
        _get_home_dir.return_value = TEST_STORAGE_ROOT
        yield


@pytest.fixture(autouse=True)
def drop_pipeline() -> Iterator[None]:
    yield
    # take existing pipeline
    p = dlt.pipeline()
    # take all schemas and if destination was set
    if p.destination:
        for schema_name in p.schemas:
            # for each schema, drop the dataset
            with p.sql_client(schema_name) as c:
                try:
                    c.drop_dataset()
                    # print("dropped")
                except Exception as exc:
                    print(exc)

    p._wipe_working_folder()
    # deactivate context
    Container()[PipelineContext].deactivate()


@pytest.mark.parametrize('destination_name', ALL_CLIENT_TYPES)
def test_default_pipeline_names(destination_name: str) -> None:
    p = dlt.pipeline()
    # this is a name of executing test harness or blank pipeline on windows
    assert p.pipeline_name in ["dlt_pytest", "dlt_pipeline"]
    assert p.working_dir == os.path.join(TEST_STORAGE_ROOT, ".dlt", "pipelines")
    assert p.dataset_name is None
    assert p._get_dataset_name() == "dlt_pytest"
    assert p.destination is None
    assert p.default_schema_name is None

    data = ["a", "b", "c"]
    with pytest.raises(ResourceNameMissing):
        p.extract(data)

    def data_fun() -> Iterator[Any]:
        yield data

    # this will create default schema
    p.extract(data_fun)
    assert p.default_schema_name == "dlt_pytest"

    # this will create additional schema
    p.extract(data_fun(), schema=dlt.Schema("names"))
    assert p.default_schema_name == "dlt_pytest"
    assert "names" in p.schemas.keys()

    with pytest.raises(PipelineConfigMissing):
        p.normalize()

    # mock the correct destinations (never to that in normal code)
    with p._managed_state():
        p.destination = DestinationReference.from_name(destination_name)
    p.normalize()
    info = p.load(dataset_name="d" + uniq_id())
    assert info.pipeline is p
    # two packages in two different schemas were loaded
    assert len(info.loads_ids) == 2

    assert_table(p, "data_fun", data, info=info)
    assert_table(p, "data_fun", data, schema_name="names", info=info)


@pytest.mark.parametrize('destination_name', ALL_CLIENT_TYPES)
def test_default_schema_name(destination_name: str) -> None:
    dataset_name = "dataset_" + uniq_id()
    data = ["a", "b", "c"]

    p = dlt.pipeline("test_default_schema_name", TEST_STORAGE_ROOT, destination=destination_name, dataset_name=dataset_name)
    p.extract(data, table_name="test", schema=Schema("default"))
    p.normalize()
    info = p.load()

    # try to restore pipeline
    r_p = dlt.restore("test_default_schema_name", TEST_STORAGE_ROOT)
    schema = r_p.default_schema
    assert schema.name == "default"

    assert_table(p, "test", data, info=info)


@pytest.mark.parametrize('destination_name', ALL_CLIENT_TYPES)
def test_restore_pipeline(destination_name: str) -> None:
    # load data and then restore the pipeline and see if data is still there
    data = ["a", "b", "c"]

    @dlt.resource(name="data_table")
    def _data():
        for d in data:
            yield d

    info = dlt.run(_data(), destination=destination_name, dataset_name="specific" + uniq_id())

    with pytest.raises(CannotRestorePipelineException):
        dlt.restore("unknown")

    # restore default pipeline
    p = dlt.restore()
    # other instance
    assert info.pipeline is not p
    # same pipe
    old_p: dlt.Pipeline = info.pipeline
    assert p.pipeline_name == old_p.pipeline_name
    assert p.pipeline_root == old_p.pipeline_root
    # secret will be different
    # TODO: make default secret deterministic
    assert p.pipeline_secret != old_p.pipeline_secret
    assert p.default_schema_name == p.default_schema_name

    # query data
    assert_table(p, "data_table", data, info=info)


@pytest.mark.parametrize('destination_name', ALL_CLIENT_TYPES)
def test_run_with_wipe(destination_name: str) -> None:

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
    print(info)

    # restore the pipeline
    p = dlt.restore()
    # restored pipeline should be never put in full refresh
    assert p.full_refresh is False
    # assert parent table (easy), None first (db order)
    assert_table(p, "lists", [None, None, "a"], info=info)
    # child tables contain nested lists
    assert_table(p, "lists__value", sorted(data[1] + data[2]))


@pytest.mark.parametrize('destination_name', ALL_CLIENT_TYPES)
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
    print(info)
    # print(p.default_schema.to_pretty_yaml())
    schema = p.default_schema
    assert "simple_rows" in schema._schema_tables
    assert "simple" not in schema._schema_tables
    assert "new_column" not in schema.get_table("simple_rows")["columns"]

    # lets violate unique constraint on postgres, redshift and BQ ignore unique indexes
    if destination_name == "postgres":
        assert p.dataset_name == dataset_name
        err_info = p.run(source(1).with_resources("simple_rows"))
        # print(err_info)
        # we have failed jobs
        assert len(next(iter(err_info.failed_jobs.values()))) == 1

    # update schema
    # - new column in "simple_rows" table
    # - new "simple" table
    info_ext = dlt.run(source(10).with_resources("extended_rows", "simple"))
    print(info_ext)
    # print(p.default_schema.to_pretty_yaml())
    schema = p.default_schema
    assert "simple_rows" in schema._schema_tables
    assert "simple" in schema._schema_tables
    assert "new_column" in schema.get_table("simple_rows")["columns"]
    assert "extended_rows" not in schema._schema_tables

    # TODO: test export and import schema
    # test data
    id_data = sorted(["level" + str(n) for n in range(10)] + ["level" + str(n) for n in range(100, 110)])
    assert_data(p, "SELECT * FROM simple_rows ORDER BY id", id_data)


# def test_evolve_schema_conflict() -> None:
#     pass


def assert_table(p: dlt.Pipeline, table_name: str, table_data: List[Any], schema_name: str = None, info: LoadInfo = None) -> None:
    assert_data(p, f"SELECT * FROM {table_name} ORDER BY 1 NULLS FIRST", table_data, schema_name, info)


def assert_data(p: dlt.Pipeline, sql: str, table_data: List[Any], schema_name: str = None, info: LoadInfo = None) -> None:
    with p.sql_client(schema_name=schema_name) as c:
        with c.execute_query(sql) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == len(table_data)
            for row, d in zip(rows, table_data):
                row = list(row)
                # first element comes from the data
                assert row[0] == d
                # the second is load id
                if info:
                    assert row[1] in info.loads_ids
