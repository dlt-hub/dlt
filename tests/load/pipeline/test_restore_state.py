import os
import shutil
from typing import Any
import pytest

import dlt
from dlt.common import pendulum
from dlt.common.schema.schema import Schema, utils
from dlt.common.schema.typing import VERSION_TABLE_NAME
from dlt.common.utils import uniq_id
from dlt.pipeline.exceptions import PipelineConfigMissing
from dlt.pipeline.pipeline import Pipeline
from dlt.pipeline.state import STATE_TABLE_NAME, load_state_from_destination, state_resource

from tests.utils import ALL_DESTINATIONS, preserve_environ, autouse_test_storage, TEST_STORAGE_ROOT
from tests.common.utils import IMPORTED_VERSION_HASH_ETH_V5, yml_case_path as common_yml_case_path
from tests.common.configuration.utils import environment
from tests.pipeline.utils import drop_dataset_from_env, patch_working_dir
from tests.load.pipeline.utils import drop_pipeline


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_restore_state_utils(destination_name: str) -> None:
    p = dlt.pipeline(pipeline_name="pipe_" + uniq_id(), destination=destination_name, dataset_name="state_test_" + uniq_id())
    schema = Schema("state")
    # inject schema into pipeline, don't do it in production
    p._inject_schema(schema)
    # try with non existing dataset
    with p._sql_job_client(p.default_schema) as job_client:
        assert load_state_from_destination(p.pipeline_name, job_client.sql_client) is None
        # sync the schema
        p.sync_schema()
        exists, _ = job_client.get_storage_table(VERSION_TABLE_NAME)
        assert exists is True
        # dataset exists, still none
        assert load_state_from_destination(p.pipeline_name, job_client.sql_client) is None
        initial_state = p._get_state(restore_from_destination=False)
        # now add table to schema and sync
        initial_state["_last_extracted_at"] = pendulum.now()
        resource = state_resource(initial_state)
        table_schema = resource.table_schema()
        # add _dlt_id and _dlt_load_id
        table_schema["columns"]["_dlt_id"] = utils.add_missing_hints({"name": "_dlt_id", "data_type": "text", "nullable": False})
        table_schema["columns"]["_dlt_load_id"] = utils.add_missing_hints({"name": "_dlt_load_id", "data_type": "text", "nullable": False})
        schema.update_schema(resource.table_schema())
        schema.bump_version()
        p.sync_schema()
        exists, _ = job_client.get_storage_table(STATE_TABLE_NAME)
        assert exists is True
        # table is there but empty
        assert load_state_from_destination(p.pipeline_name, job_client.sql_client) is None
        # extract state
        with p._managed_state(extract_state=True):
            pass
        # just run the existing extract
        p.run()
        stored_state = load_state_from_destination(p.pipeline_name, job_client.sql_client)
        assert stored_state == p._get_state()
        # extract state again
        with p._managed_state(extract_state=True) as managed_state:
            # this will be saved
            managed_state["sources"] = {"source": "test"}
        p.run()
        stored_state = load_state_from_destination(p.pipeline_name, job_client.sql_client)
        assert stored_state["sources"] == {"source": "test"}
        local_state = p._get_state()
        assert stored_state == local_state
        # use the state context manager again but do not change state
        with p._managed_state(extract_state=True):
            pass
        # version not changed
        assert local_state == p._get_state()
        info = p.run()
        assert len(info.loads_ids) == 0
        new_stored_state = load_state_from_destination(p.pipeline_name, job_client.sql_client)
        # new state should not be stored
        assert new_stored_state == stored_state

        # change the state in context manager but there's no extract
        with p._managed_state(extract_state=False) as managed_state:
            managed_state["sources"] = {"source": "test2"}
        new_local_state = p._get_state()
        assert local_state != new_local_state
        # version increased
        assert local_state["_state_version"] + 1 == new_local_state["_state_version"]
        # last extracted timestamp not present
        assert "_last_extracted_at" not in new_local_state

        # use the state context manager again but do not change state
        # because _last_extracted_at is not present, the version will not change but state will be extracted anyway
        with p._managed_state(extract_state=True):
            pass
        new_local_state_2 = p._get_state()
        assert new_local_state != new_local_state_2
        # there's extraction timestamp
        assert "_last_extracted_at" in new_local_state_2
        # but the version didn't change
        assert new_local_state["_state_version"] == new_local_state_2["_state_version"]
        info = p.run()
        assert len(info.loads_ids) == 1
        new_stored_state_2 = load_state_from_destination(p.pipeline_name, job_client.sql_client)
        # the stored state changed to next version
        assert new_stored_state != new_stored_state_2
        assert new_stored_state["_state_version"] + 1 == new_stored_state_2["_state_version"]
        # and extract timestamp increased
        assert new_stored_state["_last_extracted_at"] < new_stored_state_2["_last_extracted_at"]



@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_restore_schemas_from_destination(destination_name: str) -> None:
    # do not even enable restore the state
    pipeline_name = "pipe_" + uniq_id()
    dataset_name="state_test_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination=destination_name, dataset_name=dataset_name)
    default_schema = Schema("state")
    p._inject_schema(default_schema)
    with p._sql_job_client(default_schema) as job_client:
        p.sync_schema()
    schema_three = Schema("two")
    with p._sql_job_client(schema_three) as job_client:
        p.sync_schema()
        # this is a separate dataset
        assert job_client.sql_client.default_dataset_name == f"{dataset_name}_two"
    schema_three = Schema("three")
    with p._sql_job_client(schema_three) as job_client:
        p.sync_schema()
        # this is a separate dataset
        assert job_client.sql_client.default_dataset_name == f"{dataset_name}_three"

    # wipe and restore
    p._wipe_working_folder()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination=destination_name, dataset_name=dataset_name)
    assert not p.default_schema_name
    assert p.schema_names == []
    # no schema names - nothing will be restored
    p._restore_schemas_from_destination()
    assert p._schema_storage.list_schemas() == []
    # restore default schema
    p.default_schema_name = "state"
    p.schema_names = ["state"]
    p._restore_schemas_from_destination()
    assert p._schema_storage.list_schemas() == ["state"]
    # restore all the rest
    p.schema_names = ["state", "two", "three"]
    p._restore_schemas_from_destination()
    assert set(p._schema_storage.list_schemas()) == set(p.schema_names)


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_restore_state_pipeline(destination_name: str) -> None:
    pipeline_name = "pipe_" + uniq_id()
    dataset_name="state_test_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination=destination_name, dataset_name=dataset_name)

    @dlt.resource
    def some_data(param: str) -> Any:
        dlt.state()[param] = param
        yield param

    # extract to several schemas
    data1 = some_data("state1")
    data1.name = "state1_data"
    p.extract([data1, some_data("state2")], schema=Schema("default"))
    p.extract([some_data("state3")], schema=Schema("two"))
    p.extract([some_data("state4")], schema=Schema("three"))
    p.normalize()
    p.load()

    # wipe and restore
    p._wipe_working_folder()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination=destination_name, dataset_name=dataset_name)
    # restore was not requested so schema is empty
    assert p.default_schema_name is None
    p._wipe_working_folder()
    # request restore
    p = dlt.pipeline(pipeline_name=pipeline_name, destination=destination_name, dataset_name=dataset_name, restore_from_destination=True)
    assert p.default_schema_name == "default"
    assert set(p.schema_names) == set(["default", "two", "three"])
    assert p._get_state()["sources"] == {'state1': 'state1', 'state2': 'state2', 'state3': 'state3', 'state4': 'state4'}
    for schema in p.schemas.values():
        assert "some_data" in schema._schema_tables

    # full refresh will not restore pipeline even if requested
    p._wipe_working_folder()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination=destination_name, dataset_name=dataset_name, restore_from_destination=True, full_refresh=True)
    assert p.default_schema_name is None
    p._wipe_working_folder()
    # create pipeline without restore
    p = dlt.pipeline(pipeline_name=pipeline_name, destination=destination_name, dataset_name=dataset_name)
    # now attach locally
    p = dlt.attach(pipeline_name=pipeline_name)
    assert p.dataset_name == dataset_name
    assert p.default_schema_name is None

    # must provide explicit dataset when restoring
    with pytest.raises(PipelineConfigMissing):
        p = dlt.pipeline(pipeline_name=pipeline_name, destination=destination_name, restore_from_destination=True)


@pytest.mark.parametrize('destination_name', ALL_DESTINATIONS)
def test_restore_schemas_while_import_schemas_exist(destination_name: str) -> None:
    # restored schema should attach itself to imported schema and it should not get overwritten
    import_schema_path = os.path.join(TEST_STORAGE_ROOT, "schemas", "import")
    export_schema_path = os.path.join(TEST_STORAGE_ROOT, "schemas", "export")
    pipeline_name = "pipe_" + uniq_id()
    dataset_name="state_test_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination=destination_name, dataset_name=dataset_name, import_schema_path=import_schema_path, export_schema_path=export_schema_path)
    prepare_import_folder(p)
    # make sure schema got imported
    schema = p.schemas["ethereum"]
    assert "blocks" in schema._schema_tables

    # extract some additional data to upgrade schema in the pipeline
    p.run(["A", "B", "C"], table_name="labels", schema=schema)
    # schema should be up to date
    assert "labels" in schema._schema_tables

    # re-attach the pipeline
    p = dlt.attach(pipeline_name=pipeline_name)
    p.run(["C", "D", "E"], table_name="annotations")
    schema = p.schemas["ethereum"]
    assert "labels" in schema._schema_tables
    assert "annotations" in schema._schema_tables

    # wipe the working dir and restore
    p = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination_name,
        dataset_name=dataset_name,
        import_schema_path=import_schema_path,
        export_schema_path=export_schema_path,
        restore_from_destination=True)
    schema = p.schemas["ethereum"]
    assert "labels" in schema._schema_tables
    assert "annotations" in schema._schema_tables
    # check if attached to import schema
    assert schema._imported_version_hash == IMPORTED_VERSION_HASH_ETH_V5
    # extract some data with restored pipeline
    p.run(["C", "D", "E"], table_name="blacklist")
    assert "labels" in schema._schema_tables
    assert "annotations" in schema._schema_tables
    assert "blacklist" in schema._schema_tables


def prepare_import_folder(p: Pipeline) -> None:
    os.makedirs(p._schema_storage.config.import_schema_path, exist_ok=True)
    shutil.copy(common_yml_case_path("schemas/eth/ethereum_schema_v5"), os.path.join(p._schema_storage.config.import_schema_path, "ethereum.schema.yaml"))
