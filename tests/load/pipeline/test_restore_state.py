import os
import shutil
from typing import Any
from unittest.mock import patch
import pytest

import dlt
from dlt.common import pendulum
from dlt.common.schema.schema import Schema, utils
from dlt.common.utils import custom_environ, uniq_id
from dlt.common.exceptions import DestinationUndefinedEntity
from dlt.load import Load
from dlt.pipeline.exceptions import SqlClientNotAvailable

from dlt.pipeline.pipeline import Pipeline
from dlt.pipeline.state_sync import STATE_TABLE_COLUMNS, load_state_from_destination, state_resource
from dlt.destinations.job_client_impl import SqlJobClientBase

from tests.utils import TEST_STORAGE_ROOT
from tests.cases import JSON_TYPED_DICT, JSON_TYPED_DICT_DECODED
from tests.common.utils import IMPORTED_VERSION_HASH_ETH_V6, yml_case_path as common_yml_case_path
from tests.common.configuration.utils import environment
from tests.load.pipeline.utils import assert_query_data, drop_active_pipeline_data
from tests.load.utils import destinations_configs, DestinationTestConfiguration, get_normalized_dataset_name


@pytest.fixture(autouse=True)
def duckdb_pipeline_location() -> None:
    # this will store duckdb in working folder so it survives pipeline wipe
    if "DESTINATION__DUCKDB__CREDENTIALS" in os.environ:
        del os.environ["DESTINATION__DUCKDB__CREDENTIALS"]


@pytest.mark.parametrize("destination_config", destinations_configs(default_staging_configs=True, default_sql_configs=True, default_vector_configs=True), ids=lambda x: x.name)
def test_restore_state_utils(destination_config: DestinationTestConfiguration) -> None:

    p = destination_config.setup_pipeline(pipeline_name="pipe_" + uniq_id(), dataset_name="state_test_" + uniq_id())

    schema = Schema("state")
    # inject schema into pipeline, don't do it in production
    p._inject_schema(schema)
    # try with non existing dataset
    job_client: SqlJobClientBase
    with p.destination_client(p.default_schema.name) as job_client:  # type: ignore[assignment]
        with pytest.raises(DestinationUndefinedEntity):
            load_state_from_destination(p.pipeline_name, job_client)
        # sync the schema
        p.sync_schema()
        exists, _ = job_client.get_storage_table(schema.version_table_name)
        assert exists is True
        # dataset exists, still no table
        with pytest.raises(DestinationUndefinedEntity):
            load_state_from_destination(p.pipeline_name, job_client)
        initial_state = p._get_state()
        # now add table to schema and sync
        initial_state["_local"]["_last_extracted_at"] = pendulum.now()
        # add _dlt_id and _dlt_load_id
        resource = state_resource(initial_state)
        resource.apply_hints(columns={
            "_dlt_id": {"name": "_dlt_id", "data_type": "text", "nullable": False},
            "_dlt_load_id": {"name": "_dlt_load_id", "data_type": "text", "nullable": False},
            **STATE_TABLE_COLUMNS
        })
        schema.update_table(schema.normalize_table_identifiers(resource.compute_table_schema()))
        # do not bump version here or in sync_schema, dlt won't recognize that schema changed and it won't update it in storage
        # so dlt in normalize stage infers _state_version table again but with different column order and the column order in schema is different
        # then in database. parquet is created in schema order and in Redshift it must exactly match the order.
        # schema.bump_version()
        p.sync_schema()
        exists, _ = job_client.get_storage_table(schema.state_table_name)
        assert exists is True
        # table is there but no state
        assert load_state_from_destination(p.pipeline_name, job_client) is None
        # extract state
        with p.managed_state(extract_state=True):
            pass
        # just run the existing extract
        p.normalize(loader_file_format=destination_config.file_format)
        p.load()
        stored_state = load_state_from_destination(p.pipeline_name, job_client)
        local_state = p._get_state()
        local_state.pop("_local")
        assert stored_state == local_state
        # extract state again
        with p.managed_state(extract_state=True) as managed_state:
            # this will be saved
            managed_state["sources"] = {"source": dict(JSON_TYPED_DICT_DECODED)}
        p.normalize(loader_file_format=destination_config.file_format)
        p.load()
        stored_state = load_state_from_destination(p.pipeline_name, job_client)
        assert stored_state["sources"] == {"source": JSON_TYPED_DICT_DECODED}
        local_state = p._get_state()
        local_state.pop("_local")
        assert stored_state == local_state
        # use the state context manager again but do not change state
        with p.managed_state(extract_state=True):
            pass
        # version not changed
        new_local_state = p._get_state()
        new_local_state.pop("_local")
        assert local_state == new_local_state
        p.normalize(loader_file_format=destination_config.file_format)
        info = p.load()
        assert len(info.loads_ids) == 0
        new_stored_state = load_state_from_destination(p.pipeline_name, job_client)
        # new state should not be stored
        assert new_stored_state == stored_state

        # change the state in context manager but there's no extract
        with p.managed_state(extract_state=False) as managed_state:
            managed_state["sources"] = {"source": "test2"}  # type: ignore[dict-item]
        new_local_state = p._get_state()
        new_local_state_local = new_local_state.pop("_local")
        assert local_state != new_local_state
        # version increased
        assert local_state["_state_version"] + 1 == new_local_state["_state_version"]
        # last extracted timestamp not present
        assert "_last_extracted_at" not in new_local_state_local

        # use the state context manager again but do not change state
        # because _last_extracted_at is not present, the version will not change but state will be extracted anyway
        with p.managed_state(extract_state=True):
            pass
        new_local_state_2 = p._get_state()
        new_local_state_2_local = new_local_state_2.pop("_local")
        assert new_local_state == new_local_state_2
        # there's extraction timestamp
        assert "_last_extracted_at" in new_local_state_2_local
        # but the version didn't change
        assert new_local_state["_state_version"] == new_local_state_2["_state_version"]
        p.normalize(loader_file_format=destination_config.file_format)
        info = p.load()
        assert len(info.loads_ids) == 1
        new_stored_state_2 = load_state_from_destination(p.pipeline_name, job_client)
        # the stored state changed to next version
        assert new_stored_state != new_stored_state_2
        assert new_stored_state["_state_version"] + 1 == new_stored_state_2["_state_version"]


@pytest.mark.parametrize("destination_config", destinations_configs(default_sql_configs=True, default_vector_configs=True), ids=lambda x: x.name)
def test_silently_skip_on_invalid_credentials(destination_config: DestinationTestConfiguration, environment: Any) -> None:
    environment["CREDENTIALS"] = "postgres://loader:password@localhost:5432/dlt_data"
    environment["DESTINATION__BIGQUERY__CREDENTIALS"] = '{"project_id": "chat-analytics-","client_email": "loader@chat-analytics-317513","private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEuwIBADANBgkqhkiG9w0BAQEFAASCBKUwggShAgEAAoIBAQCNEN0bL39HmD"}'
    pipeline_name = "pipe_" + uniq_id()
    dataset_name="state_test_" + uniq_id()
    # NOTE: we are not restoring the state in __init__ anymore but the test should stay: init should not fail on lack of credentials
    destination_config.setup_pipeline(pipeline_name=pipeline_name, dataset_name=dataset_name)


@pytest.mark.parametrize("destination_config", destinations_configs(default_sql_configs=True, default_vector_configs=True), ids=lambda x: x.name)
@pytest.mark.parametrize('use_single_dataset', [True, False])
def test_get_schemas_from_destination(destination_config: DestinationTestConfiguration, use_single_dataset: bool) -> None:
    pipeline_name = "pipe_" + uniq_id()
    dataset_name="state_test_" + uniq_id()

    p = destination_config.setup_pipeline(pipeline_name=pipeline_name, dataset_name=dataset_name)
    p.config.use_single_dataset = use_single_dataset

    def _make_dn_name(schema_name: str) -> str:
        if use_single_dataset:
            return dataset_name
        else:
            return f"{dataset_name}_{schema_name}"

    default_schema = Schema("state")
    p._inject_schema(default_schema)
    with p.destination_client()  as job_client:
        # just sync schema without name - will use default schema
        p.sync_schema()
        assert get_normalized_dataset_name(job_client) == default_schema.naming.normalize_table_identifier(dataset_name)
    schema_two = Schema("two")
    with p._get_destination_clients(schema_two)[0] as job_client:
        # use the job_client to do that
        job_client.initialize_storage()
        job_client.update_stored_schema()
        # this may be a separate dataset depending in use_single_dataset setting
        assert get_normalized_dataset_name(job_client) == schema_two.naming.normalize_table_identifier(_make_dn_name("two"))
    schema_three = Schema("three")
    p._inject_schema(schema_three)
    with p._get_destination_clients(schema_three)[0] as job_client:
        # sync schema with a name
        p.sync_schema(schema_three.name)
        assert get_normalized_dataset_name(job_client) == schema_three.naming.normalize_table_identifier(_make_dn_name("three"))

    # wipe and restore
    p._wipe_working_folder()
    p = destination_config.setup_pipeline(pipeline_name=pipeline_name, dataset_name=dataset_name)
    p.config.use_single_dataset = use_single_dataset

    assert not p.default_schema_name
    assert p.schema_names == []
    assert p._schema_storage.list_schemas() == []
    # no schemas to restore
    restored_schemas = p._get_schemas_from_destination([], always_download=False)
    assert restored_schemas == []
    # restore unknown schema
    restored_schemas = p._get_schemas_from_destination(["_unknown"], always_download=False)
    assert restored_schemas == []
    # restore default schema
    p.default_schema_name = "state"
    p.schema_names = ["state"]
    restored_schemas = p._get_schemas_from_destination(p.schema_names, always_download=False)
    assert len(restored_schemas) == 1
    assert restored_schemas[0].name == "state"
    p._schema_storage.save_schema(restored_schemas[0])
    assert p._schema_storage.list_schemas() == ["state"]
    # restore all the rest
    p.schema_names = ["state", "two", "three"]
    restored_schemas = p._get_schemas_from_destination(p.schema_names, always_download=False)
    # only two restored schemas, state is already present
    assert len(restored_schemas) == 2
    for schema in restored_schemas:
        p._schema_storage.save_schema(schema)
    assert set(p._schema_storage.list_schemas()) == set(p.schema_names)
    # force download - all three schemas are restored
    restored_schemas = p._get_schemas_from_destination(p.schema_names, always_download=True)
    assert len(restored_schemas) == 3


@pytest.mark.parametrize("destination_config", destinations_configs(default_sql_configs=True, default_vector_configs=True), ids=lambda x: x.name)
def test_restore_state_pipeline(destination_config: DestinationTestConfiguration) -> None:
    os.environ["RESTORE_FROM_DESTINATION"] = "True"
    pipeline_name = "pipe_" + uniq_id()
    dataset_name="state_test_" + uniq_id()
    p = destination_config.setup_pipeline(pipeline_name=pipeline_name, dataset_name=dataset_name)

    def some_data_gen(param: str) -> Any:
        dlt.current.source_state()[param] = param
        yield param

    @dlt.resource
    def some_data(param: str):
        yield from some_data_gen(param)

    @dlt.source(schema=Schema("two"), section="two")
    def source_two(param: str):
        return some_data(param)

    @dlt.source(schema=Schema("three"), section="three")
    def source_three(param: str):
        return some_data(param)

    @dlt.source(schema=Schema("four"), section="four")
    def source_four():
        @dlt.resource
        def some_data():
            dlt.current.source_state()["state5"] = dict(JSON_TYPED_DICT_DECODED)
            yield "four"

        return some_data()

    # extract by creating ad hoc source in pipeline that keeps state under pipeline name
    data1 = some_data("state1")
    data1._pipe.name = "state1_data"
    p.extract([data1, some_data("state2")], schema=Schema("default"))

    data_two = source_two("state3")
    p.extract(data_two)

    data_three = source_three("state4")
    p.extract(data_three)

    data_four = source_four()
    p.extract(data_four)

    p.normalize()
    p.load()
    # keep the orig state
    orig_state = p.state

    # wipe and restore
    p._wipe_working_folder()
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    p = destination_config.setup_pipeline(pipeline_name=pipeline_name, dataset_name=dataset_name)
    p.run()
    # restore was not requested so schema is empty
    assert p.default_schema_name is None
    p._wipe_working_folder()
    # request restore
    os.environ["RESTORE_FROM_DESTINATION"] = "True"
    p = destination_config.setup_pipeline(pipeline_name=pipeline_name, dataset_name=dataset_name)
    p.run()
    assert p.default_schema_name == "default"
    assert set(p.schema_names) == set(["default", "two", "three", "four"])
    assert p.state["sources"] == {
        "default": {'state1': 'state1', 'state2': 'state2'}, "two": {'state3': 'state3'}, "three": {'state4': 'state4'}, "four": {"state5": JSON_TYPED_DICT_DECODED}
    }
    for schema in p.schemas.values():
        normalized_id = schema.naming.normalize_table_identifier("some_data")
        assert normalized_id in schema.tables
    # state version must be the same as the original
    restored_state = p.state
    assert restored_state["_state_version"] == orig_state["_state_version"]

    # full refresh will not restore pipeline even if requested
    p._wipe_working_folder()
    p = destination_config.setup_pipeline(pipeline_name=pipeline_name, dataset_name=dataset_name, full_refresh=True)
    p.run()
    assert p.default_schema_name is None
    drop_active_pipeline_data()

    # create pipeline without restore
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    p = destination_config.setup_pipeline(pipeline_name=pipeline_name, dataset_name=dataset_name)
    # now attach locally
    os.environ["RESTORE_FROM_DESTINATION"] = "True"
    p = dlt.attach(pipeline_name=pipeline_name)
    assert p.dataset_name == dataset_name
    assert p.default_schema_name is None
    # restore
    p.run()
    assert p.default_schema_name is not None
    restored_state = p.state
    assert restored_state["_state_version"] == orig_state["_state_version"]

    # second run will not restore
    p._inject_schema(Schema("second"))  # this will modify state, run does not sync if states are identical
    assert p.state["_state_version"] > orig_state["_state_version"]
    # print(p.state)
    p.run()
    assert set(p.schema_names) == set(["default", "two", "three", "second", "four"])  # we keep our local copy
    # clear internal flag and decrease state version so restore triggers
    state = p.state
    state["_state_version"] -= 1
    p._save_state(state)
    p._state_restored = False
    p.run()
    assert set(p.schema_names) == set(["default", "two", "three", "four"])


@pytest.mark.parametrize("destination_config", destinations_configs(default_sql_configs=True, default_vector_configs=True), ids=lambda x: x.name)
def test_ignore_state_unfinished_load(destination_config: DestinationTestConfiguration) -> None:
    pipeline_name = "pipe_" + uniq_id()
    dataset_name="state_test_" + uniq_id()
    p = destination_config.setup_pipeline(pipeline_name=pipeline_name, dataset_name=dataset_name)

    @dlt.resource
    def some_data(param: str) -> Any:
        dlt.current.source_state()[param] = param
        yield param

    def complete_package_mock(self, load_id: str, schema: Schema, aborted: bool = False):
        # complete in local storage but skip call to the database
        self.load_storage.complete_load_package(load_id, aborted)

    with patch.object(Load, "complete_package", complete_package_mock):
        p.run(some_data("fix_1"))
        # assert complete_package.called

    job_client: SqlJobClientBase
    with p._get_destination_clients(p.default_schema)[0] as job_client:  # type: ignore[assignment]
        # state without completed load id is not visible
        state = load_state_from_destination(pipeline_name, job_client)
        assert state is None


@pytest.mark.parametrize("destination_config", destinations_configs(default_sql_configs=True, default_vector_configs=True), ids=lambda x: x.name)
def test_restore_schemas_while_import_schemas_exist(destination_config: DestinationTestConfiguration) -> None:
    # restored schema should attach itself to imported schema and it should not get overwritten
    import_schema_path = os.path.join(TEST_STORAGE_ROOT, "schemas", "import")
    export_schema_path = os.path.join(TEST_STORAGE_ROOT, "schemas", "export")
    pipeline_name = "pipe_" + uniq_id()
    dataset_name="state_test_" + uniq_id()
    p = destination_config.setup_pipeline(
        pipeline_name=pipeline_name,
        dataset_name=dataset_name,
        import_schema_path=import_schema_path,
        export_schema_path=export_schema_path
    )
    prepare_import_folder(p)
    # make sure schema got imported
    schema = p.schemas["ethereum"]
    assert "blocks" in schema.tables

    # extract some additional data to upgrade schema in the pipeline
    p.run(["A", "B", "C"], table_name="labels", schema=schema)
    # schema should be up to date
    normalized_labels = schema.naming.normalize_table_identifier("labels")
    normalized_annotations = schema.naming.normalize_table_identifier("annotations")
    normalized_blacklist = schema.naming.normalize_table_identifier("blacklist")

    assert normalized_labels in schema.tables

    # re-attach the pipeline
    p = dlt.attach(pipeline_name=pipeline_name)
    p.run(["C", "D", "E"], table_name="annotations")
    schema = p.schemas["ethereum"]
    assert normalized_labels in schema.tables
    assert normalized_annotations in schema.tables

    # wipe the working dir and restore

    print("----> wipe")
    p._wipe_working_folder()
    p = dlt.pipeline(
        pipeline_name=pipeline_name,
        import_schema_path=import_schema_path,
        export_schema_path=export_schema_path
    )
    # use run to get changes
    p.run(destination=destination_config.destination, staging=destination_config.staging, dataset_name=dataset_name)
    schema = p.schemas["ethereum"]
    assert normalized_labels in schema.tables
    assert normalized_annotations in schema.tables

    # check if attached to import schema
    assert schema._imported_version_hash == IMPORTED_VERSION_HASH_ETH_V6
    # extract some data with restored pipeline
    p.run(["C", "D", "E"], table_name="blacklist")
    assert normalized_labels in schema.tables
    assert normalized_annotations in schema.tables
    assert normalized_blacklist in schema.tables


@pytest.mark.skip("Not implemented")
def test_restore_change_dataset_and_destination(destination_name: str) -> None:
    # run pipeline on ie. postgres + dataset1
    # run other pipeline on redshift + dataset2
    # then re-attach p1 and run on redshift + dataset2
    pass


@pytest.mark.parametrize("destination_config", destinations_configs(default_sql_configs=True, default_vector_configs=True), ids=lambda x: x.name)
def test_restore_state_parallel_changes(destination_config: DestinationTestConfiguration) -> None:

    pipeline_name = "pipe_" + uniq_id()
    dataset_name="state_test_" + uniq_id()
    destination_config.setup()
    p = dlt.pipeline(pipeline_name=pipeline_name)

    @dlt.resource
    def some_data(param: str) -> Any:
        dlt.current.source_state()[param] = param
        yield param

    # extract two resources that modify the state
    data1 = some_data("state1")
    data1._pipe.name = "state1_data"

    p.run([data1, some_data("state2")], schema=Schema("default"), destination=destination_config.destination, staging=destination_config.staging, dataset_name=dataset_name)
    orig_state = p.state

    # create a production pipeline in separate pipelines_dir
    production_p = dlt.pipeline(pipeline_name=pipeline_name, pipelines_dir=TEST_STORAGE_ROOT)
    production_p.run(destination=destination_config.destination, staging=destination_config.staging, dataset_name=dataset_name)
    assert production_p.default_schema_name == "default"

    prod_state = production_p.state
    assert prod_state["sources"] == {"default": {'state1': 'state1', 'state2': 'state2'}}
    assert prod_state["_state_version"] == orig_state["_state_version"]
    # generate data on production that modifies the schema but not state
    data2 = some_data("state1")
    # rename extract table/
    data2.apply_hints(table_name="state1_data2")
    print("---> run production")
    production_p.run(data2)
    assert production_p.state["_state_version"] == prod_state["_state_version"]

    normalize = production_p.default_schema.naming.normalize_table_identifier
    assert normalize("state1_data2") in production_p.default_schema.tables

    print("---> run local")
    # sync the local pipeline, state didn't change so new schema is not retrieved
    p.sync_destination()
    assert normalize("state1_data2") not in p.default_schema.tables

    # change state on production
    data3 = some_data("state3")
    data3.apply_hints(table_name="state1_data2")
    print("---> run production")
    production_p.run(data3)
    assert production_p.state["_state_version"] > prod_state["_state_version"]
    # and will be detected locally
    # print(p.default_schema)
    p.sync_destination()
    # existing schema got overwritten
    assert normalize("state1_data2") in p._schema_storage.load_schema(p.default_schema_name).tables
    # print(p.default_schema)
    assert normalize("state1_data2") in p.default_schema.tables

    # change state locally
    data4 = some_data("state4")
    data4.apply_hints(table_name="state1_data4")
    p.run(data4)
    # and on production in parallel
    data5 = some_data("state5")
    data5.apply_hints(table_name="state1_data5")
    production_p.run(data5)
    data6 = some_data("state6")
    data6.apply_hints(table_name="state1_data6")
    production_p.run(data6)
    # production state version ahead of local state version
    prod_state = production_p.state
    assert p.state["_state_version"] == prod_state["_state_version"] - 1
    # re-attach production and sync
    ra_production_p = dlt.attach(pipeline_name=pipeline_name, pipelines_dir=TEST_STORAGE_ROOT)
    ra_production_p.sync_destination()
    # state didn't change because production is ahead of local with its version
    # nevertheless this is potentially dangerous situation 🤷
    assert ra_production_p.state == prod_state

    # get all the states, notice version 4 twice (one from production, the other from local)
    try:
        with p.sql_client() as client:
            state_table = client.make_qualified_table_name(p.default_schema.state_table_name)

        assert_query_data(
            p,
            f"SELECT version FROM {state_table} ORDER BY created_at DESC",
            [5, 4, 4, 3, 2]
        )
    except SqlClientNotAvailable:
        pytest.skip(f"destination {destination_config.destination} does not support sql client")


@pytest.mark.parametrize("destination_config", destinations_configs(default_sql_configs=True, default_vector_configs=True), ids=lambda x: x.name)
def test_reset_pipeline_on_deleted_dataset(destination_config: DestinationTestConfiguration) -> None:
    pipeline_name = "pipe_" + uniq_id()
    dataset_name="state_test_" + uniq_id()
    destination_config.setup()
    p = dlt.pipeline(pipeline_name=pipeline_name)

    @dlt.resource
    def some_data(param: str) -> Any:
        dlt.current.source_state()[param] = param
        yield param

    data4 = some_data("state4")
    data4.apply_hints(table_name="state1_data4")
    p.run(data4, schema=Schema("sch1"), destination=destination_config.destination, staging=destination_config.staging, dataset_name=dataset_name)
    data5 = some_data("state4")
    data5.apply_hints(table_name="state1_data5")
    p.run(data5, schema=Schema("sch2"))
    assert p.state["_state_version"] == 3
    assert p.first_run is False
    with p.destination_client() as job_client:
        job_client.drop_storage()
    # next sync will wipe out the pipeline
    p.sync_destination()
    assert p.first_run is True
    assert p.state["_state_version"] == 0
    assert p.default_schema_name is None
    assert p.schema_names == []
    assert p.pipeline_name == pipeline_name
    assert p.dataset_name == dataset_name

    print("---> no state sync last attach")
    p = dlt.attach(pipeline_name=pipeline_name)
    # this will prevent from creating of _dlt_pipeline_state
    p.config.restore_from_destination = False
    data4 = some_data("state4")
    data4.apply_hints(table_name="state1_data4")
    p.run(data4, schema=Schema("sch1"), destination=destination_config.destination, staging=destination_config.staging, dataset_name=dataset_name)
    assert p.first_run is False
    assert p.state["_local"]["first_run"] is False
    # attach again to make the `run` method check the destination
    print("---> last attach")
    p = dlt.attach(pipeline_name=pipeline_name)
    p.config.restore_from_destination = True
    data5 = some_data("state4")
    data5.apply_hints(table_name="state1_data5")
    p.run(data5, schema=Schema("sch2"))
    # the pipeline was not wiped out, the actual presence if the dataset was checked
    assert set(p.schema_names) == set(["sch2", "sch1"])


def prepare_import_folder(p: Pipeline) -> None:
    os.makedirs(p._schema_storage.config.import_schema_path, exist_ok=True)
    shutil.copy(common_yml_case_path("schemas/eth/ethereum_schema_v5"), os.path.join(p._schema_storage.config.import_schema_path, "ethereum.schema.yaml"))
