import os
import pytest
import yaml
from dlt.common import json

from dlt.common.schema.normalizers import explicit_normalizers
from dlt.common.schema.schema import Schema
from dlt.common.storages.exceptions import (
    InStorageSchemaModified,
    SchemaNotFoundError,
    UnexpectedSchemaName,
)
from dlt.common.storages import (
    SchemaStorageConfiguration,
    SchemaStorage,
    LiveSchemaStorage,
    FileStorage,
)

from tests.utils import autouse_test_storage, TEST_STORAGE_ROOT
from tests.common.storages.utils import prepare_eth_import_folder
from tests.common.utils import (
    load_yml_case,
    COMMON_TEST_CASES_PATH,
    IMPORTED_VERSION_HASH_ETH_V10,
)


@pytest.fixture(params=[LiveSchemaStorage, SchemaStorage])
def storage(request) -> SchemaStorage:
    return init_storage(request.param, SchemaStorageConfiguration())


@pytest.fixture
def live_storage() -> LiveSchemaStorage:
    return init_storage(LiveSchemaStorage, SchemaStorageConfiguration())  # type: ignore[return-value]


@pytest.fixture(params=[LiveSchemaStorage, SchemaStorage])
def synced_storage(request) -> SchemaStorage:
    # will be created in /schemas
    return init_storage(
        request.param,
        SchemaStorageConfiguration(
            import_schema_path=TEST_STORAGE_ROOT + "/import",
            export_schema_path=TEST_STORAGE_ROOT + "/import",
        ),
    )


@pytest.fixture(params=[LiveSchemaStorage, SchemaStorage])
def ie_storage(request) -> SchemaStorage:
    # will be created in /schemas
    return init_storage(
        request.param,
        SchemaStorageConfiguration(
            import_schema_path=TEST_STORAGE_ROOT + "/import",
            export_schema_path=TEST_STORAGE_ROOT + "/export",
        ),
    )


def init_storage(cls, config: SchemaStorageConfiguration) -> SchemaStorage:
    # use live schema storage for test which must be backward compatible with schema storage
    s = cls(config, makedirs=True)
    assert config is s.config
    if config.export_schema_path:
        os.makedirs(config.export_schema_path, exist_ok=True)
    if config.import_schema_path:
        os.makedirs(config.import_schema_path, exist_ok=True)
    return s


def test_load_non_existing(storage: SchemaStorage) -> None:
    with pytest.raises(SchemaNotFoundError):
        storage.load_schema("nonexisting")


def test_load_schema_with_upgrade() -> None:
    # point the storage root to v4 schema google_spreadsheet_v3.schema
    storage = LiveSchemaStorage(
        SchemaStorageConfiguration(COMMON_TEST_CASES_PATH + "schemas/sheets")
    )
    # the hash when computed on the schema does not match the version_hash in the file so it should raise InStorageSchemaModified
    # but because the version upgrade is required, the check is skipped and the load succeeds
    storage.load_schema("google_spreadsheet_v4")


def test_import_non_existing(synced_storage: SchemaStorage) -> None:
    with pytest.raises(SchemaNotFoundError):
        synced_storage.load_schema("nonexisting")


def test_import_initial(synced_storage: SchemaStorage, storage: SchemaStorage) -> None:
    assert_schema_imported(synced_storage, storage)


def test_import_overwrites_existing_if_modified(
    synced_storage: SchemaStorage, storage: SchemaStorage
) -> None:
    schema = Schema("ethereum")
    storage.save_schema(schema)
    # now import schema that wil overwrite schema in storage as it is not linked to external schema
    assert_schema_imported(synced_storage, storage)


def test_skip_import_if_not_modified(synced_storage: SchemaStorage, storage: SchemaStorage) -> None:
    storage_schema = assert_schema_imported(synced_storage, storage)
    assert not storage_schema.is_modified
    initial_version = storage_schema.stored_version
    # stored_version = storage_schema.stored_version
    # stored_version_hash = storage_schema.stored_version_hash
    # evolve schema
    row = {"floatX": 78172.128, "confidenceX": 1.2, "strX": "STR"}
    _, new_table = storage_schema.coerce_row("event_user", None, row)
    storage_schema.update_table(new_table)
    assert storage_schema.is_modified
    storage.save_schema(storage_schema)
    assert not storage_schema.is_modified
    # now use synced storage to load schema again
    reloaded_schema = synced_storage.load_schema("ethereum")
    # the schema was not overwritten
    assert "event_user" in reloaded_schema.tables
    assert storage_schema.version == reloaded_schema.stored_version
    assert storage_schema.version_hash == reloaded_schema.stored_version_hash
    assert storage_schema._imported_version_hash == reloaded_schema._imported_version_hash
    assert storage_schema.previous_hashes == reloaded_schema.previous_hashes
    # the import schema gets modified
    storage_schema.tables["_dlt_loads"]["write_disposition"] = "append"
    storage_schema.tables.pop("event_user")
    # we save the import schema (using export method)
    synced_storage._export_schema(storage_schema, synced_storage.config.export_schema_path)
    # now load will import again
    reloaded_schema = synced_storage.load_schema("ethereum")
    # we have overwritten storage schema
    assert reloaded_schema.tables["_dlt_loads"]["write_disposition"] == "append"
    assert "event_user" not in reloaded_schema.tables

    # hash and ancestry stay the same
    assert reloaded_schema._imported_version_hash == storage_schema.version_hash
    assert storage_schema.previous_hashes == reloaded_schema.previous_hashes

    # but original version has increased twice (because it was modified twice)
    assert reloaded_schema.stored_version == storage_schema.version == initial_version + 2


def test_store_schema_tampered(synced_storage: SchemaStorage, storage: SchemaStorage) -> None:
    storage_schema = assert_schema_imported(synced_storage, storage)
    # break hash
    stored_schema = storage_schema.to_dict()
    stored_schema["version_hash"] = "broken"
    schema_file = storage._file_name_in_store(storage_schema.name, "json")
    # save schema where hash does not match the content simulating external change
    storage.storage.save(schema_file, json.dumps(stored_schema))
    # this is not allowed
    with pytest.raises(InStorageSchemaModified):
        storage.load_schema("ethereum")


def test_schema_export(ie_storage: SchemaStorage) -> None:
    schema = Schema("ethereum")

    fs = FileStorage(ie_storage.config.export_schema_path)
    exported_name = ie_storage._file_name_in_store("ethereum", "yaml")
    # no exported schema
    assert not fs.has_file(exported_name)

    ie_storage.save_schema(schema)
    # schema exists
    assert fs.has_file(exported_name)
    # does not contain import hash
    assert "imported_version_hash" not in fs.load(exported_name)


def test_list_schemas(storage: SchemaStorage) -> None:
    assert storage.list_schemas() == []
    schema = Schema("ethereum")
    storage.save_schema(schema)
    assert storage.list_schemas() == ["ethereum"]
    schema = Schema("event")
    storage.save_schema(schema)
    assert set(storage.list_schemas()) == set(["ethereum", "event"])
    storage.remove_schema("event")
    assert storage.list_schemas() == ["ethereum"]
    # add schema with _ in the name
    schema = Schema("dlt_pipeline")
    storage.save_schema(schema)
    assert set(storage.list_schemas()) == set(["ethereum", "dlt_pipeline"])


def test_remove_schema(storage: SchemaStorage) -> None:
    schema = Schema("ethereum")
    storage.save_schema(schema)
    schema = Schema("event")
    storage.save_schema(schema)
    storage.remove_schema("event")
    storage.remove_schema("ethereum")
    assert storage.list_schemas() == []


def test_getter(storage: SchemaStorage) -> None:
    # empty storage
    assert len(storage) == 0
    assert "ethereum" not in storage
    with pytest.raises(KeyError):
        storage["ethereum"]
    with pytest.raises(KeyError):
        storage[None]
    assert storage.get("ethereum") is None
    for _ in storage:
        raise AssertionError()

    # add elements
    schema = Schema("ethereum")
    storage.save_schema(schema)
    schema = Schema("event")
    storage.save_schema(schema)

    assert len(storage) == 2
    assert "ethereum" in storage
    assert storage["ethereum"].name == "ethereum"
    assert storage["event"].name == "event"
    assert set(storage.keys()) == set(["ethereum", "event"])
    assert set(name for name in storage) == set(["ethereum", "event"])
    values = storage.values()
    assert set(s.name for s in values) == set(["ethereum", "event"])
    items = storage.items()
    assert set(i[1].name for i in items) == set(["ethereum", "event"])
    assert set(i[0] for i in items) == set(["ethereum", "event"])


def test_getter_with_import(ie_storage: SchemaStorage) -> None:
    with pytest.raises(KeyError):
        ie_storage["ethereum"]
    prepare_eth_import_folder(ie_storage)
    # schema will be imported
    schema = ie_storage["ethereum"]
    assert schema.name == "ethereum"
    version_hash = schema.version_hash
    # the import schema gets modified
    schema.tables["_dlt_loads"]["write_disposition"] = "append"
    mod_version_hash = schema.version_hash
    assert schema.is_modified
    ie_storage.save_schema(schema)
    assert not schema.is_modified
    # now load via getter
    schema_copy = ie_storage["ethereum"]
    assert schema_copy.version_hash == schema_copy.stored_version_hash == mod_version_hash
    assert schema_copy._imported_version_hash == version_hash

    # now save the schema as import
    ie_storage._export_schema(schema, ie_storage.config.import_schema_path)
    # if you get the schema, import hash will change
    schema = ie_storage["ethereum"]
    assert schema._imported_version_hash == mod_version_hash
    # only true for live schema
    # assert id(schema) == id(schema_copy)


def test_save_store_schema_over_import(ie_storage: SchemaStorage) -> None:
    prepare_eth_import_folder(ie_storage)
    # we have ethereum schema to be imported but we create new schema and save it
    schema = Schema("ethereum")
    schema_hash = schema.version_hash
    ie_storage.save_schema(schema)
    assert schema.version_hash == schema_hash
    # we linked schema to import schema
    assert schema._imported_version_hash == IMPORTED_VERSION_HASH_ETH_V10()
    # load schema and make sure our new schema is here
    schema = ie_storage.load_schema("ethereum")
    assert schema._imported_version_hash == IMPORTED_VERSION_HASH_ETH_V10()
    assert schema._stored_version_hash == schema_hash
    assert schema.version_hash == schema_hash
    assert schema.previous_hashes == []
    # we have simple schema in export folder
    fs = FileStorage(ie_storage.config.export_schema_path)
    exported_name = ie_storage._file_name_in_store("ethereum", "yaml")
    exported_schema = yaml.safe_load(fs.load(exported_name))
    assert schema.version_hash == exported_schema["version_hash"]


def test_save_store_schema_over_import_sync(synced_storage: SchemaStorage) -> None:
    # as in test_save_store_schema_over_import but we export the new schema immediately to overwrite the imported schema
    prepare_eth_import_folder(synced_storage)
    schema = Schema("ethereum")
    schema_hash = schema.version_hash
    synced_storage.save_schema(schema)
    assert schema._imported_version_hash == IMPORTED_VERSION_HASH_ETH_V10()
    # import schema is overwritten
    fs = FileStorage(synced_storage.config.import_schema_path)
    exported_name = synced_storage._file_name_in_store("ethereum", "yaml")
    exported_schema = yaml.safe_load(fs.load(exported_name))
    assert schema.version_hash == exported_schema["version_hash"] == schema_hash
    assert schema.previous_hashes == []
    # when it is loaded we will import schema again which is identical to the current one but the import link
    # will be set to itself
    schema = synced_storage.load_schema("ethereum")
    assert schema.version_hash == schema_hash
    assert schema._imported_version_hash == schema_hash


# def test_schema_sync_export(synced_storage: SchemaStorage) -> None:
#     pass


def test_save_store_schema(storage: SchemaStorage) -> None:
    d_n = explicit_normalizers()
    d_n["names"] = "tests.common.normalizers.custom_normalizers"
    schema = Schema("column_event", normalizers=d_n)
    assert schema.is_new
    assert schema.is_modified
    storage.save_schema(schema)
    assert not schema.is_new
    assert not schema.is_modified
    assert storage.storage.has_file(
        SchemaStorage.NAMED_SCHEMA_FILE_PATTERN % ("column_event", "json")
    )
    loaded_schema = storage.load_schema("column_event")
    # also tables gets normalized inside so custom_ is added
    assert (
        loaded_schema.to_dict()["tables"]["column__dlt_loads"]
        == schema.to_dict()["tables"]["column__dlt_loads"]
    )
    assert loaded_schema.to_dict() == schema.to_dict()


def test_schema_from_file() -> None:
    # json has precedence
    schema = SchemaStorage.load_schema_file(
        os.path.join(COMMON_TEST_CASES_PATH, "schemas/local"), "event"
    )
    assert schema.name == "event"

    schema = SchemaStorage.load_schema_file(
        os.path.join(COMMON_TEST_CASES_PATH, "schemas/local"), "event", extensions=("yaml",)
    )
    assert schema.name == "event"
    assert "blocks" in schema.tables

    with pytest.raises(SchemaNotFoundError):
        SchemaStorage.load_schema_file(
            os.path.join(COMMON_TEST_CASES_PATH, "schemas/local"), "eth", extensions=("yaml",)
        )

    # file name and schema content mismatch
    with pytest.raises(UnexpectedSchemaName):
        SchemaStorage.load_schema_file(
            os.path.join(COMMON_TEST_CASES_PATH, "schemas/local"),
            "name_mismatch",
            extensions=("yaml",),
        )


def test_save_initial_import_schema(ie_storage: LiveSchemaStorage) -> None:
    # no schema in regular storage
    with pytest.raises(SchemaNotFoundError):
        ie_storage.load_schema("ethereum")

    # save initial import schema where processing hints are removed
    eth_V9 = load_yml_case("schemas/eth/ethereum_schema_v9")
    schema = Schema.from_dict(eth_V9)
    ie_storage.save_import_schema_if_not_exists(schema)
    # should be available now
    eth = ie_storage.load_schema("ethereum")
    assert "x-normalizer" not in eth.tables["blocks"]

    # won't overwrite initial schema
    del eth_V9["tables"]["blocks__uncles"]
    schema = Schema.from_dict(eth_V9)
    ie_storage.save_import_schema_if_not_exists(schema)
    # should be available now
    eth = ie_storage.load_schema("ethereum")
    assert "blocks__uncles" in eth.tables


def test_live_schema_instances(live_storage: LiveSchemaStorage) -> None:
    schema = Schema("simple")
    live_storage.save_schema(schema)

    # get schema via getter
    getter_schema = live_storage["simple"]
    # same id
    assert id(getter_schema) == id(schema)

    # live schema is same as in storage
    assert live_storage.is_live_schema_committed("simple")
    # modify getter schema
    getter_schema._schema_description = "this is getter schema"
    assert getter_schema.is_modified
    # getter is not committed
    assert not live_storage.is_live_schema_committed("simple")

    # separate instance via load
    load_schema = live_storage.load_schema("simple")
    assert id(load_schema) != id(schema)
    # changes not visible
    assert load_schema._schema_description is None

    # bypass live schema to simulate 3rd party change
    SchemaStorage.save_schema(live_storage, getter_schema)
    # committed because hashes are matching with file
    assert live_storage.is_live_schema_committed("simple")
    getter_schema = live_storage["simple"]
    assert id(getter_schema) == id(schema)

    SchemaStorage.save_schema(live_storage, load_schema)
    # still committed
    assert live_storage.is_live_schema_committed("simple")
    # and aware of changes in storage
    getter_schema = live_storage["simple"]
    assert id(getter_schema) == id(schema)
    assert getter_schema._schema_description is None
    getter_schema_mod_hash = getter_schema.version_hash

    # create a new "simple" schema
    second_simple = Schema("simple")
    second_simple._schema_description = "Second simple"
    live_storage.save_schema(second_simple)
    # got saved
    load_schema = live_storage.load_schema("simple")
    assert load_schema._schema_description == "Second simple"
    # live schema seamlessly updated
    assert schema._schema_description == "Second simple"
    assert not schema.is_modified
    assert getter_schema_mod_hash in schema.previous_hashes


def test_commit_live_schema(live_storage: LiveSchemaStorage) -> None:
    with pytest.raises(SchemaNotFoundError):
        live_storage.commit_live_schema("simple")
    # set live schema
    schema = Schema("simple")
    set_schema = live_storage.set_live_schema(schema)
    assert id(set_schema) == id(schema)
    assert "simple" in live_storage.live_schemas
    assert not live_storage.is_live_schema_committed("simple")
    # nothing in storage
    with pytest.raises(SchemaNotFoundError):
        SchemaStorage.__getitem__(live_storage, "simple")
    with pytest.raises(SchemaNotFoundError):
        live_storage.load_schema("simple")
    assert not live_storage.is_live_schema_committed("simple")

    # commit
    assert live_storage.commit_live_schema("simple") is not None
    # schema in storage
    live_storage.load_schema("simple")
    assert live_storage.is_live_schema_committed("simple")

    # second commit does not save
    assert live_storage.commit_live_schema("simple") is None

    # mod the schema
    schema._schema_description = "mod the schema"
    assert not live_storage.is_live_schema_committed("simple")
    mod_hash = schema.version_hash

    # save another instance under the same name
    schema_2 = Schema("simple")
    schema_2._schema_description = "instance 2"
    live_storage.save_schema(schema_2)
    assert live_storage.is_live_schema_committed("simple")
    # content replaces in place
    assert schema._schema_description == "instance 2"
    assert mod_hash in schema.previous_hashes


def test_live_schema_getter_when_committed(live_storage: LiveSchemaStorage) -> None:
    # getter on committed is aware of changes to storage (also import)
    schema = Schema("simple")
    live_storage.set_live_schema(schema)
    set_schema = live_storage["simple"]
    live_storage.commit_live_schema("simple")
    # change content in storage
    cloned = set_schema.clone()
    cloned._schema_description = "cloned"
    SchemaStorage.save_schema(live_storage, cloned)
    set_schema_2 = live_storage["simple"]
    assert set_schema_2._schema_description == "cloned"
    assert id(set_schema_2) == id(set_schema)


def test_new_live_schema_committed(live_storage: LiveSchemaStorage) -> None:
    with pytest.raises(SchemaNotFoundError):
        live_storage.is_live_schema_committed("simple")


# def test_save_empty_schema_name(storage: SchemaStorage) -> None:
#     schema = Schema("")
#     schema.settings["schema_sealed"] = True
#     storage.save_schema(schema)
#     assert storage.storage.has_file(SchemaStorage.SCHEMA_FILE_NAME % "json")
#     schema = storage.load_schema("")
#     assert schema.settings["schema_sealed"] is True


def assert_schema_imported(synced_storage: SchemaStorage, storage: SchemaStorage) -> Schema:
    prepare_eth_import_folder(synced_storage)
    schema = synced_storage.load_schema("ethereum")
    # is linked to imported schema
    schema._imported_version_hash = IMPORTED_VERSION_HASH_ETH_V10()
    # also was saved in storage
    assert synced_storage.has_schema("ethereum")
    # and has link to imported schema as well (load without import)
    schema = storage.load_schema("ethereum")
    assert schema._imported_version_hash == IMPORTED_VERSION_HASH_ETH_V10()
    return schema
