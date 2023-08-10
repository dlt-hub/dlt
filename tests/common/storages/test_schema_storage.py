import os
import shutil
import pytest
import yaml
from dlt.common import json

from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TStoredSchema
from dlt.common.schema.utils import explicit_normalizers
from dlt.common.storages.exceptions import InStorageSchemaModified, SchemaNotFoundError, UnexpectedSchemaName
from dlt.common.storages import SchemaStorageConfiguration, SchemaStorage, LiveSchemaStorage, FileStorage

from tests.utils import autouse_test_storage, TEST_STORAGE_ROOT
from tests.common.utils import load_yml_case, yml_case_path, COMMON_TEST_CASES_PATH, IMPORTED_VERSION_HASH_ETH_V6


@pytest.fixture
def storage() -> SchemaStorage:
    return init_storage(SchemaStorageConfiguration())


@pytest.fixture
def synced_storage() -> SchemaStorage:
    # will be created in /schemas
    return init_storage(SchemaStorageConfiguration(import_schema_path=TEST_STORAGE_ROOT + "/import", export_schema_path=TEST_STORAGE_ROOT + "/import"))


@pytest.fixture
def ie_storage() -> SchemaStorage:
    # will be created in /schemas
    return init_storage(SchemaStorageConfiguration(import_schema_path=TEST_STORAGE_ROOT + "/import", export_schema_path=TEST_STORAGE_ROOT + "/export"))


def init_storage(C: SchemaStorageConfiguration) -> SchemaStorage:
    # use live schema storage for test which must be backward compatible with schema storage
    s = LiveSchemaStorage(C, makedirs=True)
    assert C is s.config
    if C.export_schema_path:
        os.makedirs(C.export_schema_path, exist_ok=True)
    if C.import_schema_path:
        os.makedirs(C.import_schema_path, exist_ok=True)
    return s


def test_load_non_existing(storage: SchemaStorage) -> None:
    with pytest.raises(SchemaNotFoundError):
        storage.load_schema("nonexisting")


def test_load_schema_with_upgrade() -> None:
    # point the storage root to v4 schema google_spreadsheet_v3.schema
    storage = LiveSchemaStorage(SchemaStorageConfiguration(COMMON_TEST_CASES_PATH + "schemas/sheets"))
    # the hash when computed on the schema does not match the version_hash in the file so it should raise InStorageSchemaModified
    # but because the version upgrade is required, the check is skipped and the load succeeds
    storage.load_schema("google_spreadsheet_v4")


def test_import_non_existing(synced_storage: SchemaStorage) -> None:
    with pytest.raises(SchemaNotFoundError):
        synced_storage.load_schema("nonexisting")


def test_import_initial(synced_storage: SchemaStorage, storage: SchemaStorage) -> None:
    assert_schema_imported(synced_storage, storage)


def test_import_overwrites_existing_if_modified(synced_storage: SchemaStorage, storage: SchemaStorage) -> None:
    schema = Schema("ethereum")
    storage.save_schema(schema)
    # now import schema that wil overwrite schema in storage as it is not linked to external schema
    assert_schema_imported(synced_storage, storage)


def test_skip_import_if_not_modified(synced_storage: SchemaStorage, storage: SchemaStorage) -> None:
    storage_schema = assert_schema_imported(synced_storage, storage)
    # stored_version = storage_schema.stored_version
    # stored_version_hash = storage_schema.stored_version_hash
    # evolve schema
    row = {"floatX": 78172.128, "confidenceX": 1.2, "strX": "STR"}
    _, new_table = storage_schema.coerce_row("event_user", None, row)
    storage_schema.update_schema(new_table)
    storage.save_schema(storage_schema)
    # now use synced storage to load schema again
    reloaded_schema = synced_storage.load_schema("ethereum")
    # the schema was not overwritten
    assert "event_user" in reloaded_schema.tables
    assert storage_schema.version == reloaded_schema.stored_version
    assert storage_schema.version_hash == reloaded_schema.stored_version_hash
    assert storage_schema._imported_version_hash == reloaded_schema._imported_version_hash
    # the import schema gets modified
    storage_schema.tables["_dlt_loads"]["write_disposition"] = "append"
    storage_schema.tables.pop("event_user")
    synced_storage._export_schema(storage_schema, synced_storage.config.export_schema_path)
    # now load will import again
    reloaded_schema = synced_storage.load_schema("ethereum")
    # we have overwritten storage schema
    assert reloaded_schema.tables["_dlt_loads"]["write_disposition"] == "append"
    assert "event_user" not in reloaded_schema.tables
    assert reloaded_schema._imported_version_hash == storage_schema.version_hash
    # but original version has increased
    assert reloaded_schema.stored_version == storage_schema.version + 1


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


def test_mapping_interface(storage: SchemaStorage) -> None:
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


def test_save_store_schema_over_import(ie_storage: SchemaStorage) -> None:
    prepare_import_folder(ie_storage)
    # we have ethereum schema to be imported but we create new schema and save it
    schema = Schema("ethereum")
    schema_hash = schema.version_hash
    ie_storage.save_schema(schema)
    assert schema.version_hash == schema_hash
    # we linked schema to import schema
    assert schema._imported_version_hash == IMPORTED_VERSION_HASH_ETH_V6
    # load schema and make sure our new schema is here
    schema = ie_storage.load_schema("ethereum")
    assert schema._imported_version_hash == IMPORTED_VERSION_HASH_ETH_V6
    assert schema._stored_version_hash == schema_hash
    assert schema.version_hash == schema_hash
    # we have simple schema in export folder
    fs = FileStorage(ie_storage.config.export_schema_path)
    exported_name = ie_storage._file_name_in_store("ethereum", "yaml")
    exported_schema = yaml.safe_load(fs.load(exported_name))
    assert schema.version_hash == exported_schema["version_hash"]


def test_save_store_schema_over_import_sync(synced_storage: SchemaStorage) -> None:
    # as in test_save_store_schema_over_import but we export the new schema immediately to overwrite the imported schema
    prepare_import_folder(synced_storage)
    schema = Schema("ethereum")
    schema_hash = schema.version_hash
    synced_storage.save_schema(schema)
    assert schema._imported_version_hash == IMPORTED_VERSION_HASH_ETH_V6
    # import schema is overwritten
    fs = FileStorage(synced_storage.config.import_schema_path)
    exported_name = synced_storage._file_name_in_store("ethereum", "yaml")
    exported_schema = yaml.safe_load(fs.load(exported_name))
    assert schema.version_hash == exported_schema["version_hash"] == schema_hash
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
    storage.save_schema(schema)
    assert storage.storage.has_file(SchemaStorage.NAMED_SCHEMA_FILE_PATTERN % ("column_event", "json"))
    loaded_schema = storage.load_schema("column_event")
    # also tables gets normalized inside so custom_ is added
    assert loaded_schema.to_dict()["tables"]["column__dlt_loads"] == schema.to_dict()["tables"]["column__dlt_loads"]
    assert loaded_schema.to_dict() == schema.to_dict()


def test_schema_from_file() -> None:
    # json has precedence
    schema = SchemaStorage.load_schema_file(os.path.join(COMMON_TEST_CASES_PATH, "schemas/local"), "event")
    assert schema.name == "event"

    schema = SchemaStorage.load_schema_file(os.path.join(COMMON_TEST_CASES_PATH, "schemas/local"), "event", extensions=("yaml",))
    assert schema.name == "event"
    assert "blocks" in schema.tables

    with pytest.raises(SchemaNotFoundError):
        SchemaStorage.load_schema_file(os.path.join(COMMON_TEST_CASES_PATH, "schemas/local"), "eth", extensions=("yaml",))

    # file name and schema content mismatch
    with pytest.raises(UnexpectedSchemaName):
        SchemaStorage.load_schema_file(os.path.join(COMMON_TEST_CASES_PATH, "schemas/local"), "name_mismatch", extensions=("yaml",))


# def test_save_empty_schema_name(storage: SchemaStorage) -> None:
#     schema = Schema("")
#     schema.settings["schema_sealed"] = True
#     storage.save_schema(schema)
#     assert storage.storage.has_file(SchemaStorage.SCHEMA_FILE_NAME % "json")
#     schema = storage.load_schema("")
#     assert schema.settings["schema_sealed"] is True


def prepare_import_folder(storage: SchemaStorage) -> None:
    shutil.copy(yml_case_path("schemas/eth/ethereum_schema_v6"), os.path.join(storage.storage.storage_path, "../import/ethereum.schema.yaml"))


def assert_schema_imported(synced_storage: SchemaStorage, storage: SchemaStorage) -> Schema:
    prepare_import_folder(synced_storage)
    eth_v6: TStoredSchema = load_yml_case("schemas/eth/ethereum_schema_v6")
    schema = synced_storage.load_schema("ethereum")
    # is linked to imported schema
    schema._imported_version_hash = eth_v6["version_hash"]
    # also was saved in storage
    assert synced_storage.has_schema("ethereum")
    # and has link to imported schema s well (load without import)
    schema = storage.load_schema("ethereum")
    assert schema._imported_version_hash == eth_v6["version_hash"]
    return schema
