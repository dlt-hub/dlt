import os
import shutil
import pytest
import yaml
from dlt.common import json

from dlt.common.configuration import make_configuration
from dlt.common.file_storage import FileStorage
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TStoredSchema
from dlt.common.schema.utils import default_normalizers
from dlt.common.configuration import SchemaVolumeConfiguration
from dlt.common.storages.exceptions import InStorageSchemaModified, SchemaNotFoundError
from dlt.common.storages import SchemaStorage, LiveSchemaStorage
from dlt.common.typing import DictStrAny

from tests.utils import autouse_root_storage, TEST_STORAGE
from tests.common.utils import load_yml_case, yml_case_path


@pytest.fixture
def storage() -> SchemaStorage:
    return init_storage()


@pytest.fixture
def synced_storage() -> SchemaStorage:
    # will be created in /schemas
    return init_storage({"IMPORT_SCHEMA_PATH": TEST_STORAGE + "/import", "EXPORT_SCHEMA_PATH": TEST_STORAGE + "/import"})


@pytest.fixture
def ie_storage() -> SchemaStorage:
    # will be created in /schemas
    return init_storage({"IMPORT_SCHEMA_PATH": TEST_STORAGE + "/import", "EXPORT_SCHEMA_PATH": TEST_STORAGE + "/export"})


def init_storage(initial: DictStrAny = None) -> SchemaStorage:
    C = make_configuration(SchemaVolumeConfiguration, SchemaVolumeConfiguration, initial_values=initial)
    # use live schema storage for test which must be backward compatible with schema storage
    s = LiveSchemaStorage(C, makedirs=True)
    if C.EXPORT_SCHEMA_PATH:
        os.makedirs(C.EXPORT_SCHEMA_PATH, exist_ok=True)
    if C.IMPORT_SCHEMA_PATH:
        os.makedirs(C.IMPORT_SCHEMA_PATH, exist_ok=True)
    return s


def test_load_non_existing(storage: SchemaStorage) -> None:
    with pytest.raises(SchemaNotFoundError):
        storage.load_schema("nonexisting")


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
    synced_storage._export_schema(storage_schema, synced_storage.C.EXPORT_SCHEMA_PATH)
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

    fs = FileStorage(ie_storage.C.EXPORT_SCHEMA_PATH)
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
        assert False

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
    assert schema._imported_version_hash == "njJAySgJRs2TqGWgQXhP+3pCh1A1hXcqe77BpM7JtOU="
    # load schema and make sure our new schema is here
    schema = ie_storage.load_schema("ethereum")
    assert schema.version_hash == schema_hash
    assert schema._imported_version_hash == "njJAySgJRs2TqGWgQXhP+3pCh1A1hXcqe77BpM7JtOU="
    # we have simple schema in export folder
    fs = FileStorage(ie_storage.C.EXPORT_SCHEMA_PATH)
    exported_name = ie_storage._file_name_in_store("ethereum", "yaml")
    exported_schema = yaml.safe_load(fs.load(exported_name))
    assert schema.version_hash == exported_schema["version_hash"]


def test_save_store_schema_over_import_sync(synced_storage: SchemaStorage) -> None:
    # as in test_save_store_schema_over_import but we export the new schema immediately to overwrite the imported schema
    prepare_import_folder(synced_storage)
    schema = Schema("ethereum")
    schema_hash = schema.version_hash
    synced_storage.save_schema(schema)
    assert schema._imported_version_hash == "njJAySgJRs2TqGWgQXhP+3pCh1A1hXcqe77BpM7JtOU="
    # import schema is overwritten
    fs = FileStorage(synced_storage.C.IMPORT_SCHEMA_PATH)
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
    d_n = default_normalizers()
    d_n["names"] = "tests.common.schema.custom_normalizers"
    schema = Schema("event", normalizers=d_n)
    storage.save_schema(schema)
    assert storage.storage.has_file(SchemaStorage.NAMED_SCHEMA_FILE_PATTERN % ("event", "json"))
    loaded_schema = storage.load_schema("event")
    assert loaded_schema.to_dict()["tables"]["_dlt_loads"] == schema.to_dict()["tables"]["_dlt_loads"]
    assert loaded_schema.to_dict() == schema.to_dict()


# def test_save_empty_schema_name(storage: SchemaStorage) -> None:
#     schema = Schema("")
#     schema.settings["schema_sealed"] = True
#     storage.save_schema(schema)
#     assert storage.storage.has_file(SchemaStorage.SCHEMA_FILE_NAME % "json")
#     schema = storage.load_schema("")
#     assert schema.settings["schema_sealed"] is True


def prepare_import_folder(storage: SchemaStorage) -> None:
    shutil.copy(yml_case_path("schemas/eth/ethereum_schema_v4"), storage.storage._make_path("../import/ethereum_schema.yaml"))


def assert_schema_imported(synced_storage: SchemaStorage, storage: SchemaStorage) -> Schema:
    prepare_import_folder(synced_storage)
    eth_v4: TStoredSchema = load_yml_case("schemas/eth/ethereum_schema_v4")
    schema = synced_storage.load_schema("ethereum")
    # is linked to imported schema
    schema._imported_version_hash == eth_v4["version_hash"]
    # also was saved in storage
    assert synced_storage.has_schema("ethereum")
    # and has link to imported schema s well (load without import)
    schema = storage.load_schema("ethereum")
    assert schema._imported_version_hash == eth_v4["version_hash"]
    return schema
