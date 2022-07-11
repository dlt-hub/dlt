import pytest

from dlt.common.schema.schema import Schema
from dlt.common.schema.utils import default_normalizers
from dlt.common.storages.schema_storage import SchemaStorage

from tests.utils import autouse_root_storage, TEST_STORAGE


@pytest.fixture
def storage() -> SchemaStorage:
    s = SchemaStorage(TEST_STORAGE, makedirs=True)
    return s


def test_save_store_schema(storage: SchemaStorage) -> None:
    d_n = default_normalizers()
    d_n["names"] = "tests.common.schema.custom_normalizers"
    schema = Schema("event", normalizers=d_n)
    storage.save_store_schema(schema)
    assert storage.storage.has_file(SchemaStorage.STORE_SCHEMA_FILE_PATTERN % "event")
    loaded_schema = storage.load_store_schema("event")
    assert loaded_schema.to_dict()["tables"]["_dlt_loads"] == schema.to_dict()["tables"]["_dlt_loads"]
    assert loaded_schema.to_dict() == schema.to_dict()


def test_save_empty_schema_name(storage: SchemaStorage) -> None:
    schema = Schema("")
    schema.schema_settings["schema_sealed"] = True
    storage.save_store_schema(schema)
    assert storage.storage.has_file(SchemaStorage.STORE_SCHEMA_FILE_PATTERN % "")
    schema = storage.load_store_schema("")
    assert schema.schema_settings["schema_sealed"] is True

