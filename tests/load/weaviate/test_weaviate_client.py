import io
import pytest
from typing import Iterator

from dlt.common.schema import Schema
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.utils import uniq_id

from dlt.destinations import weaviate
from dlt.destinations.weaviate.exceptions import PropertyNameConflict
from dlt.destinations.weaviate.weaviate_client import WeaviateClient

from dlt.common.storages.file_storage import FileStorage
from dlt.common.schema.utils import new_table
from tests.load.utils import TABLE_ROW_ALL_DATA_TYPES, TABLE_UPDATE, TABLE_UPDATE_COLUMNS_SCHEMA, expect_load_file, write_dataset

from tests.utils import TEST_STORAGE_ROOT

from .utils import drop_active_pipeline_data

@pytest.fixture(autouse=True)
def drop_weaviate_schema() -> None:
    yield
    drop_active_pipeline_data()


def get_client_instance(schema: Schema) -> WeaviateClient:
    config = weaviate.spec()()
    config.dataset_name = "ClientTest" + uniq_id()
    with Container().injectable_context(ConfigSectionContext(sections=('destination', 'weaviate'))):
        return weaviate.client(schema, config)  # type: ignore[return-value]


@pytest.fixture(scope='function')
def client() -> Iterator[WeaviateClient]:
    yield from make_client("naming")


@pytest.fixture(scope='function')
def ci_client() -> Iterator[WeaviateClient]:
    yield from make_client("ci_naming")


def make_client(naming_convention: str) -> Iterator[WeaviateClient]:
    schema = Schema('test_schema', {
        'names': f"dlt.destinations.weaviate.{naming_convention}",
        'json': None
    })
    _client = get_client_instance(schema)
    try:
        yield _client
    finally:
        _client.drop_storage()


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)


@pytest.mark.parametrize('write_disposition', ["append", "replace", "merge"])
def test_all_data_types(client: WeaviateClient, write_disposition: str, file_storage: FileStorage) -> None:
    class_name = "AllTypes"
    # we should have identical content with all disposition types
    client.schema.update_schema(new_table(class_name, write_disposition=write_disposition, columns=TABLE_UPDATE))
    client.schema.bump_version()
    client.update_stored_schema()

    # write row
    with io.BytesIO() as f:
        write_dataset(client, f, [TABLE_ROW_ALL_DATA_TYPES], TABLE_UPDATE_COLUMNS_SCHEMA)
        query = f.getvalue().decode()
    expect_load_file(client, file_storage, query, class_name)
    _, table_columns = client.get_storage_table("AllTypes")
    print(table_columns)
    # for now check if all columns are there
    assert len(table_columns) == len(TABLE_UPDATE_COLUMNS_SCHEMA)
    for col_name in table_columns:
        assert col_name in TABLE_UPDATE_COLUMNS_SCHEMA
        if TABLE_UPDATE_COLUMNS_SCHEMA[col_name]["data_type"] in ["decimal", "complex", "time"]:
            # no native representation
            assert table_columns[col_name]["data_type"] == "text"
        elif TABLE_UPDATE_COLUMNS_SCHEMA[col_name]["data_type"] == "wei":
            assert table_columns[col_name]["data_type"] == "double"
        elif TABLE_UPDATE_COLUMNS_SCHEMA[col_name]["data_type"] == "date":
            assert table_columns[col_name]["data_type"] == "timestamp"
        else:
            assert table_columns[col_name]["data_type"] == TABLE_UPDATE_COLUMNS_SCHEMA[col_name]["data_type"]


def test_case_sensitive_properties_create(client: WeaviateClient) -> None:
    class_name = "col_class"
    # we have two properties which will map to the same name in Weaviate
    table_create = [
    {
        "name": "col1",
        "data_type": "bigint",
        "nullable": False
    },
    {
        "name": "coL1",
        "data_type": "double",
        "nullable": False
    },
    ]
    client.schema.update_schema(client.schema.normalize_table_identifiers(new_table(class_name, columns=table_create)))
    client.schema.bump_version()
    with pytest.raises(PropertyNameConflict):
        client.update_stored_schema()


def test_case_insensitive_properties_create(ci_client: WeaviateClient) -> None:
    class_name = "col_class"
    # we have two properties which will map to the same name in Weaviate
    table_create = [
    {
        "name": "col1",
        "data_type": "bigint",
        "nullable": False
    },
    {
        "name": "coL1",
        "data_type": "double",
        "nullable": False
    },
    ]
    ci_client.schema.update_schema(ci_client.schema.normalize_table_identifiers(new_table(class_name, columns=table_create)))
    ci_client.schema.bump_version()
    ci_client.update_stored_schema()
    _, table_columns = ci_client.get_storage_table("ColClass")
    # later column overwrites earlier one so: double
    assert table_columns == {'col1': {'name': 'col1', 'data_type': 'double'}}


def test_case_sensitive_properties_add(client: WeaviateClient) -> None:
    class_name = "col_class"
    # we have two properties which will map to the same name in Weaviate
    table_create = [{
        "name": "col1",
        "data_type": "bigint",
        "nullable": False
    }]
    table_update = [{
        "name": "coL1",
        "data_type": "double",
        "nullable": False
    },
    ]
    client.schema.update_schema(
        client.schema.normalize_table_identifiers(new_table(class_name, columns=table_create))
    )
    client.schema.bump_version()
    client.update_stored_schema()

    client.schema.update_schema(
        client.schema.normalize_table_identifiers(new_table(class_name, columns=table_update))
    )
    client.schema.bump_version()
    with pytest.raises(PropertyNameConflict):
        client.update_stored_schema()

    # _, table_columns = client.get_storage_table("ColClass")
    # print(table_columns)


def test_load_case_sensitive_data(client: WeaviateClient, file_storage: FileStorage) -> None:
    class_name = "col_class"
    # we have two properties which will map to the same name in Weaviate
    table_create = {"col1":
    {
        "name": "col1",
        "data_type": "bigint",
        "nullable": False
    }}
    client.schema.update_schema(new_table(class_name, columns=[table_create["col1"]]))
    client.schema.bump_version()
    client.update_stored_schema()
    # prepare a data item where is name clash due to Weaviate being CI
    data_clash = {"col1": 72187328, "coL1": 726171}
    # write row
    with io.BytesIO() as f:
        write_dataset(client, f, [data_clash], table_create)
        query = f.getvalue().decode()
    with pytest.raises(PropertyNameConflict):
        expect_load_file(client, file_storage, query, class_name)


def test_load_case_sensitive_data_ci(ci_client: WeaviateClient, file_storage: FileStorage) -> None:
    class_name = "col_class"
    # we have two properties which will map to the same name in Weaviate
    table_create = {"col1":
    {
        "name": "col1",
        "data_type": "bigint",
        "nullable": False
    }}
    ci_client.schema.update_schema(new_table(class_name, columns=[table_create["col1"]]))
    ci_client.schema.bump_version()
    ci_client.update_stored_schema()
    # prepare a data item where is name clash due to Weaviate being CI
    # but here we normalize the item
    data_clash = list(
        ci_client.schema.normalize_data_item({"col1": 72187328, "coL1": 726171}, "_load_id_", "col_class")
    )[0][1]

    # write row
    with io.BytesIO() as f:
        write_dataset(ci_client, f, [data_clash], table_create)
        query = f.getvalue().decode()
    expect_load_file(ci_client, file_storage, query, class_name)
    response = ci_client.query_class(class_name, ["col1"]).do()
    objects = response["data"]["Get"][ci_client.make_qualified_class_name(class_name)]
    print(objects)