import io
import pytest
from typing import Iterator, List

from dlt.common.schema import Schema
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.schema.exceptions import SchemaIdentifierNormalizationCollision
from dlt.common.utils import uniq_id
from dlt.common.schema.typing import TWriteDisposition, TColumnSchema, TTableSchemaColumns

from dlt.destinations import weaviate
from dlt.destinations.impl.weaviate.exceptions import PropertyNameConflict
from dlt.destinations.impl.weaviate.weaviate_client import WeaviateClient

from dlt.common.storages.file_storage import FileStorage
from dlt.common.schema.utils import new_table, normalize_table_identifiers
from tests.load.utils import (
    TABLE_ROW_ALL_DATA_TYPES,
    TABLE_UPDATE,
    TABLE_UPDATE_COLUMNS_SCHEMA,
    expect_load_file,
    write_dataset,
)

from tests.utils import TEST_STORAGE_ROOT

from .utils import drop_active_pipeline_data

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.fixture(autouse=True)
def drop_weaviate_schema() -> Iterator[None]:
    yield
    drop_active_pipeline_data()


def get_client_instance(schema: Schema) -> WeaviateClient:
    dest = weaviate()
    return dest.client(
        schema, dest.spec()._bind_dataset_name(dataset_name="ClientTest" + uniq_id())
    )


@pytest.fixture(scope="function")
def client() -> Iterator[WeaviateClient]:
    yield from make_client("naming")


@pytest.fixture(scope="function")
def ci_client() -> Iterator[WeaviateClient]:
    yield from make_client("ci_naming")


def make_client(naming_convention: str) -> Iterator[WeaviateClient]:
    schema = Schema(
        "test_schema",
        {"names": f"dlt.destinations.impl.weaviate.{naming_convention}", "json": None},
    )
    with get_client_instance(schema) as _client:
        try:
            yield _client
        finally:
            _client.drop_storage()


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)


@pytest.mark.parametrize("write_disposition", ["append", "replace", "merge"])
def test_all_data_types(
    client: WeaviateClient, write_disposition: TWriteDisposition, file_storage: FileStorage
) -> None:
    class_name = "AllTypes"
    # we should have identical content with all disposition types
    client.schema.update_table(
        new_table(class_name, write_disposition=write_disposition, columns=TABLE_UPDATE)
    )
    client.schema._bump_version()
    client.update_stored_schema()

    # write row
    with io.BytesIO() as f:
        write_dataset(client, f, [TABLE_ROW_ALL_DATA_TYPES], TABLE_UPDATE_COLUMNS_SCHEMA)
        query = f.getvalue().decode()
    expect_load_file(client, file_storage, query, class_name)
    _, table_columns = client.get_storage_table("AllTypes")
    # for now check if all columns are there
    assert len(table_columns) == len(TABLE_UPDATE_COLUMNS_SCHEMA)
    for col_name in table_columns:
        assert col_name in TABLE_UPDATE_COLUMNS_SCHEMA
        if TABLE_UPDATE_COLUMNS_SCHEMA[col_name]["data_type"] in ["decimal", "json", "time"]:
            # no native representation
            assert table_columns[col_name]["data_type"] == "text"
        elif TABLE_UPDATE_COLUMNS_SCHEMA[col_name]["data_type"] == "wei":
            assert table_columns[col_name]["data_type"] == "double"
        elif TABLE_UPDATE_COLUMNS_SCHEMA[col_name]["data_type"] == "date":
            assert table_columns[col_name]["data_type"] == "timestamp"
        else:
            assert (
                table_columns[col_name]["data_type"]
                == TABLE_UPDATE_COLUMNS_SCHEMA[col_name]["data_type"]
            )


def test_case_sensitive_properties_create(client: WeaviateClient) -> None:
    class_name = "col_class"
    # we have two properties which will map to the same name in Weaviate
    table_create: List[TColumnSchema] = [
        {"name": "col1", "data_type": "bigint", "nullable": False},
        {"name": "coL1", "data_type": "double", "nullable": False},
    ]
    client.schema.update_table(
        normalize_table_identifiers(
            new_table(class_name, columns=table_create), client.schema.naming
        )
    )
    client.schema._bump_version()
    with pytest.raises(SchemaIdentifierNormalizationCollision) as clash_ex:
        client.verify_schema()
        client.update_stored_schema()
    assert clash_ex.value.identifier_type == "column"
    assert clash_ex.value.identifier_name == "coL1"
    assert clash_ex.value.conflict_identifier_name == "col1"
    assert clash_ex.value.table_name == "ColClass"
    assert clash_ex.value.naming_name == "dlt.destinations.impl.weaviate.naming"


def test_case_insensitive_properties_create(ci_client: WeaviateClient) -> None:
    class_name = "col_class"
    # we have two properties which will map to the same name in Weaviate
    table_create: List[TColumnSchema] = [
        {"name": "col1", "data_type": "bigint", "nullable": False},
        {"name": "coL1", "data_type": "double", "nullable": False},
    ]
    ci_client.schema.update_table(
        normalize_table_identifiers(
            new_table(class_name, columns=table_create), ci_client.schema.naming
        )
    )
    ci_client.schema._bump_version()
    ci_client.update_stored_schema()
    _, table_columns = ci_client.get_storage_table("ColClass")
    # later column overwrites earlier one so: double
    assert table_columns == {"col1": {"name": "col1", "data_type": "double"}}


def test_case_sensitive_properties_add(client: WeaviateClient) -> None:
    class_name = "col_class"
    # we have two properties which will map to the same name in Weaviate
    table_create: List[TColumnSchema] = [{"name": "col1", "data_type": "bigint", "nullable": False}]
    table_update: List[TColumnSchema] = [
        {"name": "coL1", "data_type": "double", "nullable": False},
    ]
    client.schema.update_table(
        normalize_table_identifiers(
            new_table(class_name, columns=table_create), client.schema.naming
        )
    )
    client.schema._bump_version()
    client.update_stored_schema()

    client.schema.update_table(
        normalize_table_identifiers(
            new_table(class_name, columns=table_update), client.schema.naming
        )
    )
    client.schema._bump_version()
    with pytest.raises(SchemaIdentifierNormalizationCollision):
        client.verify_schema()
        client.update_stored_schema()

    # _, table_columns = client.get_storage_table("ColClass")
    # print(table_columns)


def test_load_case_sensitive_data(client: WeaviateClient, file_storage: FileStorage) -> None:
    class_name = "col_class"
    # we have two properties which will map to the same name in Weaviate
    table_create: TTableSchemaColumns = {
        "col1": {"name": "col1", "data_type": "bigint", "nullable": False}
    }
    client.schema.update_table(new_table(class_name, columns=[table_create["col1"]]))
    client.schema._bump_version()
    client.update_stored_schema()
    # prepare a data item where is name clash due to Weaviate being CS
    data_clash = {"col1": 72187328, "coL1": 726171}
    # write row
    with io.BytesIO() as f:
        write_dataset(client, f, [data_clash], table_create)
        query = f.getvalue().decode()
    class_name = client.schema.naming.normalize_table_identifier(class_name)
    job = expect_load_file(client, file_storage, query, class_name, "failed")
    assert type(job._exception) is PropertyNameConflict  # type: ignore


def test_load_case_sensitive_data_ci(ci_client: WeaviateClient, file_storage: FileStorage) -> None:
    class_name = "col_class"
    # we have two properties which will map to the same name in Weaviate
    table_create: TTableSchemaColumns = {
        "col1": {"name": "col1", "data_type": "bigint", "nullable": False}
    }
    ci_client.schema.update_table(new_table(class_name, columns=[table_create["col1"]]))
    ci_client.schema._bump_version()
    ci_client.update_stored_schema()
    # prepare a data item where is name clash due to Weaviate being CI
    # but here we normalize the item
    data_clash = list(
        ci_client.schema.normalize_data_item(
            {"col1": 72187328, "coL1": 726171}, "_load_id_", "col_class"
        )
    )[0][1]

    # write row
    with io.BytesIO() as f:
        write_dataset(ci_client, f, [data_clash], table_create)
        query = f.getvalue().decode()
    class_name = ci_client.schema.naming.normalize_table_identifier(class_name)
    expect_load_file(ci_client, file_storage, query, class_name)
    response = ci_client.query_class(class_name, ["col1"]).do()
    objects = response["data"]["Get"][ci_client.make_qualified_class_name(class_name)]
    # the latter of conflicting fields is stored (so data is lost)
    assert objects == [{"col1": 726171}]
