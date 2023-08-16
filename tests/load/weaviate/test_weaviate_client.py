import io
import pytest
from typing import Iterator

from dlt.common.schema import Schema
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.utils import uniq_id

from dlt.destinations import weaviate
from dlt.destinations.weaviate.weaviate_client import WeaviateClient

from dlt.common.storages.file_storage import FileStorage
from dlt.common.schema.utils import new_table
from tests.load.utils import TABLE_ROW_ALL_DATA_TYPES, TABLE_UPDATE, TABLE_UPDATE_COLUMNS_SCHEMA, expect_load_file, write_dataset

from tests.utils import TEST_STORAGE_ROOT


def get_client_instance(schema: Schema) -> WeaviateClient:
    config = weaviate.spec()()
    config.dataset_name = "ClientTest" + uniq_id()
    with Container().injectable_context(ConfigSectionContext(sections=('destination', 'weaviate'))):
        return weaviate.client(schema, config)  # type: ignore[return-value]


@pytest.fixture(scope='function')
def client() -> Iterator[WeaviateClient]:
    schema = Schema('test_schema', {
        'names': "dlt.destinations.weaviate.naming",
        'json': None
    })
    _client = get_client_instance(schema)
    try:
        yield _client
    finally:
        _client.drop_dataset()


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)


@pytest.mark.parametrize('write_disposition', ["append", "replace", "merge"])
def test_all_data_types(client: WeaviateClient, write_disposition: str, file_storage: FileStorage) -> None:
    class_name = "AllTypes"
    # we should have identical content with all disposition types
    client.schema.update_schema(new_table(class_name, write_disposition=write_disposition, columns=TABLE_UPDATE))
    client.schema.bump_version()
    client.update_storage_schema()

    # write row
    with io.BytesIO() as f:
        write_dataset(client, f, [TABLE_ROW_ALL_DATA_TYPES], TABLE_UPDATE_COLUMNS_SCHEMA)
        query = f.getvalue().decode()
    job = expect_load_file(client, file_storage, query, class_name)
    print(job)
    # TODO: use assert_class to see if all data is there, extend it to check data types
