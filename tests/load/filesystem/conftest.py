from typing import Iterator

import pytest

from dlt.common.schema import Schema
from dlt.common.utils import uniq_id
from dlt.destinations.filesystem.filesystem import FilesystemClient

from tests.utils import preserve_environ
from tests.load.filesystem.utils import get_client_instance


@pytest.fixture(scope='function')
def filesystem_client() -> Iterator[FilesystemClient]:
    schema = Schema('test_schema')
    dataset_name = 'test_' + uniq_id()
    _client = get_client_instance(schema, dataset_name)
    try:
        yield _client
    finally:
        pass
        _client.fs_client.rm(_client.dataset_path, recursive=True)
