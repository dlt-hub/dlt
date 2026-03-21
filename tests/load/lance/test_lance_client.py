from typing import Iterator, cast

import pytest

from dlt.common.utils import uniq_id
from dlt.destinations.impl.lance.lance_client import LanceClient

from tests.load.utils import yield_client


pytestmark = pytest.mark.essential


@pytest.fixture(scope="module")
def client() -> Iterator[LanceClient]:
    dataset_name = "test_" + uniq_id()
    yield from cast(Iterator[LanceClient], yield_client("lance", dataset_name=dataset_name))


def test_lance_client_namespace_methods(client: LanceClient) -> None:
    namespace_name = "foo"
    assert not client.namespace_exists(namespace_name)
    client.create_namespace(namespace_name)
    assert client.namespace_exists(namespace_name)
    client.drop_namespace(namespace_name)
    assert not client.namespace_exists(namespace_name)


def test_lance_client_storage(client: LanceClient) -> None:
    assert not client.is_storage_initialized()
    assert not client.namespace_exists(client.dataset_name)

    # initializing storage should create dataset namespace
    client.initialize_storage()
    assert client.is_storage_initialized()
    assert client.namespace_exists(client.dataset_name)

    # dropping storage should drop dataset namespace
    client.drop_storage()
    assert not client.is_storage_initialized()
    assert not client.namespace_exists(client.dataset_name)
