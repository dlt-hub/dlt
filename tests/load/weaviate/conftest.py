from typing import Iterator
import pytest

from dlt.common.schema import Schema
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.utils import uniq_id
from dlt.destinations import weaviate
from dlt.destinations.weaviate.weaviate import WeaviateClient


def get_client_instance(schema: Schema) -> WeaviateClient:
    config = weaviate.spec()()
    with Container().injectable_context(ConfigSectionContext(sections=('weaviate',))):
        return weaviate.client(schema, config)  # type: ignore[return-value]


@pytest.fixture(scope='function')
def weaviate_client() -> Iterator[WeaviateClient]:
    schema = Schema('test_schema')
    _client = get_client_instance(schema)
    try:
        yield _client
    finally:
        pass
        _client.db_client.schema.delete_all()