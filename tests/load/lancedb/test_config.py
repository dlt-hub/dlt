from typing import Iterator

import pytest

from dlt.common.configuration import resolve_configuration
from dlt.common.utils import digest128
from dlt.destinations.impl.lancedb.configuration import (
    LanceDBClientConfiguration,
    LanceDBCredentials,
)
from tests.load.pipeline.utils import (
    drop_active_pipeline_data,
)


# Mark all tests as essential, do not remove.
pytestmark = pytest.mark.essential


@pytest.fixture(autouse=True)
def drop_lancedb_data() -> Iterator[None]:
    yield
    drop_active_pipeline_data()


def test_lancedb_configuration() -> None:
    # Ensure that api key and endpoint defaults are applied without exception.

    config = resolve_configuration(
        LanceDBClientConfiguration(LanceDBCredentials())._bind_dataset_name(
            dataset_name="dataset"
        ),
        sections=("destination", "lancedb"),
    )
    assert config.embedding_model_provider == "cohere"
    assert config.embedding_model == "embed-english-v3.0"
    assert config.embedding_model_dimensions is None
    assert config.sentinel_table_name == "dltSentinelTable"
    assert config.id_field_name == "id__"
    assert config.vector_field_name == "vector__"
    assert config.dataset_separator == "___"
    assert config.credentials.uri == ".lancedb"
    assert config.credentials.api_key is None
    assert config.credentials.embedding_model_provider_api_key is None
    assert config.fingerprint() == digest128(config.credentials.uri)
