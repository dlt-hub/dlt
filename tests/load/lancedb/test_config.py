from typing import Iterator, Generator, Any

import pytest

import dlt
from dlt.common.configuration import resolve_configuration
from dlt.common.typing import DictStrStr
from dlt.common.utils import digest128
from dlt.destinations.impl.lancedb.configuration import LanceDBClientConfiguration
from dlt.destinations.impl.lancedb.lancedb_adapter import (
    lancedb_adapter,
)
from tests.load.pipeline.utils import (
    drop_active_pipeline_data,
)
from tests.load.utils import sequence_generator


# Mark all tests as essential, do not remove.
pytestmark = pytest.mark.essential


@pytest.fixture(autouse=True)
def drop_lancedb_data() -> Iterator[None]:
    yield
    drop_active_pipeline_data()


def test_adapter_and_hints() -> None:
    generator_instance1 = sequence_generator()

    @dlt.resource(columns=[{"name": "content", "data_type": "text"}])
    def some_data() -> Generator[DictStrStr, Any, None]:
        yield from next(generator_instance1)

    assert some_data.columns["content"] == {"name": "content", "data_type": "text"}  # type: ignore[index]

    lancedb_adapter(
        some_data,
        embed=["content"],
    )

    assert some_data.columns["content"] == {  # type: ignore
        "name": "content",
        "data_type": "text",
        "x-lancedb-embed": True,
    }


def test_lancedb_configuration() -> None:
    # Ensure that api key and endpoint defaults are applied without exception.

    config = resolve_configuration(
        LanceDBClientConfiguration()._bind_dataset_name(dataset_name="dataset"),
        sections=("destination", "lancedb"),
    )
    assert config.embedding_model_provider == "cohere"
    assert config.id_field_name == "id__"
    assert config.vector_field_name == "vector__"
    assert config.sentinel_table_name == "dltSentinelTable"
    assert config.embedding_model_dimensions is None
    assert config.embedding_model == "embed-english-v3.0"
    assert config.dataset_separator == "___"
    assert config.credentials.uri == ".lancedb"
    assert config.credentials.api_key is None
    assert config.credentials.embedding_model_provider_api_key is None
    assert config.fingerprint() == digest128(config.credentials.uri)
