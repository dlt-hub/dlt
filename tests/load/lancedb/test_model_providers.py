"""
Test intricacies and configuration related to each provider.
"""

import os
from typing import Iterator, Any, Generator

import pytest
from lancedb import DBConnection  # type: ignore
from lancedb.embeddings import EmbeddingFunctionRegistry  # type: ignore
from lancedb.table import Table  # type: ignore

import dlt
from dlt.common.configuration import resolve_configuration
from dlt.common.typing import DictStrStr
from dlt.common.utils import uniq_id
from dlt.destinations.impl.lancedb import lancedb_adapter
from dlt.destinations.impl.lancedb.configuration import LanceDBClientConfiguration
from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient
from tests.load.utils import drop_active_pipeline_data, sequence_generator
from tests.pipeline.utils import assert_load_info

# Mark all tests as essential, don't remove.
pytestmark = pytest.mark.essential


@pytest.fixture(autouse=True)
def drop_lancedb_data() -> Iterator[Any]:
    yield
    drop_active_pipeline_data()


def test_lancedb_ollama_endpoint_configuration() -> None:
    os.environ["DESTINATION__LANCEDB__EMBEDDING_MODEL_PROVIDER"] = "ollama"
    os.environ["DESTINATION__LANCEDB__EMBEDDING_MODEL"] = "nomic-embed-text"
    os.environ["DESTINATION__LANCEDB__EMBEDDING_MODEL_PROVIDER_HOST"] = "http://198.163.194.3:24233"

    config = resolve_configuration(
        LanceDBClientConfiguration()._bind_dataset_name(dataset_name="dataset"),
        sections=("destination", "lancedb"),
    )
    assert config.embedding_model_provider == "ollama"
    assert config.embedding_model == "nomic-embed-text"
    assert config.embedding_model_provider_host == "http://198.163.194.3:24233"


def test_lancedb_ollama_defaults() -> None:
    os.environ["DESTINATION__LANCEDB__EMBEDDING_MODEL_PROVIDER"] = "ollama"
    os.environ["DESTINATION__LANCEDB__EMBEDDING_MODEL"] = "nomic-embed-text"

    @dlt.resource
    def some_data() -> Generator[DictStrStr, Any, None]:
        yield from next(sequence_generator())

    lancedb_adapter(
        some_data,
        embed=["content"],
    )

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_append",
        destination="lancedb",
        dataset_name=f"test_pipeline_append_dataset_{uniq_id()}",
    )
    info = pipeline.run(
        some_data(),
    )
    assert_load_info(info)

    client: LanceDBClient
    with pipeline.destination_client() as client:  # type: ignore
        # Check if we can get a stored schema and state.
        schema = client.get_stored_schema(client.schema.name)
        print("Print dataset name", client.dataset_name)
        assert schema
        state = client.get_stored_state("test_pipeline_append")
        assert state
