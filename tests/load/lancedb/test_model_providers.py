"""
Test intricacies and configuration related to each provider.
"""
import os
from typing import Iterator, Any

import pytest  # type: ignore
from lancedb import DBConnection  # type: ignore
from lancedb.embeddings import EmbeddingFunctionRegistry  # type: ignore
from lancedb.table import Table  # type: ignore

from dlt.common.configuration import resolve_configuration
from dlt.destinations.impl.lancedb.configuration import LanceDBClientConfiguration
from tests.load.utils import drop_active_pipeline_data

# Mark all tests as essential, don't remove.
pytestmark = pytest.mark.essential


@pytest.fixture(autouse=True)
def drop_lancedb_data() -> Iterator[Any]:
    yield
    drop_active_pipeline_data()

def test_lancedb_ollama_endpoint_configuration() -> None:
    os.environ["DESTINATION__LANCEDB__EMBEDDING_MODEL_PROVIDER"] = "ollama"
    os.environ["DESTINATION__LANCEDB__EMBEDDING_MODEL"] = "nomic-embed-text"

    config = resolve_configuration(
        LanceDBClientConfiguration()._bind_dataset_name(dataset_name="dataset"),
        sections=("destination", "lancedb"),
    )
    assert config.embedding_model_provider == "ollama"
    assert config.embedding_model == "nomic-embed-text"
    assert config.embedding_model_provider_host == "localhost"
    assert config.embedding_model_provider_port == "11434"

    os.environ["DESTINATION__LANCEDB__EMBEDDING_MODEL_PROVIDER_HOST"] = "198.163.194.3"
    os.environ["DESTINATION__LANCEDB__EMBEDDING_MODEL_PROVIDER_PORT"] = "24233"

    config = resolve_configuration(
        LanceDBClientConfiguration()._bind_dataset_name(dataset_name="dataset"),
        sections=("destination", "lancedb"),
    )
    assert config.embedding_model_provider_host == "198.163.194.3"
    assert config.embedding_model_provider_port == "24233"


# def test_lancedb_ollama() -> None:
#     generator_instance1 = sequence_generator()
#
#     @dlt.resource
#     def some_data() -> Generator[DictStrStr, Any, None]:
#         yield from next(generator_instance1)
#
#     lancedb_adapter(
#         some_data,
#         embed=["content"],
#     )
#
#     pipeline = dlt.pipeline(
#         pipeline_name="test_pipeline_append",
#         destination="lancedb",
#         dataset_name=f"test_pipeline_append_dataset{uniq_id()}",
#     )
#     info = pipeline.run(
#         some_data(),
#     )
#     assert_load_info(info)
#
#     client: LanceDBClient
#     with pipeline.destination_client() as client:  # type: ignore
#         # Check if we can get a stored schema and state.
#         schema = client.get_stored_schema(client.schema.name)
#         print("Print dataset name", client.dataset_name)
#         assert schema
#         state = client.get_stored_state("test_pipeline_append")
#         assert state
