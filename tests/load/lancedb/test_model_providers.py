"""
Test intricacies and configuration related to each provider.
"""
import os
import pytest

from dlt.common.configuration import resolve_configuration
from dlt.destinations.impl.lancedb.configuration import LanceDBClientConfiguration

# Mark all tests as essential, don't remove.
pytestmark = pytest.mark.essential


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
