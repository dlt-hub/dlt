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
    os.environ["DESTINATION__LANCEDB__CREDENTIALS__DATABASE"] = "dlt-ci"
    os.environ["DESTINATION__LANCEDB__EMBEDDINGS__PROVIDER"] = "ollama"
    os.environ["DESTINATION__LANCEDB__EMBEDDINGS__NAME"] = "nomic-embed-text"
    # the provider host is provider specific and reaches `EmbeddingFunction.create()` via kwargs
    os.environ["DESTINATION__LANCEDB__EMBEDDINGS__KWARGS"] = (
        '{"host": "http://198.163.194.3:24233"}'
    )

    config = resolve_configuration(
        LanceDBClientConfiguration()._bind_dataset_name(dataset_name="dataset"),
        sections=("destination", "lancedb"),
    )
    assert config.embeddings.provider == "ollama"
    assert config.embeddings.name == "nomic-embed-text"
    assert config.embeddings.kwargs == {"host": "http://198.163.194.3:24233"}
