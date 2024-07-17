import os
from typing import Iterator

import pytest

from dlt.common.configuration import resolve_configuration
from dlt.common.utils import digest128
from dlt.destinations.impl.lancedb.configuration import (
    LanceDBClientConfiguration,
)
from tests.load.utils import (
    drop_active_pipeline_data,
)


# Mark all tests as essential, do not remove.
pytestmark = pytest.mark.essential


@pytest.fixture(autouse=True)
def drop_lancedb_data() -> Iterator[None]:
    yield
    drop_active_pipeline_data()


def test_lancedb_configuration() -> None:
    os.environ["DESTINATION__LANCEDB__EMBEDDING_MODEL_PROVIDER"] = "colbert"
    os.environ["DESTINATION__LANCEDB__EMBEDDING_MODEL"] = "text-embedding-3-small"

    config = resolve_configuration(
        LanceDBClientConfiguration()._bind_dataset_name(dataset_name="dataset"),
        sections=("destination", "lancedb"),
    )
    assert config.embedding_model_provider == "colbert"
    assert config.embedding_model == "text-embedding-3-small"
