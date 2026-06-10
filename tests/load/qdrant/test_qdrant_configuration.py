from typing import Optional

import pytest

from dlt.common.utils import digest128
from dlt.destinations.impl.qdrant.configuration import QdrantClientConfiguration

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "qd_location,expected_fingerprint",
    [
        pytest.param(None, "", id="empty"),
        pytest.param(":memory:", digest128(":memory:"), id="raw_memory_location"),
        pytest.param(
            "https://qdrant.example.com:6333/path",
            digest128("https://qdrant.example.com:6333/path"),
            id="raw_url_location",
        ),
    ],
)
def test_qdrant_fingerprint(qd_location: Optional[str], expected_fingerprint: str) -> None:
    config = QdrantClientConfiguration(qd_location=qd_location)

    assert config.fingerprint() == expected_fingerprint
