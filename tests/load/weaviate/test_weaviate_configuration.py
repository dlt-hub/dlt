from typing import Optional

import pytest

from dlt.common.utils import digest128
from dlt.destinations.impl.weaviate.configuration import (
    WeaviateClientConfiguration,
    WeaviateCredentials,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "credentials,expected_fingerprint",
    [
        pytest.param(None, "", id="empty"),
        pytest.param(
            WeaviateCredentials(url="https://weaviate.example.com:8080/v1"),
            digest128("weaviate.example.com"),
            id="hostname_only_url",
        ),
        pytest.param(
            WeaviateCredentials(url="http://localhost:8080"),
            digest128("localhost"),
            id="hostname_only_localhost",
        ),
    ],
)
def test_weaviate_fingerprint(
    credentials: Optional[WeaviateCredentials], expected_fingerprint: str
) -> None:
    config = WeaviateClientConfiguration(credentials=credentials)

    assert config.fingerprint() == expected_fingerprint
