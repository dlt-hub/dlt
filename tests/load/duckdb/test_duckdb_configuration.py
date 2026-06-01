from typing import Optional

import pytest

from dlt.common.utils import digest128
from dlt.destinations.impl.duckdb.configuration import (
    DuckDbClientConfiguration,
    DuckDbCredentials,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "credentials,expected_fingerprint",
    [
        pytest.param(None, "", id="empty"),
        pytest.param(
            DuckDbCredentials(":memory:"),
            digest128(":memory:"),
            id="memory_database",
        ),
        pytest.param(
            DuckDbCredentials("local.duckdb"),
            digest128("local.duckdb"),
            id="database_path",
        ),
    ],
)
def test_duckdb_fingerprint(
    credentials: Optional[DuckDbCredentials], expected_fingerprint: str
) -> None:
    config = DuckDbClientConfiguration(credentials=credentials)

    assert config.fingerprint() == expected_fingerprint
