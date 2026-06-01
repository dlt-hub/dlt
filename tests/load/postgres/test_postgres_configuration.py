import pytest

from dlt.common.utils import digest128
from dlt.destinations.impl.postgres.configuration import (
    PostgresClientConfiguration,
    PostgresCredentials,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "connection_string,expected_fingerprint",
    [
        pytest.param("", "", id="empty"),
        pytest.param(
            "postgres://user1:pass1@host1:5432/db1",
            digest128("host1"),
            id="legacy_host_only_default_port",
        ),
        pytest.param(
            "postgres://user1:pass1@host1:5433/db1",
            digest128("host1"),
            id="legacy_host_only_custom_port",
        ),
    ],
)
def test_postgres_fingerprint(connection_string: str, expected_fingerprint: str) -> None:
    if connection_string:
        credentials = PostgresCredentials(connection_string)
        config = PostgresClientConfiguration(credentials=credentials)
    else:
        config = PostgresClientConfiguration()

    assert config.fingerprint() == expected_fingerprint
