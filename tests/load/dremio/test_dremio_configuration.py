import pytest

from dlt.common.utils import digest128
from dlt.destinations.impl.dremio.configuration import (
    DremioClientConfiguration,
    DremioCredentials,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "connection_string,expected_fingerprint",
    [
        pytest.param("", "", id="empty"),
        pytest.param(
            "grpc://user1:pass1@host1:32010/db1",
            digest128("host1"),
            id="legacy_host_only_default_port",
        ),
        pytest.param(
            "grpc://user1:pass1@host1:32011/db1",
            digest128("host1"),
            id="legacy_host_only_custom_port",
        ),
    ],
)
def test_dremio_fingerprint(connection_string: str, expected_fingerprint: str) -> None:
    if connection_string:
        credentials = DremioCredentials(connection_string)
        config = DremioClientConfiguration(credentials=credentials)
    else:
        config = DremioClientConfiguration()

    assert config.fingerprint() == expected_fingerprint
