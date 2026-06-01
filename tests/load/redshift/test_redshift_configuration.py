import pytest

from dlt.common.configuration import resolve_configuration
from dlt.common.utils import digest128
from dlt.destinations.impl.redshift.configuration import (
    RedshiftClientConfiguration,
    RedshiftCredentials,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "connection_string,expected_fingerprint",
    [
        pytest.param("", "", id="empty"),
        pytest.param(
            "postgres://user1:pass1@host1:5439/db1",
            digest128("host1"),
            id="legacy_host_only_default_port",
        ),
        pytest.param(
            "postgres://user1:pass1@host1:1234/db1",
            digest128("host1"),
            id="legacy_host_only_custom_port",
        ),
    ],
)
def test_redshift_fingerprint(connection_string: str, expected_fingerprint: str) -> None:
    if connection_string:
        credentials = resolve_configuration(
            RedshiftCredentials(),
            explicit_value=connection_string,
        )
        config = RedshiftClientConfiguration(credentials=credentials)
    else:
        config = RedshiftClientConfiguration()

    assert config.fingerprint() == expected_fingerprint
