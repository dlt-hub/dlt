import pytest

from dlt.common.utils import digest128
from dlt.destinations.impl.motherduck.configuration import (
    MotherDuckClientConfiguration,
    MotherDuckCredentials,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "connection_string,expected_fingerprint",
    [
        pytest.param("", "", id="empty"),
        pytest.param(
            "md:///dlt_data?token=TOKEN",
            digest128("TOKEN"),
            id="legacy_token_query_param",
        ),
        pytest.param(
            "md:///dlt_data?motherduck_token=TOKEN",
            digest128("TOKEN"),
            id="legacy_motherduck_token_query_param",
        ),
    ],
)
def test_motherduck_fingerprint(connection_string: str, expected_fingerprint: str) -> None:
    if connection_string:
        credentials = MotherDuckCredentials(connection_string)
        config = MotherDuckClientConfiguration(credentials=credentials)
    else:
        config = MotherDuckClientConfiguration()

    assert config.fingerprint() == expected_fingerprint


def test_motherduck_fingerprint_uses_token_not_physical_location() -> None:
    config = MotherDuckClientConfiguration(
        credentials=MotherDuckCredentials("md:///dlt_data?token=TOKEN")
    )

    assert config.physical_location() == ""
    assert config.fingerprint() == digest128("TOKEN")
