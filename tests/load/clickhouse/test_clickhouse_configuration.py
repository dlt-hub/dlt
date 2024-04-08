from typing import Any

import pytest

import dlt
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.libs.sql_alchemy import make_url
from dlt.common.utils import digest128
from dlt.destinations.impl.clickhouse.configuration import (
    ClickHouseCredentials,
    ClickHouseClientConfiguration,
)
from dlt.destinations.impl.snowflake.configuration import (
    SnowflakeClientConfiguration,
    SnowflakeCredentials,
)
from tests.common.configuration.utils import environment


def test_clickhouse_connection_string_with_all_params() -> None:
    url = "clickhouse://user1:pass1@host1:9000/testdb?secure=0&connect_timeout=230&send_receive_timeout=1000"

    creds = ClickHouseCredentials()
    creds.parse_native_representation(url)

    assert creds.database == "testdb"
    assert creds.username == "user1"
    assert creds.password == "pass1"
    assert creds.host == "host1"
    assert creds.port == 9000
    assert creds.secure == 0
    assert creds.connect_timeout == 230
    assert creds.send_receive_timeout == 1000

    expected = make_url(url)

    # Test URL components regardless of query param order.
    assert make_url(creds.to_native_representation()) == expected


def test_clickhouse_configuration() -> None:
    # def empty fingerprint
    assert ClickHouseClientConfiguration().fingerprint() == ""
    # based on host
    c = resolve_configuration(
        SnowflakeCredentials(),
        explicit_value="clickhouse://user1:pass1@host1:9000/db1",
    )
    assert SnowflakeClientConfiguration(credentials=c).fingerprint() == digest128("host1")


@pytest.mark.usefixtures("environment")
def test_clickhouse_gcp_hmac_getter_accessor(environment: Any) -> None:
    environment["DESTINATION__FILESYSTEM__CREDENTIALS__GCP_ACCESS_KEY_ID"] = "25g08jaDJacj42"
    environment["DESTINATION__FILESYSTEM__CREDENTIALS__GCP_SECRET_ACCESS_KEY"] = "ascvntp45uasdf"

    assert dlt.config["destination.filesystem.credentials.gcp_access_key_id"] == "25g08jaDJacj42"
    assert (
        dlt.config["destination.filesystem.credentials.gcp_secret_access_key"] == "ascvntp45uasdf"
    )
