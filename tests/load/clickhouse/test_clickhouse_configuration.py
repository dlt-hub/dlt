from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.libs.sql_alchemy import make_url
from dlt.common.utils import digest128
from dlt.destinations.impl.clickhouse.configuration import (
    ClickhouseCredentials,
    ClickhouseClientConfiguration,
)
from dlt.destinations.impl.snowflake.configuration import (
    SnowflakeClientConfiguration,
    SnowflakeCredentials,
)


def test_connection_string_with_all_params() -> None:
    url = "clickhouse://user1:pass1@host1:9000/db1"

    creds = ClickhouseCredentials()
    creds.parse_native_representation(url)

    assert creds.database == "db1"
    assert creds.username == "user1"
    assert creds.password == "pass1"
    assert creds.host == "host1"
    assert creds.port == 9000

    expected = make_url(url)

    # Test URL components regardless of query param order
    assert make_url(creds.to_native_representation()) == expected


def test_clickhouse_configuration() -> None:
    # def empty fingerprint
    assert ClickhouseClientConfiguration().fingerprint() == ""
    # based on host
    c = resolve_configuration(
        SnowflakeCredentials(),
        explicit_value="clickhouse://user1:pass1@host1:9000/db1",
    )
    assert SnowflakeClientConfiguration(credentials=c).fingerprint() == digest128("host1")
