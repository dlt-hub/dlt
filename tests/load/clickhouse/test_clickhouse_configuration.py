from typing import Iterator

import pytest

from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.libs.sql_alchemy_compat import make_url
from dlt.common.utils import digest128
from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseClient
from dlt.destinations.impl.clickhouse.configuration import (
    ClickHouseCredentials,
    ClickHouseClientConfiguration,
)
from tests.load.utils import yield_client_with_storage


@pytest.fixture(scope="function")
def client() -> Iterator[ClickHouseClient]:
    yield from yield_client_with_storage("clickhouse")  # type: ignore


def test_clickhouse_connection_string_with_all_params() -> None:
    url = (
        "clickhouse://user1:pass1@host1:9000/testdb?allow_experimental_lightweight_delete=1&"
        "allow_experimental_object_type=1&connect_timeout=230&date_time_input_format=best_effort&"
        "enable_http_compression=1&secure=0&send_receive_timeout=1000"
    )

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
    config = resolve_configuration(
        ClickHouseCredentials(),
        explicit_value="clickhouse://user1:pass1@host1:9000/db1",
    )
    assert ClickHouseClientConfiguration(credentials=config).fingerprint() == digest128("host1")


def test_clickhouse_connection_settings(client: ClickHouseClient) -> None:
    """Test experimental settings are set correctly for the session."""
    conn = client.sql_client.open_connection()
    cursor1 = conn.cursor()
    cursor2 = conn.cursor()

    cursors = [cursor1, cursor2]

    for cursor in cursors:
        cursor.execute("SELECT name, value FROM system.settings")
        res = cursor.fetchall()

        assert ("allow_experimental_lightweight_delete", "1") in res
        assert ("enable_http_compression", "1") in res
        assert ("date_time_input_format", "best_effort") in res
