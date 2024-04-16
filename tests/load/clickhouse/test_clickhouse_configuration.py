from typing import Any, Iterator

import pytest

import dlt
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.libs.sql_alchemy import make_url
from dlt.common.storages import FileStorage
from dlt.common.utils import digest128
from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseClient
from dlt.destinations.impl.clickhouse.configuration import (
    ClickHouseCredentials,
    ClickHouseClientConfiguration,
)
from dlt.destinations.impl.snowflake.configuration import (
    SnowflakeClientConfiguration,
    SnowflakeCredentials,
)
from tests.common.configuration.utils import environment
from tests.load.utils import yield_client_with_storage
from tests.utils import TEST_STORAGE_ROOT, delete_test_storage


@pytest.fixture
def file_storage() -> FileStorage:
    return FileStorage(TEST_STORAGE_ROOT, file_type="b", makedirs=True)


@pytest.fixture(autouse=True)
def auto_delete_storage() -> None:
    delete_test_storage()


@pytest.fixture(scope="function")
def client() -> Iterator[ClickHouseClient]:
    yield from yield_client_with_storage("clickhouse")  # type: ignore


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


def test_clickhouse_connection_settings(client: ClickHouseClient) -> None:
    """Test experimental settings are set correctly for session."""
    conn = client.sql_client.open_connection()
    cursor1 = conn.cursor()
    cursor2 = conn.cursor()

    cursors = [cursor1, cursor2]

    for cursor in cursors:
        cursor.execute("SELECT name, value FROM system.settings")
        res = cursor.fetchall()

        assert ("allow_experimental_lightweight_delete", "1") in res
        assert ("allow_experimental_object_type", "1") in res
        assert ("enable_http_compression", "1") in res
