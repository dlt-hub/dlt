import pytest

from dlt.destinations.impl.clickhouse_cluster.configuration import ClickHouseClusterCredentials


# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


def test_clickhouse_cluster_credentials_parse_native_representation() -> None:
    creds = ClickHouseClusterCredentials()
    creds.parse_native_representation(
        "clickhouse://user:pass@host1:9001/db1?connect_timeout=7&alt_hosts=host1:9002,host1:9003"
    )
    assert creds.username == "user"
    assert creds.password == "pass"
    assert creds.host == "host1"
    assert creds.port == 9001
    assert creds.database == "db1"
    assert creds.connect_timeout == 7
    assert creds.alt_hosts == "host1:9002,host1:9003"


def test_clickhouse_cluster_credentials_alt_hosts() -> None:
    # without `alt_hosts` and `alt_http_hosts`
    creds = ClickHouseClusterCredentials(
        {
            "host": "host1",
            "port": 9001,
            "http_port": 8124,
        }
    )
    assert creds.alt_hosts is None
    assert creds.alt_http_hosts is None
    assert creds._http_hosts == [("host1", 8124)]
    query = creds.get_query()
    assert "alt_hosts" not in query
    assert "alt_http_hosts" not in query

    # with `alt_hosts` and `alt_http_hosts`
    creds = ClickHouseClusterCredentials(
        {
            "host": "host1",
            "port": 9001,
            "http_port": 8124,
            "alt_hosts": "host1:9002,host2:9003",
            "alt_http_hosts": "host1:8125,host2:8126",
        }
    )
    assert creds._http_hosts == [("host1", 8124), ("host1", 8125), ("host2", 8126)]
    query = creds.get_query()
    assert "alt_http_hosts" not in query  # not a query param
    assert query["alt_hosts"] == "host1:9002,host2:9003"  # should not include primary host:port

    # with query param from parent ClickHouseCredentials
    creds = ClickHouseClusterCredentials(
        {
            "host": "host1",
            "connect_timeout": 7,  # param from parent
        }
    )
    query = creds.get_query()
    assert query["connect_timeout"] == "7"  # inherited correctly
