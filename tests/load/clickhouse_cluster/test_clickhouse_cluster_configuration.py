from dlt.destinations.impl.clickhouse_cluster.configuration import ClickHouseClusterCredentials


def test_clickhouse_cluster_credentials() -> None:
    # without `alt_ports` and `alt_http_ports`
    creds = ClickHouseClusterCredentials(
        {
            "host": "host1",
            "port": 9001,
            "http_port": 8124,
        }
    )
    assert creds.alt_ports is None
    assert creds.alt_http_ports is None
    assert creds._http_ports == [8124]
    query = creds.get_query()
    assert "alt_hosts" not in query

    # with `alt_ports` and `alt_http_ports`
    creds = ClickHouseClusterCredentials(
        {
            "host": "host1",
            "port": 9001,
            "http_port": 8124,
            "alt_ports": [9002, 9003],
            "alt_http_ports": (8124, 8125, 8126),  # includes primary port to test deduplication
        }
    )
    assert creds._http_ports == [8124, 8125, 8126]  # primary port first, no duplicates
    query = creds.get_query()
    # only `alt_hosts` is valid query param for `clickhouse_driver`, `alt_ports`, `alt_http_ports`,
    # and `alt_http_hosts` are not
    assert "alt_ports" not in query
    assert "alt_http_ports" not in query
    assert "alt_http_hosts" not in query
    assert query["alt_hosts"] == "host1:9002,host1:9003"  # should not include primary host:port

    # with query param from parent ClickHouseCredentials
    creds = ClickHouseClusterCredentials(
        {
            "host": "host1",
            "connect_timeout": 15,  # param from parent
        }
    )
    query = creds.get_query()
    assert query["connect_timeout"] == "15"  # inherited correctly
