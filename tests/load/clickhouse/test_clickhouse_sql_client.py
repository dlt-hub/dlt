import pytest

from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseClient

from tests.load.clickhouse.utils import clickhouse_client


@pytest.mark.parametrize(
    "case",
    [
        # (query, params, expected result, id)
        # without params
        ("SELECT 16 % 4", (), 0, "%-as-modulo"),
        ("SELECT 'test' LIKE '%es%'", (), 1, "%-as-wildcard"),
        ("SELECT '100% sure'", (), "100% sure", "%-as-literal"),
        # with params
        ("SELECT %s AS value", ("foo",), "foo", "simple-param-val"),
        ("SELECT %s AS value", ("foo%",), "foo%", "%-in-param-val"),
    ],
    ids=lambda case: case[3],
)
def test_clickhouse_execute_query_with_pct(clickhouse_client: ClickHouseClient, case) -> None:
    query, params, expected, _ = case
    with clickhouse_client.sql_client as client:
        with client.execute_query(query, params) as cursor:
            result = cursor.fetchall()
            assert result == [(expected,)]
