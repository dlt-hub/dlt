import pytest

from dlt.destinations.impl.clickhouse_cluster.sql_client import ClickHouseClusterSqlClient

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "input_sql,expected_sql",
    [
        (
            "SELECT * FROM a WHERE x IN (SELECT x FROM b)",  # IN
            "SELECT * FROM a WHERE x GLOBAL IN (SELECT x FROM b)",
        ),
        (
            "SELECT * FROM a WHERE x NOT IN (SELECT x FROM b)",  # NOT IN
            "SELECT * FROM a WHERE x GLOBAL NOT IN (SELECT x FROM b)",
        ),
        (
            "SELECT * FROM a JOIN b ON a.x = b.x",  # JOIN
            "SELECT * FROM a GLOBAL JOIN b ON a.x = b.x",
        ),
        (
            "SELECT * FROM a LEFT JOIN b ON a.x = b.x",  # LEFT JOIN
            "SELECT * FROM a GLOBAL LEFT JOIN b ON a.x = b.x",
        ),
        (
            "SELECT * FROM a WHERE x in (SELECT x FROM b)",  # lower case IN
            "SELECT * FROM a WHERE x GLOBAL IN (SELECT x FROM b)",
        ),
        (
            "SELECT * FROM a WHERE x IN (SELECT x FROM b) AND y IN (SELECT y FROM c)",  # multiple IN
            "SELECT * FROM a WHERE x GLOBAL IN (SELECT x FROM b) AND y GLOBAL IN (SELECT y FROM c)",
        ),
        (
            "SELECT * FROM a WHERE x GLOBAL IN (SELECT x FROM b)",  # already has GLOBAL
            "SELECT * FROM a WHERE x GLOBAL IN (SELECT x FROM b)",
        ),
    ],
)
def test_ensure_global(input_sql: str, expected_sql: str):
    assert ClickHouseClusterSqlClient._ensure_global(input_sql) == expected_sql
