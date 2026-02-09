import pytest

from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster import ClickHouseClusterClient

from tests.cases import TABLE_UPDATE
from tests.load.clickhouse_cluster.utils import client

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


def test_create_table(client: ClickHouseClusterClient) -> None:
    stmts = client._get_table_update_sql("event_test_table", TABLE_UPDATE, False)
    assert len(stmts) == 1
    sql = stmts[0]

    qualified_name = client.sql_client.make_qualified_table_name("event_test_table")
    assert sql.startswith(f"CREATE TABLE {qualified_name} ON CLUSTER {client.config.cluster}")
