import pytest

from dlt.common.schema import Schema
from dlt.common.utils import uniq_id
from dlt.destinations import clickhouse_cluster
from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster import ClickHouseClusterClient
from dlt.destinations.impl.clickhouse_cluster.configuration import (
    ClickHouseClusterClientConfiguration,
)
from tests.cases import TABLE_UPDATE

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.fixture
def client(empty_schema: Schema) -> ClickHouseClusterClient:
    # return client without opening connection
    return clickhouse_cluster().client(
        empty_schema,
        ClickHouseClusterClientConfiguration()._bind_dataset_name(dataset_name="test_" + uniq_id()),
    )


def test_create_table(client: ClickHouseClusterClient) -> None:
    stmts = client._get_table_update_sql("event_test_table", TABLE_UPDATE, False)
    assert len(stmts) == 1
    sql = stmts[0]

    qualified_name = client.sql_client.make_qualified_table_name("event_test_table")
    assert sql.startswith(f"CREATE TABLE {qualified_name} ON CLUSTER {client.config.cluster}")
