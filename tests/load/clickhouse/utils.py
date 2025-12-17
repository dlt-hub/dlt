import pytest

from dlt.common.schema import Schema
from dlt.common.utils import uniq_id
from dlt.destinations import clickhouse
from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseClient
from dlt.destinations.impl.clickhouse.configuration import (
    ClickHouseClientConfiguration,
    ClickHouseCredentials,
)
from dlt.destinations.impl.clickhouse.sql_client import ClickHouseSqlClient
from dlt.destinations.impl.clickhouse.typing import TDeployment


@pytest.fixture
def clickhouse_client(empty_schema: Schema) -> ClickHouseClient:
    # Return a client without opening connection.
    creds = ClickHouseCredentials()
    return clickhouse().client(
        empty_schema,
        ClickHouseClientConfiguration(credentials=creds)._bind_dataset_name(f"test_{uniq_id()}"),
    )


def get_deployment_type(client: ClickHouseSqlClient) -> TDeployment:
    cloud_mode = int(client.execute_sql("""
        SELECT value FROM system.settings WHERE name = 'cloud_mode'
    """)[0][0])
    return "ClickHouseCloud" if cloud_mode else "ClickHouseOSS"


def get_partition_key(sql_client: ClickHouseSqlClient, table_name: str) -> str:
    """Returns partition key of given table.

    Returns empty string if no partition key is set.
    """
    qry = "SELECT partition_key FROM system.tables WHERE database = %s AND name = %s;"
    table_name = sql_client.make_qualified_table_name(table_name, quote=False)
    database, name = table_name.split(".")
    with sql_client:
        result = sql_client.execute_sql(qry, database, name)
    return result[0][0]
