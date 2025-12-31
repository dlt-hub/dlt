import pytest

from typing import Literal

import dlt
from dlt.common.schema import Schema
from dlt.common.utils import uniq_id
from dlt.destinations import clickhouse
from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseClient
from dlt.destinations.impl.clickhouse.configuration import (
    ClickHouseClientConfiguration,
    ClickHouseCredentials,
)
from dlt.destinations.impl.clickhouse.sql_client import ClickHouseSqlClient
from dlt.destinations.impl.clickhouse.typing import TColumnCodecs, TDeployment, TMergeTreeSettings
from dlt.extract import DltResource


# this constant relates to `clickhouse_adapter_resource`
CLICKHOUSE_ADAPTER_CASES = (
    # `sort`/`partition` input, expected ORDER BY / PARTITION clause, expected sorting/partition key
    # expressions (we do not mix in uppercase names because expressions are not normalized)
    pytest.param("town", "town", "town", id="expr-simple"),
    pytest.param("(town, number % 4)", "(town, number % 4)", "town, number % 4", id="expr-complex"),
    # sequences of column names (we mix in uppercase names to verify normalization)
    pytest.param(["TOWN"], "(town)", "town", id="seq-single"),
    pytest.param(("street", "TOWN"), "(street, town)", "street, town", id="seq-multi"),
)

CLICKHOUSE_ADAPTER_SETTINGS_CASE: tuple[TMergeTreeSettings, str] = (
    # settings dict, expected SETTINGS clause
    {
        "allow_nullable_key": True,
        "max_suspicious_broken_parts": 500,
        "deduplicate_merge_projection_mode": "ignore",
        "merge_selecting_sleep_slowdown_factor": 1.2,
    },
    (
        "allow_nullable_key = true,"
        " max_suspicious_broken_parts = 500,"
        " deduplicate_merge_projection_mode = 'ignore',"
        " merge_selecting_sleep_slowdown_factor = 1.2"
    ),
)


# this fixture relates to `CLICKHOUSE_ADAPTER_CASES`
@pytest.fixture
def clickhouse_adapter_resource() -> DltResource:
    @dlt.resource(
        columns={
            "TOWN": {"nullable": False, "data_type": "text"},
            "street": {"nullable": False, "data_type": "text"},
            "number": {"nullable": False, "data_type": "bigint"},
        }
    )
    def data():
        yield [{"TOWN": "Dubai", "street": "Sheikh Zayed Road", "number": 1}]

    return data()


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


def get_codecs(sql_client: ClickHouseSqlClient, table_name: str) -> TColumnCodecs:
    """Returns mapping of column names to their codecs for given table.

    If no codec is set for a column, its value is an empty string.
    """
    qry = "SELECT name, compression_codec FROM system.columns WHERE database = %s AND table = %s;"
    table_name = sql_client.make_qualified_table_name(table_name, quote=False)
    database, name = table_name.split(".")
    with sql_client:
        columns = sql_client.execute_sql(qry, database, name)

    return {col[0]: col[1] for col in columns}


def get_sorting_key(sql_client: ClickHouseSqlClient, table_name: str) -> str:
    """Returns sorting key of given table.

    - returns empty string if no sorting key is set
    - returns composite key as tuple WITHOUT parentheses
    """
    return _get_key(sql_client, table_name, "sorting")


def get_partition_key(sql_client: ClickHouseSqlClient, table_name: str) -> str:
    """Returns partition key of given table.

    - returns empty string if no partition key is set
    - returns composite key as tuple WITH parentheses
    """
    return _get_key(sql_client, table_name, "partition")


def _get_key(
    sql_client: ClickHouseSqlClient, table_name: str, key_type: Literal["sorting", "partition"]
) -> str:
    """Returns sorting or partition key of given table.

    - returns empty string if no such key is set
    - returns composite key as tuple WITHOUT parentheses
    """
    qry = f"SELECT {key_type}_key FROM system.tables WHERE database = %s AND name = %s;"
    table_name = sql_client.make_qualified_table_name(table_name, quote=False)
    database, name = table_name.split(".")
    with sql_client:
        result = sql_client.execute_sql(qry, database, name)

    key = result[0][0]

    # partition key has parentheses if it's composite; remove them for uniformity
    if key.startswith("(") and key.endswith(")"):
        return key[1:-1]

    return key


def get_create_table_query(sql_client: ClickHouseSqlClient, table_name: str) -> str:
    qry = "SELECT create_table_query FROM system.tables WHERE database = %s AND name = %s;"
    table_name = sql_client.make_qualified_table_name(table_name, quote=False)
    database, name = table_name.split(".")
    with sql_client:
        result = sql_client.execute_sql(qry, database, name)

    return result[0][0]
