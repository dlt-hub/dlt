from typing import List, Optional

import clickhouse_connect

from dlt.common import logger
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.destinations.impl.clickhouse.sql_client import ClickHouseSqlClient
from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster_adapter import (
    CREATE_DISTRIBUTED_TABLE_HINT,
    SHARDING_KEY_HINT,
    DISTRIBUTED_TABLE_SUFFIX_HINT,
)
from dlt.destinations.impl.clickhouse_cluster.configuration import (
    ClickHouseClusterClientConfiguration,
    ClickHouseClusterCredentials,
)
from dlt.destinations.sql_client import raise_open_connection_error


class ClickHouseClusterSqlClient(ClickHouseSqlClient):
    def __init__(
        self,
        dataset_name: Optional[str],
        staging_dataset_name: str,
        known_table_names: List[str],
        credentials: ClickHouseClusterCredentials,
        capabilities: DestinationCapabilitiesContext,
        config: ClickHouseClusterClientConfiguration,
    ) -> None:
        super().__init__(
            dataset_name,
            staging_dataset_name,
            known_table_names,
            credentials,
            capabilities,
            config,
        )
        self.credentials: ClickHouseClusterCredentials = credentials
        self.config: ClickHouseClusterClientConfiguration = config

    @property
    def distributed_tables_database_name(self) -> Optional[str]:
        return self.config.distributed_tables_database or self.database_name

    @raise_open_connection_error
    def clickhouse_connect_client(self) -> clickhouse_connect.driver.client.Client:  # type: ignore[return]
        # unlike `clickhouse_driver`, `clickhouse_connect` does not support `alt_hosts` (or similar)
        # parameter, so we implement our own failover logic
        # https://github.com/ClickHouse/clickhouse-connect/issues/74
        http_hosts = self.credentials._http_hosts
        for idx, host_port in enumerate(http_hosts):
            host, port = host_port
            try:
                return self._clickhouse_connect_client(host=host, port=port)
            except clickhouse_connect.driver.exceptions.OperationalError as ex:
                is_timeout = "timed out" in str(ex)
                has_next = idx + 1 < len(http_hosts)
                if is_timeout and has_next:
                    next_host, next_port = http_hosts[idx + 1]
                    logger.warning(
                        f"Connection attempt to ClickHouse cluster on {host}:{port} timed out."
                        f" Trying next: {next_host}:{next_port}."
                    )
                    continue
                raise

    def _insert_file_table(self, table_name: str, database_name: str) -> str:
        with self.with_alternative_database_name(database_name):
            return self.make_qualified_table_name(table_name)

    def _make_create_table(
        self, qualified_name: str, or_replace: bool = False, if_not_exists: bool = False
    ) -> str:
        create_table_sql = super()._make_create_table(qualified_name, or_replace, if_not_exists)
        return f"{create_table_sql} ON CLUSTER {self.config.cluster}"

    def _make_drop_table(self, qualified_table_name: str, if_exists: bool = False) -> str:
        if_exists_sql = "IF EXISTS " if if_exists else ""
        cluster = self.config.cluster
        return f"DROP TABLE {if_exists_sql}{qualified_table_name} ON CLUSTER {cluster} SYNC"

    def _make_create_distributed_table(self, table_schema: PreparedTableSchema) -> str:
        table_name = table_schema["name"]

        # generate CREATE TABLE sql
        dist_table_name = table_name + table_schema[DISTRIBUTED_TABLE_SUFFIX_HINT]  # type: ignore[typeddict-item]
        with self.with_alternative_database_name(self.distributed_tables_database_name):
            qual_dist_table_name = self.make_qualified_table_name(dist_table_name)
        create_table_sql = self._make_create_table(qual_dist_table_name)

        # generate AS sql
        as_sql = "AS " + self.make_qualified_table_name(table_name)

        # generate ENGINE clause
        cluster = self.config.cluster
        database, table = self.make_qualified_table_name(table_name, quote=False).split(".")
        sharding_key = table_schema[SHARDING_KEY_HINT]  # type: ignore[typeddict-item]
        engine_sql = self._make_distributed_engine_clause(cluster, database, table, sharding_key)

        return f"{create_table_sql} {as_sql} {engine_sql};"

    @staticmethod
    def _make_distributed_engine_clause(
        cluster: str, database: str, table: str, sharding_key: str
    ) -> str:
        return f"ENGINE = Distributed('{cluster}', '{database}', '{table}', {sharding_key})"
