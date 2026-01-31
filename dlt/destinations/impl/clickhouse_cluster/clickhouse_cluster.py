from typing import List, Optional, Sequence, cast

from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TColumnSchema
from dlt.common.schema.utils import get_inherited_table_hint
from dlt.destinations.impl.clickhouse.clickhouse import (
    ClickHouseClient,
    ClickHouseLoadJob,
)
from dlt.destinations.impl.clickhouse_cluster.clickhouse_cluster_adapter import (
    CONFIG_HINT_MAP,
    CREATE_DISTRIBUTED_TABLES_HINT,
    DISTRIBUTED_TABLE_SUFFIX_HINT,
)
from dlt.destinations.impl.clickhouse_cluster.configuration import (
    ClickHouseClusterClientConfiguration,
)
from dlt.destinations.impl.clickhouse_cluster.sql_client import ClickHouseClusterSqlClient


class ClickHouseClusterLoadJob(ClickHouseLoadJob):
    def __init__(
        self,
        file_path: str,
        config: ClickHouseClusterClientConfiguration,
        staging_credentials: Optional[CredentialsConfiguration] = None,
    ) -> None:
        super().__init__(file_path, config, staging_credentials)
        self._job_client: "ClickHouseClusterClient" = None

    @property
    def load_into_distributed_table(self) -> bool:
        return cast(bool, self._load_table.get(CREATE_DISTRIBUTED_TABLES_HINT, False))

    @property
    def load_table_name(self) -> str:
        return self._job_client.sql_client.get_insert_table_name(self._load_table)

    @property
    def load_database_name(self) -> str:
        if self.load_into_distributed_table:
            return self._job_client.sql_client.distributed_tables_database_name
        return super().load_database_name


class ClickHouseClusterClient(ClickHouseClient):
    def __init__(
        self,
        schema: Schema,
        config: ClickHouseClusterClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)
        self.config: ClickHouseClusterClientConfiguration = config
        self.sql_client: ClickHouseClusterSqlClient = self.sql_client

    @property
    def sql_client_class(self) -> type[ClickHouseClusterSqlClient]:
        return ClickHouseClusterSqlClient

    @property
    def load_job_class(self) -> type[ClickHouseClusterLoadJob]:
        return ClickHouseClusterLoadJob

    def prepare_load_table(self, table_name: str) -> PreparedTableSchema:
        table = super().prepare_load_table(table_name)

        # inherit distributed table hints if not set
        # NOTE: we don't inherit SHARDING_KEY_HINT, because it may contain columns not
        # present in child table; instead, we fall back to robust default from config
        if CREATE_DISTRIBUTED_TABLES_HINT not in table:
            table[CREATE_DISTRIBUTED_TABLES_HINT] = get_inherited_table_hint(  # type: ignore[typeddict-unknown-key]
                self.schema.tables,
                table_name,
                CREATE_DISTRIBUTED_TABLES_HINT,
                allow_none=True,
            )
        if DISTRIBUTED_TABLE_SUFFIX_HINT not in table:
            table[DISTRIBUTED_TABLE_SUFFIX_HINT] = get_inherited_table_hint(  # type: ignore[typeddict-unknown-key]
                self.schema.tables,
                table_name,
                DISTRIBUTED_TABLE_SUFFIX_HINT,
                allow_none=True,
            )

        # fall back to default values from config if hints are still not set
        for config_key, hint_key in CONFIG_HINT_MAP.items():
            if table.get(hint_key) is None:
                table[hint_key] = self.config.get(config_key)  # type: ignore[literal-required]

        return table

    def _get_table_update_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> List[str]:
        sql = super()._get_table_update_sql(table_name, new_columns, generate_alter)

        table = self.prepare_load_table(table_name)

        if table.get(CREATE_DISTRIBUTED_TABLES_HINT):
            sql.append(self.sql_client._make_create_or_replace_distributed_table(table))

        return sql
