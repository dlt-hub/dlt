from typing import Dict

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.client import (
    PreparedTableSchema,
    LoadJob,
)
from dlt.common.schema import TColumnHint, Schema
from dlt.destinations.impl.cratedb.configuration import CrateDbClientConfiguration
from dlt.destinations.impl.cratedb.sql_client import CrateDbSqlClient
from dlt.destinations.impl.postgres.postgres import PostgresClient
from dlt.destinations.insert_job_client import InsertValuesJobClient

# FIXME: The `UNIQUE` constraint is dearly missing.
#        When loading data multiple times, duplicates will happen.
HINT_TO_CRATEDB_ATTR: Dict[TColumnHint, str] = {"unique": ""}


class CrateDbClient(PostgresClient):
    def __init__(
        self,
        schema: Schema,
        config: CrateDbClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        sql_client = CrateDbSqlClient(
            config.normalize_dataset_name(schema),
            config.normalize_staging_dataset_name(schema),
            config.credentials,
            capabilities,
        )
        InsertValuesJobClient.__init__(self, schema, config, sql_client)
        self.config: CrateDbClientConfiguration = config
        self.sql_client: CrateDbSqlClient = sql_client
        self.active_hints = HINT_TO_CRATEDB_ATTR if self.config.create_indexes else {}
        self.type_mapper = self.capabilities.get_type_mapper()

    def create_load_job(
        self,
        table: PreparedTableSchema,
        file_path: str,
        load_id: str,
        restore: bool = False,
    ) -> LoadJob:
        job = InsertValuesJobClient.create_load_job(self, table, file_path, load_id, restore)
        if job is not None:
            return job
        return None

    def complete_load(self, load_id: str) -> None:
        """
        Intercept to invoke a `REFRESH TABLE ...` statement.
        """
        result = super().complete_load(load_id=load_id)
        table_name = self.sql_client.make_qualified_table_name(self.schema.loads_table_name)
        self.sql_client.execute_sql(f"REFRESH TABLE {table_name}")
        return result

    def _commit_schema_update(self, schema: Schema, schema_str: str) -> None:
        """
        Intercept to invoke a `REFRESH TABLE ...` statement.
        """
        result = super()._commit_schema_update(schema=schema, schema_str=schema_str)
        table_name = self.sql_client.make_qualified_table_name(self.schema.version_table_name)
        self.sql_client.execute_sql(f"REFRESH TABLE {table_name}")
        return result

    def _delete_schema_in_storage(self, schema: Schema) -> None:
        """
        Intercept to invoke a `REFRESH TABLE ...` statement.
        """
        result = super()._delete_schema_in_storage(schema=schema)
        table_name = self.sql_client.make_qualified_table_name(self.schema.version_table_name)
        self.sql_client.execute_sql(f"REFRESH TABLE {table_name}")
        return result

    def _insert_statement_from_select_statement(
        self, select_dialect: str, select_statement: str
    ) -> str:
        """
        Intercept to invoke a `REFRESH TABLE ...` statement.
        """
        result = super()._insert_statement_from_select_statement(
            select_dialect=select_dialect, select_statement=select_statement
        )
        table_name = self.sql_client.make_qualified_table_name(self._load_table["name"])
        self.sql_client.execute_sql(f"REFRESH TABLE {table_name}")
        return result
