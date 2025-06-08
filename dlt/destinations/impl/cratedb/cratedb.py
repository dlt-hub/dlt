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
