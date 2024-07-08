from typing import Optional

from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient
from dlt.destinations.impl.motherduck.configuration import MotherDuckCredentials


class MotherDuckSqlClient(DuckDbSqlClient):
    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: MotherDuckCredentials,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(dataset_name, staging_dataset_name, credentials, capabilities)
        self.database_name = credentials.database

    def catalog_name(self, escape: bool = True) -> Optional[str]:
        database_name = self.database_name
        if escape:
            database_name = self.capabilities.escape_identifier(database_name)
        return database_name
