import dlt

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.impl.duckdb.duck import DuckDbClient
from dlt.destinations.impl.ducklake.sql_client import DuckLakeSqlClient
from dlt.destinations.impl.ducklake.configuration import DuckLakeClientConfiguration


class DuckLakeClient(DuckDbClient):
    def __init__(
        self,
        schema: dlt.Schema,
        config: DuckLakeClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)       
        self.sql_client: DuckLakeSqlClient = DuckLakeSqlClient(
            dataset_name=config.normalize_dataset_name(schema),
            staging_dataset_name=config.normalize_staging_dataset_name(schema),
            credentials=config.credentials,
            capabilities=capabilities,
        ) 
        self.config: DuckLakeClientConfiguration = config