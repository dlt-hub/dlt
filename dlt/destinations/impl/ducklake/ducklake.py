import dlt

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.impl.duckdb.duck import DuckDbClient
from dlt.destinations.impl.ducklake.sql_client import DuckLakeSqlClient
from dlt.destinations.impl.ducklake.configuration import DuckLakeClientConfiguration


class DuckLakeClient(DuckDbClient):
    """Destination client to interact with a DuckLake

    A DuckLake has 3 components:
        - ducklake client: this is a `duckdb` instance with the `ducklake` extension
        - catalog: this is an SQL database storing metadata. It can be a duckdb instance
            (typically the ducklake client) or a remote database (sqlite, postgres, mysql)
        - storage: this is a filesystem where data is stored in files

    The dlt DuckLake destination gives access to the "ducklake client".
    You never have to manage the catalog and storage directly;
    this is done through the ducklake client.
    """

    def __init__(
        self,
        schema: dlt.Schema,
        config: DuckLakeClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)       
        self.config: DuckLakeClientConfiguration = config
        self.sql_client: DuckLakeSqlClient = DuckLakeSqlClient(
            dataset_name=config.normalize_dataset_name(schema),
            staging_dataset_name=config.normalize_staging_dataset_name(schema),
            credentials=config.credentials,
            capabilities=capabilities,
        ) 
