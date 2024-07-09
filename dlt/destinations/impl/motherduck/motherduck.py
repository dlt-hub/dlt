from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.schema import Schema


from dlt.destinations.impl.duckdb.duck import DuckDbClient
from dlt.destinations.impl.motherduck.sql_client import MotherDuckSqlClient
from dlt.destinations.impl.motherduck.configuration import MotherDuckClientConfiguration


class MotherDuckClient(DuckDbClient):
    def __init__(
        self,
        schema: Schema,
        config: MotherDuckClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)  # type: ignore
        sql_client = MotherDuckSqlClient(
            config.normalize_dataset_name(schema),
            config.normalize_staging_dataset_name(schema),
            config.credentials,
            capabilities,
        )
        self.config: MotherDuckClientConfiguration = config  # type: ignore
        self.sql_client: MotherDuckSqlClient = sql_client
