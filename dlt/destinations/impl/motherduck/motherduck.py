from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.schema import Schema

from dlt.destinations.impl.duckdb.duck import DuckDbClient
from dlt.destinations.insert_job_client import InsertValuesJobClient
from dlt.destinations.impl.motherduck.sql_client import MotherDuckSqlClient
from dlt.destinations.impl.motherduck.configuration import MotherDuckClientConfiguration


class MotherDuckClient(DuckDbClient):
    def __init__(
        self,
        schema: Schema,
        config: MotherDuckClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        # IMPORTANT:
        # We intentionally DO NOT call DuckDbClient.__init__
        # because it would create a DuckDbSqlClient.
        #
        # Instead, we replicate its constructor logic
        # but inject MotherDuckSqlClient.

        dataset_name, staging_dataset_name = InsertValuesJobClient.create_dataset_names(
            schema, config
        )

        sql_client = MotherDuckSqlClient(
            dataset_name,
            staging_dataset_name,
            config.credentials,
            capabilities,
        )

        InsertValuesJobClient.__init__(self, schema, config, sql_client)

        self.config: MotherDuckClientConfiguration = config  # type: ignore
        self.sql_client: MotherDuckSqlClient = sql_client
        self.active_hints = {}  # DuckDB behavior (create_indexes=False)
        self.type_mapper = capabilities.get_type_mapper()
