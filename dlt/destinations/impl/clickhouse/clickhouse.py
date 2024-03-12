from typing import ClassVar, Optional

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import SupportsStagingDestination
from dlt.common.schema import Schema, TColumnSchema
from dlt.common.schema.typing import TColumnType, TTableFormat
from dlt.destinations.impl.clickhouse import capabilities
from dlt.destinations.impl.clickhouse.configuration import ClickhouseClientConfiguration
from dlt.destinations.job_client_impl import SqlJobClientWithStaging
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.typing import TNativeConn


class ClickhouseClient(SqlJobClientWithStaging, SupportsStagingDestination):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(
        self,
        schema: Schema,
        config: ClickhouseClientConfiguration,
        sql_client: SqlClientBase[TNativeConn],
    ) -> None:
        super().__init__(schema, config, sql_client)
        ...

    def _get_column_def_sql(self, c: TColumnSchema, table_format: TTableFormat = None) -> str:
        pass

    def _from_db_type(
        self, db_type: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        pass
