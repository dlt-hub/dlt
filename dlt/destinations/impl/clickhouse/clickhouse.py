from typing import ClassVar, Optional

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import SupportsStagingDestination
from dlt.common.schema import Schema, TColumnSchema
from dlt.common.schema.typing import TColumnType, TTableFormat
from dlt.destinations.impl.clickhouse import capabilities
from dlt.destinations.impl.clickhouse.configuration import ClickhouseClientConfiguration
from dlt.destinations.job_client_impl import SqlJobClientWithStaging
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.type_mapping import TypeMapper
from dlt.destinations.typing import TNativeConn


class ClickhouseTypeMapper(TypeMapper):
    sct_to_unbound_dbt = {
        "complex": "JSON",
        "text": "String",
        "double": "Float64",
        "bool": "Boolean",
        "date": "Date",
        "timestamp": "DateTime",
        "bigint": "Int64",
        "binary": "String",
        "wei": "Decimal",
    }

    sct_to_dbt = {
        "decimal": "Decimal(%i,%i)",
        "wei": "Decimal(%i,%i)",
    }

    dbt_to_sct = {
        "String": "text",
        "Float64": "double",
        "Boolean": "bool",
        "Date": "date",
        "DateTime": "timestamp",
        "Int64": "bigint",
        "JSON": "complex",
        "Decimal": "decimal",
    }

    def to_db_time_type(self, precision: Optional[int], table_format: TTableFormat = None) -> str:
        return "DateTime"


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
