from typing import Type, Union, Dict, Optional, Any
from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.destinations.impl.starrocks.configuration import (
    StarrocksClientConfiguration,
    StarrocksCredentials
)
from dlt.destinations.type_mapping import TypeMapperImpl
from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.destinations.impl.sqlalchemy.factory import sqlalchemy
from dlt.common.schema.typing import TColumnSchema
from dlt.common.destination.typing import PreparedTableSchema
import sqlalchemy as sa
from sqlalchemy.sql import sqltypes
from sqlalchemy.types import VARCHAR, BIGINT, DATETIME, DOUBLE, BOOLEAN, BINARY, JSON, DATE, DATETIME
from starrocks.datatype import DECIMAL

class StarrocksTypeMapper(TypeMapperImpl):
    sct_to_unbound_dbt = {
        "text": VARCHAR(1000000),
        "double": DOUBLE,
        "bool": BOOLEAN,
        "bigint": BIGINT,
        "timestamp": DATETIME,
        "binary": BINARY,
        "json": JSON,
        "decimal": DECIMAL,
        "date": DATE,
        "datetime": DATETIME
    }

    # sct_to_dbt = {
    #     "text": "VARCHAR(%i)",
    #     "decimal": "DECIMAL(%i,%i)"
    # }

    dbt_to_sct = {
    }

    def to_destination_type(  # type: ignore[override]
        self, column: TColumnSchema, table: PreparedTableSchema = None
    ) -> sqltypes.TypeEngine:
        sc_t = column['data_type']
        precision, scale = column.get("precision"), column.get("scale")

        if sc_t == 'decimal':
            return self.to_db_decimal_type(column)
        elif sc_t == 'text':
            if precision != None:
                return VARCHAR(precision)
            else:
                return VARCHAR(1000000)
        else:
            return self.sct_to_unbound_dbt[sc_t]

        # return super().to_destination_type(column, table)

    def to_db_decimal_type(self, column: TColumnSchema) -> sa.types.TypeEngine:
        precision, scale = column.get("precision"), column.get("scale")
        if precision is None and scale is None:
            precision, scale = self.capabilities.decimal_precision
        return DECIMAL(precision, scale)


class starrocks(sqlalchemy):
    spec = StarrocksClientConfiguration

    @property
    def client_class(self) -> Type["StarrocksJobClient"]:
        from dlt.destinations.impl.starrocks.job_client import StarrocksJobClient
        return StarrocksJobClient

    def __init__(
        self,
        credentials: Union[StarrocksCredentials, Dict[str, Any]] = None,
        # default_table_type: str = 'primary_key',
        destination_name: Optional[str] = None,
        environment: Optional[str] = None,
        engine_args: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            credentials=credentials,
            destination_name=destination_name,
            environment=environment,
            engine_args=engine_args,
            **kwargs,
        )
    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = super()._raw_capabilities()
        caps.preferred_load_file_format = "typed-jsonl"
        caps.supported_loader_file_formats = ["typed-jsonl"]
        caps.preferred_staging_file_format = "parquet"
        caps.supported_staging_file_formats = ["parquet"]
        caps.sqlglog_dialect = "starrocks"
        caps.naming_convention = "dlt.destinations.impl.starrocks.naming"
        caps.decimal_precision = (DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE)
        caps.type_mapper = StarrocksTypeMapper

        return caps



starrocks.register()
