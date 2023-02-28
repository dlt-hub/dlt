import platform

from dlt.common.wei import EVM_DECIMAL_PRECISION
if platform.python_implementation() == "PyPy":
    import psycopg2cffi as psycopg2
    from psycopg2cffi.sql import SQL, Composed
else:
    import psycopg2
    from psycopg2.sql import SQL, Composed


from typing import ClassVar, Dict, Optional

from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.data_types import TDataType
from dlt.common.schema import TColumnSchema, TColumnHint, Schema

from dlt.destinations.insert_job_client import InsertValuesJobClient

from dlt.destinations.postgres import capabilities
from dlt.destinations.postgres.sql_client import Psycopg2SqlClient
from dlt.destinations.postgres.configuration import PostgresClientConfiguration


SCT_TO_PGT: Dict[TDataType, str] = {
    "complex": "jsonb",
    "text": "varchar",
    "double": "double precision",
    "bool": "boolean",
    "timestamp": "timestamp with time zone",
    "date": "date",
    "bigint": "bigint",
    "binary": "bytea",
    "decimal": f"numeric({DEFAULT_NUMERIC_PRECISION},{DEFAULT_NUMERIC_SCALE})"
}

PGT_TO_SCT: Dict[str, TDataType] = {
    "varchar": "text",
    "jsonb": "complex",
    "double precision": "double",
    "boolean": "bool",
    "timestamp with time zone": "timestamp",
    "date": "date",
    "bigint": "bigint",
    "bytea": "binary",
    "numeric": "decimal"
}

HINT_TO_POSTGRES_ATTR: Dict[TColumnHint, str] = {
    "unique": "UNIQUE"
}

class PostgresClient(InsertValuesJobClient):

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: PostgresClientConfiguration) -> None:
        sql_client = Psycopg2SqlClient(
            self.make_dataset_name(schema, config.dataset_name, config.default_schema_name),
            config.credentials
        )
        super().__init__(schema, config, sql_client)
        self.config: PostgresClientConfiguration = config
        self.sql_client = sql_client
        self.active_hints = HINT_TO_POSTGRES_ATTR if self.config.create_indexes else {}

    def _get_column_def_sql(self, c: TColumnSchema) -> str:
        hints_str = " ".join(self.active_hints.get(h, "") for h in self.active_hints.keys() if c.get(h, False) is True)
        column_name = self.capabilities.escape_identifier(c["name"])
        return f"{column_name} {self._to_db_type(c['data_type'])} {hints_str} {self._gen_not_null(c['nullable'])}"

    @staticmethod
    def _to_db_type(sc_t: TDataType) -> str:
        if sc_t == "wei":
            return f"numeric({2*EVM_DECIMAL_PRECISION},{EVM_DECIMAL_PRECISION})"
        return SCT_TO_PGT[sc_t]

    @staticmethod
    def _from_db_type(pq_t: str, precision: Optional[int], scale: Optional[int]) -> TDataType:
        if pq_t == "numeric":
            if precision == 2*EVM_DECIMAL_PRECISION and scale == EVM_DECIMAL_PRECISION:
                return "wei"
        return PGT_TO_SCT.get(pq_t, "text")
