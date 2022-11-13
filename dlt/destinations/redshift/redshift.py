import platform

from dlt.destinations.postgres.sql_client import Psycopg2SqlClient
if platform.python_implementation() == "PyPy":
    import psycopg2cffi as psycopg2
    # from psycopg2cffi.sql import SQL, Composed
else:
    import psycopg2
    # from psycopg2.sql import SQL, Composed

from typing import Dict, Optional

from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.schema import TColumnSchema, TDataType, TColumnHint, Schema

from dlt.destinations.exceptions import DatabaseTerminalException

from dlt.destinations.redshift import capabilities
from dlt.destinations.redshift.configuration import RedshiftClientConfiguration
from dlt.destinations.postgres.postgres import PostgresClientBase


SCT_TO_PGT: Dict[TDataType, str] = {
    "complex": "varchar(max)",
    "text": "varchar(max)",
    "double": "double precision",
    "bool": "boolean",
    "timestamp": "timestamp with time zone",
    "bigint": "bigint",
    "binary": "varbinary",
    "decimal": f"numeric({DEFAULT_NUMERIC_PRECISION},{DEFAULT_NUMERIC_SCALE})"
}

PGT_TO_SCT: Dict[str, TDataType] = {
    "varchar(max)": "text",
    "double precision": "double",
    "boolean": "bool",
    "timestamp with time zone": "timestamp",
    "bigint": "bigint",
    "binary varying": "binary",
    "numeric": "decimal"
}

HINT_TO_REDSHIFT_ATTR: Dict[TColumnHint, str] = {
    "cluster": "DISTKEY",
    # it is better to not enforce constraints in redshift
    # "primary_key": "PRIMARY KEY",
    "sort": "SORTKEY"
}


class RedshiftSqlClient(Psycopg2SqlClient):
    @staticmethod
    def _maybe_make_terminal_exception_from_data_error(pg_ex: psycopg2.DataError) -> Optional[Exception]:
        if "Cannot insert a NULL value into column" in pg_ex.pgerror:
            # NULL violations is internal error, probably a redshift thing
            return DatabaseTerminalException(pg_ex)
        if "Numeric data overflow" in pg_ex.pgerror:
            return DatabaseTerminalException(pg_ex)
        if "Precision exceeds maximum" in pg_ex.pgerror:
            return DatabaseTerminalException(pg_ex)
        return None


class RedshiftClient(PostgresClientBase):

    def __init__(self, schema: Schema, config: RedshiftClientConfiguration) -> None:
        sql_client = RedshiftSqlClient (
            schema.normalize_make_dataset_name(config.dataset_name, config.default_schema_name, schema.name),
            config.credentials
        )
        super().__init__(schema, config, sql_client)
        self.sql_client = sql_client
        self.config: RedshiftClientConfiguration = config
        self.caps = self.capabilities()

    def _get_column_def_sql(self, c: TColumnSchema) -> str:
        hints_str = " ".join(HINT_TO_REDSHIFT_ATTR.get(h, "") for h in HINT_TO_REDSHIFT_ATTR.keys() if c.get(h, False) is True)
        column_name = self.caps.escape_identifier(c["name"])
        return f"{column_name} {self._sc_t_to_pq_t(c['data_type'])} {hints_str} {self._gen_not_null(c['nullable'])}"

    @staticmethod
    def _sc_t_to_pq_t(sc_t: TDataType) -> str:
        if sc_t == "wei":
            return f"numeric({DEFAULT_NUMERIC_PRECISION},0)"
        return SCT_TO_PGT[sc_t]

    @staticmethod
    def _pq_t_to_sc_t(pq_t: str, precision: Optional[int], scale: Optional[int]) -> TDataType:
        if pq_t == "numeric":
            if precision == DEFAULT_NUMERIC_PRECISION and scale == 0:
                return "wei"
        return PGT_TO_SCT.get(pq_t, "text")

    @classmethod
    def capabilities(cls) -> DestinationCapabilitiesContext:
        return capabilities()
