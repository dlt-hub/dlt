import platform
import os

from dlt.destinations.postgres.sql_client import Psycopg2SqlClient
if platform.python_implementation() == "PyPy":
    import psycopg2cffi as psycopg2
    # from psycopg2cffi.sql import SQL, Composed
else:
    import psycopg2
    # from psycopg2.sql import SQL, Composed

from typing import ClassVar, Dict, List, Optional, Sequence, Type

from dlt.common.arithmetics import DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import NewLoadJob
from dlt.common.data_types import TDataType
from dlt.common.schema import TColumnSchema, TColumnHint, Schema
from dlt.common.schema.typing import TTableSchema

from dlt.common.configuration.accessors import config
from dlt.common.configuration import with_config, known_sections
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import AwsCredentials


from dlt.destinations.insert_job_client import InsertValuesJobClient
from dlt.destinations.sql_jobs import SqlMergeJob
from dlt.destinations.exceptions import DatabaseTerminalException
from dlt.destinations.job_client_impl import CopyFileLoadJob, LoadJob

from dlt.destinations.redshift import capabilities
from dlt.destinations.redshift.configuration import RedshiftClientConfiguration



SCT_TO_PGT: Dict[TDataType, str] = {
    "complex": "super",
    "text": "varchar(max)",
    "double": "double precision",
    "bool": "boolean",
    "date": "date",
    "timestamp": "timestamp with time zone",
    "bigint": "bigint",
    "binary": "varbinary",
    "decimal": f"numeric({DEFAULT_NUMERIC_PRECISION},{DEFAULT_NUMERIC_SCALE})"
}

PGT_TO_SCT: Dict[str, TDataType] = {
    "super": "complex",
    "varchar(max)": "text",
    "double precision": "double",
    "boolean": "bool",
    "date": "date",
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


@with_config(spec=AwsCredentials, sections=(known_sections.DESTINATION, "filesystem", "credentials"))
def _s3_config(config: AwsCredentials = config.value) -> AwsCredentials:
    return config


class RedshiftSqlClient(Psycopg2SqlClient):

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

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

class RedshiftCopyFileLoadJob(CopyFileLoadJob):
    def execute(self, table: TTableSchema, bucket_path: str) -> None:

        # we assume s3 credentials where provided for the staging
        config = _s3_config()
        aws_access_key = config.aws_access_key_id if self._forward_staging_credentials else ""
        aws_secret_key = config.aws_secret_access_key if self._forward_staging_credentials else ""
        credentials = f"CREDENTIALS 'aws_access_key_id={aws_access_key};aws_secret_access_key={aws_secret_key}'"
        table_name = table["name"]

        # get format
        ext = os.path.splitext(bucket_path)[1][1:]
        file_type = ""
        dateformat = ""
        compression = ""
        if ext == "jsonl":
            file_type = "FORMAT AS JSON 'auto'"
            dateformat = "dateformat 'auto' timeformat 'auto'"
            compression = "GZIP"
        if ext == "parquet":
            file_type = "PARQUET"
        with self._sql_client.with_staging_dataset(table["write_disposition"] in ["merge", "replace"]):
            with self._sql_client.begin_transaction():
                dataset_name = self._sql_client.dataset_name
                self._sql_client.execute_sql(f"""
                    COPY {dataset_name}.{table_name}
                    FROM '{bucket_path}'
                    {file_type}
                    {dateformat}
                    {compression}
                    {credentials};""")

    def exception(self) -> str:
        # this part of code should be never reached
        raise NotImplementedError()

class RedshiftMergeJob(SqlMergeJob):

    @classmethod
    def gen_key_table_clauses(cls, root_table_name: str, staging_root_table_name: str, key_clauses: Sequence[str], for_delete: bool) -> List[str]:
        """Generate sql clauses that may be used to select or delete rows in root table of destination dataset

            A list of clauses may be returned for engines that do not support OR in subqueries. Like BigQuery
        """
        if for_delete:
            return [f"FROM {root_table_name} WHERE EXISTS (SELECT 1 FROM {staging_root_table_name} WHERE {' OR '.join([c.format(d=root_table_name,s=staging_root_table_name) for c in key_clauses])})"]
        return SqlMergeJob.gen_key_table_clauses(root_table_name, staging_root_table_name, key_clauses, for_delete)


class RedshiftClient(InsertValuesJobClient):

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: RedshiftClientConfiguration) -> None:
        sql_client = RedshiftSqlClient (
            self.make_dataset_name(schema, config.dataset_name, config.default_schema_name),
            config.credentials
        )
        super().__init__(schema, config, sql_client)
        self.sql_client = sql_client
        self.config: RedshiftClientConfiguration = config

    def create_merge_job(self, table_chain: Sequence[TTableSchema]) -> NewLoadJob:
        return RedshiftMergeJob.from_table_chain(table_chain, self.sql_client)

    def _get_column_def_sql(self, c: TColumnSchema) -> str:
        hints_str = " ".join(HINT_TO_REDSHIFT_ATTR.get(h, "") for h in HINT_TO_REDSHIFT_ATTR.keys() if c.get(h, False) is True)
        column_name = self.capabilities.escape_identifier(c["name"])
        return f"{column_name} {self._to_db_type(c['data_type'])} {hints_str} {self._gen_not_null(c['nullable'])}"

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        """Starts SqlLoadJob for files ending with .sql or returns None to let derived classes to handle their specific jobs"""
        if RedshiftCopyFileLoadJob.is_reference_job(file_path):
            return RedshiftCopyFileLoadJob(table, file_path, self.sql_client, forward_staging_credentials=self.config.forward_staging_credentials)
        return super().start_file_load(table, file_path, load_id)

    @staticmethod
    def _to_db_type(sc_t: TDataType) -> str:
        if sc_t == "wei":
            return f"numeric({DEFAULT_NUMERIC_PRECISION},0)"
        return SCT_TO_PGT[sc_t]

    @staticmethod
    def _from_db_type(pq_t: str, precision: Optional[int], scale: Optional[int]) -> TDataType:
        if pq_t == "numeric":
            if precision == DEFAULT_NUMERIC_PRECISION and scale == 0:
                return "wei"
        return PGT_TO_SCT.get(pq_t, "text")

