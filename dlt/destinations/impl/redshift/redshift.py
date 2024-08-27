import platform
import os

if platform.python_implementation() == "PyPy":
    import psycopg2cffi as psycopg2

    # from psycopg2cffi.sql import SQL, Composed
else:
    import psycopg2

    # from psycopg2.sql import SQL, Composed

from typing import Dict, List, Optional, Sequence, Any, Tuple


from dlt.common.destination.reference import (
    FollowupJobRequest,
    CredentialsConfiguration,
    SupportsStagingDestination,
    LoadJob,
)
from dlt.common.data_types import TDataType
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.schema import TColumnSchema, TColumnHint, Schema
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema.utils import table_schema_has_type, table_schema_has_type_with_precision
from dlt.common.schema.typing import TTableSchema, TColumnType, TTableFormat, TTableSchemaColumns
from dlt.common.configuration.specs import AwsCredentialsWithoutDefaults

from dlt.destinations.insert_job_client import InsertValuesJobClient
from dlt.destinations.sql_jobs import SqlMergeFollowupJob
from dlt.destinations.exceptions import DatabaseTerminalException, LoadJobTerminalException
from dlt.destinations.job_client_impl import CopyRemoteFileLoadJob
from dlt.destinations.impl.postgres.sql_client import Psycopg2SqlClient
from dlt.destinations.impl.redshift.configuration import RedshiftClientConfiguration
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.type_mapping import TypeMapper


HINT_TO_REDSHIFT_ATTR: Dict[TColumnHint, str] = {
    "cluster": "DISTKEY",
    # it is better to not enforce constraints in redshift
    # "primary_key": "PRIMARY KEY",
    "sort": "SORTKEY",
}


class RedshiftTypeMapper(TypeMapper):
    sct_to_unbound_dbt = {
        "complex": "super",
        "text": "varchar(max)",
        "double": "double precision",
        "bool": "boolean",
        "date": "date",
        "timestamp": "timestamp with time zone",
        "bigint": "bigint",
        "binary": "varbinary",
        "time": "time without time zone",
    }

    sct_to_dbt = {
        "decimal": "numeric(%i,%i)",
        "wei": "numeric(%i,%i)",
        "text": "varchar(%i)",
        "binary": "varbinary(%i)",
    }

    dbt_to_sct = {
        "super": "complex",
        "varchar(max)": "text",
        "double precision": "double",
        "boolean": "bool",
        "date": "date",
        "timestamp with time zone": "timestamp",
        "bigint": "bigint",
        "binary varying": "binary",
        "numeric": "decimal",
        "time without time zone": "time",
        "varchar": "text",
        "smallint": "bigint",
        "integer": "bigint",
    }

    def to_db_integer_type(
        self, precision: Optional[int], table_format: TTableFormat = None
    ) -> str:
        if precision is None:
            return "bigint"
        if precision <= 16:
            return "smallint"
        elif precision <= 32:
            return "integer"
        elif precision <= 64:
            return "bigint"
        raise TerminalValueError(
            f"bigint with {precision} bits precision cannot be mapped into postgres integer type"
        )

    def from_db_type(
        self, db_type: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        if db_type == "numeric":
            if (precision, scale) == self.capabilities.wei_precision:
                return dict(data_type="wei")
        return super().from_db_type(db_type, precision, scale)


class RedshiftSqlClient(Psycopg2SqlClient):
    @staticmethod
    def _maybe_make_terminal_exception_from_data_error(
        pg_ex: psycopg2.DataError,
    ) -> Optional[Exception]:
        if "Cannot insert a NULL value into column" in pg_ex.pgerror:
            # NULL violations is internal error, probably a redshift thing
            return DatabaseTerminalException(pg_ex)
        if "Numeric data overflow" in pg_ex.pgerror:
            return DatabaseTerminalException(pg_ex)
        if "Precision exceeds maximum" in pg_ex.pgerror:
            return DatabaseTerminalException(pg_ex)
        return None


class RedshiftCopyFileLoadJob(CopyRemoteFileLoadJob):
    def __init__(
        self,
        file_path: str,
        staging_credentials: Optional[CredentialsConfiguration] = None,
        staging_iam_role: str = None,
    ) -> None:
        super().__init__(file_path, staging_credentials)
        self._staging_iam_role = staging_iam_role
        self._job_client: "RedshiftClient" = None

    def run(self) -> None:
        self._sql_client = self._job_client.sql_client
        # we assume s3 credentials where provided for the staging
        credentials = ""
        if self._staging_iam_role:
            credentials = f"IAM_ROLE '{self._staging_iam_role}'"
        elif self._staging_credentials and isinstance(
            self._staging_credentials, AwsCredentialsWithoutDefaults
        ):
            aws_access_key = self._staging_credentials.aws_access_key_id
            aws_secret_key = self._staging_credentials.aws_secret_access_key
            credentials = (
                "CREDENTIALS"
                f" 'aws_access_key_id={aws_access_key};aws_secret_access_key={aws_secret_key}'"
            )

        # get format
        ext = os.path.splitext(self._bucket_path)[1][1:]
        file_type = ""
        dateformat = ""
        compression = ""
        if table_schema_has_type(self._load_table, "time"):
            raise LoadJobTerminalException(
                self.file_name(),
                f"Redshift cannot load TIME columns from {ext} files. Switch to direct INSERT file"
                " format or convert `datetime.time` objects in your data to `str` or"
                " `datetime.datetime`",
            )
        if ext == "jsonl":
            if table_schema_has_type(self._load_table, "binary"):
                raise LoadJobTerminalException(
                    self.file_name(),
                    "Redshift cannot load VARBYTE columns from json files. Switch to parquet to"
                    " load binaries.",
                )
            file_type = "FORMAT AS JSON 'auto'"
            dateformat = "dateformat 'auto' timeformat 'auto'"
            compression = "GZIP"
        elif ext == "parquet":
            if table_schema_has_type_with_precision(self._load_table, "binary"):
                raise LoadJobTerminalException(
                    self.file_name(),
                    f"Redshift cannot load fixed width VARBYTE columns from {ext} files. Switch to"
                    " direct INSERT file format or use binary columns without precision.",
                )
            file_type = "PARQUET"
            # if table contains complex types then SUPER field will be used.
            # https://docs.aws.amazon.com/redshift/latest/dg/ingest-super.html
            if table_schema_has_type(self._load_table, "complex"):
                file_type += " SERIALIZETOJSON"
        else:
            raise ValueError(f"Unsupported file type {ext} for Redshift.")

        with self._sql_client.begin_transaction():
            # TODO: if we ever support csv here remember to add column names to COPY
            self._sql_client.execute_sql(f"""
                COPY {self._sql_client.make_qualified_table_name(self.load_table_name)}
                FROM '{self._bucket_path}'
                {file_type}
                {dateformat}
                {compression}
                {credentials} MAXERROR 0;""")


class RedshiftMergeJob(SqlMergeFollowupJob):
    @classmethod
    def gen_key_table_clauses(
        cls,
        root_table_name: str,
        staging_root_table_name: str,
        key_clauses: Sequence[str],
        for_delete: bool,
    ) -> List[str]:
        """Generate sql clauses that may be used to select or delete rows in root table of destination dataset

        A list of clauses may be returned for engines that do not support OR in subqueries. Like BigQuery
        """
        if for_delete:
            return [
                f"FROM {root_table_name} WHERE EXISTS (SELECT 1 FROM"
                f" {staging_root_table_name} WHERE"
                f" {' OR '.join([c.format(d=root_table_name,s=staging_root_table_name) for c in key_clauses])})"
            ]
        return SqlMergeFollowupJob.gen_key_table_clauses(
            root_table_name, staging_root_table_name, key_clauses, for_delete
        )


class RedshiftClient(InsertValuesJobClient, SupportsStagingDestination):
    def __init__(
        self,
        schema: Schema,
        config: RedshiftClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        sql_client = RedshiftSqlClient(
            config.normalize_dataset_name(schema),
            config.normalize_staging_dataset_name(schema),
            config.credentials,
            capabilities,
        )
        super().__init__(schema, config, sql_client)
        self.sql_client = sql_client
        self.config: RedshiftClientConfiguration = config
        self.type_mapper = RedshiftTypeMapper(self.capabilities)

    def _create_merge_followup_jobs(
        self, table_chain: Sequence[TTableSchema]
    ) -> List[FollowupJobRequest]:
        return [RedshiftMergeJob.from_table_chain(table_chain, self.sql_client)]

    def _get_column_def_sql(self, c: TColumnSchema, table_format: TTableFormat = None) -> str:
        hints_str = " ".join(
            HINT_TO_REDSHIFT_ATTR.get(h, "")
            for h in HINT_TO_REDSHIFT_ATTR.keys()
            if c.get(h, False) is True
        )
        column_name = self.sql_client.escape_column_name(c["name"])
        return (
            f"{column_name} {self.type_mapper.to_db_type(c)} {hints_str} {self._gen_not_null(c.get('nullable', True))}"
        )

    def create_load_job(
        self, table: TTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        """Starts SqlLoadJob for files ending with .sql or returns None to let derived classes to handle their specific jobs"""
        job = super().create_load_job(table, file_path, load_id, restore)
        if not job:
            assert ReferenceFollowupJobRequest.is_reference_job(
                file_path
            ), "Redshift must use staging to load files"
            job = RedshiftCopyFileLoadJob(
                file_path,
                staging_credentials=self.config.staging_config.credentials,
                staging_iam_role=self.config.staging_iam_role,
            )
        return job

    def _from_db_type(
        self, pq_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_db_type(pq_t, precision, scale)

    def should_truncate_table_before_load_on_staging_destination(self, table: TTableSchema) -> bool:
        return self.config.truncate_tables_on_staging_destination_before_load
