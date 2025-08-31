import platform
import os
from dlt.common import logger

if platform.python_implementation() == "PyPy":
    import psycopg2cffi as psycopg2

    # from psycopg2cffi.sql import SQL, Composed
else:
    import psycopg2

    # from psycopg2.sql import SQL, Composed

from typing import Dict, List, Optional, Sequence

from dlt.common.destination.client import (
    FollowupJobRequest,
    CredentialsConfiguration,
    PreparedTableSchema,
    SupportsStagingDestination,
    LoadJob,
)
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.schema import TColumnSchema, TColumnHint, Schema
from dlt.common.schema.utils import table_schema_has_type
from dlt.common.schema.typing import TColumnType
from dlt.common.configuration.specs import AwsCredentialsWithoutDefaults

from dlt.destinations.insert_job_client import InsertValuesJobClient
from dlt.destinations.sql_jobs import SqlMergeFollowupJob
from dlt.destinations.exceptions import DatabaseTerminalException
from dlt.destinations.job_client_impl import CopyRemoteFileLoadJob
from dlt.destinations.impl.postgres.sql_client import Psycopg2SqlClient
from dlt.destinations.impl.redshift.configuration import RedshiftClientConfiguration
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.path_utils import get_file_format_and_compression


HINT_TO_REDSHIFT_ATTR: Dict[TColumnHint, str] = {
    "cluster": "DISTKEY",
    # it is better to not enforce constraints in redshift
    # "primary_key": "PRIMARY KEY",
    "sort": "SORTKEY",
}


class RedshiftSqlClient(Psycopg2SqlClient):
    def has_dataset(self) -> bool:
        # In Redshift, the 'public' schema always exists but may not be
        # returned by INFORMATION_SCHEMA.SCHEMATA query, so we handle it as a special case
        if self.dataset_name == "public":
            return True

        return super().has_dataset()

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
        aws_region = (
            self._staging_credentials.get("region_name", "") if self._staging_credentials else ""
        )
        region = f"region '{aws_region}'" if aws_region else ""
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
        file_format, is_compressed = get_file_format_and_compression(self._bucket_path)
        file_type = ""
        dateformat = ""
        compression = ""
        if file_format == "jsonl":
            file_type = "FORMAT AS JSON 'auto'"
            dateformat = "dateformat 'auto' timeformat 'auto'"
            compression = "" if not is_compressed else "GZIP"
        elif file_format == "parquet":
            # Redshift doesn't support copying across regions for columnar data formats
            # https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-columnar.html  # noqa: E501
            if region:
                logger.warning(
                    "Redshift doesn't support copying Parquet files across regions. The region"
                    " parameter will be ignored."
                )
                region = ""

            file_type = "PARQUET"
            # if table contains json types then SUPER field will be used.
            # https://docs.aws.amazon.com/redshift/latest/dg/ingest-super.html
            if table_schema_has_type(self._load_table, "json"):
                file_type += " SERIALIZETOJSON"
        else:
            raise ValueError(f"Unsupported file type `{file_format}` for Redshift.")

        with self._sql_client.begin_transaction():
            # TODO: if we ever support csv here remember to add column names to COPY
            self._sql_client.execute_sql(f"""
                COPY {self._sql_client.make_qualified_table_name(self.load_table_name)}
                FROM '{self._bucket_path}'
                {file_type}
                {dateformat}
                {compression}
                {credentials}
                {region} MAXERROR 0;""")


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
        self.active_hints = HINT_TO_REDSHIFT_ATTR
        self.sql_client = sql_client
        self.config: RedshiftClientConfiguration = config
        self.type_mapper = self.capabilities.get_type_mapper()

    def _create_merge_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        return [RedshiftMergeJob.from_table_chain(table_chain, self.sql_client)]

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
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
        return self.type_mapper.from_destination_type(pq_t, precision, scale)

    def should_truncate_table_before_load_on_staging_destination(self, table_name: str) -> bool:
        return self.config.truncate_tables_on_staging_destination_before_load
