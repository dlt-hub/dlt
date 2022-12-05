
from contextlib import contextmanager
from typing import Any, AnyStr, Iterator, List, Optional, Sequence
from dlt.common.typing import StrAny
from dlt.destinations.exceptions import DatabaseTerminalException, DatabaseTransientException, DatabaseUndefinedRelation
import google.cloud.bigquery as bigquery  # noqa: I250
from google.cloud.bigquery.dbapi import Connection as DbApiConnection
from google.cloud import exceptions as gcp_exceptions
from google.cloud.bigquery.dbapi import exceptions as dbapi_exceptions
from google.api_core import exceptions as api_core_exceptions

from dlt.common import logger
from dlt.common.configuration.specs import GcpClientCredentials

from dlt.destinations.typing import DBCursor
from dlt.destinations.sql_client import SqlClientBase, raise_database_error, raise_open_connection_error

# terminal reasons as returned in BQ gRPC error response
# https://cloud.google.com/bigquery/docs/error-messages
BQ_TERMINAL_REASONS = ["billingTierLimitExceeded", "duplicate", "invalid", "notFound", "notImplemented", "stopped", "tableUnavailable"]
# invalidQuery is an transient error -> must be fixed by programmer


class BigQuerySqlClient(SqlClientBase[bigquery.Client]):
    def __init__(self, dataset_name: str, credentials: GcpClientCredentials) -> None:
        self._client: bigquery.Client = None
        self.credentials: GcpClientCredentials = credentials
        super().__init__(dataset_name)

        self.default_retry = bigquery.DEFAULT_RETRY.with_deadline(credentials.retry_deadline)
        self.default_query = bigquery.QueryJobConfig(default_dataset=self.fully_qualified_dataset_name())

    @raise_open_connection_error
    def open_connection(self) -> None:
        self._client = bigquery.Client(
            self.credentials.project_id,
            credentials=self.credentials.to_service_account_credentials(),
            location=self.credentials.location
        )

        # patch the client query so our defaults are used
        query_orig = self._client.query

        def query_patch(
            query: str,
            retry: Any = self.default_retry,
            timeout: Any = self.credentials.http_timeout,
            **kwargs: Any
        ) -> Any:
            return query_orig(query, retry=retry, timeout=timeout, **kwargs)

        self._client.query = query_patch


    def close_connection(self) -> None:
        if self._client:
            self._client.close()
            self._client = None

    @property
    def native_connection(self) -> bigquery.Client:
        return self._client

    def has_dataset(self) -> bool:
        try:
            self._client.get_dataset(self.fully_qualified_dataset_name(), retry=self.default_retry, timeout=self.credentials.http_timeout)
            return True
        except gcp_exceptions.NotFound:
            return False

    def create_dataset(self) -> None:
        self._client.create_dataset(
            self.fully_qualified_dataset_name(),
            exists_ok=False,
            retry=self.default_retry,
            timeout=self.credentials.http_timeout
        )

    def drop_dataset(self) -> None:
        self._client.delete_dataset(
            self.fully_qualified_dataset_name(),
            not_found_ok=True,
            delete_contents=True,
            retry=self.default_retry,
            timeout=self.credentials.http_timeout
        )

    def execute_sql(self, sql: AnyStr, *args: Any, **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]:
        with self.execute_query(sql, *args, **kwargs) as curr:
            if not curr.description:
                return None
            else:
                try:
                    f = curr.fetchall()
                    return f
                except api_core_exceptions.InvalidArgument as ia_ex:
                    if "non-table entities cannot be read" in str(ia_ex):
                        return None
                    raise

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr,  *args: Any, **kwargs: Any) -> Iterator[DBCursor]:
        conn: DbApiConnection = None
        curr: DBCursor = None
        db_args = args if args else kwargs if kwargs else None
        logger.debug(f"Will execute query {query} {db_args}")  # type: ignore
        try:
            conn = DbApiConnection(client=self._client)
            curr = conn.cursor()
            curr.execute(query, db_args, job_config=self.default_query)
            yield curr
        finally:
            if conn:
                # will also close all cursors
                conn.close()

    def fully_qualified_dataset_name(self) -> str:
        return f"{self.credentials.project_id}.{self.dataset_name}"

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if cls.is_dbapi_exception(ex):
            # google cloud exception in first argument: https://github.com/googleapis/python-bigquery/blob/main/google/cloud/bigquery/dbapi/cursor.py#L205
            cloud_ex = ex.args[0]
            reason = cls._get_reason_from_errors(cloud_ex)
            if reason is None:
                if isinstance(ex, (dbapi_exceptions.DataError, dbapi_exceptions.IntegrityError)):
                    return DatabaseTerminalException(ex)
                elif isinstance(ex, dbapi_exceptions.ProgrammingError):
                    return DatabaseTransientException(ex)
            if reason == "notFound":
                return DatabaseUndefinedRelation(ex)
            if reason == "invalidQuery" and "Unrecognized name" in str(ex):
                # unknown column etc.
                return DatabaseTerminalException(ex)
            if reason in BQ_TERMINAL_REASONS:
                return DatabaseTerminalException(ex)
            # anything else is transient
            return DatabaseTransientException(ex)
        return ex

    @staticmethod
    def _get_reason_from_errors(gace: api_core_exceptions.GoogleAPICallError) -> Optional[str]:
        errors: List[StrAny] = getattr(gace, "errors", None)
        if errors and isinstance(errors, Sequence):
            return errors[0].get("reason")  # type: ignore
        return None

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, dbapi_exceptions.Error)
