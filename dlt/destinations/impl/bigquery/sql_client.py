from contextlib import contextmanager
from typing import Any, AnyStr, ClassVar, Iterator, List, Optional, Sequence

import google.cloud.bigquery as bigquery  # noqa: I250
from google.api_core import exceptions as api_core_exceptions
from google.cloud import exceptions as gcp_exceptions
from google.cloud.bigquery import dbapi as bq_dbapi
from google.cloud.bigquery.dbapi import Connection as DbApiConnection, Cursor as BQDbApiCursor
from google.cloud.bigquery.dbapi import exceptions as dbapi_exceptions

from dlt.common.configuration.specs import GcpServiceAccountCredentialsWithoutDefaults
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.typing import StrAny
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.impl.bigquery import capabilities
from dlt.destinations.sql_client import (
    DBApiCursorImpl,
    SqlClientBase,
    raise_database_error,
    raise_open_connection_error,
)
from dlt.destinations.typing import DBApi, DBApiCursor, DBTransaction, DataFrame


# terminal reasons as returned in BQ gRPC error response
# https://cloud.google.com/bigquery/docs/error-messages
BQ_TERMINAL_REASONS = [
    "billingTierLimitExceeded",
    "duplicate",
    "invalid",
    "notFound",
    "notImplemented",
    "stopped",
    "tableUnavailable",
]
# invalidQuery is a transient error -> must be fixed by programmer


class BigQueryDBApiCursorImpl(DBApiCursorImpl):
    """Use native BigQuery data frame support if available"""

    native_cursor: BQDbApiCursor  # type: ignore

    def df(self, chunk_size: int = None, **kwargs: Any) -> DataFrame:
        if chunk_size is not None:
            return super().df(chunk_size=chunk_size)
        query_job: bigquery.QueryJob = self.native_cursor._query_job

        try:
            return query_job.to_dataframe(**kwargs)
        except ValueError:
            # no pyarrow/db-types, fallback to our implementation
            return super().df()


class BigQuerySqlClient(SqlClientBase[bigquery.Client], DBTransaction):
    dbapi: ClassVar[DBApi] = bq_dbapi
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(
        self,
        dataset_name: str,
        credentials: GcpServiceAccountCredentialsWithoutDefaults,
        location: str = "US",
        http_timeout: float = 15.0,
        retry_deadline: float = 60.0,
    ) -> None:
        self._client: bigquery.Client = None
        self.credentials: GcpServiceAccountCredentialsWithoutDefaults = credentials
        self.location = location
        self.http_timeout = http_timeout
        super().__init__(credentials.project_id, dataset_name)

        self._default_retry = bigquery.DEFAULT_RETRY.with_deadline(retry_deadline)
        self._default_query = bigquery.QueryJobConfig(
            default_dataset=self.fully_qualified_dataset_name(escape=False)
        )
        self._session_query: bigquery.QueryJobConfig = None

    @raise_open_connection_error
    def open_connection(self) -> bigquery.Client:
        self._client = bigquery.Client(
            self.credentials.project_id,
            credentials=self.credentials.to_native_credentials(),
            location=self.location,
        )

        # patch the client query, so our defaults are used
        query_orig = self._client.query

        def query_patch(
            query: str,
            retry: Any = self._default_retry,
            timeout: Any = self.http_timeout,
            **kwargs: Any,
        ) -> Any:
            return query_orig(query, retry=retry, timeout=timeout, **kwargs)

        self._client.query = query_patch  # type: ignore
        return self._client

    def close_connection(self) -> None:
        if self._session_query:
            self.rollback_transaction()
        if self._client:
            self._client.close()
            self._client = None

    @contextmanager
    @raise_database_error
    def begin_transaction(self) -> Iterator[DBTransaction]:
        try:
            if self._session_query:
                raise dbapi_exceptions.ProgrammingError(
                    "Nested transactions not supported on BigQuery"
                )
            job = self._client.query(
                "BEGIN TRANSACTION;",
                job_config=bigquery.QueryJobConfig(
                    create_session=True,
                    default_dataset=self.fully_qualified_dataset_name(escape=False),
                ),
            )
            self._session_query = bigquery.QueryJobConfig(
                create_session=False,
                default_dataset=self.fully_qualified_dataset_name(escape=False),
                connection_properties=[
                    bigquery.query.ConnectionProperty(
                        key="session_id", value=job.session_info.session_id
                    )
                ],
            )
            try:
                job.result()
            except Exception:
                # if session creation fails
                self._session_query = None
                raise
            yield self
            self.commit_transaction()
        except Exception:
            self.rollback_transaction()
            raise

    def commit_transaction(self) -> None:
        if not self._session_query:
            # allow committing without transaction
            return
        self.execute_sql("COMMIT TRANSACTION;CALL BQ.ABORT_SESSION();")
        self._session_query = None

    def rollback_transaction(self) -> None:
        if not self._session_query:
            raise dbapi_exceptions.ProgrammingError("Transaction was not started")
        self.execute_sql("ROLLBACK TRANSACTION;CALL BQ.ABORT_SESSION();")
        self._session_query = None

    @property
    def native_connection(self) -> bigquery.Client:
        return self._client

    def has_dataset(self) -> bool:
        try:
            self._client.get_dataset(
                self.fully_qualified_dataset_name(escape=False),
                retry=self._default_retry,
                timeout=self.http_timeout,
            )
            return True
        except gcp_exceptions.NotFound:
            return False

    def create_dataset(self) -> None:
        self._client.create_dataset(
            self.fully_qualified_dataset_name(escape=False),
            retry=self._default_retry,
            timeout=self.http_timeout,
        )

    def drop_dataset(self) -> None:
        self._client.delete_dataset(
            self.fully_qualified_dataset_name(escape=False),
            not_found_ok=True,
            delete_contents=True,
            retry=self._default_retry,
            timeout=self.http_timeout,
        )

    def execute_sql(
        self, sql: AnyStr, *args: Any, **kwargs: Any
    ) -> Optional[Sequence[Sequence[Any]]]:
        with self.execute_query(sql, *args, **kwargs) as curr:
            if not curr.description:
                return None
            try:
                return curr.fetchall()
            except api_core_exceptions.InvalidArgument as ia_ex:
                if "non-table entities cannot be read" in str(ia_ex):
                    return None
                raise

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        conn: DbApiConnection = None
        db_args = args or (kwargs or None)
        try:
            conn = DbApiConnection(client=self._client)
            curr = conn.cursor()
            # if session exists give it a preference
            curr.execute(query, db_args, job_config=self._session_query or self._default_query)
            yield BigQueryDBApiCursorImpl(curr)  # type: ignore
        finally:
            if conn:
                # will close all cursors
                conn.close()

    def fully_qualified_dataset_name(self, escape: bool = True) -> str:
        if escape:
            project_id = self.capabilities.escape_identifier(self.credentials.project_id)
            dataset_name = self.capabilities.escape_identifier(self.dataset_name)
        else:
            project_id = self.credentials.project_id
            dataset_name = self.dataset_name
        return f"{project_id}.{dataset_name}"

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if not cls.is_dbapi_exception(ex):
            return ex
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
        if reason == "invalidQuery" and "was not found" in str(ex) and "Dataset" in str(ex):
            return DatabaseUndefinedRelation(ex)
        if (
            reason == "invalidQuery"
            and "Not found" in str(ex)
            and ("Dataset" in str(ex) or "Table" in str(ex))
        ):
            return DatabaseUndefinedRelation(ex)
        if reason == "accessDenied" and "Dataset" in str(ex) and "not exist" in str(ex):
            return DatabaseUndefinedRelation(ex)
        if reason == "invalidQuery" and (
            "Unrecognized name" in str(ex) or "cannot be null" in str(ex)
        ):
            # unknown column, inserting NULL into required field
            return DatabaseTerminalException(ex)
        if reason in BQ_TERMINAL_REASONS:
            return DatabaseTerminalException(ex)
        # anything else is transient
        return DatabaseTransientException(ex)

    @staticmethod
    def _get_reason_from_errors(gace: api_core_exceptions.GoogleAPICallError) -> Optional[str]:
        errors: List[StrAny] = getattr(gace, "errors", None)
        if errors and isinstance(errors, Sequence):
            return errors[0].get("reason")  # type: ignore
        return None

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, dbapi_exceptions.Error)


class TransactionsNotImplementedError(NotImplementedError):
    def __init__(self) -> None:
        super().__init__(
            "BigQuery does not support transaction management. Instead you may wrap your SQL script"
            " in BEGIN TRANSACTION; ... COMMIT TRANSACTION;"
        )
