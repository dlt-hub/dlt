
from contextlib import contextmanager
from typing import Any, AnyStr, Iterator, Optional, Sequence
from dlt.common.storages.file_storage import FileStorage
import google.cloud.bigquery as bigquery  # noqa: I250
from google.cloud.bigquery.dbapi import Connection as DbApiConnection
from google.cloud import exceptions as gcp_exceptions

from dlt.common import logger
from dlt.common.configuration.specs import GcpClientCredentials

from dlt.destinations.typing import DBCursor
from dlt.destinations.sql_client import SqlClientBase


class BigQuerySqlClient(SqlClientBase[bigquery.Client]):
    def __init__(self, default_dataset_name: str, credentials: GcpClientCredentials) -> None:
        self._client: bigquery.Client = None
        self.credentials: GcpClientCredentials = credentials
        super().__init__(default_dataset_name)

        self.default_retry = bigquery.DEFAULT_RETRY.with_deadline(credentials.retry_deadline)
        self.default_query = bigquery.QueryJobConfig(default_dataset=self.fully_qualified_dataset_name())

    def open_connection(self) -> None:
        self._client = bigquery.Client(
            self.credentials.project_id,
            credentials=self.credentials.to_service_account_credentials(),
            location=self.credentials.location
        )

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
        conn: DbApiConnection = None
        db_args = args if args else kwargs
        logger.debug(f"Will execute sql {sql} {db_args}")  # type: ignore
        try:
            conn = DbApiConnection(client=self._client)
            curr: DBCursor = conn.cursor()
            curr.execute(sql, db_args, job_config=self.default_query)
            if not curr.description:
                return None
            else:
                f = curr.fetchall()
                return f
        finally:
            if conn:
                # will also close all cursors
                conn.close()

    @contextmanager
    def execute_query(self, query: AnyStr,  *args: Any, **kwargs: Any) -> Iterator[DBCursor]:
        conn: DbApiConnection = None
        curr: DBCursor = None
        db_args = args if args else kwargs
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
        return f"{self.credentials.project_id}.{self.default_dataset_name}"
