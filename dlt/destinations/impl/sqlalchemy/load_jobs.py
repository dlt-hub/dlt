from __future__ import annotations

from typing import IO, Any, Dict, Iterator, List, Sequence, TYPE_CHECKING, Optional
import math
from urllib.parse import quote_plus, urlencode

import sqlalchemy as sa

from dlt.common import logger
from dlt.common.destination.client import (
    RunnableLoadJob,
    HasFollowupJobs,
    PreparedTableSchema,
)
from dlt.common.storages import FileStorage
from dlt.common.json import json, PY_DATETIME_DECODERS

from dlt.destinations._adbc_jobs import AdbcParquetCopyJob
from dlt.destinations.sql_jobs import SqlFollowupJob
from dlt.destinations.impl.sqlalchemy.db_api_client import SqlalchemyClient
from dlt.destinations.impl.sqlalchemy.merge_job import SqlalchemyMergeFollowupJob

if TYPE_CHECKING:
    from dlt.destinations.impl.sqlalchemy.sqlalchemy_job_client import SqlalchemyJobClient


def build_mysql_adbc_dsn(
    username: Optional[str] = None,
    password: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    params: Optional[Dict[str, str]] = None,
) -> str:
    """Build a DSN connection string for the go-mysql ADBC driver.

    The go-mysql driver (github.com/go-sql-driver/mysql) has specific DSN format:
        [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]

    Based on the driver's source code (dsn.go):
    - Username and password are NOT URL-decoded by the driver
    - Database name IS URL-decoded using url.PathUnescape
    - Query parameter values ARE URL-decoded using url.QueryUnescape

    Args:
        username: MySQL username (not URL-encoded)
        password: MySQL password (not URL-encoded)
        host: MySQL host
        port: MySQL port (default: 3306)
        database: Database/schema name (will be URL-encoded)
        params: Query parameters (values will be URL-encoded)

    Returns:
        DSN connection string for go-mysql driver
    """
    # Build auth part - username and password are NOT encoded per go-mysql driver behavior
    auth = ""
    if username or password:
        auth = f"{username or ''}:{password or ''}@"

    # Host and port
    host = host or "localhost"
    port = port or 3306

    # Database name is URL-encoded using PathEscape equivalent (quote_plus with safe='')
    # The go-mysql driver uses url.PathUnescape to decode it
    db_encoded = quote_plus(database or "", safe="")

    base = f"{auth}tcp({host}:{port})/{db_encoded}"

    # Query parameters - values are URL-encoded
    if params:
        conn_str = f"{base}?{urlencode(params)}"
    else:
        conn_str = base

    return conn_str


class SqlalchemyJsonLInsertJob(RunnableLoadJob, HasFollowupJobs):
    def __init__(self, file_path: str, table: sa.Table) -> None:
        super().__init__(file_path)
        self._job_client: "SqlalchemyJobClient" = None
        self.table = table

    def _open_load_file(self) -> IO[bytes]:
        return FileStorage.open_zipsafe_ro(self._file_path, "rb")

    def _iter_data_items(self) -> Iterator[Dict[str, Any]]:
        all_cols = {col.name: None for col in self.table.columns}
        with FileStorage.open_zipsafe_ro(self._file_path, "rb") as f:
            for line in f:
                # Decode date/time to py datetime objects. Some drivers have issues with pendulum objects
                for item in json.typed_loadb(line, decoders=PY_DATETIME_DECODERS):
                    # Fill any missing columns in item with None. Bulk insert fails when items have different keys
                    if item.keys() != all_cols.keys():
                        yield {**all_cols, **item}
                    else:
                        yield item

    def _iter_data_item_chunks(self) -> Iterator[Sequence[Dict[str, Any]]]:
        max_rows = self._job_client.capabilities.max_rows_per_insert or math.inf
        # Limit by max query length should not be needed,
        # bulk insert generates an INSERT template with a single VALUES tuple of placeholders
        # If any dialects don't do that we need to check the str length of the query
        # TODO: Max params may not be needed. Limits only apply to placeholders in sql string (mysql/sqlite)
        max_params = self._job_client.capabilities.max_query_parameters or math.inf
        chunk: List[Dict[str, Any]] = []
        params_count = 0
        for item in self._iter_data_items():
            if len(chunk) + 1 == max_rows or params_count + len(item) > max_params:
                # Rotate chunk
                yield chunk
                chunk = []
                params_count = 0
            params_count += len(item)
            chunk.append(item)

        if chunk:
            yield chunk

    def run(self) -> None:
        _sql_client = self._job_client.sql_client
        # Copy the table to the current dataset (i.e. staging) if needed
        # This is a no-op if the table is already in the correct schema
        table = self.table.to_metadata(
            self.table.metadata, schema=_sql_client.dataset_name  # type: ignore[attr-defined]
        )

        with _sql_client.begin_transaction():
            for chunk in self._iter_data_item_chunks():
                _sql_client.execute_sql(table.insert(), chunk)


class SqlalchemyParquetADBCJob(AdbcParquetCopyJob):
    """ADBC Parquet copy job for SQLAlchemy (sqlite, mysql) with query param handling."""

    def __init__(self, file_path: str, table: sa.Table) -> None:
        super().__init__(file_path)
        self._job_client: "SqlalchemyJobClient" = None
        self.table = table

    if TYPE_CHECKING:
        from adbc_driver_manager.dbapi import Connection

    def _connect(self) -> Connection:
        from adbc_driver_manager import dbapi

        engine = self._job_client.config.credentials.engine
        dialect = engine.dialect.name.lower()
        url = engine.url

        query = dict(url.query or {})

        if dialect == "sqlite":
            # disable schema and catalog when ingest
            self._connect_schema_name = ""
            self._connect_catalog_name = ""

            # attach directly to dataset sqlite file as "main"
            if self._job_client.sql_client.dataset_name == "main":
                db_path = url.database
            else:
                db_path = self._job_client.sql_client._sqlite_dataset_filename(
                    self._job_client.sql_client.dataset_name
                )
            conn_str = f"file:{db_path}"

            if query:
                conn_str = f"{conn_str}?{urlencode(query)}"

            conn = dbapi.connect(driver="sqlite", db_kwargs={"uri": conn_str})
            # WAL mode already set, add busy timeout to handle multiple writers
            with conn.cursor() as c:
                c.execute("PRAGMA busy_timeout=1000;")
            return conn

        elif dialect == "mysql":
            # disable schema and catalog when ingest
            self._connect_schema_name = ""
            self._connect_catalog_name = ""

            # mysql: convert SSL params into go-mysql ADBC parameters
            mapped = {}
            for k, v in query.items():
                lk = k.lower()
                if lk == "ssl_ca":
                    mapped["tls-ca"] = v
                elif lk == "ssl_cert":
                    mapped["tls-cert"] = v
                elif lk == "ssl_key":
                    mapped["tls-key"] = v
                elif lk == "ssl_mode":
                    mapped["tls"] = v
                else:
                    mapped[k] = v

            conn_str = build_mysql_adbc_dsn(
                username=url.username,
                password=str(url.password),
                host=url.host,
                port=url.port,
                database=self._job_client.sql_client.dataset_name,
                params=mapped,  # type: ignore
            )

            return dbapi.connect(driver="mysql", db_kwargs={"uri": conn_str})

        else:
            raise NotImplementedError(f"ADBC not supported for sqlalchemy dialect {dialect}")


class SqlalchemyParquetInsertJob(SqlalchemyJsonLInsertJob):
    def _iter_data_item_chunks(self) -> Iterator[Sequence[Dict[str, Any]]]:
        from dlt.common.libs.pyarrow import ParquetFile

        num_cols = len(self.table.columns)
        max_rows = self._job_client.capabilities.max_rows_per_insert or None
        max_params = self._job_client.capabilities.max_query_parameters or None
        read_limit = None

        with ParquetFile(self._file_path) as reader:
            if max_params is not None:
                read_limit = math.floor(max_params / num_cols)

            if max_rows is not None:
                if read_limit is None:
                    read_limit = max_rows
                else:
                    read_limit = min(read_limit, max_rows)

            if read_limit is None:
                yield reader.read().to_pylist()
                return

            for chunk in reader.iter_batches(batch_size=read_limit):
                yield chunk.to_pylist()


class SqlalchemyReplaceJob(SqlFollowupJob):
    @classmethod
    def generate_sql(
        cls,
        table_chain: Sequence[PreparedTableSchema],
        sql_client: SqlalchemyClient,  # type: ignore[override]
    ) -> List[str]:
        statements: List[str] = []
        for table in table_chain:
            # Tables must have already been created in metadata
            table_obj = sql_client.get_existing_table(table["name"])
            staging_table_obj = table_obj.to_metadata(
                sql_client.metadata, schema=sql_client.staging_dataset_name
            )
            stmt = str(table_obj.delete().compile(dialect=sql_client.dialect))
            statements.append(stmt)

            stmt = str(
                table_obj.insert()
                .from_select(
                    [col.name for col in staging_table_obj.columns], staging_table_obj.select()
                )
                .compile(dialect=sql_client.dialect)
            )

            statements.append(stmt)

        return statements


__all__ = [
    "SqlalchemyJsonLInsertJob",
    "SqlalchemyParquetInsertJob",
    "SqlalchemyReplaceJob",
    "SqlalchemyMergeFollowupJob",
]
