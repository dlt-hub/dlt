from abc import abstractmethod
import re
import duckdb
import semver
from pathlib import Path
import sqlglot
import sqlglot.expressions as exp
from urllib.parse import urlparse
import math
from contextlib import contextmanager
from typing import Any, AnyStr, ClassVar, Dict, Iterator, List, Optional, Sequence, Generator, cast

from dlt.common import logger
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.configuration.specs import (
    AwsCredentials,
    AzureServicePrincipalCredentialsWithoutDefaults,
    AzureCredentialsWithoutDefaults,
)
from dlt.common.destination.client import JobClientBase
from dlt.common.destination.dataset import DBApiCursor

from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.storages.configuration import FileSystemCredentials
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.impl.duckdb.exceptions import IcebergViewException
from dlt.destinations.typing import DBApi, DBTransaction, DataFrame, ArrowTable
from dlt.destinations.sql_client import (
    SqlClientBase,
    DBApiCursorImpl,
    raise_database_error,
    raise_open_connection_error,
)

from dlt.destinations.impl.duckdb.configuration import (
    DuckDbBaseCredentials,
    DuckDbClientConfiguration,
    DuckDbCredentials,
)


class DuckDBDBApiCursorImpl(DBApiCursorImpl):
    """Use native duckdb data frame support if available"""

    native_cursor: duckdb.DuckDBPyConnection  # type: ignore
    vector_size: ClassVar[int] = 2048  # vector size is 2048

    def _get_page_count(self, chunk_size: int) -> int:
        """get the page count for vector size"""
        if chunk_size < self.vector_size:
            return 1
        return math.floor(chunk_size / self.vector_size)

    def iter_df(self, chunk_size: int) -> Generator[DataFrame, None, None]:
        # full frame
        if not chunk_size:
            yield self.native_cursor.fetch_df()
            return
        # iterate
        while True:
            df = self.native_cursor.fetch_df_chunk(self._get_page_count(chunk_size))
            if df.shape[0] == 0:
                break
            yield df

    def iter_arrow(self, chunk_size: int) -> Generator[ArrowTable, None, None]:
        if not chunk_size:
            yield self.native_cursor.fetch_arrow_table()
            return
        # iterate
        for item in self.native_cursor.fetch_record_batch(chunk_size):
            yield ArrowTable.from_batches([item])


class DuckDbSqlClient(SqlClientBase[duckdb.DuckDBPyConnection], DBTransaction):
    dbapi: ClassVar[DBApi] = duckdb

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: DuckDbBaseCredentials,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(None, dataset_name, staging_dataset_name, capabilities)
        self._conn: duckdb.DuckDBPyConnection = None
        self.credentials = credentials
        # set additional connection options so derived class can change it
        # TODO: move that to methods that can be overridden, include local_config
        self._pragmas = ["enable_checkpoint_on_shutdown"]
        self._global_config: Dict[str, Any] = {
            "TimeZone": "UTC",
            "checkpoint_threshold": "1gb",
        }

    @raise_open_connection_error
    def open_connection(self) -> duckdb.DuckDBPyConnection:
        self._conn = self.credentials.borrow_conn(
            pragmas=self._pragmas,
            global_config=self._global_config,
            local_config={
                "search_path": self.fully_qualified_dataset_name(),
            },
        )
        return self._conn

    def close_connection(self) -> None:
        if self._conn:
            self.credentials.return_conn(self._conn)
            self._conn = None

    @contextmanager
    @raise_database_error
    def begin_transaction(self) -> Iterator[DBTransaction]:
        try:
            self._conn.begin()
            yield self
            self.commit_transaction()
        except Exception:
            # in some cases duckdb rollback the transaction automatically
            try:
                self.rollback_transaction()
            except DatabaseTransientException:
                pass
            raise

    @raise_database_error
    def commit_transaction(self) -> None:
        self._conn.commit()

    @raise_database_error
    def rollback_transaction(self) -> None:
        self._conn.rollback()

    @property
    def native_connection(self) -> duckdb.DuckDBPyConnection:
        return self._conn

    def execute_sql(
        self, sql: AnyStr, *args: Any, **kwargs: Any
    ) -> Optional[Sequence[Sequence[Any]]]:
        with self.execute_query(sql, *args, **kwargs) as curr:
            if curr.description is None:
                return None
            else:
                f = curr.fetchall()
                return f

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        assert isinstance(query, str)
        db_args = args if args else kwargs if kwargs else None
        if db_args:
            # TODO: must provide much better refactoring of params
            query = query.replace("%s", "?")
        try:
            self._conn.execute(query, db_args)
            yield DuckDBDBApiCursorImpl(self._conn)  # type: ignore
        except duckdb.Error as outer:
            self.close_connection()
            self.open_connection()
            raise outer

    def warn_if_catalog_equals_dataset_name(self) -> None:
        """
        Checks if the DuckDB connection's current catalog equals the dataset name (schema).
        """
        try:
            # First try to get the catalog via a function that (if available) returns the current database.
            result = self._conn.execute("SELECT current_database()").fetchone()
            if result and len(result) > 0:
                catalog = result[0]
            else:
                # fallback: use PRAGMA database_list to fetch the first (default) database name.
                result = self._conn.execute("PRAGMA database_list").fetchone()
                catalog = result[0] if result else None
        except Exception:
            return

        if catalog is None:
            return

        if catalog == self.dataset_name:
            logger.warning(
                "The current catalog (typically database file name) '%s' is identical to the"
                " dataset name '%s'. This may lead to confusion in the DuckDB binder. Consider"
                " using distinct names. Most typically you use the same name for your pipeline and"
                " dataset or the same name for your destination and the dataset.",
                catalog,
                self.dataset_name,
            )

    def create_secret_name(self, scope: str) -> str:
        regex = re.compile("[^a-zA-Z]")
        escaped_bucket_name = regex.sub("", scope.lower())
        return f"{self.dataset_name}_{escaped_bucket_name}"

    @raise_database_error
    def list_secrets(self) -> Sequence[str]:
        """List secrets that belong to this dataset"""
        secrets = self._conn.sql(
            f"SELECT name FROM duckdb_secrets() WHERE name LIKE '{self.dataset_name}%'"
        ).fetchall()
        return [s[0] for s in secrets]

    @raise_database_error
    def drop_secret(self, secret_name: str) -> None:
        if not secret_name.startswith(self.dataset_name):
            raise ValueError(
                f"Secret name must start with dataset name `{self.dataset_name}`, got"
                f" `{secret_name}`."
            )

        self._conn.sql(f"DROP SECRET {secret_name}")

    @raise_database_error
    def create_secret(
        self,
        scope: str,
        credentials: FileSystemCredentials,
        secret_name: str = None,
    ) -> bool:
        #  home dir is a bad choice, it should be more explicit
        if not secret_name:
            secret_name = self.create_secret_name(scope)

        if not secret_name.startswith(self.dataset_name):
            raise ValueError(
                f"Secret name must start with dataset name {self.dataset_name}, got {secret_name}"
            )

        if self.persist_secrets and self.memory_db:
            raise Exception("Creating persistent secrets for in memory db is not allowed.")

        secrets_path = Path(
            self._conn.sql(
                "SELECT current_setting('secret_directory') AS secret_directory"
            ).fetchone()[0]
        )

        is_default_secrets_directory = (
            len(secrets_path.parts) >= 2
            and secrets_path.parts[-1] == "stored_secrets"
            and secrets_path.parts[-2] == ".duckdb"
        )

        if is_default_secrets_directory and self.persist_secrets:
            logger.warn(
                "You are persisting duckdb secrets but are storing them in the default folder"
                f" {secrets_path}. These secrets are saved there unencrypted, we"
                " recommend using a custom secret directory."
            )

        persistent_stmt = ""
        if self.persist_secrets:
            persistent_stmt = " PERSISTENT "

        if "@" in scope:
            scope = scope.split("@")[0]

        protocol = urlparse(scope).scheme
        sql: List[str] = []

        # add secrets required for creating views
        if protocol == "s3":
            aws_creds = cast(AwsCredentials, credentials)
            session_token = (
                "" if aws_creds.aws_session_token is None else aws_creds.aws_session_token
            )

            use_ssl = "true"
            endpoint = aws_creds.endpoint_url or "s3.amazonaws.com"
            if aws_creds.endpoint_url and "http://" in aws_creds.endpoint_url:
                use_ssl = "false"
                endpoint = aws_creds.endpoint_url.replace("http://", "")
            elif aws_creds.endpoint_url and "https://" in aws_creds.endpoint_url:
                endpoint = aws_creds.endpoint_url.replace("https://", "")

            s3_url_style = aws_creds.s3_url_style or "vhost"
            sql.append(f"""
                CREATE OR REPLACE {persistent_stmt} SECRET {secret_name} (
                    TYPE S3,
                    KEY_ID '{aws_creds.aws_access_key_id}',
                    SECRET '{aws_creds.aws_secret_access_key}',
                    SESSION_TOKEN '{session_token}',
                    REGION '{aws_creds.region_name}',
                    ENDPOINT '{endpoint}',
                    SCOPE '{scope}',
                    URL_STYLE '{s3_url_style}',
                    USE_SSL {use_ssl}
                )""")

        # azure with storage account creds
        elif protocol in ["az", "abfss"]:
            # the line below solves problems with certificate path lookup on linux
            # see duckdb docs
            sql.append("SET azure_transport_option_type = 'curl'")

            if isinstance(credentials, AzureCredentialsWithoutDefaults):
                sql.append(f"""
                CREATE OR REPLACE {persistent_stmt} SECRET {secret_name} (
                    TYPE AZURE,
                    CONNECTION_STRING 'AccountName={credentials.azure_storage_account_name};AccountKey={credentials.azure_storage_account_key}',
                    SCOPE '{scope}'
                )""")

            # azure with service principal creds
            elif isinstance(credentials, AzureServicePrincipalCredentialsWithoutDefaults):
                sql.append(f"""
                CREATE OR REPLACE {persistent_stmt} SECRET {secret_name} (
                    TYPE AZURE,
                    PROVIDER SERVICE_PRINCIPAL,
                    TENANT_ID '{credentials.azure_tenant_id}',
                    CLIENT_ID '{credentials.azure_client_id}',
                    CLIENT_SECRET '{credentials.azure_client_secret}',
                    ACCOUNT_NAME '{credentials.azure_storage_account_name}',
                    SCOPE '{scope}'
                )""")
        elif self.persist_secrets:
            raise ValueError(
                "Cannot create persistent secret for filesystem protocol"
                f" `{protocol}`. If you are trying to use persistent secrets"
                " with gs/gcs, please use the s3 compatibility layer."
            )
        else:
            # could not create secret
            return False
        self._conn.sql(";\n".join(sql))
        return True

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if isinstance(ex, (duckdb.CatalogException)):
            if "already exists" in str(ex):
                raise DatabaseTerminalException(ex)
            else:
                raise DatabaseUndefinedRelation(ex)
        elif isinstance(ex, duckdb.InvalidInputException):
            if "Catalog Error" in str(ex):
                raise DatabaseUndefinedRelation(ex)
            # duckdb raises TypeError on malformed query parameters
            return DatabaseTransientException(duckdb.ProgrammingError(ex))
        elif isinstance(ex, duckdb.IOException):
            message = str(ex)
            if (
                "delta" in message and "No files in log segment" in message
            ) or "Path does not exist" in message:
                # delta scanner with no delta data and metadata exist in the location
                return DatabaseUndefinedRelation(ex)
            if "Could not guess Iceberg table version" in message:
                # same but iceberg
                return DatabaseUndefinedRelation(ex)
            if "No files found" in message:
                # glob patterns not found
                return DatabaseUndefinedRelation(ex)
            return DatabaseTransientException(ex)
        elif isinstance(ex, duckdb.InternalException):
            if "INTERNAL Error: Value::LIST(values)" in str(ex):
                return IcebergViewException(
                    ex,
                    "duckdb Iceberg extension raises this error when empty (no data) Iceberg table"
                    " is queried. https://github.com/duckdb/duckdb-iceberg/issues/65",
                )
            else:
                return DatabaseTransientException(ex)
        elif isinstance(
            ex,
            (
                duckdb.OperationalError,
                duckdb.InternalError,
                duckdb.SyntaxException,
                duckdb.ParserException,
            ),
        ):
            return DatabaseTransientException(ex)
        elif isinstance(ex, (duckdb.DataError, duckdb.ProgrammingError, duckdb.IntegrityError)):
            return DatabaseTerminalException(ex)
        elif cls.is_dbapi_exception(ex):
            return DatabaseTransientException(ex)
        else:
            return ex

    @staticmethod
    def is_dbapi_exception(ex: Exception) -> bool:
        return isinstance(ex, duckdb.Error)


class WithTableScanners(DuckDbSqlClient):
    memory_db: duckdb.DuckDBPyConnection = None
    """Internally created in-mem database in case external is not provided"""

    def __init__(
        self,
        remote_client: JobClientBase,
        dataset_name: str,
        cache_db: DuckDbCredentials = None,
        persist_secrets: bool = False,
    ) -> None:
        """Allows to maps data in tables accessed via `remote_client` as VIEWs in duckdb database.
        Creates in memory "cache" database by default or allows for external database via "cache_db".
        Will attempt to create views lazily by parsing SQL queries, identifying tables and adding views
        before execution.
        """
        # if no credentials are passed from the outside
        # we know to keep an in memory instance here
        if not cache_db:
            self.memory_db = duckdb.connect(":memory:")
            cache_db = DuckDbCredentials(self.memory_db)

        from dlt.destinations.impl.duckdb.factory import duckdb as duckdb_factory

        super().__init__(
            dataset_name=dataset_name,
            staging_dataset_name=None,
            credentials=cache_db,
            capabilities=duckdb_factory().capabilities(
                DuckDbClientConfiguration(credentials=cache_db), naming=remote_client.schema.naming
            ),
        )
        self.remote_client = remote_client
        self.schema = remote_client.schema
        self.persist_secrets = persist_secrets
        self._global_config.update(
            {
                "enable_http_metadata_cache": True,
            }
        )

        if semver.Version.parse(duckdb.__version__) >= semver.Version.parse("1.2.0"):
            self._global_config.update(
                {
                    # prevents HEAD command by caching parquet metadata
                    "parquet_metadata_cache": True,
                }
            )

    @raise_database_error
    def open_connection(self) -> duckdb.DuckDBPyConnection:
        # NOTE: do not self.execute*** methods when opening connection, may end in endless recursion
        # we keep the in memory instance around, so if this prop is set, return it
        first_connection = self.credentials.never_borrowed
        super().open_connection()

        if first_connection:
            # set up dataset
            q_dataset_name = self.fully_qualified_dataset_name()
            create_schema_sql = "CREATE SCHEMA IF NOT EXISTS %s" % q_dataset_name
            self._conn.sql(f"{create_schema_sql};USE {self.fully_qualified_dataset_name()}")

        return self._conn

    @abstractmethod
    def should_replace_view(self, view_name: str, table_schema: PreparedTableSchema) -> bool:
        """Tells if view `view_name` should be replaced"""
        pass

    @abstractmethod
    def create_view(self, view_name: str, table_schema: PreparedTableSchema) -> None:
        pass

    @abstractmethod
    def can_create_view(self, table_schema: PreparedTableSchema) -> bool:
        """Tells if a view for a table `table_schema` can be created"""
        pass

    def create_views_for_all_tables(self) -> None:
        self.create_views_for_tables({v: v for v in self.schema.tables.keys()})

    def create_views_for_tables(self, tables: Dict[str, str]) -> None:
        """Add the required tables as views to the duckdb in memory instance"""

        # this also gets all views
        existing_tables = [tname[0] for tname in self._conn.execute("SHOW TABLES").fetchall()]
        # map only tables with data
        tables_with_data = self.schema.dlt_table_names() + self.schema.data_table_names(
            seen_data_only=True
        )

        for table_name in tables.keys():
            view_name = tables[table_name]

            if table_name not in tables_with_data:
                # unknown views will not be created
                continue
            # NOTE: if this is staging configuration then `prepare_load_table` will remove some info
            # from table schema, if we ever extend this to handle staging destination, this needs to change
            schema_table = self.remote_client.prepare_load_table(table_name)

            needs_replace = self.should_replace_view(view_name, schema_table)
            # skip if view already exists and does not need to be replaced each time
            if view_name in existing_tables and not needs_replace:
                continue

            if not self.can_create_view(schema_table):
                continue

            self.create_view(view_name, schema_table)

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        # skip parametrized queries, we could also render them but currently user is not able to
        # do parametrized queries via dataset interface
        if args or kwargs:
            # this is hack
            query = query.replace("%s", "?")  # type: ignore
        # find all tables to preload
        expression = sqlglot.parse_one(query, read="duckdb")  # type: ignore
        load_tables: Dict[str, str] = {}
        for table in expression.find_all(exp.Table):
            # sqlglot has tables without tables ie. schemas are tables
            if not table.this:
                continue
            schema = table.db
            # add only tables from the dataset schema
            if schema or schema.lower() != self.dataset_name.lower():
                load_tables[table.name] = table.name

        if load_tables:
            self.create_views_for_tables(load_tables)
        with super().execute_query(query, *args, **kwargs) as cursor:
            yield cursor

    @staticmethod
    def _setup_iceberg(conn: duckdb.DuckDBPyConnection) -> None:
        if semver.Version.parse(duckdb.__version__) <= semver.Version.parse("1.1.2"):
            raise NotImplementedError(
                f"Iceberg scanner for duckdb `{duckdb.__version__}` does not implement recent"
                " snapshot discovery. Please install duckdb >= 1.1.3"
            )
        # needed to make persistent secrets work in new connection
        # https://github.com/duckdb/duckdb_iceberg/issues/83
        conn.execute("FROM duckdb_secrets()")

        # `duckdb_iceberg` extension does not support autoloading
        # https://github.com/duckdb/duckdb_iceberg/issues/71
        if semver.Version.parse(duckdb.__version__) < semver.Version.parse("1.2.0"):
            conn.execute("INSTALL Iceberg FROM core_nightly; LOAD iceberg")

    def __del__(self) -> None:
        if self.memory_db:
            self.memory_db.close()
            self.memory_db = None
