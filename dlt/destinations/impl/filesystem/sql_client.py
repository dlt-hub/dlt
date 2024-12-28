from typing import Any, Iterator, AnyStr, List, cast, TYPE_CHECKING, Dict

import os
import re

import duckdb

import sqlglot
import sqlglot.expressions as exp
from dlt.common import logger

from contextlib import contextmanager

from dlt.common.destination.reference import DBApiCursor

from dlt.common.storages.fsspec_filesystem import AZURE_BLOB_STORAGE_PROTOCOLS
from dlt.destinations.sql_client import raise_database_error

from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient
from dlt.destinations.impl.duckdb.factory import duckdb as duckdb_factory, DuckDbCredentials
from dlt.common.configuration.specs import (
    AwsCredentials,
    AzureServicePrincipalCredentialsWithoutDefaults,
    AzureCredentialsWithoutDefaults,
)
from dlt.destinations.utils import is_compression_disabled

from pathlib import Path

SUPPORTED_PROTOCOLS = ["gs", "gcs", "s3", "file", "memory", "az", "abfss"]

if TYPE_CHECKING:
    from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
else:
    FilesystemClient = Any


class FilesystemSqlClient(DuckDbSqlClient):
    memory_db: duckdb.DuckDBPyConnection = None
    """Internally created in-mem database in case external is not provided"""

    def __init__(
        self,
        fs_client: FilesystemClient,
        dataset_name: str = None,
        credentials: DuckDbCredentials = None,
    ) -> None:
        # if no credentials are passed from the outside
        # we know to keep an in memory instance here
        if not credentials:
            self.memory_db = duckdb.connect(":memory:")
            credentials = DuckDbCredentials(self.memory_db)

        super().__init__(
            dataset_name=dataset_name or fs_client.dataset_name,
            staging_dataset_name=None,
            credentials=credentials,
            capabilities=duckdb_factory()._raw_capabilities(),
        )
        self.fs_client = fs_client

        if self.fs_client.config.protocol not in SUPPORTED_PROTOCOLS:
            raise NotImplementedError(
                f"Protocol {self.fs_client.config.protocol} currently not supported for"
                f" FilesystemSqlClient. Supported protocols are {SUPPORTED_PROTOCOLS}."
            )

    def _create_default_secret_name(self) -> str:
        regex = re.compile("[^a-zA-Z]")
        escaped_bucket_name = regex.sub("", self.fs_client.config.bucket_url.lower())
        return f"secret_{escaped_bucket_name}"

    def drop_authentication(self, secret_name: str = None) -> None:
        if not secret_name:
            secret_name = self._create_default_secret_name()
        self._conn.sql(f"DROP PERSISTENT SECRET IF EXISTS {secret_name}")

    def create_authentication(self, persistent: bool = False, secret_name: str = None) -> None:
        #  home dir is a bad choice, it should be more explicit
        if not secret_name:
            secret_name = self._create_default_secret_name()

        if persistent and self.memory_db:
            raise Exception("Creating persistent secrets for in memory db is not allowed.")

        secrets_path = Path(
            self._conn.sql(
                "SELECT current_setting('secret_directory') AS secret_directory;"
            ).fetchone()[0]
        )

        is_default_secrets_directory = (
            len(secrets_path.parts) >= 2
            and secrets_path.parts[-1] == "stored_secrets"
            and secrets_path.parts[-2] == ".duckdb"
        )

        if is_default_secrets_directory and persistent:
            logger.warn(
                "You are persisting duckdb secrets but are storing them in the default folder"
                f" {secrets_path}. These secrets are saved there unencrypted, we"
                " recommend using a custom secret directory."
            )

        persistent_stmt = ""
        if persistent:
            persistent_stmt = " PERSISTENT "

        # abfss buckets have an @ compontent
        scope = self.fs_client.config.bucket_url
        if "@" in scope:
            scope = scope.split("@")[0]

        # add secrets required for creating views
        if self.fs_client.config.protocol == "s3":
            aws_creds = cast(AwsCredentials, self.fs_client.config.credentials)
            session_token = (
                "" if aws_creds.aws_session_token is None else aws_creds.aws_session_token
            )
            endpoint = (
                aws_creds.endpoint_url.replace("https://", "")
                if aws_creds.endpoint_url
                else "s3.amazonaws.com"
            )
            self._conn.sql(f"""
            CREATE OR REPLACE {persistent_stmt} SECRET {secret_name} (
                TYPE S3,
                KEY_ID '{aws_creds.aws_access_key_id}',
                SECRET '{aws_creds.aws_secret_access_key}',
                SESSION_TOKEN '{session_token}',
                REGION '{aws_creds.region_name}',
                ENDPOINT '{endpoint}',
                SCOPE '{scope}'
            );""")

        # azure with storage account creds
        elif self.fs_client.config.protocol in ["az", "abfss"] and isinstance(
            self.fs_client.config.credentials, AzureCredentialsWithoutDefaults
        ):
            azsa_creds = self.fs_client.config.credentials
            self._conn.sql(f"""
            CREATE OR REPLACE {persistent_stmt} SECRET {secret_name} (
                TYPE AZURE,
                CONNECTION_STRING 'AccountName={azsa_creds.azure_storage_account_name};AccountKey={azsa_creds.azure_storage_account_key}',
                SCOPE '{scope}'
            );""")

        # azure with service principal creds
        elif self.fs_client.config.protocol in ["az", "abfss"] and isinstance(
            self.fs_client.config.credentials, AzureServicePrincipalCredentialsWithoutDefaults
        ):
            azsp_creds = self.fs_client.config.credentials
            self._conn.sql(f"""
            CREATE OR REPLACE {persistent_stmt} SECRET {secret_name} (
                TYPE AZURE,
                PROVIDER SERVICE_PRINCIPAL,
                TENANT_ID '{azsp_creds.azure_tenant_id}',
                CLIENT_ID '{azsp_creds.azure_client_id}',
                CLIENT_SECRET '{azsp_creds.azure_client_secret}',
                ACCOUNT_NAME '{azsp_creds.azure_storage_account_name}',
                SCOPE '{scope}'
            );""")
        elif persistent:
            raise Exception(
                "Cannot create persistent secret for filesystem protocol"
                f" {self.fs_client.config.protocol}. If you are trying to use persistent secrets"
                " with gs/gcs, please use the s3 compatibility layer."
            )

        # native google storage implementation is not supported..
        elif self.fs_client.config.protocol in ["gs", "gcs"]:
            logger.warn(
                "For gs/gcs access via duckdb please use the gs/gcs s3 compatibility layer if"
                " possible (not supported when using `iceberg` table format). Falling back to"
                " fsspec."
            )
            self._conn.register_filesystem(self.fs_client.fs_client)

        # for memory we also need to register filesystem
        elif self.fs_client.config.protocol == "memory":
            self._conn.register_filesystem(self.fs_client.fs_client)

    def open_connection(self) -> duckdb.DuckDBPyConnection:
        # we keep the in memory instance around, so if this prop is set, return it
        first_connection = self.credentials.has_open_connection
        super().open_connection()

        if first_connection:
            # set up dataset
            if not self.has_dataset():
                self.create_dataset()
            self._conn.sql(f"USE {self.fully_qualified_dataset_name()}")
            self.create_authentication()

        # the line below solves problems with certificate path lookup on linux
        # see duckdb docs
        if self.fs_client.config.protocol in AZURE_BLOB_STORAGE_PROTOCOLS:
            self._conn.sql("SET azure_transport_option_type = 'curl';")

        return self._conn

    def create_views_for_all_tables(self) -> None:
        self.create_views_for_tables({v: v for v in self.fs_client.schema.tables.keys()})

    @raise_database_error
    def create_views_for_tables(self, tables: Dict[str, str]) -> None:
        """Add the required tables as views to the duckdb in memory instance"""

        # create all tables in duck instance
        for table_name in tables.keys():
            view_name = tables[table_name]

            if table_name not in self.fs_client.schema.tables:
                # unknown views will not be created
                continue

            # NOTE: if this is staging configuration then `prepare_load_table` will remove some info
            # from table schema, if we ever extend this to handle staging destination, this needs to change
            schema_table = self.fs_client.prepare_load_table(table_name)
            table_format = schema_table.get("table_format")

            # skip if view already exists and does not need to be replaced each time
            existing_tables = [tname[0] for tname in self._conn.execute("SHOW TABLES").fetchall()]
            needs_replace = table_format == "iceberg" or self.fs_client.config.protocol == "abfss"
            if view_name in existing_tables and not needs_replace:
                continue

            # discover file type
            folder = self.fs_client.get_table_dir(table_name)
            files = self.fs_client.list_table_files(table_name)
            first_file_type = os.path.splitext(files[0])[1][1:]

            # build files string
            supports_wildcard_notation = self.fs_client.config.protocol != "abfss"
            protocol = (
                "" if self.fs_client.is_local_filesystem else f"{self.fs_client.config.protocol}://"
            )
            resolved_folder = f"{protocol}{folder}"
            resolved_files_string = f"'{resolved_folder}/**/*.{first_file_type}'"
            if not supports_wildcard_notation:
                resolved_files_string = ",".join(map(lambda f: f"'{protocol}{f}'", files))

            # build columns definition
            type_mapper = self.capabilities.get_type_mapper()
            columns = ",".join(
                map(
                    lambda c: (
                        f'{self.escape_column_name(c["name"])}:'
                        f' "{type_mapper.to_destination_type(c, schema_table)}"'
                    ),
                    self.fs_client.schema.tables[table_name]["columns"].values(),
                )
            )

            # discover whether compression is enabled
            compression = "" if is_compression_disabled() else ", compression = 'gzip'"

            # dlt tables are never compressed for now...
            if table_name in self.fs_client.schema.dlt_table_names():
                compression = ""

            # create from statement
            from_statement = ""
            if table_format == "delta":
                from_statement = f"delta_scan('{resolved_folder}')"
            elif table_format == "iceberg":
                from dlt.common.libs.pyiceberg import _get_last_metadata_file

                self._setup_iceberg(self._conn)
                metadata_path = f"{resolved_folder}/metadata"
                last_metadata_file = _get_last_metadata_file(metadata_path, self.fs_client)
                # skip schema inference to make nested data types work
                # https://github.com/duckdb/duckdb_iceberg/issues/47
                from_statement = f"iceberg_scan('{last_metadata_file}', skip_schema_inference=True)"
            elif first_file_type == "parquet":
                from_statement = f"read_parquet([{resolved_files_string}])"
            elif first_file_type == "jsonl":
                from_statement = (
                    f"read_json([{resolved_files_string}], columns = {{{columns}}}{compression})"
                )
            else:
                raise NotImplementedError(
                    f"Unknown filetype {first_file_type} for table {table_name}. Currently only"
                    " jsonl and parquet files as well as delta and iceberg tables are supported."
                )

            # create table
            view_name = self.make_qualified_table_name(view_name)
            create_table_sql_base = (
                f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {from_statement}"
            )
            self._conn.execute(create_table_sql_base)

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        # skip parametrized queries, we could also render them but currently user is not able to
        # do parametrized queries via dataset interface
        if not args and not kwargs:
            # find all tables to preload
            expression = sqlglot.parse_one(query, read="duckdb")  # type: ignore
            load_tables: Dict[str, str] = {}
            for table in expression.find_all(exp.Table):
                # sqlglot has tables without tables ie. schemas are tables
                if not table.this:
                    continue
                schema = table.db
                # add only tables from the dataset schema
                if not schema or schema.lower() == self.dataset_name.lower():
                    load_tables[table.name] = table.name

            if load_tables:
                self.create_views_for_tables(load_tables)

        with super().execute_query(query, *args, **kwargs) as cursor:
            yield cursor

    @staticmethod
    def _setup_iceberg(conn: duckdb.DuckDBPyConnection) -> None:
        # needed to make persistent secrets work in new connection
        # https://github.com/duckdb/duckdb_iceberg/issues/83
        conn.execute("FROM duckdb_secrets();")

        # `duckdb_iceberg` extension does not support autoloading
        # https://github.com/duckdb/duckdb_iceberg/issues/71
        conn.execute("INSTALL iceberg; LOAD iceberg;")

    def __del__(self) -> None:
        if self.memory_db:
            self.memory_db.close()
            self.memory_db = None
