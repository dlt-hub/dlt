from typing import Any, Iterator, AnyStr, List, cast, TYPE_CHECKING, Dict

import os

import duckdb

import sqlglot
import sqlglot.expressions as exp
from dlt.common import logger

from contextlib import contextmanager

from dlt.common.destination.reference import DBApiCursor
from dlt.common.destination.typing import PreparedTableSchema

from dlt.destinations.sql_client import raise_database_error

from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient
from dlt.destinations.impl.duckdb.factory import duckdb as duckdb_factory, DuckDbCredentials
from dlt.common.configuration.specs import (
    AwsCredentials,
    AzureServicePrincipalCredentialsWithoutDefaults,
    AzureCredentialsWithoutDefaults,
)

SUPPORTED_PROTOCOLS = ["gs", "gcs", "s3", "file", "memory", "az", "abfss"]

if TYPE_CHECKING:
    from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
else:
    FilesystemClient = Any


class FilesystemSqlClient(DuckDbSqlClient):
    def __init__(
        self,
        fs_client: FilesystemClient,
        dataset_name: str = None,
        duckdb_connection: duckdb.DuckDBPyConnection = None,
    ) -> None:
        super().__init__(
            dataset_name=dataset_name or fs_client.dataset_name,
            staging_dataset_name=None,
            credentials=DuckDbCredentials(duckdb_connection or ":memory:"),
            capabilities=duckdb_factory()._raw_capabilities(),
        )
        self.fs_client = fs_client
        self.using_external_database = duckdb_connection is not None
        self.create_persistent_secrets = False
        self.autocreate_required_views = False

        if self.fs_client.config.protocol not in SUPPORTED_PROTOCOLS:
            raise NotImplementedError(
                f"Protocol {self.fs_client.config.protocol} currently not supported for"
                f" FilesystemSqlClient. Supported protocols are {SUPPORTED_PROTOCOLS}."
            )

    def open_connection(self) -> duckdb.DuckDBPyConnection:
        # we keep the in memory instance around, so if this prop is set, return it
        if self._conn:
            return self._conn
        super().open_connection()

        # set up connection and dataset
        self._existing_views: List[str] = []  # remember which views already where created
        if not self.has_dataset():
            self.create_dataset()
        self._conn.sql(f"USE {self.dataset_name}")
        self.autocreate_required_views = True

        persistent = ""
        if self.create_persistent_secrets:
            persistent = " PERSISTENT "

        # add secrets required for creating views
        if self.fs_client.config.protocol == "s3":
            aws_creds = cast(AwsCredentials, self.fs_client.config.credentials)
            endpoint = (
                aws_creds.endpoint_url.replace("https://", "")
                if aws_creds.endpoint_url
                else "s3.amazonaws.com"
            )
            self._conn.sql(f"""
            CREATE {persistent} SECRET secret_aws (
                TYPE S3,
                KEY_ID '{aws_creds.aws_access_key_id}',
                SECRET '{aws_creds.aws_secret_access_key}',
                REGION '{aws_creds.region_name}',
                ENDPOINT '{endpoint}'
            );""")

        # azure with storage account creds
        elif self.fs_client.config.protocol in ["az", "abfss"] and isinstance(
            self.fs_client.config.credentials, AzureCredentialsWithoutDefaults
        ):
            azsa_creds = self.fs_client.config.credentials
            self._conn.sql(f"""
            CREATE {persistent} SECRET secret_az (
                TYPE AZURE,
                CONNECTION_STRING 'AccountName={azsa_creds.azure_storage_account_name};AccountKey={azsa_creds.azure_storage_account_key}'
            );""")

        # azure with service principal creds
        elif self.fs_client.config.protocol in ["az", "abfss"] and isinstance(
            self.fs_client.config.credentials, AzureServicePrincipalCredentialsWithoutDefaults
        ):
            azsp_creds = self.fs_client.config.credentials
            self._conn.sql(f"""
            CREATE SECRET secret_az (
                TYPE AZURE,
                PROVIDER SERVICE_PRINCIPAL,
                TENANT_ID '{azsp_creds.azure_tenant_id}',
                CLIENT_ID '{azsp_creds.azure_client_id}',
                CLIENT_SECRET '{azsp_creds.azure_client_secret}',
                ACCOUNT_NAME '{azsp_creds.azure_storage_account_name}'
            );""")

        # native google storage implementation is not supported..
        elif self.fs_client.config.protocol in ["gs", "gcs"]:
            logger.warn(
                "For gs/gcs access via duckdb please use the gs/gcs s3 compatibility layer. Falling"
                " back to fsspec."
            )
            self._conn.register_filesystem(self.fs_client.fs_client)

        # for memory we also need to register filesystem
        elif self.fs_client.config.protocol == "memory":
            self._conn.register_filesystem(self.fs_client.fs_client)

        return self._conn

    def close_connection(self) -> None:
        # we keep the local memory instance around as long this client exists
        if self.using_external_database:
            return super().close_connection()

    @raise_database_error
    def create_view_for_tables(self, tables: Dict[str, str]) -> None:
        """Add the required tables as views to the duckdb in memory instance"""

        # create all tables in duck instance
        for table_name in tables.keys():
            view_name = tables[table_name]
            if view_name in self._existing_views:
                continue
            if table_name not in self.fs_client.schema.tables:
                # unknown tables will not be created
                continue
            self._existing_views.append(view_name)

            folder = self.fs_client.get_table_dir(table_name)
            files = self.fs_client.list_table_files(table_name)

            # discover tables files
            file_type = os.path.splitext(files[0])[1][1:]
            columns_string = ""
            if file_type == "jsonl":
                read_command = "read_json"
                # for json we need to provide types
                type_mapper = self.capabilities.get_type_mapper()
                schema_table = cast(PreparedTableSchema, self.fs_client.schema.tables[table_name])
                columns = map(
                    lambda c: (
                        f'{self.escape_column_name(c["name"])}:'
                        f' "{type_mapper.to_destination_type(c, schema_table)}"'
                    ),
                    self.fs_client.schema.tables[table_name]["columns"].values(),
                )
                columns_string = ",columns = {" + ",".join(columns) + "}"

            elif file_type == "parquet":
                read_command = "read_parquet"
            else:
                raise NotImplementedError(
                    f"Unknown filetype {file_type} for table {table_name}. Currently only jsonl and"
                    " parquet files are supported."
                )

            # build files string
            protocol = (
                "" if self.fs_client.is_local_filesystem else f"{self.fs_client.config.protocol}://"
            )
            supports_wildcard_notation = self.fs_client.config.protocol != "abfss"
            files_string = f"'{protocol}{folder}/**/*.{file_type}'"
            if not supports_wildcard_notation:
                files_string = ",".join(
                    map(lambda f: f"'{self.fs_client.config.protocol }{f}'", files)
                )

            # create table
            view_name = self.make_qualified_table_name(view_name)
            create_table_sql_base = (
                f"CREATE VIEW {view_name} AS SELECT * FROM"
                f" {read_command}([{files_string}] {columns_string})"
            )
            create_table_sql_gzipped = (
                f"CREATE VIEW {view_name} AS SELECT * FROM"
                f" {read_command}([{files_string}] {columns_string} , compression = 'gzip')"
            )
            try:
                self._conn.execute(create_table_sql_base)
            except (duckdb.InvalidInputException, duckdb.IOException):
                # try to load non gzipped files
                self._conn.execute(create_table_sql_gzipped)

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        # find all tables to preload
        if self.autocreate_required_views:  # skip this step when operating on the schema..
            expression = sqlglot.parse_one(query, read="duckdb")  # type: ignore
            load_tables = {t.name: t.name for t in expression.find_all(exp.Table)}
            self.create_view_for_tables(load_tables)

        # TODO: raise on non-select queries here, they do not make sense in this context
        with super().execute_query(query, *args, **kwargs) as cursor:
            yield cursor
