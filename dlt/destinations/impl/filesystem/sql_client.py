from typing import Any, Iterator, AnyStr, List, cast, TYPE_CHECKING, Dict

import os

import dlt

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
        self.autocreate_required_views = True

        if self.fs_client.config.protocol not in SUPPORTED_PROTOCOLS:
            raise NotImplementedError(
                f"Protocol {self.fs_client.config.protocol} currently not supported for"
                f" FilesystemSqlClient. Supported protocols are {SUPPORTED_PROTOCOLS}."
            )

    def create_authentication(self, persistent: bool = False, secret_name: str = None) -> None:
        if not secret_name:
            secret_name = f"secret_{self.fs_client.config.protocol}"

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
                "For gs/gcs access via duckdb please use the gs/gcs s3 compatibility layer. Falling"
                " back to fsspec."
            )
            self._conn.register_filesystem(self.fs_client.fs_client)

        # for memory we also need to register filesystem
        elif self.fs_client.config.protocol == "memory":
            self._conn.register_filesystem(self.fs_client.fs_client)

    def open_connection(self) -> duckdb.DuckDBPyConnection:
        # we keep the in memory instance around, so if this prop is set, return it
        if not self._conn:
            super().open_connection()

            # set up connection and dataset
            self._existing_views: List[str] = []  # remember which views already where created

            self.autocreate_required_views = False
            if not self.has_dataset():
                self.create_dataset()
            self.autocreate_required_views = True
            self._conn.sql(f"USE {self.dataset_name}")

            # create authentication to data provider
            self.create_authentication()

        # the line below solves problems with certificate path lookup on linux
        # see duckdb docs
        self._conn.sql("SET azure_transport_option_type = 'curl';")

        return self._conn

    def close_connection(self) -> None:
        # we keep the local memory instance around as long this client exists
        if self.using_external_database:
            return super().close_connection()

    @raise_database_error
    def create_views_for_tables(self, tables: Dict[str, str]) -> None:
        """Add the required tables as views to the duckdb in memory instance"""

        # create all tables in duck instance
        for table_name in tables.keys():
            view_name = tables[table_name]
            if view_name in self._existing_views:
                continue
            if table_name not in self.fs_client.schema.tables:
                # unknown tables will not be created
                continue

            # discover file type
            schema_table = cast(PreparedTableSchema, self.fs_client.schema.tables[table_name])
            self._existing_views.append(view_name)
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

            # discover wether compression is enabled
            compression = (
                ""
                if dlt.config.get("data_writer.disable_compression")
                else ", compression = 'gzip'"
            )

            # create from statement
            from_statement = ""
            if schema_table.get("table_format") == "delta":
                from_statement = f"delta_scan('{resolved_folder}')"
            elif first_file_type == "parquet":
                from_statement = f"read_parquet([{resolved_files_string}])"
            elif first_file_type == "jsonl":
                from_statement = (
                    f"read_json([{resolved_files_string}], columns = {{{columns}}}) {compression}"
                )
            else:
                raise NotImplementedError(
                    f"Unknown filetype {first_file_type} for table {table_name}. Currently only"
                    " jsonl and parquet files as well as delta tables are supported."
                )

            # create table
            view_name = self.make_qualified_table_name(view_name)
            create_table_sql_base = f"CREATE VIEW {view_name} AS SELECT * FROM {from_statement}"
            self._conn.execute(create_table_sql_base)

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        # find all tables to preload
        if self.autocreate_required_views:  # skip this step when operating on the schema..
            expression = sqlglot.parse_one(query, read="duckdb")  # type: ignore
            load_tables = {t.name: t.name for t in expression.find_all(exp.Table)}
            self.create_views_for_tables(load_tables)

        # TODO: raise on non-select queries here, they do not make sense in this context
        with super().execute_query(query, *args, **kwargs) as cursor:
            yield cursor
