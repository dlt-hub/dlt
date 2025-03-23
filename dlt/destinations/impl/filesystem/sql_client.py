from typing import Any, TYPE_CHECKING
import os
import re
import duckdb

from dlt.common import logger
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.storages.configuration import FileSystemCredentials

from dlt.destinations.sql_client import raise_database_error
from dlt.destinations.impl.duckdb.sql_client import WithTableScanners
from dlt.destinations.impl.duckdb.factory import DuckDbCredentials

from dlt.destinations.utils import is_compression_disabled

SUPPORTED_PROTOCOLS = ["gs", "gcs", "s3", "file", "memory", "az", "abfss"]

if TYPE_CHECKING:
    from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
else:
    FilesystemClient = Any


class FilesystemSqlClient(WithTableScanners):
    def __init__(
        self,
        remote_client: FilesystemClient,
        dataset_name: str,
        cache_db: DuckDbCredentials = None,
    ) -> None:
        if remote_client.config.protocol not in SUPPORTED_PROTOCOLS:
            raise NotImplementedError(
                f"Protocol {remote_client.config.protocol} currently not supported for"
                f" FilesystemSqlClient. Supported protocols are {SUPPORTED_PROTOCOLS}."
            )
        super().__init__(remote_client, dataset_name, cache_db)
        self.remote_client: FilesystemClient = remote_client
        self.is_abfss = self.remote_client.config.protocol == "abfss"
        self.iceberg_initialized = False

    def _create_default_secret_name(self) -> str:
        regex = re.compile("[^a-zA-Z]")
        escaped_bucket_name = regex.sub("", self.remote_client.config.bucket_url.lower())
        return f"secret_{escaped_bucket_name}"

    def create_authentication(
        self,
        scope: str,
        credentials: FileSystemCredentials,
        persistent: bool = False,
        secret_name: str = None,
    ) -> bool:
        if not super().create_authentication(
            scope, credentials, persistent=persistent, secret_name=secret_name
        ):
            # native google storage implementation is not supported..
            protocol = self.remote_client.config.protocol
            if protocol in ["gs", "gcs"]:
                logger.warn(
                    "For gs/gcs access via duckdb please use the gs/gcs s3 compatibility layer if"
                    " possible (not supported when using `iceberg` table format). Falling back to"
                    " fsspec."
                )
                self._conn.register_filesystem(self.remote_client.fs_client)
            # for memory we also need to register filesystem
            elif protocol == "memory":
                self._conn.register_filesystem(self.remote_client.fs_client)
            elif protocol == "file":
                # authentication for local filesystem not needed
                pass
            else:
                raise ValueError(
                    f"Cannot create secret or register filesystem for protocol {protocol}"
                )
        return True

    def open_connection(self) -> duckdb.DuckDBPyConnection:
        first_connection = self.credentials.never_borrowed
        super().open_connection()

        if first_connection:
            # create single authentication for the whole client
            self.create_authentication(
                self.remote_client.config.bucket_url, self.remote_client.config.credentials
            )

        return self._conn

    def should_replace_view(self, view_name: str, table_schema: PreparedTableSchema) -> bool:
        # we use alternative method to get snapshot on abfss and we need to replace
        # the view each time to control the freshness (abfss cannot glob)
        return self.is_abfss  # and table_format == "iceberg"

    @raise_database_error
    def create_view(self, view_name: str, table_schema: PreparedTableSchema) -> None:
        # NOTE: data freshness
        # iceberg - currently we glob the most recent snapshot (via built in duckdb mechanism) so data is fresh
        #           (but not very efficient)
        # delta - newest version is always read
        # files - newest files
        table_name = table_schema["name"]
        table_format = table_schema.get("table_format")
        # discover file type
        folder = self.remote_client.get_table_dir(table_name)
        protocol = (
            ""
            if self.remote_client.is_local_filesystem
            else f"{self.remote_client.config.protocol}://"
        )
        resolved_folder = f"{protocol}{folder}"

        # discover whether compression is enabled
        compression = "" if is_compression_disabled() else ", compression = 'gzip'"

        # dlt tables are never compressed for now...
        if view_name in self.remote_client.schema.dlt_table_names():
            compression = ""

        # create from statement
        from_statement = ""
        if table_format == "delta":
            from_statement = f"delta_scan('{resolved_folder}')"
        elif table_format == "iceberg":
            if not self.iceberg_initialized:
                self._setup_iceberg(self._conn)
                self.iceberg_initialized = True
            # TODO: get version from a catalog if implemented

            if self.is_abfss:
                # duckdb can't glob on abfss 🤯
                from dlt.common.libs.pyiceberg import get_last_metadata_file

                metadata_path = f"{resolved_folder}/metadata"
                last_metadata_file = get_last_metadata_file(
                    metadata_path, self.remote_client.fs_client, self.remote_client.config
                )
                from_statement = f"iceberg_scan('{last_metadata_file}', skip_schema_inference=True)"
            else:
                # skip schema inference to make nested data types work
                # https://github.com/duckdb/duckdb_iceberg/issues/47
                from_statement = (
                    f"iceberg_scan('{resolved_folder}', version='?', allow_moved_paths = true,"
                    " skip_schema_inference=True)"
                )
        else:
            # discover file type
            files = self.remote_client.list_table_files(table_name)
            first_file_type = os.path.splitext(files[0])[1][1:]

            # build files string
            supports_wildcard_notation = self.remote_client.config.protocol != "abfss"
            resolved_files_string = f"'{resolved_folder}/**/*.{first_file_type}'"
            if not supports_wildcard_notation:
                resolved_files_string = ",".join(map(lambda f: f"'{protocol}{f}'", files))

            if first_file_type == "parquet":
                from_statement = f"read_parquet([{resolved_files_string}])"
            elif first_file_type == "jsonl":
                # build columns definition
                type_mapper = self.capabilities.get_type_mapper()
                columns = ",".join(
                    map(
                        lambda c: (
                            f'{self.escape_column_name(c["name"])}:'
                            f' "{type_mapper.to_destination_type(c, table_schema)}"'
                        ),
                        self.remote_client.schema.tables[table_name]["columns"].values(),
                    )
                )
                from_statement = (
                    f"read_json([{resolved_files_string}], columns = {{{columns}}}{compression})"
                )
            else:
                raise NotImplementedError(
                    f"Unknown filetype {first_file_type} for table {table_name}. Currently only"
                    " jsonl and parquet files as well as delta and iceberg tables are"
                    " supported."
                )

        # create table
        view_name = self.make_qualified_table_name(view_name)
        create_table_sql_base = (
            f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {from_statement}"
        )
        self._conn.execute(create_table_sql_base)
