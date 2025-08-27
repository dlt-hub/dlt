from typing import Any, TYPE_CHECKING, Tuple, List
import semver
import duckdb

from dlt.common import logger
from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.utils import is_nullable_column
from dlt.common.storages.configuration import FileSystemCredentials

from dlt.common.typing import TLoaderFileFormat
from dlt.destinations.sql_client import raise_database_error
from dlt.destinations.impl.duckdb.sql_client import WithTableScanners
from dlt.destinations.impl.duckdb.factory import DuckDbCredentials

from dlt.destinations.utils import is_compression_disabled
from dlt.destinations.path_utils import get_file_format_and_compression

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
        persist_secrets: bool = False,
    ) -> None:
        if remote_client.config.protocol not in SUPPORTED_PROTOCOLS:
            raise NotImplementedError(
                f"Received invalid value `protocol={remote_client.config.protocol}` for"
                f" `FilesystemSqlClient`. Valid values are: {SUPPORTED_PROTOCOLS}"
            )

        super().__init__(remote_client, dataset_name, cache_db, persist_secrets=persist_secrets)
        self.remote_client: FilesystemClient = remote_client
        self.is_abfss = self.remote_client.config.protocol == "abfss"
        self.iceberg_initialized = False
        if self.is_abfss:
            self._global_config["azure_transport_option_type"] = "curl"

    def can_create_view(self, table_schema: PreparedTableSchema) -> bool:
        if table_schema.get("table_format") in ("delta", "iceberg"):
            return True
        # checking file type is expensive so we optimistically allow to create view and prune later
        return True

    def get_file_format_and_files(
        self, table_schema: PreparedTableSchema
    ) -> Tuple[str, List[str], bool]:
        table_name = table_schema["name"]
        files = self.remote_client.list_table_files(table_name)
        if len(files) == 0:
            raise DestinationUndefinedEntity(table_name)
        file_format, is_compressed = get_file_format_and_compression(files[0])
        return file_format, files, is_compressed

    def create_secret(
        self,
        scope: str,
        credentials: FileSystemCredentials,
        secret_name: str = None,
    ) -> bool:
        protocol = self.remote_client.config.protocol
        if protocol == "file":
            return True
        if not super().create_secret(scope, credentials, secret_name=secret_name):
            # native google storage implementation is not supported..
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
                raise ValueError(f"Cannot create secret or register filesystem for `{protocol=:}`")

        return True

    def open_connection(self) -> duckdb.DuckDBPyConnection:
        first_connection = self.credentials.never_borrowed
        super().open_connection()

        if first_connection:
            # TODO: we need to frontload the httpfs extension for abfss for some reason
            if self.is_abfss:
                self._conn.sql("INSTALL httpfs; LOAD httpfs")

            # create single authentication for the whole client
            self.create_secret(
                self.remote_client.config.bucket_url, self.remote_client.config.credentials
            )
        return self._conn

    def should_replace_view(self, view_name: str, table_schema: PreparedTableSchema) -> bool:
        if self.remote_client.config.always_refresh_views:
            table_format = table_schema.get("table_format")
            if table_format == "delta":
                # delta will auto refresh
                return False
        return self.remote_client.config.always_refresh_views

    @raise_database_error
    def create_view(self, view_name: str, table_schema: PreparedTableSchema) -> None:
        # NOTE: data freshness
        # iceberg - currently we glob the most recent snapshot (via built in duckdb mechanism) so data is fresh
        #           (but not very efficient)
        # delta - newest version is always read
        # files - newest files
        table_name = table_schema["name"]
        table_format = table_schema.get("table_format")
        protocol = self.remote_client.config.protocol
        table_location = self.remote_client.get_open_table_location(table_format, table_name)

        dlt_table_names = self.remote_client.schema.dlt_table_names()

        def _escape_column_name(col_name: str) -> str:
            col_name = self.escape_column_name(col_name)
            # dlt tables are stored as json and never normalized
            if table_name in dlt_table_names:
                col_name = col_name.lower()
            return col_name

        # get columns to select from table schema
        columns = [_escape_column_name(c) for c in self.schema.get_table_columns(table_name).keys()]

        # create from statement
        from_statement = ""
        if table_format == "delta":
            table_location = table_location.rstrip("/")
            from_statement = f"delta_scan('{table_location}')"
        elif table_format == "iceberg":
            table_location = table_location.rstrip("/")
            if not self.iceberg_initialized:
                self._setup_iceberg(self._conn)
                self.iceberg_initialized = True

            from dlt.common.libs.pyiceberg import get_last_metadata_file

            metadata_path = f"{table_location}/metadata"
            last_metadata_file = get_last_metadata_file(
                metadata_path, self.remote_client.fs_client, self.remote_client.config
            )
            if ".gz." in last_metadata_file:
                compression = ", metadata_compression_codec = 'gzip'"
            else:
                compression = ""

            if semver.Version.parse(duckdb.__version__) > semver.Version.parse("1.3.0"):
                scanner_options = "union_by_name=true"
            else:
                scanner_options = "skip_schema_inference=false"

            from_statement = f"iceberg_scan('{last_metadata_file}'{compression}, {scanner_options})"
            # TODO: on duckdb > 1.2.1 register self.remote_client.fs_client as abfss fsspec filesystem
            #   this will enable iceberg but with lower performance
        else:
            # get file format and list of table files
            # NOTE: this does not support cases where table contains many different file formats
            # NOTE: since we must list all the files anyway we just pass them to duckdb without further globbing
            #   list is in the memory already and query size in duckdb is very large
            first_file_type, files, _ = self.get_file_format_and_files(table_schema)
            if protocol == "file":
                resolved_files_string = ",".join(map(lambda f: f"'{f}'", files))
            else:
                resolved_files_string = ",".join(map(lambda f: f"'{protocol}://{f}'", files))

            # NOTE: duckdb automatically handles compression based on the .gz extension
            # so we don't need to specify it
            if first_file_type == "parquet":
                from_statement = f"read_parquet([{resolved_files_string}], union_by_name=true)"
            elif first_file_type in ("jsonl", "csv"):
                # build columns definition
                type_mapper = self.capabilities.get_type_mapper()
                columns_defs = self.schema.get_table_columns(table_name).values()
                column_types = ",".join(
                    map(
                        lambda c: (
                            f'{_escape_column_name(c["name"])}:'
                            f' "{type_mapper.to_destination_type(c, table_schema)}"'
                        ),
                        columns_defs,
                    )
                )
                if first_file_type == "jsonl":
                    # swap binary types
                    for idx, column_def in enumerate(columns_defs):
                        if column_def["data_type"] == "binary":
                            columns[idx] = f"from_base64(decode({columns[idx]})) as {columns[idx]}"
                    from_statement = (
                        f"read_json([{resolved_files_string}], columns = {{{column_types}}})"
                    )
                if first_file_type == "csv":
                    # TODO: use default csv_format config to set params below
                    not_null_columns = [
                        _escape_column_name(c["name"])
                        for c in columns_defs
                        if not is_nullable_column(c)
                    ]
                    if not_null_columns:
                        force_not_null = f"force_not_null=[{','.join(not_null_columns)}],"
                    else:
                        force_not_null = ""
                    # use `types` (and not `columns`) options below. columns does not
                    # work with multiple csv files with evolving schemas. they disable
                    # autodetect and lock schema for all files
                    from_statement = (
                        f"read_csv([{resolved_files_string}],{force_not_null} union_by_name=true,header=true,null_padding=true,types="
                        f" {{{column_types}}})"
                    )

                # if the dataset is a legacy version where .gz is not added by default,
                # we need to check configs
                if (
                    table_name not in dlt_table_names
                    and self.remote_client.storage_versions[0] == 1
                    and not is_compression_disabled()
                ):
                    from_statement = from_statement[:-1] + ", compression = 'gzip')"

            else:
                # we skipped checking file type in can_create_view to not repeat globs which are expensive
                # so we skip here.
                return
                # raise NotImplementedError(
                #     f"Unknown filetype {first_file_type} for table {table_name}. Currently only"
                #     " jsonl and parquet files as well as delta and iceberg tables are"
                #     " supported."
                # )

        # create table
        view_name = self.make_qualified_table_name(view_name)
        create_table_sql_base = (
            f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(columns)} FROM"
            f" {from_statement}"
        )
        self._conn.execute(create_table_sql_base)
