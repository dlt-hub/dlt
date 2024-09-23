from typing import Any, Iterator, AnyStr, List, cast

import os

import duckdb

import sqlglot
import sqlglot.expressions as exp

from contextlib import contextmanager

from dlt.common.destination.reference import DBApiCursor
from dlt.common.destination.typing import PreparedTableSchema

from dlt.destinations.sql_client import raise_database_error
from dlt.destinations.fs_client import FSClientBase

from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient
from dlt.destinations.impl.duckdb.factory import duckdb as duckdb_factory

SUPPORTED_PROTOCOLS = ["gs", "gcs", "s3", "file", "memory"]


class FilesystemSqlClient(DuckDbSqlClient):
    def __init__(self, fs_client: FSClientBase, protocol: str, dataset_name: str) -> None:
        super().__init__(
            dataset_name=dataset_name,
            staging_dataset_name=None,
            credentials=None,
            capabilities=duckdb_factory()._raw_capabilities(),
        )
        self.fs_client = fs_client
        self.existing_views: List[str] = []  # remember which views already where created
        self.protocol = protocol
        self.is_local_filesystem = protocol == "file"

        if protocol not in SUPPORTED_PROTOCOLS:
            raise NotImplementedError(
                f"Protocol {protocol} currently not supported for FilesystemSqlClient. Supported"
                f" protocols are {SUPPORTED_PROTOCOLS}."
            )

        # set up duckdb instance
        self._conn = duckdb.connect(":memory:")
        self._conn.sql(f"CREATE SCHEMA {self.dataset_name}")
        self._conn.register_filesystem(self.fs_client.fs_client)

    @raise_database_error
    def populate_duckdb(self, tables: List[str]) -> None:
        """Add the required tables as views to the duckdb in memory instance"""

        # create all tables in duck instance
        for table_name in tables:
            if table_name in self.existing_views:
                continue
            self.existing_views.append(table_name)

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

            # create table
            protocol = "" if self.is_local_filesystem else f"{self.protocol}://"
            files_string = f"'{protocol}{folder}/**/*.{file_type}'"
            table_name = self.make_qualified_table_name(table_name)
            create_table_sql_base = (
                f"CREATE VIEW {table_name} AS SELECT * FROM"
                f" {read_command}([{files_string}] {columns_string})"
            )
            create_table_sql_gzipped = (
                f"CREATE VIEW {table_name} AS SELECT * FROM"
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
        expression = sqlglot.parse_one(query, read="duckdb")  # type: ignore
        load_tables = [t.name for t in expression.find_all(exp.Table)]
        self.populate_duckdb(load_tables)

        # TODO: raise on non-select queries here, they do not make sense in this context
        with super().execute_query(query, *args, **kwargs) as cursor:
            yield cursor

    def open_connection(self) -> None:
        """we are using an in memory instance, nothing to do"""
        pass

    def close_connection(self) -> None:
        """we are using an in memory instance, nothing to do"""
        pass
