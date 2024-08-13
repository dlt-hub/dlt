from typing import Any, Iterator, AnyStr, List

import os

import duckdb

import sqlglot
import sqlglot.expressions as exp

from contextlib import contextmanager

from dlt.destinations.typing import DBApiCursor

from dlt.destinations.sql_client import raise_database_error
from dlt.destinations.fs_client import FSClientBase

from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient, DuckDBDBApiCursorImpl
from dlt.destinations.impl.duckdb.configuration import DuckDbBaseCredentials


class FilesystemSqlClient(DuckDbSqlClient):
    def __init__(self, fs_client: FSClientBase, protocol: str) -> None:
        """For now we do not use any capabilities and do all operations in the default dataset"""
        """TODO: is this ok?"""
        super().__init__(
            dataset_name="default",
            staging_dataset_name=None,
            credentials=None,
            capabilities=None,
        )
        self.fs_client = fs_client
        self._conn = duckdb.connect(":memory:")
        self._conn.register_filesystem(self.fs_client.fs_client)
        self.existing_views: List[str] = []  # remember which views already where created
        self.protocol = protocol
        self.is_local_filesystem = protocol == "file"

    @raise_database_error
    def populate_duckdb(self, tables: List[str]) -> None:
        """Add the required tables as views to the duckdb in memory instance"""

        # create all tables in duck instance
        for ptable in tables:
            if ptable in self.existing_views:
                continue
            self.existing_views.append(ptable)

            folder = self.fs_client.get_table_dir(ptable)
            files = self.fs_client.list_table_files(ptable)

            # discover tables files
            file_type = os.path.splitext(files[0])[1][1:]
            if file_type == "jsonl":
                read_command = "read_json"
            elif file_type == "parquet":
                read_command = "read_parquet"
            else:
                raise AssertionError(f"Unknown filetype {file_type} for table {ptable}")

            # create table
            protocol = "" if self.is_local_filesystem else f"{self.protocol}://"
            files_string = f"'{protocol}{folder}/**/*.{file_type}'"
            create_table_sql_base = (
                f"CREATE VIEW {ptable} AS SELECT * FROM {read_command}([{files_string}])"
            )
            create_table_sql_gzipped = (
                f"CREATE VIEW {ptable} AS SELECT * FROM {read_command}([{files_string}],"
                " compression = 'gzip')"
            )
            try:
                self._conn.execute(create_table_sql_base)
            except (duckdb.InvalidInputException, duckdb.IOException):
                # try to load non gzipped files
                self._conn.execute(create_table_sql_gzipped)

    @contextmanager
    @raise_database_error
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBApiCursor]:
        assert isinstance(query, str)

        # find all tables to preload
        expression = sqlglot.parse_one(query, read="duckdb")
        load_tables = [t.name for t in expression.find_all(exp.Table)]
        self.populate_duckdb(load_tables)

        # TODO: raise on non-select queries here, they do not make sense in this context

        db_args = args if args else kwargs if kwargs else None
        if db_args:
            # TODO: must provide much better refactoring of params
            query = query.replace("%s", "?")
        try:
            self._conn.execute(query, db_args)
            yield DuckDBDBApiCursorImpl(self._conn)  # type: ignore
        except duckdb.Error as outer:
            raise outer

    def open_connection(self) -> None:
        """we are using an in memory instance, nothing to do"""
        pass

    def close_connection(self) -> None:
        """we are using an in memory instance, nothing to do"""
        pass
