from typing import Iterable, cast, Any, List, Literal

import gzip
from abc import ABC, abstractmethod
from fsspec import AbstractFileSystem

from dlt.common.typing import DuckDBPyConnection
from dlt.common.destination.reference import SupportsDataAccess


class FSClientBase(SupportsDataAccess, ABC):
    fs_client: AbstractFileSystem

    @property
    @abstractmethod
    def dataset_path(self) -> str:
        pass

    @abstractmethod
    def get_table_dir(self, table_name: str) -> str:
        """returns directory for given table"""
        pass

    @abstractmethod
    def get_table_dirs(self, table_names: Iterable[str]) -> List[str]:
        """returns directories for given table"""
        pass

    @abstractmethod
    def list_table_files(self, table_name: str) -> List[str]:
        """returns all filepaths for a given table"""
        pass

    @abstractmethod
    def truncate_tables(self, table_names: List[str]) -> None:
        """truncates the given table"""
        pass

    def read_bytes(self, path: str, start: Any = None, end: Any = None, **kwargs: Any) -> bytes:
        """reads given file to bytes object"""
        return cast(bytes, self.fs_client.read_bytes(path, start, end, **kwargs))

    def read_text(
        self,
        path: str,
        encoding: Any = "utf-8",
        errors: Any = None,
        newline: Any = None,
        compression: str = None,
        **kwargs: Any
    ) -> str:
        """reads given file into string, tries gzip and pure text"""
        if compression is None:
            try:
                return self.read_text(path, encoding, errors, newline, "gzip", **kwargs)
            except (gzip.BadGzipFile, OSError):
                pass
        with self.fs_client.open(
            path, mode="rt", compression=compression, encoding=encoding, newline=newline
        ) as f:
            return cast(str, f.read())

    @abstractmethod
    def get_duckdb(
        self,
        tables: List[str],
        db: DuckDBPyConnection = None,
        table_type: Literal["view", "table"] = "view",
    ) -> DuckDBPyConnection:
        """
        Returns a duckdb in memory instance with given tables loaded as views or tables.
        Can also take an existing duckdb object to add tables from the filesystem.
        """
        pass
