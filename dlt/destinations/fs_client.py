from typing import Iterable, cast, Any, List
from abc import ABC, abstractmethod
from fsspec import AbstractFileSystem


class FSClientBase(ABC):
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
        encoding: Any = None,
        errors: Any = None,
        newline: Any = None,
        **kwargs: Any
    ) -> str:
        """reads given file into string"""
        return cast(str, self.fs_client.read_text(path, encoding, errors, newline, **kwargs))
