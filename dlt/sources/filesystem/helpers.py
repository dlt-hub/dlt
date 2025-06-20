"""Helpers for the filesystem resource."""
from typing import Any, Dict, Iterable, List, Optional, Type, Union
from fsspec import AbstractFileSystem

import dlt
from dlt.common.configuration import resolve_type
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.storages.fsspec_filesystem import fsspec_from_config
from dlt.common.storages import FilesystemConfigurationWithLocalFiles
from dlt.common.typing import TDataItem

from dlt.sources import DltResource
from dlt.sources.config import configspec, with_config
from dlt.sources.credentials import (
    CredentialsConfiguration,
    FilesystemConfiguration,
    FileSystemCredentials,
)

from .settings import DEFAULT_CHUNK_SIZE


@configspec
class FilesystemConfigurationResource(FilesystemConfigurationWithLocalFiles):
    credentials: Union[FileSystemCredentials, AbstractFileSystem] = None
    file_glob: Optional[str] = "*"
    files_per_page: int = DEFAULT_CHUNK_SIZE
    extract_content: bool = False

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        # also allow AbstractFileSystem to be directly passed
        return Union[self.PROTOCOL_CREDENTIALS.get(self.protocol) or Optional[CredentialsConfiguration], AbstractFileSystem]  # type: ignore[return-value]


def fsspec_from_resource(filesystem_instance: DltResource) -> AbstractFileSystem:
    """Extract authorized fsspec client from a filesystem resource"""

    @with_config(
        spec=FilesystemConfiguration,
        sections=(known_sections.SOURCES, filesystem_instance.section, filesystem_instance.name),
        sections_merge_style=ConfigSectionContext.resource_merge_style,
    )
    def _get_fsspec(
        bucket_url: str,
        credentials: Optional[FileSystemCredentials],
        _spec: FilesystemConfiguration = None,
    ) -> AbstractFileSystem:
        return fsspec_from_config(_spec)[0]

    return _get_fsspec(
        filesystem_instance.explicit_args.get("bucket_url", dlt.config.value),
        filesystem_instance.explicit_args.get("credentials", dlt.secrets.value),
    )


def add_columns(columns: List[str], rows: List[List[Any]]) -> List[Dict[str, Any]]:
    """Adds column names to the given rows.

    Args:
        columns (List[str]): The column names.
        rows (List[List[Any]]): The rows.

    Returns:
        List[Dict[str, Any]]: The rows with column names.
    """
    result = []
    for row in rows:
        result.append(dict(zip(columns, row)))

    return result


def fetch_arrow(file_data, chunk_size: int) -> Iterable[TDataItem]:  # type: ignore
    """Fetches data from the given CSV file.

    Args:
        file_data (DuckDBPyRelation): The CSV file data.
        chunk_size (int): The number of rows to read at once.

    Yields:
        Iterable[TDataItem]: Data items, read from the given CSV file.
    """
    batcher = file_data.fetch_arrow_reader(batch_size=chunk_size)
    yield from batcher


def fetch_json(file_data, chunk_size: int) -> List[Dict[str, Any]]:  # type: ignore
    """Fetches data from the given CSV file.

    Args:
        file_data (DuckDBPyRelation): The CSV file data.
        chunk_size (int): The number of rows to read at once.

    Yields:
        Iterable[TDataItem]: Data items, read from the given CSV file.
    """
    while True:
        batch = file_data.fetchmany(chunk_size)
        if not batch:
            break

        yield add_columns(file_data.columns, batch)
