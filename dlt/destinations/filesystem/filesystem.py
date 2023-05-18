import random
from pathlib import Path
from copy import copy
from types import TracebackType
from typing import ClassVar, Dict, Optional, Sequence, Type, Iterable
from fsspec.core import url_to_fs
from fsspec import AbstractFileSystem
import fsspec

from dlt.common import pendulum
from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.schema.typing import TWriteDisposition
from dlt.common.storages import FileStorage
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import FollowupJob, NewLoadJob, TLoadJobState, LoadJob, JobClientBase
from dlt.destinations.job_impl import EmptyLoadJob

from dlt.destinations.exceptions import (LoadJobNotExistsException, LoadJobInvalidStateTransitionException,
                                            DestinationTerminalException, DestinationTransientException)

from dlt.destinations.filesystem import capabilities
from dlt.destinations.filesystem.configuration import FilesystemClientConfiguration
from dlt.destinations.filesystem.filesystem_client import client_from_config
from dlt.common.storages import LoadStorage
from fsspec import AbstractFileSystem


class LoadFilesystemJob(LoadJob, FollowupJob):
    def __init__(self, file_path: str, write_disposition: TWriteDisposition, schema_name: str, load_id: str, fs_client: AbstractFileSystem, fs_path: str, dataset_name: str) -> None:
        file_name = FileStorage.get_file_name_from_file_path(file_path)
        super().__init__(file_name)

        job_info = LoadStorage.parse_job_file_name(file_name)

        root_path = Path(fs_path).joinpath(dataset_name)
        fs_client.makedirs(root_path, exist_ok=True)

        if write_disposition == 'replace':
            glob_path = str(root_path.joinpath(f"{schema_name}.{job_info.table_name}.*"))
            items = fs_client.glob(glob_path)
            if items:
                fs_client.rm(items)

        destination_file_name = f"{schema_name}.{job_info.table_name}.{load_id}.{job_info.file_id}.{job_info.file_format}"
        fs_client.put_file(file_path, root_path.joinpath(destination_file_name))

    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()


class FilesystemClient(JobClientBase):
    """filesystem client storing jobs in memory"""

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()
    fs_client: AbstractFileSystem
    fs_path: str

    def __init__(self, schema: Schema, config: FilesystemClientConfiguration) -> None:
        super().__init__(schema, config)
        self.fs_client, self.fs_path = client_from_config(config)
        self.config: FilesystemClientConfiguration = config

    def initialize_storage(self, staging: bool = False, truncate_tables: Iterable[str] = None) -> None:
        self.fs_client.makedirs(Path(self.fs_path).joinpath(self.config.dataset_name), exist_ok=True)

    def is_storage_initialized(self, staging: bool = False) -> bool:
        return self.fs_client.isdir(self.config.dataset_name)  # type: ignore[no-any-return]

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        return LoadFilesystemJob(
            file_path, table['write_disposition'], self.schema.name, load_id, self.fs_client, self.fs_path, self.config.dataset_name
        )

    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")

    def create_merge_job(self, table_chain: Sequence[TTableSchema]) -> NewLoadJob:
        return None

    def complete_load(self, load_id: str) -> None:
        pass

    def __enter__(self) -> "FilesystemClient":
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        pass
