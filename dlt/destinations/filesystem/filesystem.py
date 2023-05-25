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
    def __init__(
            self,
            *,
            file_path: str,
            write_disposition: TWriteDisposition,
            has_merge_keys: bool,
            schema_name: str,
            dataset_name: str,
            load_id: str,
            fs_client: AbstractFileSystem,
            fs_path: str,
    ) -> None:
        file_name = FileStorage.get_file_name_from_file_path(file_path)
        super().__init__(file_name)

        root_path = Path(fs_path).joinpath(dataset_name)

        if write_disposition == 'merge':
            write_disposition = 'append' if has_merge_keys else 'replace'

        if write_disposition == 'replace':
            job_info = LoadStorage.parse_job_file_name(file_name)
            glob_path = root_path.joinpath(f"{schema_name}.{job_info.table_name}.*").as_posix()
            items = fs_client.glob(glob_path)
            if items:
                fs_client.rm(items)

        destination_file_name = LoadFilesystemJob.make_destination_filename(file_name, schema_name, load_id)
        fs_client.put_file(file_path, root_path.joinpath(destination_file_name).as_posix())

    @staticmethod
    def make_destination_filename(file_name: str, schema_name: str, load_id: str) -> str:
        job_info = LoadStorage.parse_job_file_name(file_name)
        return f"{schema_name}.{job_info.table_name}.{load_id}.{job_info.file_id}.{job_info.file_format}"

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

    @property
    def dataset_path(self) -> Path:
        return Path(self.fs_path).joinpath(self.config.dataset_name)

    def initialize_storage(self, staging: bool = False, truncate_tables: Iterable[str] = None) -> None:
        self.fs_client.makedirs(self.dataset_path.as_posix(), exist_ok=True)

    def is_storage_initialized(self, staging: bool = False) -> bool:
        return self.fs_client.isdir(self.dataset_path.as_posix())  # type: ignore[no-any-return]

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        has_merge_keys = any(col['merge_key'] or col['primary_key'] for col in table['columns'].values())
        return LoadFilesystemJob(
            file_path=file_path,
            write_disposition=table['write_disposition'],
            has_merge_keys=has_merge_keys,
            schema_name=self.schema.name,
            dataset_name=self.config.dataset_name,
            load_id=load_id,
            fs_client=self.fs_client,
            fs_path=self.fs_path
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
