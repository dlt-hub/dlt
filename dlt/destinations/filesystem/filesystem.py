import posixpath
from types import TracebackType
from typing import ClassVar, Sequence, Type, Iterable
from fsspec import AbstractFileSystem

from dlt.common.schema import Schema, TTableSchema
from dlt.common.schema.typing import TWriteDisposition, LOADS_TABLE_NAME
from dlt.common.storages import FileStorage
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import FollowupJob, NewLoadJob, TLoadJobState, LoadJob, JobClientBase
from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.filesystem import capabilities
from dlt.destinations.filesystem.configuration import FilesystemClientConfiguration
from dlt.destinations.filesystem.filesystem_client import client_from_config
from dlt.common.storages import LoadStorage


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

        root_path = posixpath.join(fs_path, dataset_name)

        if write_disposition == 'merge':
            write_disposition = 'append' if has_merge_keys else 'replace'

        if write_disposition == 'replace':
            job_info = LoadStorage.parse_job_file_name(file_name)
            glob_path = posixpath.join(root_path, f"{schema_name}.{job_info.table_name}.*")
            items = fs_client.glob(glob_path)
            if items:
                fs_client.rm(items)

        destination_file_name = LoadFilesystemJob.make_destination_filename(file_name, schema_name, load_id)
        fs_client.put_file(file_path, posixpath.join(root_path, destination_file_name))

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
    def dataset_path(self) -> str:
        return posixpath.join(self.fs_path, self.config.dataset_name)

    def initialize_storage(self, staging: bool = False, truncate_tables: Iterable[str] = None) -> None:
        self.fs_client.makedirs(self.dataset_path, exist_ok=True)

    def is_storage_initialized(self, staging: bool = False) -> bool:
        return self.fs_client.isdir(self.dataset_path)  # type: ignore[no-any-return]

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
        schema_name = self.schema.name
        table_name = LOADS_TABLE_NAME
        file_name = f"{schema_name}.{table_name}.{load_id}"
        self.fs_client.touch(posixpath.join(self.dataset_path, file_name))

    def __enter__(self) -> "FilesystemClient":
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        pass
