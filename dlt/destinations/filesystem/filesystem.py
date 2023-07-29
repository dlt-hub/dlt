import posixpath
import threading
import os
from types import TracebackType
from typing import ClassVar, List, Sequence, Type, Iterable, cast
from fsspec import AbstractFileSystem

from dlt.common.schema import Schema, TTableSchema
from dlt.common.schema.typing import TWriteDisposition, LOADS_TABLE_NAME
from dlt.common.storages import FileStorage
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import NewLoadJob, TLoadJobState, LoadJob, JobClientBase, FollowupJob
from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.filesystem import capabilities
from dlt.destinations.filesystem.configuration import FilesystemClientConfiguration
from dlt.destinations.filesystem.filesystem_client import client_from_config
from dlt.common.storages import LoadStorage
from dlt.destinations.job_impl import NewLoadJobImpl
from dlt.destinations.job_impl import NewReferenceJob


class LoadFilesystemJob(LoadJob):
    def __init__(
            self,
            local_path: str,
            dataset_path: str,
            *,
            config: FilesystemClientConfiguration,
            write_disposition: TWriteDisposition,
            has_merge_keys: bool,
            schema_name: str,
            load_id: str
    ) -> None:
        file_name = FileStorage.get_file_name_from_file_path(local_path)
        self.config = config
        self.dataset_path = dataset_path
        self.destination_file_name = LoadFilesystemJob.make_destination_filename(file_name, schema_name, load_id)

        super().__init__(file_name)
        fs_client, _ = client_from_config(config)
        self.destination_file_name = LoadFilesystemJob.make_destination_filename(file_name, schema_name, load_id)
        fs_client.put_file(local_path, self.make_remote_path())

    @staticmethod
    def make_destination_filename(file_name: str, schema_name: str, load_id: str) -> str:
        job_info = LoadStorage.parse_job_file_name(file_name)
        return f"{schema_name}.{job_info.table_name}.{load_id}.{job_info.file_id}.{job_info.file_format}"

    def make_remote_path(self) -> str:
        return f"{self.config.protocol}://{posixpath.join(self.dataset_path, self.destination_file_name)}"

    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()


class FollowupFilesystemJob(FollowupJob, LoadFilesystemJob):
    def create_followup_jobs(self, next_state: str) -> List[NewLoadJob]:
        jobs = super().create_followup_jobs(next_state)
        if next_state == "completed":
            ref_job = NewReferenceJob(file_name=self.file_name(), status="running", remote_path=self.make_remote_path())
            jobs.append(ref_job)
        return jobs


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

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        # clean up existing files for tables selected for truncating
        if truncate_tables and self.fs_client.isdir(self.dataset_path):
            all_files = self.fs_client.ls(self.dataset_path, detail=False, refresh=True)
            for table in truncate_tables:
                search_prefix = posixpath.join(self.dataset_path, f"{self.schema.name}.{table}.")
                for item in all_files:
                    # NOTE: glob implementation in fsspec does not look thread safe, way better is to use ls and then filter
                    if item.startswith(search_prefix):
                        # NOTE: deleting in chunks on s3 does not raise on access denied, file non existing and probably other errors
                        self.fs_client.rm_file(item)

        # create destination dir
        self.fs_client.makedirs(self.dataset_path, exist_ok=True)

    def is_storage_initialized(self) -> bool:
        return self.fs_client.isdir(self.dataset_path)  # type: ignore[no-any-return]

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        cls = FollowupFilesystemJob if self.config.as_staging else LoadFilesystemJob
        has_merge_keys = any(col['merge_key'] or col['primary_key'] for col in table['columns'].values())
        return cls(
            file_path,
            self.dataset_path,
            config=self.config,
            write_disposition=table['write_disposition'],
            has_merge_keys=has_merge_keys,
            schema_name=self.schema.name,
            load_id=load_id
        )

    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")

    def complete_load(self, load_id: str) -> None:
        schema_name = self.schema.name
        table_name = LOADS_TABLE_NAME
        file_name = f"{schema_name}.{table_name}.{load_id}"
        self.fs_client.touch(posixpath.join(self.dataset_path, file_name))

    def __enter__(self) -> "FilesystemClient":
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        pass
