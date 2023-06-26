import posixpath
import threading
from types import TracebackType
from typing import ClassVar, List, Sequence, Type, Iterable
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

lock = threading.Lock()

class NewReferenceJob(NewLoadJobImpl):
    pass

class LoadFilesystemJob(LoadJob, FollowupJob):
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
        super().__init__(file_name)
        fs_client, _ = client_from_config(config)

        # fallback to replace for merge without any merge keys
        if write_disposition == 'merge':
            write_disposition = 'append' if has_merge_keys else 'replace'

        # replace existing files. also check if dir exists for bucket storages that cannot create dirs
        if write_disposition == 'replace' and fs_client.isdir(dataset_path):
            job_info = LoadStorage.parse_job_file_name(file_name)
            search_prefix = posixpath.join(dataset_path, f"{schema_name}.{job_info.table_name}.")
            # NOTE: glob implementation in fsspec does not look thread safe, way better is to use ls and then filter
            all_files: List[str] = fs_client.ls(dataset_path, detail=False, refresh=True)
            items = [item for item in all_files if item.startswith(search_prefix)]
            if items:
                fs_client.rm(items)

        self.destination_file_name = LoadFilesystemJob.make_destination_filename(file_name, schema_name, load_id)
        fs_client.put_file(local_path, posixpath.join(dataset_path, self.destination_file_name))

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

    def create_merge_job(self, table_chain: Sequence[TTableSchema]) -> NewLoadJob:
        return None
    
    def create_reference_job(self, job: LoadFilesystemJob) -> NewLoadJob:
        file_name = (".").join(job.file_name().split(".")[0:-1] + ["reference"])
        ref_job = NewReferenceJob(file_name=file_name, status="running")
        remote_path = f"{self.config.protocol}://{posixpath.join(self.dataset_path, job.destination_file_name)}"
        ref_job._save_text_file(remote_path)
        return ref_job

    def complete_load(self, load_id: str) -> None:
        schema_name = self.schema.name
        table_name = LOADS_TABLE_NAME
        file_name = f"{schema_name}.{table_name}.{load_id}"
        self.fs_client.touch(posixpath.join(self.dataset_path, file_name))

    def __enter__(self) -> "FilesystemClient":
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        pass
