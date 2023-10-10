import posixpath
import os
from types import TracebackType
from typing import ClassVar, List, Type, Iterable, Set
from fsspec import AbstractFileSystem

from dlt.common import logger
from dlt.common.schema import Schema, TSchemaTables, TTableSchema
from dlt.common.storages import FileStorage, LoadStorage, fsspec_from_config
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import NewLoadJob, TLoadJobState, LoadJob, JobClientBase, FollowupJob

from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.filesystem import capabilities
from dlt.destinations.filesystem.configuration import FilesystemDestinationClientConfiguration
from dlt.destinations.job_impl import NewReferenceJob
from dlt.destinations import path_utils


class LoadFilesystemJob(LoadJob):
    def __init__(
            self,
            local_path: str,
            dataset_path: str,
            *,
            config: FilesystemDestinationClientConfiguration,
            schema_name: str,
            load_id: str
    ) -> None:
        file_name = FileStorage.get_file_name_from_file_path(local_path)
        self.config = config
        self.dataset_path = dataset_path
        self.destination_file_name = LoadFilesystemJob.make_destination_filename(config.layout, file_name, schema_name, load_id)

        super().__init__(file_name)
        fs_client, _ = fsspec_from_config(config)
        self.destination_file_name = LoadFilesystemJob.make_destination_filename(config.layout, file_name, schema_name, load_id)
        item = self.make_remote_path()
        logger.info("PUT file {item}")
        fs_client.put_file(local_path, item)

    @staticmethod
    def make_destination_filename(layout: str, file_name: str, schema_name: str, load_id: str) -> str:
        job_info = LoadStorage.parse_job_file_name(file_name)
        return path_utils.create_path(layout,
                                      schema_name=schema_name,
                                      table_name=job_info.table_name,
                                      load_id=load_id,
                                      file_id=job_info.file_id,
                                      ext=job_info.file_format)

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

    def __init__(self, schema: Schema, config: FilesystemDestinationClientConfiguration) -> None:
        super().__init__(schema, config)
        self.fs_client, self.fs_path = fsspec_from_config(config)
        self.config: FilesystemDestinationClientConfiguration = config
        # verify files layout. we need {table_name} and only allow {schema_name} before it, otherwise tables
        # cannot be replaced and we cannot initialize folders consistently
        self.table_prefix_layout = path_utils.get_table_prefix_layout(config.layout)

    @property
    def dataset_path(self) -> str:
        ds_path = posixpath.join(self.fs_path, self.config.normalize_dataset_name(self.schema))
        return ds_path

    def drop_storage(self) -> None:
        if self.is_storage_initialized():
            self.fs_client.rm(self.dataset_path, recursive=True)

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        # clean up existing files for tables selected for truncating
        if truncate_tables and self.fs_client.isdir(self.dataset_path):
            # get all dirs with table data to delete. the table data are guaranteed to be files in those folders
            # TODO: when we do partitioning it is no longer the case and we may remove folders below instead
            truncated_dirs = self._get_table_dirs(truncate_tables)
            # print(f"TRUNCATE {truncated_dirs}")
            truncate_prefixes: Set[str] = set()
            for table in truncate_tables:
                table_prefix = self.table_prefix_layout.format(schema_name=self.schema.name, table_name=table)
                truncate_prefixes.add(posixpath.join(self.dataset_path, table_prefix))
            # print(f"TRUNCATE PREFIXES {truncate_prefixes}")

            for truncate_dir in truncated_dirs:
                # get files in truncate dirs
                # NOTE: glob implementation in fsspec does not look thread safe, way better is to use ls and then filter
                # NOTE: without refresh you get random results here
                logger.info(f"Will truncate tables in {truncate_dir}")
                try:
                    all_files = self.fs_client.ls(truncate_dir, detail=False, refresh=True)
                    logger.info(f"Found {len(all_files)} CANDIDATE files in {truncate_dir}")
                    # print(f"in truncate dir {truncate_dir}: {all_files}")
                    for item in all_files:
                        # check every file against all the prefixes
                        for search_prefix in truncate_prefixes:
                            if item.startswith(search_prefix):
                                # NOTE: deleting in chunks on s3 does not raise on access denied, file non existing and probably other errors
                                logger.info(f"DEL {item}")
                                # print(f"DEL {item}")
                                self.fs_client.rm(item)
                except FileNotFoundError:
                    logger.info(f"Directory or path to truncate tables {truncate_dir} does not exist but it should be created previously!")

    def update_stored_schema(self, only_tables: Iterable[str] = None, expected_update: TSchemaTables = None) -> TSchemaTables:
        # create destination dirs for all tables
        dirs_to_create = self._get_table_dirs(only_tables or self.schema.tables.keys())
        for directory in dirs_to_create:
            self.fs_client.makedirs(directory, exist_ok=True)
        return expected_update

    def _get_table_dirs(self, table_names: Iterable[str]) -> Set[str]:
        """Gets unique directories where table data is stored."""
        table_dirs: Set[str] = set()
        for table_name in table_names:
            table_prefix = self.table_prefix_layout.format(schema_name=self.schema.name, table_name=table_name)
            destination_dir = posixpath.join(self.dataset_path, table_prefix)
            # extract the path component
            table_dirs.add(os.path.dirname(destination_dir))
        return table_dirs

    def is_storage_initialized(self) -> bool:
        return self.fs_client.isdir(self.dataset_path)  # type: ignore[no-any-return]

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        cls = FollowupFilesystemJob if self.config.as_staging else LoadFilesystemJob
        return cls(
            file_path,
            self.dataset_path,
            config=self.config,
            schema_name=self.schema.name,
            load_id=load_id
        )

    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")

    def complete_load(self, load_id: str) -> None:
        schema_name = self.schema.name
        table_name = self.schema.loads_table_name
        file_name = f"{schema_name}.{table_name}.{load_id}"
        self.fs_client.touch(posixpath.join(self.dataset_path, file_name))

    def __enter__(self) -> "FilesystemClient":
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        pass
