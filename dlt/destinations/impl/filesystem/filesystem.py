import os
import posixpath
from types import TracebackType
from typing import ClassVar, List, Tuple, Type, Iterable, Set, Iterator

import dlt

from fsspec import AbstractFileSystem
from contextlib import contextmanager

from dlt.common import logger
from dlt.common.schema import Schema, TSchemaTables, TTableSchema
from dlt.common.storages import FileStorage, fsspec_from_config
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    NewLoadJob,
    TLoadJobState,
    LoadJob,
    JobClientBase,
    FollowupJob,
    WithStagingDataset,
)

from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.impl.filesystem import capabilities
from dlt.destinations.impl.filesystem.configuration import FilesystemDestinationClientConfiguration
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
        load_id: str,
    ) -> None:
        file_name = FileStorage.get_file_name_from_file_path(local_path)
        self.config = config
        self.dataset_path = dataset_path
        self.destination_file_name = path_utils.create_path(
            config.layout,
            file_name,
            schema_name,
            load_id,
            load_package_timestamp=dlt.current.load_package()["state"]["created_at"],
            current_datetime=config.current_datetime,
            datetime_format=config.datetime_format,
            extra_placeholders=config.extra_placeholders,
        )

        super().__init__(file_name)
        fs_client, _ = fsspec_from_config(config)
        self.destination_file_name = path_utils.create_path(
            config.layout,
            file_name,
            schema_name,
            load_id,
            load_package_timestamp=dlt.current.load_package()["state"]["created_at"],
            current_datetime=config.current_datetime,
            datetime_format=config.datetime_format,
            extra_placeholders=config.extra_placeholders,
        )
        item = self.make_remote_path()
        fs_client.put_file(local_path, item)

    def make_remote_path(self) -> str:
        return (
            f"{self.config.protocol}://{posixpath.join(self.dataset_path, self.destination_file_name)}"
        )

    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()


class FollowupFilesystemJob(FollowupJob, LoadFilesystemJob):
    def create_followup_jobs(self, final_state: TLoadJobState) -> List[NewLoadJob]:
        jobs = super().create_followup_jobs(final_state)
        if final_state == "completed":
            ref_job = NewReferenceJob(
                file_name=self.file_name(),
                status="running",
                remote_path=self.make_remote_path(),
            )
            jobs.append(ref_job)
        return jobs


class FilesystemClient(JobClientBase, WithStagingDataset):
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
        self.table_prefix_layout = path_utils.get_table_prefix_layout(
            config.layout,
            current_datetime=config.current_datetime,
            datetime_format=config.datetime_format,
            extra_placeholders=config.extra_placeholders,
        )
        self._dataset_path = self.config.normalize_dataset_name(self.schema)

    def drop_storage(self) -> None:
        if self.is_storage_initialized():
            self.fs_client.rm(self.dataset_path, recursive=True)

    @property
    def dataset_path(self) -> str:
        return posixpath.join(self.fs_path, self._dataset_path)

    @contextmanager
    def with_staging_dataset(self) -> Iterator["FilesystemClient"]:
        current_dataset_path = self._dataset_path
        try:
            self._dataset_path = self.schema.naming.normalize_table_identifier(
                current_dataset_path + "_staging"
            )
            yield self
        finally:
            # restore previous dataset name
            self._dataset_path = current_dataset_path

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        # clean up existing files for tables selected for truncating
        if truncate_tables and self.fs_client.isdir(self.dataset_path):
            # get all dirs with table data to delete. the table data are guaranteed to be files in those folders
            # TODO: when we do partitioning it is no longer the case and we may remove folders below instead
            truncated_dirs = self._get_table_dirs(truncate_tables)
            # print(f"TRUNCATE {truncated_dirs}")
            truncate_prefixes = self._get_table_prefixes(truncate_tables=truncate_tables)

            # print(f"TRUNCATE PREFIXES {truncate_prefixes} on {truncate_tables}")
            # We would like to gather all files and folders
            # first set of `truncated_dirs` will be top_level
            # directories thus later we only iterate through them
            # and delete them once all files have been deleted first
            directories, files = self._get_items_to_remove(
                truncated_dirs=truncated_dirs,
                prefixes=truncate_prefixes,
                top_level=True,
            )

            for file in files:
                try:
                    self.fs_client.rm_file(file)
                except NotImplementedError:
                    # not all filesystem implement the above
                    self.fs_client.rm(file)
                    if self.fs_client.exists(file):
                        raise FileExistsError(file)
                except FileNotFoundError:
                    logger.info(
                        f"Directory or path to truncate tables {file} does not exist but it should"
                        " be created previously!"
                    )

            for directory, is_top_level in directories:
                if not is_top_level:
                    continue

                try:
                    logger.info(f"Will truncate tables in {directory}")
                    if self.fs_client.exists(directory):
                        self.fs_client.rmdir(directory)
                    else:
                        logger.info(
                            f"Directory or path to truncate tables {directory} does not exist but"
                            " it should have been created previously!"
                        )
                except OSError:
                    logger.info(f"Directory or path to truncate {directory} is not empty")

    def update_stored_schema(
        self, only_tables: Iterable[str] = None, expected_update: TSchemaTables = None
    ) -> TSchemaTables:
        # create destination dirs for all tables
        dirs_to_create = self._get_table_dirs(only_tables or self.schema.tables.keys())
        for directory in dirs_to_create:
            self.fs_client.makedirs(directory, exist_ok=True)
        return expected_update

    def _get_table_prefixes(self, truncate_tables: Iterable[str]) -> Set[str]:
        truncate_prefixes: Set[str] = set()
        for table in truncate_tables:
            table_prefix = self.table_prefix_layout.format(
                schema_name=self.schema.name, table_name=table
            )
            truncate_prefixes.add(posixpath.join(self.dataset_path, table_prefix))

        return truncate_prefixes

    def _get_items_to_remove(
        self,
        truncated_dirs: Iterable[str],
        prefixes: Set[str],
        top_level: bool = False,
    ) -> Tuple[List[Tuple[str, bool]], List[str]]:
        """Gets the list of directories and files starting with the given prefixes

        Returns:
            (directories, files): tuple with directories and files
        """
        # list of tuples of directories and a boolean flag
        # indicating that the a directory is a top level directory
        # so later we remove only top level directories once
        directories: List[Tuple[str, bool]] = []
        files: List[str] = []
        for truncate_dir in truncated_dirs:
            all_files = self.fs_client.ls(truncate_dir, detail=True, refresh=True)
            for item in all_files:
                item_path = item["name"]
                item_type = item["type"]

                # check every file against all the prefixes
                for search_prefix in prefixes:
                    if item_path.startswith(search_prefix):
                        if item_type == "file":
                            files.append(item_path)

                        # when we get the directory we need to descend
                        # and collect all files in sub-directories
                        if item_type == "directory":
                            directories.append((item_path, top_level))
                            nested_dirs, nested_files = self._get_items_to_remove(
                                [item_path],
                                prefixes,
                                top_level=False,
                            )
                            if nested_dirs:
                                directories.extend(nested_dirs)

                            if nested_files:
                                files.extend(nested_files)

        return directories, files

    def _get_table_dirs(self, table_names: Iterable[str]) -> Set[str]:
        """Gets unique directories where table data is stored."""
        table_dirs: Set[str] = set()
        for table_name in table_names:
            table_prefix = self.table_prefix_layout.format(
                schema_name=self.schema.name, table_name=table_name
            )
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
            load_id=load_id,
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

    def __exit__(
        self,
        exc_type: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        pass

    def should_load_data_to_staging_dataset(self, table: TTableSchema) -> bool:
        return False
