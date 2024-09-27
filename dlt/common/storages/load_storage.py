from os.path import join
from typing import Iterable, List, Optional, Sequence

from dlt.common.data_writers.exceptions import DataWriterNotFound
from dlt.common.json import json
from dlt.common.configuration import known_sections
from dlt.common.configuration.inject import with_config
from dlt.common.destination import LOADER_FILE_FORMATS, TLoaderFileFormat
from dlt.common.configuration.accessors import config
from dlt.common.schema import TSchemaTables
from dlt.common.storages.file_storage import FileStorage
from dlt.common.storages.configuration import LoadStorageConfiguration
from dlt.common.storages.versioned_storage import VersionedStorage
from dlt.common.storages.data_item_storage import DataItemStorage
from dlt.common.storages.load_package import (
    LoadJobInfo,
    LoadPackageInfo,
    PackageStorage,
    ParsedLoadJobFileName,
    TPackageJobState,
    TLoadPackageState,
    TJobFileFormat,
)
from dlt.common.data_writers import DataWriter, FileWriterSpec, TDataItemFormat
from dlt.common.storages.exceptions import JobFileFormatUnsupported, LoadPackageNotFound


class LoadItemStorage(DataItemStorage):
    def __init__(self, package_storage: PackageStorage, writer_spec: FileWriterSpec) -> None:
        """Data item storage using `storage` to manage load packages"""
        super().__init__(writer_spec)
        self.package_storage = package_storage

    def _get_data_item_path_template(self, load_id: str, _: str, table_name: str) -> str:
        # implements DataItemStorage._get_data_item_path_template
        file_name = PackageStorage.build_job_file_name(table_name, "%s")
        file_path = self.package_storage.get_job_file_path(
            load_id, PackageStorage.NEW_JOBS_FOLDER, file_name
        )
        return self.package_storage.storage.make_full_path(file_path)


class LoadStorage(VersionedStorage):
    STORAGE_VERSION = "1.0.0"
    NORMALIZED_FOLDER = "normalized"  # folder within the volume where load packages are stored
    LOADED_FOLDER = "loaded"  # folder to keep the loads that were completely processed
    NEW_PACKAGES_FOLDER = "new"  # folder where new packages are created

    ALL_SUPPORTED_FILE_FORMATS = LOADER_FILE_FORMATS

    @with_config(spec=LoadStorageConfiguration, sections=(known_sections.LOAD,))
    def __init__(
        self,
        is_owner: bool,
        supported_file_formats: Iterable[TLoaderFileFormat],
        config: LoadStorageConfiguration = config.value,
    ) -> None:
        self.supported_loader_file_formats = supported_file_formats
        # store job file formats to add internal job formats as needed
        self.supported_job_file_formats: List[TJobFileFormat] = list(supported_file_formats)
        self.config = config
        super().__init__(
            LoadStorage.STORAGE_VERSION,
            is_owner,
            FileStorage(config.load_volume_path, "t", makedirs=is_owner),
        )
        if is_owner:
            self.initialize_storage()
        # create package storages
        self.new_packages = PackageStorage(
            FileStorage(join(config.load_volume_path, LoadStorage.NEW_PACKAGES_FOLDER)), "new"
        )
        self.normalized_packages = PackageStorage(
            FileStorage(join(config.load_volume_path, LoadStorage.NORMALIZED_FOLDER)), "normalized"
        )
        self.loaded_packages = PackageStorage(
            FileStorage(join(config.load_volume_path, LoadStorage.LOADED_FOLDER)), "loaded"
        )

    def initialize_storage(self) -> None:
        self.storage.create_folder(LoadStorage.NEW_PACKAGES_FOLDER, exists_ok=True)
        self.storage.create_folder(LoadStorage.NORMALIZED_FOLDER, exists_ok=True)
        self.storage.create_folder(LoadStorage.LOADED_FOLDER, exists_ok=True)

    def create_item_storage(self, writer_spec: FileWriterSpec) -> DataItemStorage:
        return LoadItemStorage(self.new_packages, writer_spec)

    def import_extracted_package(
        self, load_id: str, extract_package_storage: PackageStorage
    ) -> None:
        # pass the original state
        self.new_packages.create_package(
            load_id, extract_package_storage.get_load_package_state(load_id)
        )

    def list_new_jobs(self, load_id: str) -> Sequence[str]:
        """Lists all jobs in new jobs folder of normalized package storage and checks if file formats are supported"""
        new_jobs = self.normalized_packages.list_new_jobs(load_id)
        # make sure all jobs have supported writers
        wrong_job = next(
            (
                j
                for j in new_jobs
                if ParsedLoadJobFileName.parse(j).file_format not in self.supported_job_file_formats
            ),
            None,
        )
        if wrong_job is not None:
            raise JobFileFormatUnsupported(load_id, self.supported_job_file_formats, wrong_job)
        return new_jobs

    def commit_new_load_package(self, load_id: str) -> None:
        self.storage.rename_tree(
            self.get_new_package_path(load_id), self.get_normalized_package_path(load_id)
        )

    def list_normalized_packages(self) -> Sequence[str]:
        """Lists all packages that are normalized and will be loaded or are currently loaded"""
        return self.normalized_packages.list_packages()

    def list_loaded_packages(self) -> Sequence[str]:
        """List packages that are completely loaded"""
        return self.loaded_packages.list_packages()

    def list_failed_jobs_in_loaded_package(self, load_id: str) -> Sequence[LoadJobInfo]:
        """List all failed jobs and associated error messages for a completed load package with `load_id`"""
        return self.loaded_packages.list_failed_jobs_infos(load_id)

    def begin_schema_update(self, load_id: str) -> Optional[TSchemaTables]:
        package_path = self.get_normalized_package_path(load_id)
        if not self.storage.has_folder(package_path):
            raise FileNotFoundError(package_path)
        schema_update_file = join(package_path, PackageStorage.SCHEMA_UPDATES_FILE_NAME)
        if self.storage.has_file(schema_update_file):
            schema_update: TSchemaTables = json.loads(self.storage.load(schema_update_file))
            return schema_update
        else:
            return None

    def commit_schema_update(self, load_id: str, applied_update: TSchemaTables) -> None:
        """Marks schema update as processed and stores the update that was applied at the destination"""
        load_path = self.get_normalized_package_path(load_id)
        schema_update_file = join(load_path, PackageStorage.SCHEMA_UPDATES_FILE_NAME)
        applied_schema_update_file = join(
            load_path, PackageStorage.APPLIED_SCHEMA_UPDATES_FILE_NAME
        )
        # delete initial schema update
        self.storage.delete(schema_update_file)
        # save applied update
        self.storage.save(applied_schema_update_file, json.dumps(applied_update))

    def import_new_job(
        self, load_id: str, job_file_path: str, job_state: TPackageJobState = "new_jobs"
    ) -> None:
        """Adds new job by moving the `job_file_path` into `new_jobs` of package `load_id`"""
        # TODO: use normalize storage and add file type checks
        return self.normalized_packages.import_job(load_id, job_file_path, job_state)

    # def atomic_import(self, external_file_path: str, to_folder: str) -> str:
    #     """Copies or links a file at `external_file_path` into the `to_folder` effectively importing file into storage"""
    #     # LoadStorage.parse_job_file_name
    #     return self.storage.to_relative_path(
    #         FileStorage.move_atomic_to_folder(
    #             external_file_path, self.storage.make_full_path(to_folder)
    #         )
    #     )

    def complete_load_package(self, load_id: str, aborted: bool) -> None:
        self.normalized_packages.complete_loading_package(
            load_id, "aborted" if aborted else "loaded"
        )
        # move to completed
        completed_path = self.get_loaded_package_path(load_id)
        self.storage.rename_tree(self.get_normalized_package_path(load_id), completed_path)

    def maybe_remove_completed_jobs(self, load_id: str) -> None:
        """Deletes completed jobs if delete_completed_jobs config flag is set. If package has failed jobs, nothing gets deleted."""
        if self.config.delete_completed_jobs:
            self.loaded_packages.remove_completed_jobs(load_id)

    def delete_loaded_package(self, load_id: str) -> None:
        self.loaded_packages.delete_package(load_id)

    def wipe_normalized_packages(self) -> None:
        self.storage.delete_folder(self.NORMALIZED_FOLDER, recursively=True)

    def get_new_package_path(self, load_id: str) -> str:
        return join(LoadStorage.NEW_PACKAGES_FOLDER, self.new_packages.get_package_path(load_id))

    def get_normalized_package_path(self, load_id: str) -> str:
        return join(
            LoadStorage.NORMALIZED_FOLDER, self.normalized_packages.get_package_path(load_id)
        )

    def get_loaded_package_path(self, load_id: str) -> str:
        return join(LoadStorage.LOADED_FOLDER, self.loaded_packages.get_package_path(load_id))

    def get_load_package_info(self, load_id: str) -> LoadPackageInfo:
        """Gets information on normalized OR loaded package with given load_id, all jobs and their statuses."""
        try:
            return self.loaded_packages.get_load_package_info(load_id)
        except LoadPackageNotFound:
            return self.normalized_packages.get_load_package_info(load_id)

    def get_load_package_state(self, load_id: str) -> TLoadPackageState:
        """Gets state of normlized or loaded package with given load_id, all jobs and their statuses."""
        try:
            return self.loaded_packages.get_load_package_state(load_id)
        except LoadPackageNotFound:
            return self.normalized_packages.get_load_package_state(load_id)
