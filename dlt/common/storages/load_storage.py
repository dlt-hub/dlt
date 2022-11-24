import os
from os.path import join
from pathlib import Path
from typing import Iterable, NamedTuple, Literal, Optional, Sequence, Set, get_args, overload

from dlt.common import json, pendulum
from dlt.common.configuration.inject import with_config
from dlt.common.typing import DictStrAny, StrAny
from dlt.common.storages.file_storage import FileStorage
from dlt.common.data_writers import TLoaderFileFormat, DataWriter
from dlt.common.configuration.specs import LoadVolumeConfiguration
from dlt.common.configuration.accessors import config
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import Schema, TSchemaUpdate, TTableSchemaColumns
from dlt.common.storages.versioned_storage import VersionedStorage
from dlt.common.storages.data_item_storage import DataItemStorage
from dlt.common.storages.exceptions import JobWithUnsupportedWriterException


# folders to manage load jobs in a single load package
TWorkingFolder = Literal["new_jobs", "failed_jobs", "started_jobs", "completed_jobs"]
class TParsedJobFileName(NamedTuple):
    table_name: str
    file_id: str
    retry_count: int
    file_format: TLoaderFileFormat


class LoadStorage(DataItemStorage, VersionedStorage):

    STORAGE_VERSION = "1.0.0"
    NORMALIZED_FOLDER = "normalized"  # folder within the volume where load packages are stored
    LOADED_FOLDER = "loaded"  # folder to keep the loads that were completely processed

    NEW_JOBS_FOLDER: TWorkingFolder = "new_jobs"
    FAILED_JOBS_FOLDER: TWorkingFolder = "failed_jobs"
    STARTED_JOBS_FOLDER: TWorkingFolder = "started_jobs"
    COMPLETED_JOBS_FOLDER: TWorkingFolder = "completed_jobs"

    SCHEMA_UPDATES_FILE_NAME = "schema_updates.json"
    PROCESSED_SCHEMA_UPDATES_FILE_NAME = "processed_" + "schema_updates.json"
    SCHEMA_FILE_NAME = "schema.json"

    ALL_SUPPORTED_FILE_FORMATS: Set[TLoaderFileFormat] = set(get_args(TLoaderFileFormat))

    @with_config(spec=LoadVolumeConfiguration, namespaces=("load",))
    def __init__(
        self,
        is_owner: bool,
        preferred_file_format: TLoaderFileFormat,
        supported_file_formats: Iterable[TLoaderFileFormat],
        config: LoadVolumeConfiguration = config.value
    ) -> None:
        if not LoadStorage.ALL_SUPPORTED_FILE_FORMATS.issuperset(supported_file_formats):
            raise TerminalValueError(supported_file_formats)
        if preferred_file_format not in supported_file_formats:
            raise TerminalValueError(preferred_file_format)
        self.supported_file_formats = supported_file_formats
        self.config = config
        super().__init__(
            preferred_file_format,
            LoadStorage.STORAGE_VERSION,
            is_owner, FileStorage(config.load_volume_path, "t", makedirs=is_owner)
        )
        if is_owner:
            self.initialize_storage()

    def initialize_storage(self) -> None:
        self.storage.create_folder(LoadStorage.LOADED_FOLDER, exists_ok=True)
        self.storage.create_folder(LoadStorage.NORMALIZED_FOLDER, exists_ok=True)

    def create_temp_load_package(self, load_id: str) -> None:
        # delete previous version
        if self.storage.has_folder(load_id):
            self.storage.delete_folder(load_id, recursively=True)
        self.storage.create_folder(load_id)
        # create processing directories
        self.storage.create_folder(join(load_id, LoadStorage.NEW_JOBS_FOLDER))
        self.storage.create_folder(join(load_id, LoadStorage.COMPLETED_JOBS_FOLDER))
        self.storage.create_folder(join(load_id, LoadStorage.FAILED_JOBS_FOLDER))
        self.storage.create_folder(join(load_id, LoadStorage.STARTED_JOBS_FOLDER))

    def _get_data_item_path_template(self, load_id: str, _: str, table_name: str) -> str:
        file_name = self.build_job_file_name(table_name, "%s", with_extension=False)
        return self.storage.make_full_path(join(load_id, LoadStorage.NEW_JOBS_FOLDER, file_name))

    def write_temp_job_file(self, load_id: str, table_name: str, table: TTableSchemaColumns, file_id: str, rows: Sequence[StrAny]) -> str:
        file_name = self._get_data_item_path_template(load_id, None, table_name) % file_id + "." + self.loader_file_format
        with self.storage.open_file(file_name, mode="w") as f:
            writer = DataWriter.from_file_format(self.loader_file_format, f)
            writer.write_all(table, rows)
        return Path(file_name).name

    def load_package_schema(self, load_id: str) -> Schema:
        # load schema from a load package to be processed
        schema_path = join(self.get_package_path(load_id), LoadStorage.SCHEMA_FILE_NAME)
        return self._load_schema(schema_path)

    def load_temp_schema(self, load_id: str) -> Schema:
        # load schema from a temporary load package
        schema_path = join(load_id, LoadStorage.SCHEMA_FILE_NAME)
        return self._load_schema(schema_path)

    def save_temp_schema(self, schema: Schema, load_id: str) -> str:
        # save a schema to a temporary load package
        dump = json.dumps(schema.to_dict())
        return self.storage.save(join(load_id, LoadStorage.SCHEMA_FILE_NAME), dump)

    def save_temp_schema_updates(self, load_id: str, schema_updates: Sequence[TSchemaUpdate]) -> None:
        with self.storage.open_file(join(load_id, LoadStorage.SCHEMA_UPDATES_FILE_NAME), mode="w") as f:
            json.dump(schema_updates, f)

    def commit_temp_load_package(self, load_id: str) -> None:
        self.storage.atomic_rename(load_id, self.get_package_path(load_id))

    def list_packages(self) -> Sequence[str]:
        loads = self.storage.list_folder_dirs(LoadStorage.NORMALIZED_FOLDER, to_root=False)
        # start from the oldest packages
        return sorted(loads)

    def list_completed_packages(self) -> Sequence[str]:
        loads = self.storage.list_folder_dirs(LoadStorage.LOADED_FOLDER, to_root=False)
        # start from the oldest packages
        return sorted(loads)

    def list_new_jobs(self, load_id: str) -> Sequence[str]:
        new_jobs = self.storage.list_folder_files(self._get_job_folder_path(load_id, LoadStorage.NEW_JOBS_FOLDER))
        # make sure all jobs have supported writers
        wrong_job = next((j for j in new_jobs if LoadStorage.parse_job_file_name(j).file_format not in self.supported_file_formats), None)
        if wrong_job is not None:
            raise JobWithUnsupportedWriterException(load_id, self.supported_file_formats, wrong_job)
        return new_jobs

    def list_started_jobs(self, load_id: str) -> Sequence[str]:
        return self.storage.list_folder_files(self._get_job_folder_path(load_id, LoadStorage.STARTED_JOBS_FOLDER))

    def list_failed_jobs(self, load_id: str) -> Sequence[str]:
        return self.storage.list_folder_files(self._get_job_folder_path(load_id, LoadStorage.FAILED_JOBS_FOLDER))

    def list_completed_failed_jobs(self, load_id: str) -> Sequence[str]:
        return self.storage.list_folder_files(join(self.get_completed_package_path(load_id), LoadStorage.FAILED_JOBS_FOLDER))

    def begin_schema_update(self, load_id: str) -> Optional[TSchemaUpdate]:
        package_path = self.get_package_path(load_id)
        if not self.storage.has_folder(package_path):
            raise FileNotFoundError(package_path)
        schema_update_file = join(package_path, LoadStorage.SCHEMA_UPDATES_FILE_NAME)
        if self.storage.has_file(schema_update_file):
            schema_update: TSchemaUpdate = json.loads(self.storage.load(schema_update_file))
            return schema_update
        else:
            return None

    def commit_schema_update(self, load_id: str) -> None:
        load_path = self.get_package_path(load_id)
        schema_update_file = join(load_path, LoadStorage.SCHEMA_UPDATES_FILE_NAME)
        processed_schema_update_file = join(load_path, LoadStorage.PROCESSED_SCHEMA_UPDATES_FILE_NAME)
        self.storage.atomic_rename(schema_update_file, processed_schema_update_file)

    def start_job(self, load_id: str, file_name: str) -> str:
        return self._move_job(load_id, LoadStorage.NEW_JOBS_FOLDER, LoadStorage.STARTED_JOBS_FOLDER, file_name)

    def fail_job(self, load_id: str, file_name: str, failed_message: Optional[str]) -> str:
        # save the exception to failed jobs
        if failed_message:
            self.storage.save(
                self._get_job_file_path(load_id, LoadStorage.FAILED_JOBS_FOLDER, file_name + ".exception"),
                failed_message
            )
        # move to failed jobs
        return self._move_job(load_id, LoadStorage.STARTED_JOBS_FOLDER, LoadStorage.FAILED_JOBS_FOLDER, file_name)

    def retry_job(self, load_id: str, file_name: str) -> str:
        # when retrying job we must increase the retry count
        source_fn = LoadStorage.parse_job_file_name(file_name)
        dest_fn = self.build_job_file_name(source_fn.table_name, source_fn.file_id, source_fn.retry_count + 1)
        # move it directly to new file name
        return self._move_job(load_id, LoadStorage.STARTED_JOBS_FOLDER, LoadStorage.NEW_JOBS_FOLDER, file_name, dest_fn)

    def complete_job(self, load_id: str, file_name: str) -> str:
        return self._move_job(load_id, LoadStorage.STARTED_JOBS_FOLDER, LoadStorage.COMPLETED_JOBS_FOLDER, file_name)

    def complete_load_package(self, load_id: str) -> None:
        load_path = self.get_package_path(load_id)
        has_failed_jobs = len(self.list_failed_jobs(load_id)) > 0
        # delete load that does not contain failed jobs
        if self.config.delete_completed_jobs and not has_failed_jobs:
            self.storage.delete_folder(load_path, recursively=True)
        else:
            completed_path = self.get_completed_package_path(load_id)
            self.storage.atomic_rename(load_path, completed_path)

    def get_package_path(self, load_id: str) -> str:
        return join(LoadStorage.NORMALIZED_FOLDER, load_id)

    def get_completed_package_path(self, load_id: str) -> str:
        return join(LoadStorage.LOADED_FOLDER, load_id)

    def job_elapsed_time_seconds(self, file_path: str) -> float:
        return pendulum.now().timestamp() - os.path.getmtime(file_path)  # type: ignore

    def _save_schema(self, schema: Schema, load_id: str) -> str:
        dump = json.dumps(schema.to_dict())
        schema_path = join(self.get_package_path(load_id), LoadStorage.SCHEMA_FILE_NAME)
        return self.storage.save(schema_path, dump)

    def _load_schema(self, schema_path: str) -> Schema:
        stored_schema: DictStrAny = json.loads(self.storage.load(schema_path))
        return Schema.from_dict(stored_schema)

    def _move_job(self, load_id: str, source_folder: TWorkingFolder, dest_folder: TWorkingFolder, file_name: str, new_file_name: str = None) -> str:
        load_path = self.get_package_path(load_id)
        dest_path = join(load_path, dest_folder, new_file_name or file_name)
        self.storage.atomic_rename(join(load_path, source_folder, file_name), dest_path)
        return self.storage.make_full_path(dest_path)

    def _get_job_folder_path(self, load_id: str, folder: TWorkingFolder) -> str:
        return join(self.get_package_path(load_id), folder)

    def _get_job_file_path(self, load_id: str, folder: TWorkingFolder, file_name: str) -> str:
        return join(self._get_job_folder_path(load_id, folder), file_name)

    def build_job_file_name(self, table_name: str, file_id: str, retry_count: int = 0, validate_components: bool = True, with_extension: bool = True) -> str:
        if validate_components:
            FileStorage.validate_file_name_component(table_name)
            # FileStorage.validate_file_name_component(file_id)
        fn = f"{table_name}.{file_id}.{int(retry_count)}"
        if with_extension:
            return fn + f".{self.loader_file_format}"
        return fn

    @staticmethod
    def parse_job_file_name(file_name: str) -> TParsedJobFileName:
        p = Path(file_name)
        parts = p.name.split(".")
        if len(parts) != 4:
            raise TerminalValueError(parts)
        # verify we know the extension
        ext: TLoaderFileFormat = parts[-1]  # type: ignore
        if ext not in LoadStorage.ALL_SUPPORTED_FILE_FORMATS:
            raise TerminalValueError(ext)

        return TParsedJobFileName(parts[0], parts[1], int(parts[2]), ext)
