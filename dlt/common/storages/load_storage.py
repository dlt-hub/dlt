import contextlib
from copy import deepcopy
import os
import datetime  # noqa: 251
import humanize
from os.path import join
from pathlib import Path
from pendulum.datetime import DateTime
from typing import Dict, Iterable, List, NamedTuple, Literal, Optional, Sequence, Set, get_args, cast

from dlt.common import json, pendulum
from dlt.common.configuration import known_sections
from dlt.common.configuration.inject import with_config
from dlt.common.typing import DictStrAny, StrAny
from dlt.common.storages.file_storage import FileStorage
from dlt.common.data_writers import TLoaderFileFormat, DataWriter
from dlt.common.configuration.accessors import config
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import Schema, TSchemaTables, TTableSchemaColumns
from dlt.common.storages.configuration import LoadStorageConfiguration
from dlt.common.storages.versioned_storage import VersionedStorage
from dlt.common.storages.data_item_storage import DataItemStorage
from dlt.common.storages.exceptions import JobWithUnsupportedWriterException, LoadPackageNotFound
from dlt.common.utils import flatten_list_or_items


# folders to manage load jobs in a single load package
TJobState = Literal["new_jobs", "failed_jobs", "started_jobs", "completed_jobs"]
WORKING_FOLDERS = set(get_args(TJobState))
TLoadPackageState = Literal["normalized", "loaded", "aborted"]


class ParsedLoadJobFileName(NamedTuple):
    table_name: str
    file_id: str
    retry_count: int
    file_format: TLoaderFileFormat

    def job_id(self) -> str:
        return f"{self.table_name}.{self.file_id}.{int(self.retry_count)}.{self.file_format}"

    @staticmethod
    def parse(file_name: str) -> "ParsedLoadJobFileName":
        p = Path(file_name)
        parts = p.name.split(".")
        if len(parts) != 4:
            raise TerminalValueError(parts)

        return ParsedLoadJobFileName(parts[0], parts[1], int(parts[2]), cast(TLoaderFileFormat, parts[3]))


class LoadJobInfo(NamedTuple):
    state: TJobState
    file_path: str
    file_size: int
    created_at: datetime.datetime
    elapsed: float
    job_file_info: ParsedLoadJobFileName
    failed_message: str

    def asdict(self) -> DictStrAny:
        d = self._asdict()
        # flatten
        del d["job_file_info"]
        d.update(self.job_file_info._asdict())
        return d

    def asstr(self, verbosity: int = 0) -> str:
        failed_msg = "The job FAILED TERMINALLY and cannot be restarted." if self.failed_message else ""
        elapsed_msg = humanize.precisedelta(pendulum.duration(seconds=self.elapsed)) if self.elapsed else "---"
        msg = f"Job: {self.job_file_info.job_id()}, table: {self.job_file_info.table_name} in {self.state}. "
        msg += f"File type: {self.job_file_info.file_format}, size: {humanize.naturalsize(self.file_size, binary=True, gnu=True)}. "
        msg += f"Started on: {self.created_at} and completed in {elapsed_msg}."
        if failed_msg:
            msg += "\nThe job FAILED TERMINALLY and cannot be restarted."
            if verbosity > 0:
                msg += "\n" + self.failed_message
        return msg

    def __str__(self) -> str:
        return self.asstr(verbosity=0)


class LoadPackageInfo(NamedTuple):
    load_id: str
    package_path: str
    state: TLoadPackageState
    schema_name: str
    schema_update: TSchemaTables
    completed_at: datetime.datetime
    jobs: Dict[TJobState, List[LoadJobInfo]]

    def asdict(self) -> DictStrAny:
        d = self._asdict()
        # job as list
        d["jobs"] = [job.asdict() for job in flatten_list_or_items(iter(self.jobs.values()))]  # type: ignore
        # flatten update into list of columns
        tables: List[DictStrAny] = deepcopy(list(self.schema_update.values()))  # type: ignore
        for table in tables:
            table.pop("filters", None)
            columns: List[DictStrAny] = []
            table["schema_name"] = self.schema_name
            table["load_id"] = self.load_id
            for column in table["columns"].values():
                column["table_name"] = table["name"]
                column["schema_name"] = self.schema_name
                column["load_id"] = self.load_id
                columns.append(column)
            table["columns"] = columns
        d.pop("schema_update")
        d["tables"] = tables
        return d

    def asstr(self, verbosity: int = 0) -> str:
        completed_msg = f"The package was {self.state.upper()} at {self.completed_at}" if self.completed_at else "The package is being PROCESSED"
        msg = f"The package with load id {self.load_id} for schema {self.schema_name} is in {self.state} state. It updated schema for {len(self.schema_update)} tables. {completed_msg}.\n"
        msg += "Jobs details:\n"
        msg += "\n".join(job.asstr(verbosity) for job in flatten_list_or_items(iter(self.jobs.values())))  # type: ignore
        return msg

    def __str__(self) -> str:
        return self.asstr(verbosity=0)


class LoadStorage(DataItemStorage, VersionedStorage):

    STORAGE_VERSION = "1.0.0"
    NORMALIZED_FOLDER = "normalized"  # folder within the volume where load packages are stored
    LOADED_FOLDER = "loaded"  # folder to keep the loads that were completely processed

    NEW_JOBS_FOLDER: TJobState = "new_jobs"
    FAILED_JOBS_FOLDER: TJobState = "failed_jobs"
    STARTED_JOBS_FOLDER: TJobState = "started_jobs"
    COMPLETED_JOBS_FOLDER: TJobState = "completed_jobs"

    SCHEMA_UPDATES_FILE_NAME = "schema_updates.json"  # updates to the tables in schema created by normalizer
    APPLIED_SCHEMA_UPDATES_FILE_NAME = "applied_" + "schema_updates.json"  # updates applied to the destination
    SCHEMA_FILE_NAME = "schema.json"  # package schema
    PACKAGE_COMPLETED_FILE_NAME = "package_completed.json"  # completed package marker file, currently only to store data with os.stat

    ALL_SUPPORTED_FILE_FORMATS: Set[TLoaderFileFormat] = set(get_args(TLoaderFileFormat))

    @with_config(spec=LoadStorageConfiguration, sections=(known_sections.LOAD,))
    def __init__(
        self,
        is_owner: bool,
        preferred_file_format: TLoaderFileFormat,
        supported_file_formats: Iterable[TLoaderFileFormat],
        config: LoadStorageConfiguration = config.value
    ) -> None:
        if not LoadStorage.ALL_SUPPORTED_FILE_FORMATS.issuperset(supported_file_formats):
            raise TerminalValueError(supported_file_formats)
        if preferred_file_format and preferred_file_format not in supported_file_formats:
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
        format_spec = DataWriter.data_format_from_file_format(self.loader_file_format)
        mode = "wb" if format_spec.is_binary_format else "w"
        with self.storage.open_file(file_name, mode=mode) as f:
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

    def save_temp_schema_updates(self, load_id: str, schema_update: TSchemaTables) -> None:
        with self.storage.open_file(join(load_id, LoadStorage.SCHEMA_UPDATES_FILE_NAME), mode="wb") as f:
            json.dump(schema_update, f)

    def commit_temp_load_package(self, load_id: str) -> None:
        self.storage.rename_tree(load_id, self.get_package_path(load_id))

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

    def list_jobs_for_table(self, load_id: str, table_name: str) -> Sequence[LoadJobInfo]:
        info = self.get_load_package_info(load_id)
        return [job for job in flatten_list_or_items(iter(info.jobs.values())) if job.job_file_info.table_name == table_name]  # type: ignore

    def list_completed_failed_jobs(self, load_id: str) -> Sequence[str]:
        return self.storage.list_folder_files(self._get_job_folder_completed_path(load_id, LoadStorage.FAILED_JOBS_FOLDER))

    def list_failed_jobs_in_completed_package(self, load_id: str) -> Sequence[LoadJobInfo]:
        """List all failed jobs and associated error messages for a completed load package with `load_id`"""
        failed_jobs: List[LoadJobInfo] = []
        package_path = self.get_completed_package_path(load_id)
        package_created_at = pendulum.from_timestamp(
            os.path.getmtime(self.storage.make_full_path(join(package_path, LoadStorage.PACKAGE_COMPLETED_FILE_NAME)))
        )
        for file in self.list_completed_failed_jobs(load_id):
            if not file.endswith(".exception"):
                failed_jobs.append(self._read_job_file_info("failed_jobs", file, package_created_at))
        return failed_jobs

    def get_load_package_info(self, load_id: str) -> LoadPackageInfo:
        """Gets information on normalized/completed package with given load_id, all jobs and their statuses."""
        # check if package is completed or in process
        package_created_at: DateTime = None
        package_state: TLoadPackageState = "normalized"
        package_path = self.get_package_path(load_id)
        applied_update: TSchemaTables = {}
        if not self.storage.has_folder(package_path):
            package_path = self.get_completed_package_path(load_id)
            if not self.storage.has_folder(package_path):
                raise LoadPackageNotFound(load_id)
            completed_file_path = self.storage.make_full_path(join(package_path, LoadStorage.PACKAGE_COMPLETED_FILE_NAME))
            package_created_at = pendulum.from_timestamp(os.path.getmtime(completed_file_path))
            package_state = self.storage.load(completed_file_path)
        applied_schema_update_file = join(package_path, LoadStorage.APPLIED_SCHEMA_UPDATES_FILE_NAME)
        if self.storage.has_file(applied_schema_update_file):
            applied_update = json.loads(self.storage.load(applied_schema_update_file))
        schema = self._load_schema(join(package_path, LoadStorage.SCHEMA_FILE_NAME))
        # read jobs with all statuses
        all_jobs: Dict[TJobState, List[LoadJobInfo]] = {}
        for state in WORKING_FOLDERS:
            jobs: List[LoadJobInfo] = []
            with contextlib.suppress(FileNotFoundError):
                # we ignore if load package lacks one of working folders. completed_jobs may be deleted on archiving
                for file in self.storage.list_folder_files(join(package_path, state)):
                    if not file.endswith(".exception"):
                        jobs.append(self._read_job_file_info(state, file, package_created_at))
            all_jobs[state] = jobs

        return LoadPackageInfo(load_id, self.storage.make_full_path(package_path), package_state, schema.name, applied_update, package_created_at, all_jobs)

    def begin_schema_update(self, load_id: str) -> Optional[TSchemaTables]:
        package_path = self.get_package_path(load_id)
        if not self.storage.has_folder(package_path):
            raise FileNotFoundError(package_path)
        schema_update_file = join(package_path, LoadStorage.SCHEMA_UPDATES_FILE_NAME)
        if self.storage.has_file(schema_update_file):
            schema_update: TSchemaTables = json.loads(self.storage.load(schema_update_file))
            return schema_update
        else:
            return None

    def commit_schema_update(self, load_id: str, applied_update: TSchemaTables) -> None:
        """Marks schema update as processed and stores the update that was applied at the destination"""
        load_path = self.get_package_path(load_id)
        schema_update_file = join(load_path, LoadStorage.SCHEMA_UPDATES_FILE_NAME)
        processed_schema_update_file = join(load_path, LoadStorage.APPLIED_SCHEMA_UPDATES_FILE_NAME)
        # delete initial schema update
        self.storage.delete(schema_update_file)
        # save applied update
        self.storage.save(processed_schema_update_file, json.dumps(applied_update))

    def add_new_job(self, load_id: str, job_file_path: str, job_state: TJobState = "new_jobs") -> None:
        """Adds new job by moving the `job_file_path` into `new_jobs` of package `load_id`"""
        self.storage.atomic_import(job_file_path, self._get_job_folder_path(load_id, job_state))

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
        source_fn = ParsedLoadJobFileName.parse(file_name)
        dest_fn = ParsedLoadJobFileName(source_fn.table_name, source_fn.file_id, source_fn.retry_count + 1, source_fn.file_format)
        # move it directly to new file name
        return self._move_job(load_id, LoadStorage.STARTED_JOBS_FOLDER, LoadStorage.NEW_JOBS_FOLDER, file_name, dest_fn.job_id())

    def complete_job(self, load_id: str, file_name: str) -> str:
        return self._move_job(load_id, LoadStorage.STARTED_JOBS_FOLDER, LoadStorage.COMPLETED_JOBS_FOLDER, file_name)

    def complete_load_package(self, load_id: str, aborted: bool) -> None:
        load_path = self.get_package_path(load_id)
        has_failed_jobs = len(self.list_failed_jobs(load_id)) > 0
        # delete completed jobs
        if self.config.delete_completed_jobs and not has_failed_jobs:
            self.storage.delete_folder(
                self._get_job_folder_path(load_id, LoadStorage.COMPLETED_JOBS_FOLDER),
            recursively=True)
        # save marker file
        completed_state: TLoadPackageState = "aborted" if aborted else "loaded"
        self.storage.save(join(load_path, LoadStorage.PACKAGE_COMPLETED_FILE_NAME), completed_state)
        # move to completed
        completed_path = self.get_completed_package_path(load_id)
        self.storage.rename_tree(load_path, completed_path)

    def delete_completed_package(self, load_id: str) -> None:
        package_path = self.get_completed_package_path(load_id)
        if not self.storage.has_folder(package_path):
            raise LoadPackageNotFound(load_id)
        self.storage.delete_folder(package_path, recursively=True)

    def wipe_normalized_packages(self) -> None:
        self.storage.delete_folder(self.NORMALIZED_FOLDER, recursively=True)

    def get_package_path(self, load_id: str) -> str:
        return join(LoadStorage.NORMALIZED_FOLDER, load_id)

    def get_completed_package_path(self, load_id: str) -> str:
        return join(LoadStorage.LOADED_FOLDER, load_id)

    def job_elapsed_time_seconds(self, file_path: str, now_ts: float = None) -> float:
        return (now_ts or pendulum.now().timestamp()) - os.path.getmtime(file_path)

    def _save_schema(self, schema: Schema, load_id: str) -> str:
        dump = json.dumps(schema.to_dict())
        schema_path = join(self.get_package_path(load_id), LoadStorage.SCHEMA_FILE_NAME)
        return self.storage.save(schema_path, dump)

    def _load_schema(self, schema_path: str) -> Schema:
        stored_schema: DictStrAny = json.loads(self.storage.load(schema_path))
        return Schema.from_dict(stored_schema)

    def _move_job(self, load_id: str, source_folder: TJobState, dest_folder: TJobState, file_name: str, new_file_name: str = None) -> str:
        # ensure we move file names, not paths
        assert file_name == FileStorage.get_file_name_from_file_path(file_name)
        load_path = self.get_package_path(load_id)
        dest_path = join(load_path, dest_folder, new_file_name or file_name)
        self.storage.atomic_rename(join(load_path, source_folder, file_name), dest_path)
        # print(f"{join(load_path, source_folder, file_name)} -> {dest_path}")
        return self.storage.make_full_path(dest_path)

    def _get_job_folder_path(self, load_id: str, folder: TJobState) -> str:
        return join(self.get_package_path(load_id), folder)

    def _get_job_file_path(self, load_id: str, folder: TJobState, file_name: str) -> str:
        return join(self._get_job_folder_path(load_id, folder), file_name)

    def _get_job_folder_completed_path(self, load_id: str, folder: TJobState) -> str:
        return join(self.get_completed_package_path(load_id), folder)

    def _read_job_file_info(self, state: TJobState, file: str, now: DateTime = None) -> LoadJobInfo:
        try:
            failed_message = self.storage.load(file + ".exception")
        except FileNotFoundError:
            failed_message = None
        full_path = self.storage.make_full_path(file)
        st = os.stat(full_path)
        return LoadJobInfo(
            state,
            full_path,
            st.st_size,
            pendulum.from_timestamp(st.st_mtime),
            self.job_elapsed_time_seconds(full_path, now.timestamp() if now else None),
            self.parse_job_file_name(file),
            failed_message
        )

    def build_job_file_name(self, table_name: str, file_id: str, retry_count: int = 0, validate_components: bool = True, with_extension: bool = True) -> str:
        if validate_components:
            FileStorage.validate_file_name_component(table_name)
            # FileStorage.validate_file_name_component(file_id)
        fn = f"{table_name}.{file_id}.{int(retry_count)}"
        if with_extension:
            return fn + f".{self.loader_file_format}"
        return fn

    @staticmethod
    def parse_job_file_name(file_name: str) -> ParsedLoadJobFileName:
        p = Path(file_name)
        parts = p.name.split(".")
        # verify we know the extension
        ext: TLoaderFileFormat = parts[-1]  # type: ignore
        if ext not in LoadStorage.ALL_SUPPORTED_FILE_FORMATS:
            raise TerminalValueError(ext)

        return ParsedLoadJobFileName.parse(file_name)
