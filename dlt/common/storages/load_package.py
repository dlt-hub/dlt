import contextlib
import os
from copy import deepcopy
import datetime  # noqa: 251
import humanize
from pathlib import Path
from pendulum.datetime import DateTime
from typing import (
    ClassVar,
    Dict,
    List,
    NamedTuple,
    Literal,
    Optional,
    Sequence,
    Set,
    get_args,
    cast,
)

from dlt.common import pendulum, json
from dlt.common.data_writers import DataWriter, new_file_id
from dlt.common.destination import TLoaderFileFormat
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import Schema, TSchemaTables
from dlt.common.schema.typing import TStoredSchema, TTableSchemaColumns
from dlt.common.storages import FileStorage
from dlt.common.storages.exceptions import LoadPackageNotFound
from dlt.common.typing import DictStrAny, StrAny, SupportsHumanize
from dlt.common.utils import flatten_list_or_items

# folders to manage load jobs in a single load package
TJobState = Literal["new_jobs", "failed_jobs", "started_jobs", "completed_jobs"]
WORKING_FOLDERS: Set[TJobState] = set(get_args(TJobState))
TLoadPackageState = Literal["new", "extracted", "normalized", "loaded", "aborted"]


class ParsedLoadJobFileName(NamedTuple):
    """Represents a file name of a job in load package. The file name contains name of a table, number of times the job was retired, extension
    and a 5 bytes random string to make job file name unique.
    The job id does not contain retry count and is immutable during loading of the data
    """

    table_name: str
    file_id: str
    retry_count: int
    file_format: TLoaderFileFormat

    def job_id(self) -> str:
        """Unique identifier of the job"""
        return f"{self.table_name}.{self.file_id}.{self.file_format}"

    def file_name(self) -> str:
        """A name of the file with the data to be loaded"""
        return f"{self.table_name}.{self.file_id}.{int(self.retry_count)}.{self.file_format}"

    def with_retry(self) -> "ParsedLoadJobFileName":
        """Returns a job with increased retry count"""
        return self._replace(retry_count=self.retry_count + 1)

    @staticmethod
    def parse(file_name: str) -> "ParsedLoadJobFileName":
        p = Path(file_name)
        parts = p.name.split(".")
        if len(parts) != 4:
            raise TerminalValueError(parts)

        return ParsedLoadJobFileName(
            parts[0], parts[1], int(parts[2]), cast(TLoaderFileFormat, parts[3])
        )

    @staticmethod
    def new_file_id() -> str:
        return new_file_id()

    def __str__(self) -> str:
        return self.job_id()


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
        failed_msg = (
            "The job FAILED TERMINALLY and cannot be restarted." if self.failed_message else ""
        )
        elapsed_msg = (
            humanize.precisedelta(pendulum.duration(seconds=self.elapsed))
            if self.elapsed
            else "---"
        )
        msg = (
            f"Job: {self.job_file_info.job_id()}, table: {self.job_file_info.table_name} in"
            f" {self.state}. "
        )
        msg += (
            f"File type: {self.job_file_info.file_format}, size:"
            f" {humanize.naturalsize(self.file_size, binary=True, gnu=True)}. "
        )
        msg += f"Started on: {self.created_at} and completed in {elapsed_msg}."
        if failed_msg:
            msg += "\nThe job FAILED TERMINALLY and cannot be restarted."
            if verbosity > 0:
                msg += "\n" + self.failed_message
        return msg

    def __str__(self) -> str:
        return self.asstr(verbosity=0)


class _LoadPackageInfo(NamedTuple):
    load_id: str
    package_path: str
    state: TLoadPackageState
    schema: Schema
    schema_update: TSchemaTables
    completed_at: datetime.datetime
    jobs: Dict[TJobState, List[LoadJobInfo]]


class LoadPackageInfo(SupportsHumanize, _LoadPackageInfo):
    @property
    def schema_name(self) -> str:
        return self.schema.name

    @property
    def schema_hash(self) -> str:
        return self.schema.stored_version_hash

    def asdict(self) -> DictStrAny:
        d = self._asdict()
        # job as list
        d["jobs"] = [job.asdict() for job in flatten_list_or_items(iter(self.jobs.values()))]  # type: ignore
        d["schema_hash"] = self.schema_hash
        d["schema_name"] = self.schema_name
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
        d.pop("schema")
        d["tables"] = tables

        return d

    def asstr(self, verbosity: int = 0) -> str:
        completed_msg = (
            f"The package was {self.state.upper()} at {self.completed_at}"
            if self.completed_at
            else "The package is NOT YET LOADED to the destination"
        )
        msg = (
            f"The package with load id {self.load_id} for schema {self.schema_name} is in"
            f" {self.state.upper()} state. It updated schema for {len(self.schema_update)} tables."
            f" {completed_msg}.\n"
        )
        msg += "Jobs details:\n"
        msg += "\n".join(job.asstr(verbosity) for job in flatten_list_or_items(iter(self.jobs.values())))  # type: ignore
        return msg

    def __str__(self) -> str:
        return self.asstr(verbosity=0)


class PackageStorage:
    NEW_JOBS_FOLDER: ClassVar[TJobState] = "new_jobs"
    FAILED_JOBS_FOLDER: ClassVar[TJobState] = "failed_jobs"
    STARTED_JOBS_FOLDER: ClassVar[TJobState] = "started_jobs"
    COMPLETED_JOBS_FOLDER: ClassVar[TJobState] = "completed_jobs"

    SCHEMA_FILE_NAME: ClassVar[str] = "schema.json"
    SCHEMA_UPDATES_FILE_NAME = (  # updates to the tables in schema created by normalizer
        "schema_updates.json"
    )
    APPLIED_SCHEMA_UPDATES_FILE_NAME = (
        "applied_" + "schema_updates.json"
    )  # updates applied to the destination
    PACKAGE_COMPLETED_FILE_NAME = (  # completed package marker file, currently only to store data with os.stat
        "package_completed.json"
    )

    def __init__(self, storage: FileStorage, initial_state: TLoadPackageState) -> None:
        """Creates storage that manages load packages with root at `storage` and initial package state `initial_state`"""
        self.storage = storage
        self.initial_state = initial_state

    #
    # List jobs
    #

    def get_package_path(self, load_id: str) -> str:
        return load_id

    def get_job_folder_path(self, load_id: str, folder: TJobState) -> str:
        return os.path.join(self.get_package_path(load_id), folder)

    def get_job_file_path(self, load_id: str, folder: TJobState, file_name: str) -> str:
        return os.path.join(self.get_job_folder_path(load_id, folder), file_name)

    def list_packages(self) -> Sequence[str]:
        """Lists all load ids in storage, earliest first

        NOTE: Load ids are sorted alphabetically. This class does not store package creation time separately.
        """
        loads = self.storage.list_folder_dirs(".", to_root=False)
        # start from the oldest packages
        return sorted(loads)

    def list_new_jobs(self, load_id: str) -> Sequence[str]:
        new_jobs = self.storage.list_folder_files(
            self.get_job_folder_path(load_id, PackageStorage.NEW_JOBS_FOLDER)
        )
        return new_jobs

    def list_started_jobs(self, load_id: str) -> Sequence[str]:
        return self.storage.list_folder_files(
            self.get_job_folder_path(load_id, PackageStorage.STARTED_JOBS_FOLDER)
        )

    def list_failed_jobs(self, load_id: str) -> Sequence[str]:
        return self.storage.list_folder_files(
            self.get_job_folder_path(load_id, PackageStorage.FAILED_JOBS_FOLDER)
        )

    def list_jobs_for_table(self, load_id: str, table_name: str) -> Sequence[LoadJobInfo]:
        return [
            job for job in self.list_all_jobs(load_id) if job.job_file_info.table_name == table_name
        ]

    def list_all_jobs(self, load_id: str) -> Sequence[LoadJobInfo]:
        info = self.get_load_package_info(load_id)
        return [job for job in flatten_list_or_items(iter(info.jobs.values()))]  # type: ignore

    def list_failed_jobs_infos(self, load_id: str) -> Sequence[LoadJobInfo]:
        """List all failed jobs and associated error messages for a load package with `load_id`"""
        failed_jobs: List[LoadJobInfo] = []
        package_path = self.get_package_path(load_id)
        package_created_at = pendulum.from_timestamp(
            os.path.getmtime(
                self.storage.make_full_path(
                    os.path.join(package_path, PackageStorage.PACKAGE_COMPLETED_FILE_NAME)
                )
            )
        )
        for file in self.list_failed_jobs(load_id):
            if not file.endswith(".exception"):
                failed_jobs.append(
                    self._read_job_file_info("failed_jobs", file, package_created_at)
                )
        return failed_jobs

    #
    # Move jobs
    #

    def import_job(
        self, load_id: str, job_file_path: str, job_state: TJobState = "new_jobs"
    ) -> None:
        """Adds new job by moving the `job_file_path` into `new_jobs` of package `load_id`"""
        self.storage.atomic_import(job_file_path, self.get_job_folder_path(load_id, job_state))

    def start_job(self, load_id: str, file_name: str) -> str:
        return self._move_job(
            load_id, PackageStorage.NEW_JOBS_FOLDER, PackageStorage.STARTED_JOBS_FOLDER, file_name
        )

    def fail_job(self, load_id: str, file_name: str, failed_message: Optional[str]) -> str:
        # save the exception to failed jobs
        if failed_message:
            self.storage.save(
                self.get_job_file_path(
                    load_id, PackageStorage.FAILED_JOBS_FOLDER, file_name + ".exception"
                ),
                failed_message,
            )
        # move to failed jobs
        return self._move_job(
            load_id,
            PackageStorage.STARTED_JOBS_FOLDER,
            PackageStorage.FAILED_JOBS_FOLDER,
            file_name,
        )

    def retry_job(self, load_id: str, file_name: str) -> str:
        # when retrying job we must increase the retry count
        source_fn = ParsedLoadJobFileName.parse(file_name)
        dest_fn = source_fn.with_retry()
        # move it directly to new file name
        return self._move_job(
            load_id,
            PackageStorage.STARTED_JOBS_FOLDER,
            PackageStorage.NEW_JOBS_FOLDER,
            file_name,
            dest_fn.file_name(),
        )

    def complete_job(self, load_id: str, file_name: str) -> str:
        return self._move_job(
            load_id,
            PackageStorage.STARTED_JOBS_FOLDER,
            PackageStorage.COMPLETED_JOBS_FOLDER,
            file_name,
        )

    #
    # Create and drop entities
    #

    def create_package(self, load_id: str) -> None:
        self.storage.create_folder(load_id)
        # create processing directories
        self.storage.create_folder(os.path.join(load_id, PackageStorage.NEW_JOBS_FOLDER))
        self.storage.create_folder(os.path.join(load_id, PackageStorage.COMPLETED_JOBS_FOLDER))
        self.storage.create_folder(os.path.join(load_id, PackageStorage.FAILED_JOBS_FOLDER))
        self.storage.create_folder(os.path.join(load_id, PackageStorage.STARTED_JOBS_FOLDER))

    def complete_loading_package(self, load_id: str, load_state: TLoadPackageState) -> str:
        """Completes loading the package by writing marker file with`package_state. Returns path to the completed package"""
        load_path = self.get_package_path(load_id)
        # save marker file
        self.storage.save(
            os.path.join(load_path, PackageStorage.PACKAGE_COMPLETED_FILE_NAME), load_state
        )
        return load_path

    def remove_completed_jobs(self, load_id: str) -> None:
        """Deletes completed jobs. If package has failed jobs, nothing gets deleted."""
        has_failed_jobs = len(self.list_failed_jobs(load_id)) > 0
        # delete completed jobs
        if not has_failed_jobs:
            self.storage.delete_folder(
                self.get_job_folder_path(load_id, PackageStorage.COMPLETED_JOBS_FOLDER),
                recursively=True,
            )

    def delete_package(self, load_id: str) -> None:
        package_path = self.get_package_path(load_id)
        if not self.storage.has_folder(package_path):
            raise LoadPackageNotFound(load_id)
        self.storage.delete_folder(package_path, recursively=True)

    def load_schema(self, load_id: str) -> Schema:
        return Schema.from_dict(self._load_schema(load_id))

    def schema_name(self, load_id: str) -> str:
        """Gets schema name associated with the package"""
        schema_dict: TStoredSchema = self._load_schema(load_id)  # type: ignore[assignment]
        return schema_dict["name"]

    def save_schema(self, load_id: str, schema: Schema) -> str:
        # save a schema to a temporary load package
        dump = json.dumps(schema.to_dict())
        return self.storage.save(os.path.join(load_id, PackageStorage.SCHEMA_FILE_NAME), dump)

    def save_schema_updates(self, load_id: str, schema_update: TSchemaTables) -> None:
        with self.storage.open_file(
            os.path.join(load_id, PackageStorage.SCHEMA_UPDATES_FILE_NAME), mode="wb"
        ) as f:
            json.dump(schema_update, f)

    #
    # Get package info
    #

    def get_load_package_info(self, load_id: str) -> LoadPackageInfo:
        """Gets information on normalized/completed package with given load_id, all jobs and their statuses."""
        package_path = self.get_package_path(load_id)
        if not self.storage.has_folder(package_path):
            raise LoadPackageNotFound(load_id)

        package_created_at: DateTime = None
        package_state = self.initial_state
        applied_update: TSchemaTables = {}

        # check if package completed
        completed_file_path = os.path.join(package_path, PackageStorage.PACKAGE_COMPLETED_FILE_NAME)
        if self.storage.has_file(completed_file_path):
            package_created_at = pendulum.from_timestamp(
                os.path.getmtime(self.storage.make_full_path(completed_file_path))
            )
            package_state = self.storage.load(completed_file_path)

        # check if schema updates applied
        applied_schema_update_file = os.path.join(
            package_path, PackageStorage.APPLIED_SCHEMA_UPDATES_FILE_NAME
        )
        if self.storage.has_file(applied_schema_update_file):
            applied_update = json.loads(self.storage.load(applied_schema_update_file))
        schema = Schema.from_dict(self._load_schema(load_id))

        # read jobs with all statuses
        all_jobs: Dict[TJobState, List[LoadJobInfo]] = {}
        for state in WORKING_FOLDERS:
            jobs: List[LoadJobInfo] = []
            with contextlib.suppress(FileNotFoundError):
                # we ignore if load package lacks one of working folders. completed_jobs may be deleted on archiving
                for file in self.storage.list_folder_files(os.path.join(package_path, state)):
                    if not file.endswith(".exception"):
                        jobs.append(self._read_job_file_info(state, file, package_created_at))
            all_jobs[state] = jobs

        return LoadPackageInfo(
            load_id,
            self.storage.make_full_path(package_path),
            package_state,
            schema,
            applied_update,
            package_created_at,
            all_jobs,
        )

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
            PackageStorage._job_elapsed_time_seconds(full_path, now.timestamp() if now else None),
            ParsedLoadJobFileName.parse(file),
            failed_message,
        )

    def _move_job(
        self,
        load_id: str,
        source_folder: TJobState,
        dest_folder: TJobState,
        file_name: str,
        new_file_name: str = None,
    ) -> str:
        # ensure we move file names, not paths
        assert file_name == FileStorage.get_file_name_from_file_path(file_name)
        load_path = self.get_package_path(load_id)
        dest_path = os.path.join(load_path, dest_folder, new_file_name or file_name)
        self.storage.atomic_rename(os.path.join(load_path, source_folder, file_name), dest_path)
        # print(f"{join(load_path, source_folder, file_name)} -> {dest_path}")
        return self.storage.make_full_path(dest_path)

    def _load_schema(self, load_id: str) -> DictStrAny:
        schema_path = os.path.join(load_id, PackageStorage.SCHEMA_FILE_NAME)
        return json.loads(self.storage.load(schema_path))  # type: ignore[no-any-return]

    @staticmethod
    def build_job_file_name(
        table_name: str,
        file_id: str,
        retry_count: int = 0,
        validate_components: bool = True,
        loader_file_format: TLoaderFileFormat = None,
    ) -> str:
        if validate_components:
            FileStorage.validate_file_name_component(table_name)
        fn = f"{table_name}.{file_id}.{int(retry_count)}"
        if loader_file_format:
            format_spec = DataWriter.data_format_from_file_format(loader_file_format)
            return fn + f".{format_spec.file_extension}"
        return fn

    @staticmethod
    def is_package_partially_loaded(package_info: LoadPackageInfo) -> bool:
        """Checks if package is partially loaded - has jobs that are not new."""
        if package_info.state == "normalized":
            pending_jobs: Sequence[TJobState] = ["new_jobs"]
        else:
            pending_jobs = ["completed_jobs", "failed_jobs"]
        return (
            sum(
                len(package_info.jobs[job_state])
                for job_state in WORKING_FOLDERS
                if job_state not in pending_jobs
            )
            > 0
        )

    @staticmethod
    def _job_elapsed_time_seconds(file_path: str, now_ts: float = None) -> float:
        return (now_ts or pendulum.now().timestamp()) - os.path.getmtime(file_path)
