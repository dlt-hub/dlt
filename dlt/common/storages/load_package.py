import contextlib
import os
from copy import deepcopy
import threading

import datetime  # noqa: 251
import humanize
from pathlib import PurePath
from pendulum.datetime import DateTime
from typing import (
    ClassVar,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Literal,
    Optional,
    Sequence,
    Set,
    cast,
    Any,
    Tuple,
)
from typing_extensions import NotRequired

from dlt.common.typing import TypedDict, get_args, DictStrAny, SupportsHumanize
from dlt.common.pendulum import pendulum
from dlt.common.json import json
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ContainerInjectableContext
from dlt.common.configuration.exceptions import ContextDefaultCannotBeCreated
from dlt.common.configuration.container import Container
from dlt.common.data_writers import DataWriter, new_file_id
from dlt.common.destination import TLoaderFileFormat
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema import Schema, TSchemaTables
from dlt.common.schema.typing import TStoredSchema, TTableSchemaColumns, TTableSchema
from dlt.common.storages import FileStorage
from dlt.common.storages.exceptions import (
    LoadPackageAlreadyCompleted,
    LoadPackageNotCompleted,
    LoadPackageNotFound,
    CurrentLoadPackageStateNotAvailable,
)
from dlt.common.utils import flatten_list_or_items
from dlt.common.versioned_state import (
    generate_state_version_hash,
    bump_state_version_if_modified,
    TVersionedState,
    default_versioned_state,
    json_decode_state,
    json_encode_state,
)
from dlt.common.time import precise_time

TJobFileFormat = Literal["sql", "reference", TLoaderFileFormat]
"""Loader file formats with internal job types"""
JOB_EXCEPTION_EXTENSION = ".exception"


class TPipelineStateDoc(TypedDict, total=False):
    """Corresponds to the StateInfo Tuple"""

    version: int
    engine_version: int
    pipeline_name: str
    state: str
    created_at: datetime.datetime
    version_hash: str
    _dlt_load_id: NotRequired[str]


class TLoadPackageDropTablesState(TypedDict):
    dropped_tables: NotRequired[List[TTableSchema]]
    """List of tables that are to be dropped from the schema and destination (i.e. when `refresh` mode is used)"""
    truncated_tables: NotRequired[List[TTableSchema]]
    """List of tables that are to be truncated in the destination (i.e. when `refresh='drop_data'` mode is used)"""


class TLoadPackageState(TVersionedState, TLoadPackageDropTablesState, total=False):
    created_at: DateTime
    """Timestamp when the load package was created"""
    pipeline_state: NotRequired[TPipelineStateDoc]
    """Pipeline state, added at the end of the extraction phase"""

    """A section of state that does not participate in change merging and version control"""
    destination_state: NotRequired[Dict[str, Any]]
    """private space for destinations to store state relevant only to the load package"""


class TLoadPackage(TypedDict, total=False):
    load_id: str
    """Load id"""
    state: TLoadPackageState
    """State of the load package"""


# allows to upgrade state when restored with a new version of state logic/schema
LOAD_PACKAGE_STATE_ENGINE_VERSION = 1


def generate_loadpackage_state_version_hash(state: TLoadPackageState) -> str:
    return generate_state_version_hash(state)


def bump_loadpackage_state_version_if_modified(state: TLoadPackageState) -> Tuple[int, str, str]:
    return bump_state_version_if_modified(state)


def migrate_load_package_state(
    state: DictStrAny, from_engine: int, to_engine: int
) -> TLoadPackageState:
    # TODO: if you start adding new versions, we need proper tests for these migrations!
    # NOTE: do not touch destinations state, it is not versioned
    if from_engine == to_engine:
        return cast(TLoadPackageState, state)

    # check state engine
    if from_engine != to_engine:
        raise Exception("No upgrade path for loadpackage state")

    state["_state_engine_version"] = from_engine
    return cast(TLoadPackageState, state)


def default_load_package_state() -> TLoadPackageState:
    return {
        **default_versioned_state(),
        "_state_engine_version": LOAD_PACKAGE_STATE_ENGINE_VERSION,
    }


def create_load_id() -> str:
    """Creates new package load id which is the current unix timestamp converted to string.
    Load ids must have the following properties:
    - They must maintain increase order over time for a particular dlt schema loaded to particular destination and dataset
    `dlt` executes packages in order of load ids
    `dlt` considers a state with the highest load id to be the most up to date when restoring state from destination
    """
    return str(precise_time())


# folders to manage load jobs in a single load package
TPackageJobState = Literal["new_jobs", "failed_jobs", "started_jobs", "completed_jobs"]
WORKING_FOLDERS: Set[TPackageJobState] = set(get_args(TPackageJobState))
TLoadPackageStatus = Literal["new", "extracted", "normalized", "loaded", "aborted"]


class ParsedLoadJobFileName(NamedTuple):
    """Represents a file name of a job in load package. The file name contains name of a table, number of times the job was retried, extension
    and a 5 bytes random string to make job file name unique.
    The job id does not contain retry count and is immutable during loading of the data
    """

    table_name: str
    file_id: str
    retry_count: int
    file_format: TJobFileFormat
    is_compressed: bool = False

    def job_id(self) -> str:
        """Unique identifier of the job"""
        compression_extension = ".gz" if self.is_compressed else ""
        return f"{self.table_name}.{self.file_id}.{self.file_format}{compression_extension}"

    def file_name(self) -> str:
        """A name of the file with the data to be loaded"""
        compression_extension = ".gz" if self.is_compressed else ""
        return f"{self.table_name}.{self.file_id}.{int(self.retry_count)}.{self.file_format}{compression_extension}"

    def with_retry(self) -> "ParsedLoadJobFileName":
        """Returns a job with increased retry count"""
        return self._replace(retry_count=self.retry_count + 1)

    def to_reference_file_name(self) -> str:
        """Returns a file name for a reference job"""
        return f"{self.table_name}.{self.file_id}.{self.retry_count}.reference"

    def full_extension(self) -> str:
        """Returns the full file extension"""
        return f"{self.file_format}.gz" if self.is_compressed else f"{self.file_format}"

    @staticmethod
    def parse(file_name: str) -> "ParsedLoadJobFileName":
        p = PurePath(file_name)
        parts = p.name.split(".")

        if len(parts) == 4:
            # No compression extension: table_name.file_id.retry_count.file_format
            return ParsedLoadJobFileName(
                parts[0], parts[1], int(parts[2]), cast(TJobFileFormat, parts[3]), False
            )
        elif len(parts) == 5 and parts[4] == "gz":
            # With compression extension: table_name.file_id.retry_count.file_format.gz
            return ParsedLoadJobFileName(
                parts[0], parts[1], int(parts[2]), cast(TJobFileFormat, parts[3]), True
            )
        else:
            raise TerminalValueError(parts)

    @staticmethod
    def new_file_id() -> str:
        return new_file_id()

    def __str__(self) -> str:
        return self.job_id()


class LoadJobInfo(NamedTuple):
    state: TPackageJobState
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
        d["job_id"] = self.job_file_info.job_id()
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
    state: TLoadPackageStatus
    schema: Schema
    schema_update: TSchemaTables
    completed_at: datetime.datetime
    jobs: Dict[TPackageJobState, List[LoadJobInfo]]


class LoadPackageInfo(SupportsHumanize, _LoadPackageInfo):
    @property
    def schema_name(self) -> str:
        return self.schema.name

    @property
    def schema_hash(self) -> str:
        return self.schema.version_hash

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
    NEW_JOBS_FOLDER: ClassVar[TPackageJobState] = "new_jobs"
    FAILED_JOBS_FOLDER: ClassVar[TPackageJobState] = "failed_jobs"
    STARTED_JOBS_FOLDER: ClassVar[TPackageJobState] = "started_jobs"
    COMPLETED_JOBS_FOLDER: ClassVar[TPackageJobState] = "completed_jobs"

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
    LOAD_PACKAGE_STATE_FILE_NAME = (  # internal state of the load package, will not be synced to the destination
        "load_package_state.json"
    )

    def __init__(self, storage: FileStorage, initial_state: TLoadPackageStatus) -> None:
        """Creates storage that manages load packages with root at `storage` and initial package state `initial_state`"""
        self.storage = storage
        self.initial_state = initial_state

    #
    # List jobs
    #

    def get_package_path(self, load_id: str) -> str:
        """Gets path of the package relative to storage root"""
        return load_id

    def get_job_state_folder_path(self, load_id: str, state: TPackageJobState) -> str:
        """Gets path to the jobs in `state` in package `load_id`, relative to the storage root"""
        return os.path.join(self.get_package_path(load_id), state)

    def get_job_file_path(self, load_id: str, state: TPackageJobState, file_name: str) -> str:
        """Get path to job with `file_name` in `state` in package `load_id`, relative to the storage root"""
        return os.path.join(self.get_job_state_folder_path(load_id, state), file_name)

    def list_packages(self) -> Sequence[str]:
        """Lists all load ids in storage, earliest first

        NOTE: Load ids are sorted alphabetically. This class does not store package creation time separately.
        """
        loads = self.storage.list_folder_dirs(".", to_root=False)
        # start from the oldest packages
        return sorted(loads)

    def list_new_jobs(self, load_id: str) -> Sequence[str]:
        new_jobs = self.storage.list_folder_files(
            self.get_job_state_folder_path(load_id, PackageStorage.NEW_JOBS_FOLDER)
        )
        return new_jobs

    def list_started_jobs(self, load_id: str) -> Sequence[str]:
        return self.storage.list_folder_files(
            self.get_job_state_folder_path(load_id, PackageStorage.STARTED_JOBS_FOLDER)
        )

    def list_failed_jobs(self, load_id: str) -> Sequence[str]:
        return [
            file
            for file in self.storage.list_folder_files(
                self.get_job_state_folder_path(load_id, PackageStorage.FAILED_JOBS_FOLDER)
            )
            if not file.endswith(JOB_EXCEPTION_EXTENSION)
        ]

    def list_job_with_states_for_table(
        self, load_id: str, table_name: str
    ) -> Sequence[Tuple[TPackageJobState, ParsedLoadJobFileName]]:
        return self.filter_jobs_for_table(self.list_all_jobs_with_states(load_id), table_name)

    def list_all_jobs_with_states(
        self, load_id: str
    ) -> Sequence[Tuple[TPackageJobState, ParsedLoadJobFileName]]:
        info = self.get_load_package_jobs(load_id)
        state_jobs = []
        for state, jobs in info.items():
            state_jobs.extend([(state, job) for job in jobs])
        return state_jobs

    def list_failed_jobs_infos(self, load_id: str) -> Sequence[LoadJobInfo]:
        """List all failed jobs and associated error messages for a load package with `load_id`"""
        if not self.is_package_completed(load_id):
            raise LoadPackageNotCompleted(load_id)
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
            failed_jobs.append(
                self._read_job_file_info(
                    load_id, "failed_jobs", ParsedLoadJobFileName.parse(file), package_created_at
                )
            )
        return failed_jobs

    def is_package_completed(self, load_id: str) -> bool:
        package_path = self.get_package_path(load_id)
        return self.storage.has_file(
            os.path.join(package_path, PackageStorage.PACKAGE_COMPLETED_FILE_NAME)
        )

    #
    # Move jobs
    #

    def import_job(
        self, load_id: str, job_file_path: str, job_state: TPackageJobState = "new_jobs"
    ) -> None:
        """Adds new job by moving the `job_file_path` into `new_jobs` of package `load_id`"""
        self.storage.atomic_import(
            job_file_path, self.get_job_state_folder_path(load_id, job_state)
        )

    def start_job(self, load_id: str, file_name: str) -> str:
        return self._move_job(
            load_id, PackageStorage.NEW_JOBS_FOLDER, PackageStorage.STARTED_JOBS_FOLDER, file_name
        )

    def fail_job(self, load_id: str, file_name: str, failed_message: Optional[str]) -> str:
        # save the exception to failed jobs
        if failed_message:
            self.storage.save(
                self.get_job_file_path(
                    load_id, PackageStorage.FAILED_JOBS_FOLDER, file_name + JOB_EXCEPTION_EXTENSION
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

    def create_package(self, load_id: str, initial_state: TLoadPackageState = None) -> None:
        self.storage.create_folder(load_id)
        # create processing directories
        self.storage.create_folder(os.path.join(load_id, PackageStorage.NEW_JOBS_FOLDER))
        self.storage.create_folder(os.path.join(load_id, PackageStorage.COMPLETED_JOBS_FOLDER))
        self.storage.create_folder(os.path.join(load_id, PackageStorage.FAILED_JOBS_FOLDER))
        self.storage.create_folder(os.path.join(load_id, PackageStorage.STARTED_JOBS_FOLDER))
        # use initial state or create a new by loading non existing state
        state = self.get_load_package_state(load_id) if initial_state is None else initial_state
        if not state.get("created_at"):
            # try to parse load_id as unix timestamp
            try:
                created_at = float(load_id)
            except Exception:
                created_at = precise_time()
            state["created_at"] = pendulum.from_timestamp(created_at)
        self.save_load_package_state(load_id, state)

    def complete_loading_package(self, load_id: str, load_state: TLoadPackageStatus) -> str:
        """Completes loading the package by writing marker file with`package_state. Returns path to the completed package"""
        load_path = self.get_package_path(load_id)
        if self.is_package_completed(load_id):
            raise LoadPackageAlreadyCompleted(load_id)
        # save marker file
        self.storage.save(
            os.path.join(load_path, PackageStorage.PACKAGE_COMPLETED_FILE_NAME), load_state
        )
        # TODO: also modify state
        return load_path

    def remove_completed_jobs(self, load_id: str) -> None:
        """Deletes completed jobs. If package has failed jobs, nothing gets deleted."""
        has_failed_jobs = len(self.list_failed_jobs(load_id)) > 0
        # delete completed jobs
        if not has_failed_jobs:
            self.storage.delete_folder(
                self.get_job_state_folder_path(load_id, PackageStorage.COMPLETED_JOBS_FOLDER),
                recursively=True,
            )

    def delete_package(self, load_id: str, not_exists_ok: bool = False) -> None:
        package_path = self.get_package_path(load_id)
        if not self.storage.has_folder(package_path):
            if not_exists_ok:
                return
            raise LoadPackageNotFound(load_id)
        self.storage.delete_folder(package_path, recursively=True)

    def load_schema(self, load_id: str) -> Schema:
        return Schema.from_dict(self._load_schema(load_id), validate_schema=False)

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
    # Loadpackage state
    #
    def get_load_package_state(self, load_id: str) -> TLoadPackageState:
        package_path = self.get_package_path(load_id)
        if not self.storage.has_folder(package_path):
            raise LoadPackageNotFound(load_id)
        try:
            state_dump = self.storage.load(self.get_load_package_state_path(load_id))
            state = json_decode_state(state_dump)
            return migrate_load_package_state(
                state, state["_state_engine_version"], LOAD_PACKAGE_STATE_ENGINE_VERSION
            )
        except FileNotFoundError:
            return default_load_package_state()

    def save_load_package_state(self, load_id: str, state: TLoadPackageState) -> None:
        package_path = self.get_package_path(load_id)
        if not self.storage.has_folder(package_path):
            raise LoadPackageNotFound(load_id)
        bump_loadpackage_state_version_if_modified(state)
        self.storage.save(
            self.get_load_package_state_path(load_id),
            json_encode_state(state),
        )

    def get_load_package_state_path(self, load_id: str) -> str:
        package_path = self.get_package_path(load_id)
        return os.path.join(package_path, PackageStorage.LOAD_PACKAGE_STATE_FILE_NAME)

    #
    # Get package info
    #

    def get_load_package_jobs(
        self, load_id: str
    ) -> Dict[TPackageJobState, List[ParsedLoadJobFileName]]:
        """Gets all jobs in a package and returns them as lists assigned to a particular state."""
        package_path = self.get_package_path(load_id)
        if not self.storage.has_folder(package_path):
            raise LoadPackageNotFound(load_id)
        all_jobs: Dict[TPackageJobState, List[ParsedLoadJobFileName]] = {}
        for state in WORKING_FOLDERS:
            jobs: List[ParsedLoadJobFileName] = []
            with contextlib.suppress(FileNotFoundError):
                # we ignore if load package lacks one of working folders. completed_jobs may be deleted on archiving
                for file in self.storage.list_folder_files(
                    self.get_job_state_folder_path(load_id, state), to_root=False
                ):
                    if not file.endswith(JOB_EXCEPTION_EXTENSION):
                        jobs.append(ParsedLoadJobFileName.parse(file))
            all_jobs[state] = jobs
        return all_jobs

    def get_load_package_info(self, load_id: str) -> LoadPackageInfo:
        """Gets information on normalized/completed package with given load_id, all jobs and their statuses.

        Will reach to the file system to get additional stats, mtime, also collects exceptions for failed jobs.
        NOTE: do not call this function often. it should be used only to generate metrics
        """
        package_path = self.get_package_path(load_id)
        package_jobs = self.get_load_package_jobs(load_id)

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
        schema = Schema.from_dict(self._load_schema(load_id), validate_schema=False)

        # read jobs with all statuses
        all_job_infos: Dict[TPackageJobState, List[LoadJobInfo]] = {}
        for state, jobs in package_jobs.items():
            all_job_infos[state] = [
                self._read_job_file_info(load_id, state, job, package_created_at) for job in jobs
            ]

        return LoadPackageInfo(
            load_id,
            self.storage.make_full_path(package_path),
            package_state,
            schema,
            applied_update,
            package_created_at,
            all_job_infos,
        )

    def get_job_failed_message(self, load_id: str, job: ParsedLoadJobFileName) -> str:
        """Get exception message of a failed job."""
        rel_path = self.get_job_file_path(load_id, "failed_jobs", job.file_name())
        if not self.storage.has_file(rel_path):
            raise FileNotFoundError(rel_path)
        failed_message: str = None
        with contextlib.suppress(FileNotFoundError):
            failed_message = self.storage.load(rel_path + JOB_EXCEPTION_EXTENSION)
        return failed_message

    def job_to_job_info(
        self, load_id: str, state: TPackageJobState, job: ParsedLoadJobFileName
    ) -> LoadJobInfo:
        """Creates partial job info by converting job object. size, mtime and failed message will not be populated"""
        full_path = os.path.join(
            self.storage.storage_path, self.get_job_file_path(load_id, state, job.file_name())
        )
        return LoadJobInfo(
            state,
            full_path,
            0,
            None,
            0,
            job,
            None,
        )

    def _read_job_file_info(
        self,
        load_id: str,
        state: TPackageJobState,
        job: ParsedLoadJobFileName,
        now: DateTime = None,
    ) -> LoadJobInfo:
        """Creates job info by reading additional props from storage"""
        failed_message = None
        if state == "failed_jobs":
            failed_message = self.get_job_failed_message(load_id, job)
        full_path = os.path.join(
            self.storage.storage_path, self.get_job_file_path(load_id, state, job.file_name())
        )
        st = os.stat(full_path)
        return LoadJobInfo(
            state,
            full_path,
            st.st_size,
            pendulum.from_timestamp(st.st_mtime),
            PackageStorage._job_elapsed_time_seconds(full_path, now.timestamp() if now else None),
            job,
            failed_message,
        )

    #
    # Utils
    #

    def _move_job(
        self,
        load_id: str,
        source_folder: TPackageJobState,
        dest_folder: TPackageJobState,
        file_name: str,
        new_file_name: str = None,
    ) -> str:
        # ensure we move file names, not paths
        assert file_name == FileStorage.get_file_name_from_file_path(file_name)

        dest_path = self.get_job_file_path(load_id, dest_folder, new_file_name or file_name)
        self.storage.atomic_rename(
            self.get_job_file_path(load_id, source_folder, file_name), dest_path
        )
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
            format_spec = DataWriter.writer_spec_from_file_format(loader_file_format, "object")
            return fn + f".{format_spec.file_extension}"
        return fn

    @staticmethod
    def is_package_partially_loaded(package_info: LoadPackageInfo) -> bool:
        """Checks if package is partially loaded - has jobs that are completed and jobs that are not."""
        all_jobs_count = sum(len(package_info.jobs[job_state]) for job_state in WORKING_FOLDERS)
        completed_jobs_count = len(package_info.jobs["completed_jobs"])
        if completed_jobs_count and all_jobs_count - completed_jobs_count > 0:
            return True
        return False

    @staticmethod
    def _job_elapsed_time_seconds(file_path: str, now_ts: float = None) -> float:
        return (now_ts or precise_time()) - os.path.getmtime(file_path)

    @staticmethod
    def filter_jobs_for_table(
        all_jobs: Iterable[Tuple[TPackageJobState, ParsedLoadJobFileName]], table_name: str
    ) -> Sequence[Tuple[TPackageJobState, ParsedLoadJobFileName]]:
        return [job for job in all_jobs if job[1].table_name == table_name]


@configspec
class LoadPackageStateInjectableContext(ContainerInjectableContext):
    storage: PackageStorage = None
    load_id: str = None
    can_create_default: ClassVar[bool] = False
    global_affinity: ClassVar[bool] = False

    def commit(self) -> None:
        with self.state_save_lock:
            self.storage.save_load_package_state(self.load_id, self.state)

    def on_resolved(self) -> None:
        self.state_save_lock = threading.Lock()
        self.state = self.storage.get_load_package_state(self.load_id)


def load_package_state() -> TLoadPackage:
    """Get full load package state present in current context. Across all threads this will be the same in memory dict."""
    container = Container()
    # get injected state if present. injected load package state is typically "managed" so changes will be persisted
    # if you need to save the load package state during a load, you need to call commit_load_package_state
    try:
        state_ctx = container[LoadPackageStateInjectableContext]
    except ContextDefaultCannotBeCreated:
        raise CurrentLoadPackageStateNotAvailable()
    return TLoadPackage(state=state_ctx.state, load_id=state_ctx.load_id)


def commit_load_package_state() -> None:
    """Commit load package state present in current context. This is thread safe."""
    container = Container()
    try:
        state_ctx = container[LoadPackageStateInjectableContext]
    except ContextDefaultCannotBeCreated:
        raise CurrentLoadPackageStateNotAvailable()
    state_ctx.commit()


def destination_state() -> DictStrAny:
    """Get segment of load package state that is specific to the current destination."""
    lp = load_package_state()
    return lp["state"].setdefault("destination_state", {})


def clear_destination_state(commit: bool = True) -> None:
    """Clear segment of load package state that is specific to the current destination. Optionally commit to load package."""
    lp = load_package_state()
    lp["state"].pop("destination_state", None)
    if commit:
        commit_load_package_state()
