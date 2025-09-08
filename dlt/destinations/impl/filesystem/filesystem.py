import posixpath
import os
import base64
from contextlib import contextmanager
from types import TracebackType
from typing import (
    ContextManager,
    List,
    Type,
    Iterable,
    Iterator,
    Optional,
    Tuple,
    Sequence,
    cast,
    Generator,
    Literal,
    Any,
    Dict,
)
from fsspec import AbstractFileSystem

import dlt
from dlt.common import logger, time, json, pendulum
from dlt.common.destination.utils import resolve_merge_strategy, resolve_replace_strategy
from dlt.common.metrics import LoadJobMetrics
from dlt.common.schema.exceptions import TableNotFound
from dlt.common.schema.typing import (
    C_DLT_LOAD_ID,
    TTableFormat,
    TTableSchemaColumns,
    DLT_NAME_PREFIX,
)
from dlt.common.storages.exceptions import CurrentLoadPackageStateNotAvailable
from dlt.common.storages.fsspec_filesystem import glob_files
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import DictStrAny
from dlt.common.schema import Schema, TSchemaTables
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.common.storages import FileStorage, fsspec_from_config
from dlt.common.storages.load_package import (
    LoadJobInfo,
    ParsedLoadJobFileName,
    TPipelineStateDoc,
    load_package as current_load_package,
)
from dlt.destinations.sql_client import WithSqlClient, SqlClientBase
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.client import (
    FollowupJobRequest,
    PreparedTableSchema,
    SupportsOpenTables,
    TLoadJobState,
    RunnableLoadJob,
    JobClientBase,
    HasFollowupJobs,
    WithStagingDataset,
    WithStateSync,
    StorageSchemaInfo,
    StateInfo,
    LoadJob,
)
from dlt.common.destination.exceptions import (
    DestinationUndefinedEntity,
    OpenTableCatalogNotSupported,
    OpenTableFormatNotSupported,
)

from dlt.destinations.job_impl import (
    ReferenceFollowupJobRequest,
    FinalizedLoadJob,
    FinalizedLoadJobWithFollowupJobs,
)
from dlt.destinations.impl.filesystem.configuration import FilesystemDestinationClientConfiguration
from dlt.destinations import path_utils
from dlt.destinations.fs_client import FSClientBase
from dlt.destinations.utils import (
    verify_schema_merge_disposition,
    verify_schema_replace_disposition,
)

INIT_FILE_NAME = "init"
FILENAME_SEPARATOR = "__"


class FilesystemLoadJob(RunnableLoadJob):
    def __init__(
        self,
        file_path: str,
    ) -> None:
        super().__init__(file_path)
        self._job_client: FilesystemClient = None

    def run(self) -> None:
        self.__is_local_filesystem = self._job_client.config.is_local_filesystem
        # We would like to avoid failing for local filesystem where
        # deeply nested directory will not exist before writing a file.
        # It `auto_mkdir` is disabled by default in fsspec so we made some
        # trade offs between different options and decided on this.
        # remote_path = f"{client.config.protocol}://{posixpath.join(dataset_path, destination_file_name)}"
        remote_path = self.make_remote_path()
        if self.__is_local_filesystem:
            # use os.path for local file name
            self._job_client.fs_client.makedirs(os.path.dirname(remote_path), exist_ok=True)
        self._job_client.fs_client.put_file(self._file_path, remote_path)

    def make_remote_path(self) -> str:
        """Returns path on the remote filesystem to which copy the file, without scheme. For local filesystem a native path is used"""

        # package state is optional
        try:
            package_timestamp = current_load_package()["state"]["created_at"]
        except CurrentLoadPackageStateNotAvailable:
            package_timestamp = None

        destination_file_name = path_utils.create_path(
            self._job_client.config.layout,
            self._file_name,
            self._job_client.schema.name,
            self._load_id,
            current_datetime=self._job_client.config.current_datetime,
            load_package_timestamp=package_timestamp,
            extra_placeholders=self._job_client.config.extra_placeholders,
        )
        # pick local filesystem pathlib or posix for buckets
        pathlib = self._job_client.config.pathlib
        # path.join does not normalize separators and available
        # normalization functions are very invasive and may string the trailing separator
        return pathlib.join(  # type: ignore[no-any-return]
            self._job_client.dataset_path,
            path_utils.normalize_path_sep(pathlib, destination_file_name),
        )

    def make_remote_url(self) -> str:
        """Returns path on a remote filesystem as a full url including scheme."""
        return self._job_client.make_remote_url(self.make_remote_path())

    def metrics(self) -> Optional[LoadJobMetrics]:
        m = super().metrics()
        return m._replace(remote_url=self.make_remote_url())


class TableFormatLoadFilesystemJob(FilesystemLoadJob):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path=file_path)

        self.file_paths = ReferenceFollowupJobRequest.resolve_references(self._file_path)

    def make_remote_path(self) -> str:
        return self._job_client.get_table_dir(self.load_table_name)

    @property
    def arrow_dataset(self) -> Any:
        from dlt.common.libs.pyarrow import pyarrow

        return pyarrow.dataset.dataset(self.file_paths)

    @property
    def _partition_columns(self) -> List[str]:
        return get_columns_names_with_prop(self._load_table, "partition")


class DeltaLoadFilesystemJob(TableFormatLoadFilesystemJob):
    def run(self) -> None:
        # create Arrow dataset from Parquet files
        from dlt.common.libs.pyarrow import pyarrow as pa
        from dlt.common.libs.deltalake import (
            write_delta_table,
            merge_delta_table,
            DeltaTable,
            deltalake_storage_options,
        )

        logger.info(
            f"Will copy file(s) {self.file_paths} to delta table {self.make_remote_url()} [arrow"
            f" buffer: {pa.total_allocated_bytes()}]"
        )
        source_ds = self.arrow_dataset
        storage_options = deltalake_storage_options(self._job_client.config)
        try:
            delta_table: DeltaTable = self._job_client.load_open_table(
                "delta", self.load_table_name
            )
        except DestinationUndefinedEntity:
            delta_table = None

        with source_ds.scanner().to_reader() as arrow_rbr:  # RecordBatchReader
            if self._load_table["write_disposition"] == "merge" and delta_table is not None:
                merge_delta_table(
                    table=delta_table,
                    data=arrow_rbr,
                    schema=self._load_table,
                )
            else:
                location = self._job_client.get_open_table_location("delta", self.load_table_name)
                write_delta_table(
                    table_or_uri=location if delta_table is None else delta_table,
                    data=arrow_rbr,
                    write_disposition=self._load_table["write_disposition"],
                    partition_by=self._partition_columns,
                    storage_options=storage_options,
                )
        # release memory ASAP by deleting objects explicitly
        del source_ds
        del delta_table
        logger.info(
            f"Copied {self.file_paths} to delta table {self.make_remote_url()} [arrow buffer:"
            f" {pa.total_allocated_bytes()}]"
        )


class IcebergLoadFilesystemJob(TableFormatLoadFilesystemJob):
    def run(self) -> None:
        from dlt.common.libs.pyiceberg import write_iceberg_table, create_table

        try:
            table = self._job_client.load_open_table(
                "iceberg",
                self.load_table_name,
                schema=self.arrow_dataset.schema,
            )
        except DestinationUndefinedEntity:
            location = self._job_client.get_open_table_location("iceberg", self.load_table_name)
            table_id = f"{self._job_client.dataset_name}.{self.load_table_name}"
            create_table(
                self._job_client.get_open_table_catalog("iceberg"),
                table_id,
                table_location=location,
                schema=self.arrow_dataset.schema,
                partition_columns=self._partition_columns,
            )
            # run again with created table
            self.run()
            return

        write_iceberg_table(
            table=table,
            data=self.arrow_dataset.to_table(),
            write_disposition=self._load_table["write_disposition"],
        )


class FilesystemLoadJobWithFollowup(HasFollowupJobs, FilesystemLoadJob):
    def create_followup_jobs(self, final_state: TLoadJobState) -> List[FollowupJobRequest]:
        jobs = super().create_followup_jobs(final_state)
        if final_state == "completed":
            ref_job = ReferenceFollowupJobRequest(
                original_file_name=self.file_name(),
                remote_paths=[self._job_client.make_remote_url(self.make_remote_path())],
            )
            jobs.append(ref_job)
        return jobs


class FilesystemClient(
    FSClientBase,
    WithSqlClient,
    JobClientBase,
    WithStagingDataset,
    WithStateSync,
    SupportsOpenTables,
):
    fs_client: AbstractFileSystem
    # a path (without the scheme) to a location in the bucket where dataset is present
    bucket_path: str
    # name of the dataset
    dataset_name: str

    def __init__(
        self,
        schema: Schema,
        config: FilesystemDestinationClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)
        self.fs_client, fs_path = fsspec_from_config(config)
        self.is_local_filesystem = config.is_local_filesystem
        self.bucket_path = (
            config.make_local_path(config.bucket_url) if self.is_local_filesystem else fs_path
        )
        # pick local filesystem pathlib or posix for buckets
        self.pathlib = os.path if self.is_local_filesystem else posixpath

        self.config: FilesystemDestinationClientConfiguration = config
        # verify files layout. we need {table_name} and only allow {schema_name} before it, otherwise tables
        # cannot be replaced and we cannot initialize folders consistently
        self.table_prefix_layout = path_utils.get_table_prefix_layout(config.layout)
        self.dataset_name = self.config.normalize_dataset_name(self.schema)
        self._sql_client: SqlClientBase[Any] = None
        # iceberg catalog
        self._catalog: Any = None

    @property
    def sql_client_class(self) -> Type[SqlClientBase[Any]]:
        from dlt.destinations.impl.filesystem.sql_client import FilesystemSqlClient

        return FilesystemSqlClient

    @property
    def sql_client(self) -> SqlClientBase[Any]:
        # we use an inner import here, since the sql client depends on duckdb and will
        # only be used for read access on data, some users will not need the dependency
        from dlt.destinations.impl.filesystem.sql_client import FilesystemSqlClient

        if not self._sql_client:
            self._sql_client = FilesystemSqlClient(self, self.dataset_name)
        return self._sql_client

    @sql_client.setter
    def sql_client(self, client: SqlClientBase[Any]) -> None:
        self._sql_client = client

    def drop_storage(self) -> None:
        if self.is_storage_initialized():
            self.fs_client.rm(self.dataset_path, recursive=True)

    @property
    def dataset_path(self) -> str:
        """A path within a bucket to tables in a dataset
        NOTE: dataset_name changes if with_staging_dataset is active
        """
        return self.pathlib.join(self.bucket_path, self.dataset_name)  # type: ignore[no-any-return]

    @contextmanager
    def with_staging_dataset(self) -> Iterator["FilesystemClient"]:
        current_dataset_name = self.dataset_name
        try:
            self.dataset_name = self.config.normalize_staging_dataset_name(self.schema)
            yield self
        finally:
            # restore previous dataset name
            self.dataset_name = current_dataset_name

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        # clean up existing files for tables selected for truncating
        if truncate_tables and self.fs_client.isdir(self.dataset_path):
            # get all dirs with table data to delete. the table data are guaranteed to be files in those folders
            # TODO: when we do partitioning it is no longer the case and we may remove folders below instead
            logger.info(f"Will truncate tables {truncate_tables}")
            self.truncate_tables(list(truncate_tables))

        # we mark the storage folder as initialized
        self.fs_client.makedirs(self.dataset_path, exist_ok=True)
        self.fs_client.touch(self.pathlib.join(self.dataset_path, INIT_FILE_NAME))

    def drop_tables(self, *tables: str, delete_schema: bool = True) -> None:
        self.truncate_tables(list(tables))
        if not delete_schema:
            return
        # Delete all stored schemas
        for filename, fileparts in self._iter_stored_schema_files():
            if fileparts[0] == self.schema.name:
                self._delete_file(filename)

    def get_storage_tables(
        self, table_names: Iterable[str]
    ) -> Iterable[Tuple[str, TTableSchemaColumns]]:
        """Yields tables that have files in storage, returns columns from current schema"""
        for table_name in table_names:
            table_dir = self.get_table_dir(table_name)
            if self.fs_client.exists(table_dir) and (
                self.fs_client.exists(self.pathlib.join(table_dir, INIT_FILE_NAME))
                or len(self.list_table_files(table_name)) > 0
            ):
                if table_name in self.schema.tables:
                    yield (table_name, self.schema.get_table_columns(table_name))
                else:
                    yield (table_name, {"_column": {}})
            else:
                # if no columns we assume that table does not exist
                yield (table_name, {})

    def get_storage_table(self, table_name: str) -> Tuple[str, TTableSchemaColumns]:
        return list(self.get_storage_tables([table_name]))[0]

    def truncate_tables(self, table_names: List[str]) -> None:
        """Truncate a set of regular tables with given `table_names`"""
        table_dirs = set(self.get_table_dirs(table_names))
        table_prefixes = [self.get_table_prefix(t) for t in table_names]
        for table_dir in table_dirs:
            if self.fs_client.exists(table_dir):
                for table_file in self.list_files_with_prefixes(table_dir, table_prefixes):
                    # NOTE: deleting in chunks on s3 does not raise on access denied, file non existing and probably other errors
                    # print(f"DEL {table_file}")
                    try:
                        self._delete_file(table_file)
                    except FileNotFoundError:
                        logger.info(
                            f"Directory or path to truncate tables {table_names} does not exist but"
                            " it should have been created previously!"
                        )

    def _delete_file(self, file_path: str) -> None:
        try:
            # NOTE: must use rm_file to get errors on delete
            self.fs_client.rm_file(file_path)
        except NotImplementedError:
            # not all filesystems implement the above
            self.fs_client.rm(file_path)
            if self.fs_client.exists(file_path):
                raise FileExistsError(file_path)

    def verify_schema(
        self, only_tables: Iterable[str] = None, new_jobs: Iterable[ParsedLoadJobFileName] = None
    ) -> List[PreparedTableSchema]:
        loaded_tables = super().verify_schema(only_tables, new_jobs)
        # TODO: finetune verify_schema_merge_disposition ie. hard deletes are not supported
        if exceptions := verify_schema_merge_disposition(
            self.schema, loaded_tables, self.capabilities, warnings=True
        ):
            for exception in exceptions:
                logger.error(str(exception))
            raise exceptions[0]
        if exceptions := verify_schema_replace_disposition(
            self.schema,
            loaded_tables,
            self.capabilities,
            self.config.replace_strategy,
            warnings=True,
        ):
            for exception in exceptions:
                logger.error(str(exception))
            raise exceptions[0]
        return loaded_tables

    def update_stored_schema(
        self,
        only_tables: Iterable[str] = None,
        expected_update: TSchemaTables = None,
    ) -> TSchemaTables:
        applied_update = super().update_stored_schema(only_tables, expected_update)
        # create destination dirs for all tables
        table_names = only_tables or self.schema.tables.keys()
        dirs_to_create = self.get_table_dirs(table_names)
        for tables_name, directory in zip(table_names, dirs_to_create):
            self.fs_client.makedirs(directory, exist_ok=True)
            # we need to mark the folders of the data tables as initialized
            if tables_name in self.schema.dlt_table_names():
                self.fs_client.touch(self.pathlib.join(directory, INIT_FILE_NAME))

        # don't store schema when used as staging
        if not self.config.as_staging_destination:
            # check if schema with hash exists
            current_hash = self.schema.stored_version_hash
            if not self._get_stored_schema_by_hash_or_newest(current_hash):
                logger.info(
                    f"Schema with hash {self.schema.stored_version_hash} not found in the storage."
                    " upgrading"
                )
                self._update_schema_in_storage(self.schema)

        # we assume that expected_update == applied_update so table schemas in dest were not
        # externally changed
        return applied_update

    def prepare_load_table(self, table_name: str) -> PreparedTableSchema:
        table = super().prepare_load_table(table_name)
        if self.config.as_staging_destination:
            if table["write_disposition"] in ("merge", "replace"):
                table["write_disposition"] = "append"
            table.pop("table_format", None)
        merge_strategy = resolve_merge_strategy(self.schema.tables, table, self.capabilities)
        if table["write_disposition"] == "merge":
            if merge_strategy is None:
                # no supported merge strategies, fall back to append
                table["write_disposition"] = "append"
            else:
                table["x-merge-strategy"] = merge_strategy  # type: ignore[typeddict-unknown-key]
        if table["write_disposition"] == "replace":
            replace_strategy = resolve_replace_strategy(
                table, self.config.replace_strategy, self.capabilities
            )
            assert replace_strategy, f"Must be able to get replace strategy for {table_name}"
            table["x-replace-strategy"] = replace_strategy  # type: ignore[typeddict-unknown-key]
        if table["name"].startswith(DLT_NAME_PREFIX):
            table.pop("table_format", None)
        return table

    def get_table_dir(self, table_name: str, remote: bool = False) -> str:
        # dlt tables do not respect layout (for now)
        table_prefix = self.get_table_prefix(table_name)
        table_dir: str = self.pathlib.dirname(table_prefix)
        if remote:
            table_dir = self.make_remote_url(table_dir)
        return table_dir

    def get_table_prefix(self, table_name: str) -> str:
        # dlt tables do not respect layout (for now)
        if table_name.startswith(self.schema._dlt_tables_prefix):
            # dlt tables get layout where each tables is a folder
            # it is crucial to append and keep "/" at the end
            table_prefix = self.pathlib.join(table_name, "")
        else:
            table_prefix = self.table_prefix_layout.format(
                schema_name=self.schema.name, table_name=table_name
            )
        return self.pathlib.join(  # type: ignore[no-any-return]
            self.dataset_path, path_utils.normalize_path_sep(self.pathlib, table_prefix)
        )

    def get_table_dirs(self, table_names: Iterable[str], remote: bool = False) -> List[str]:
        """Gets directories where table data is stored."""
        return [self.get_table_dir(t, remote=remote) for t in table_names]

    def list_table_files(self, table_name: str) -> List[str]:
        """gets list of files associated with one table"""
        table_dir = self.get_table_dir(table_name)
        # we need the table prefix so we separate table files if in the same folder
        table_prefix = self.get_table_prefix(table_name)
        try:
            return self.list_files_with_prefixes(table_dir, [table_prefix])
        except FileNotFoundError as file_ex:
            raise DestinationUndefinedEntity(file_ex)

    def list_files_with_prefixes(self, table_dir: str, prefixes: List[str]) -> List[str]:
        """returns all files in a directory that match given prefixes"""
        result = []
        # we fallback to our own glob implementation that is tested to return consistent results for
        # filesystems we support. we were not able to use `find` or `walk` because they were selecting
        # files wrongly (on azure walk on path1/path2/ would also select files from path1/path2_v2/ but returning wrong dirs)
        for details in glob_files(self.fs_client, self.make_remote_url(table_dir), "**"):
            file = details["file_name"]
            filepath = self.pathlib.join(table_dir, details["relative_path"])
            # skip INIT files
            if file == INIT_FILE_NAME:
                continue
            for p in prefixes:
                if filepath.startswith(p):
                    result.append(filepath)
                    break
        return result

    def is_storage_initialized(self) -> bool:
        return self.fs_client.exists(self.pathlib.join(self.dataset_path, INIT_FILE_NAME))  # type: ignore[no-any-return]

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        # skip the state table, we create a jsonl file in the complete_load step
        # this does not apply to scenarios where we are using filesystem as staging
        # where we want to load the state the regular way
        if table["name"] == self.schema.state_table_name and not self.config.as_staging_destination:
            return FinalizedLoadJob(file_path)

        table_format = table.get("table_format")
        if table_format in ("delta", "iceberg"):
            # a reference job for a delta table indicates a table chain followup job
            if ReferenceFollowupJobRequest.is_reference_job(file_path):
                if table_format == "delta":
                    import dlt.common.libs.deltalake

                    return DeltaLoadFilesystemJob(file_path)
                elif table_format == "iceberg":
                    import dlt.common.libs.pyiceberg

                    return IcebergLoadFilesystemJob(file_path)

            # otherwise just continue
            return FinalizedLoadJobWithFollowupJobs(file_path)

        cls = (
            FilesystemLoadJobWithFollowup
            if self.config.as_staging_destination
            else FilesystemLoadJob
        )
        return cls(file_path)

    def make_remote_url(self, remote_path: str) -> str:
        """Returns uri to the remote filesystem to which copy the file"""
        if self.is_local_filesystem:
            return self.config.make_file_url(remote_path)
        else:
            return self.config.make_url(remote_path)

    def __enter__(self) -> "FilesystemClient":
        return self

    def __exit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType
    ) -> None:
        pass

    def should_load_data_to_staging_dataset(self, table_name: str) -> bool:
        return False

    def should_truncate_table_before_load(self, table_name: str) -> bool:
        table = self.prepare_load_table(table_name)
        return table["write_disposition"] == "replace" and not table.get("table_format") in (
            "delta",
            "iceberg",
        )  # Delta/Iceberg can do a logical replace

    #
    # state stuff
    #

    def _write_to_json_file(self, filepath: str, data: DictStrAny) -> None:
        dirname = self.pathlib.dirname(filepath)
        if not self.fs_client.isdir(dirname):
            return
        self.fs_client.write_text(filepath, json.dumps(data), encoding="utf-8")

    def _to_path_safe_string(self, s: str) -> str:
        """for base64 strings"""
        return base64.b64decode(s).hex() if s else None

    def _list_dlt_table_files(
        self, table_name: str, pipeline_name: str = None
    ) -> Iterator[Tuple[str, List[str]]]:
        dirname = self.get_table_dir(table_name)
        if not self.fs_client.exists(self.pathlib.join(dirname, INIT_FILE_NAME)):
            raise DestinationUndefinedEntity({"dir": dirname})
        for filepath in self.list_table_files(table_name):
            filename = os.path.splitext(os.path.basename(filepath))[0]
            fileparts = filename.split(FILENAME_SEPARATOR)
            if len(fileparts) != 3:
                continue
            # Filters only if pipeline_name provided
            if pipeline_name is None or fileparts[0] == pipeline_name:
                yield filepath, fileparts

    def _store_load(self, load_id: str) -> None:
        # write entry to load "table"
        # TODO: this is also duplicate across all destinations. DRY this.
        load_data = {
            "load_id": load_id,
            "schema_name": self.schema.name,
            "status": 0,
            "inserted_at": pendulum.now().isoformat(),
            "schema_version_hash": self.schema.version_hash,
        }
        filepath = self.pathlib.join(
            self.dataset_path,
            self.schema.loads_table_name,
            f"{self.schema.name}{FILENAME_SEPARATOR}{load_id}.jsonl",
        )
        self._write_to_json_file(filepath, load_data)

    def complete_load(self, load_id: str) -> None:
        # store current state
        self._store_current_state(load_id)
        self._store_load(load_id)

    #
    # state read/write
    #

    def _get_state_file_name(self, pipeline_name: str, version_hash: str, load_id: str) -> str:
        """gets full path for schema file for a given hash"""
        return self.pathlib.join(  # type: ignore[no-any-return]
            self.get_table_dir(self.schema.state_table_name),
            f"{pipeline_name}{FILENAME_SEPARATOR}{load_id}{FILENAME_SEPARATOR}{self._to_path_safe_string(version_hash)}.jsonl",
        )

    def _cleanup_pipeline_states(self, pipeline_name: str) -> None:
        state_table_files = list(
            self._list_dlt_table_files(self.schema.state_table_name, pipeline_name)
        )

        if len(state_table_files) > self.config.max_state_files:
            # filter and collect a list of state files
            state_file_info: List[Dict[str, Any]] = [
                {
                    "load_id": float(fileparts[1]),  # convert load_id to float for comparison
                    "filepath": filepath,
                }
                for filepath, fileparts in state_table_files
            ]

            # sort state file info by load_id in descending order
            state_file_info.sort(key=lambda x: x["load_id"], reverse=True)

            # keeping only the most recent MAX_STATE_HISTORY files
            files_to_delete = state_file_info[self.config.max_state_files :]

            # delete the old files
            for file_info in files_to_delete:
                self._delete_file(file_info["filepath"])

    def _store_current_state(self, load_id: str) -> None:
        # don't save the state this way when used as staging
        if self.config.as_staging_destination:
            return

        # get state doc from current pipeline
        try:
            pipeline_state_doc = current_load_package()["state"].get("pipeline_state")
        except CurrentLoadPackageStateNotAvailable:
            pipeline_state_doc = None

        if not pipeline_state_doc:
            return

        # get paths
        pipeline_name = pipeline_state_doc["pipeline_name"]
        hash_path = self._get_state_file_name(
            pipeline_name, self.schema.stored_version_hash, load_id
        )

        # write
        self._write_to_json_file(hash_path, cast(DictStrAny, pipeline_state_doc))

        # perform state cleanup only if max_state_files is set to a positive value
        if self.config.max_state_files >= 1:
            self._cleanup_pipeline_states(pipeline_name)

    def get_stored_state(self, pipeline_name: str) -> Optional[StateInfo]:
        # search newest state
        selected_path = None
        newest_load_id = "0"
        for filepath, fileparts in self._list_dlt_table_files(self.schema.state_table_name):
            if fileparts[0] == pipeline_name and fileparts[1] > newest_load_id:
                newest_load_id = fileparts[1]
                selected_path = filepath

        # Load compressed state from destination
        if selected_path:
            state_json: TPipelineStateDoc = json.loads(
                self.fs_client.read_text(selected_path, encoding="utf-8")
            )
            # we had dlt_load_id stored until version 0.5 and since we do not have any version control
            # we always migrate
            if load_id := state_json.pop("dlt_load_id", None):  # type: ignore[typeddict-item]
                state_json[C_DLT_LOAD_ID] = load_id  # type: ignore[literal-required]
            return StateInfo(**state_json)

        return None

    #
    # Schema read/write
    #

    def _get_schema_file_name(self, version_hash: str, load_id: str) -> str:
        """gets full path for schema file for a given hash"""

        return self.pathlib.join(  # type: ignore[no-any-return]
            self.get_table_dir(self.schema.version_table_name),
            f"{self.schema.name}{FILENAME_SEPARATOR}{load_id}{FILENAME_SEPARATOR}{self._to_path_safe_string(version_hash)}.jsonl",
        )

    def _iter_stored_schema_files(self) -> Iterator[Tuple[str, List[str]]]:
        """Iterator over all stored schema files"""
        for filepath, fileparts in self._list_dlt_table_files(self.schema.version_table_name):
            yield filepath, fileparts

    def _get_stored_schema_by_hash_or_newest(
        self, version_hash: str = None, schema_name: str = None
    ) -> Optional[StorageSchemaInfo]:
        """Get the schema by supplied hash, falls back to getting the newest version matching the existing schema name"""
        version_hash = self._to_path_safe_string(version_hash)
        # find newest schema for pipeline or by version hash
        try:
            selected_path = None
            newest_load_id = "0"
            for filepath, fileparts in self._iter_stored_schema_files():
                if (
                    not version_hash
                    and (fileparts[0] == schema_name or (not schema_name))
                    and fileparts[1] > newest_load_id
                ):
                    newest_load_id = fileparts[1]
                    selected_path = filepath
                elif fileparts[2] == version_hash:
                    selected_path = filepath
                    break

            if selected_path:
                info = json.loads(self.fs_client.read_text(selected_path, encoding="utf-8"))
                info["inserted_at"] = ensure_pendulum_datetime(info["inserted_at"])
                return StorageSchemaInfo(**info)
        except DestinationUndefinedEntity:
            # ignore missing table
            pass

        return None

    def _update_schema_in_storage(self, schema: Schema) -> None:
        # get paths
        schema_id = str(time.precise_time())
        filepath = self._get_schema_file_name(schema.stored_version_hash, schema_id)

        # TODO: duplicate of weaviate implementation, should be abstracted out
        version_info = {
            "version_hash": schema.stored_version_hash,
            "schema_name": schema.name,
            "version": schema.version,
            "engine_version": schema.ENGINE_VERSION,
            "inserted_at": pendulum.now(),
            "schema": json.dumps(schema.to_dict()),
        }

        # we always keep tabs on what the current schema is
        self._write_to_json_file(filepath, version_info)

    def get_stored_schema(self, schema_name: str = None) -> Optional[StorageSchemaInfo]:
        """Retrieves newest schema from destination storage"""
        return self._get_stored_schema_by_hash_or_newest(schema_name=schema_name)

    def get_stored_schema_by_hash(self, version_hash: str) -> Optional[StorageSchemaInfo]:
        return self._get_stored_schema_by_hash_or_newest(version_hash)

    def create_table_chain_completed_followup_jobs(
        self,
        table_chain: Sequence[PreparedTableSchema],
        completed_table_chain_jobs: Optional[Sequence[LoadJobInfo]] = None,
    ) -> List[FollowupJobRequest]:
        assert completed_table_chain_jobs is not None
        jobs = super().create_table_chain_completed_followup_jobs(
            table_chain, completed_table_chain_jobs
        )
        if table_chain[0].get("table_format") in ("delta", "iceberg"):
            for table in table_chain:
                table_job_paths = [
                    job.file_path
                    for job in completed_table_chain_jobs
                    if job.job_file_info.table_name == table["name"]
                ]
                if table_job_paths:
                    file_name = FileStorage.get_file_name_from_file_path(table_job_paths[0])
                    jobs.append(ReferenceFollowupJobRequest(file_name, table_job_paths))
                else:
                    # file_name = ParsedLoadJobFileName(table["name"], "empty", 0, "reference").file_name()
                    # TODO: if we implement removal od orphaned rows, we may need to propagate such job without files
                    # to the delta load job
                    pass

        return jobs

    # SupportsOpenTables implementation

    def load_open_table(self, table_format: TTableFormat, table_name: str, **kwargs: Any) -> Any:
        """Locates, loads and returns native table client for table `table_name` in delta or iceberg formats"""

        try:
            prepared_table = self.prepare_load_table(table_name)
        except TableNotFound:
            raise DestinationUndefinedEntity(table_name)
        detected_format = prepared_table.get("table_format")
        if detected_format != table_format:
            raise OpenTableFormatNotSupported(table_format, table_name, detected_format)
        table_location = self.get_open_table_location(table_format, table_name)

        if table_format == "iceberg":
            catalog = self.get_open_table_catalog("iceberg")
            # TODO: move all the fs related code (metadata search, path conversion) to here
            # TODO: if table is not in the catalog, load the static table
            from dlt.common.libs.pyiceberg import evolve_table, NoSuchTableError

            table_id = f"{self.dataset_name}.{table_name}"
            try:
                return evolve_table(
                    catalog=catalog,
                    client=self,
                    table_id=table_id,
                    table_location=table_location,
                    **kwargs,
                )
            except (NoSuchTableError, FileNotFoundError):
                raise DestinationUndefinedEntity(table_name)
        elif table_format == "delta":
            from dlt.common.libs.deltalake import deltalake_storage_options, DeltaTable

            storage_options = deltalake_storage_options(self.config)

            if not DeltaTable.is_deltatable(table_location, storage_options):
                raise DestinationUndefinedEntity(table_name)
            return DeltaTable(table_location, storage_options=storage_options)
        else:
            raise NotImplementedError(
                f"Cannot load tables in {table_format} format in filesystem destination."
            )

    def get_open_table_catalog(self, table_format: TTableFormat, catalog_name: str = None) -> Any:
        """Gets a native catalog for a table `table_name` with format `table_format`

        Returns: currently pyiceberg Catalog is supported
        """
        if table_format != "iceberg":
            raise OpenTableCatalogNotSupported(table_format, "filesystem")

        if self._catalog:
            return self._catalog

        from dlt.common.libs.pyiceberg import get_sql_catalog, IcebergCatalog

        # create in-memory catalog
        catalog: IcebergCatalog
        catalog = self._catalog = get_sql_catalog(
            catalog_name or "default", "sqlite:///:memory:", self.config.credentials
        )

        # create namespace
        catalog.create_namespace(self.dataset_name)

        return catalog

    def get_open_table_location(self, table_format: TTableFormat, table_name: str) -> str:
        """All tables have location, also those in "native" table format."""
        folder = self.get_table_dir(table_name)
        location = self.make_remote_url(folder)
        if self.config.is_local_filesystem and os.name == "nt":
            # pyiceberg cannot deal with windows absolute urls
            location = location.replace("file:///", "file://")
        return location

    def is_open_table(self, table_format: TTableFormat, table_name: str) -> bool:
        if table_name in self.schema.dlt_table_names():
            return False
        try:
            prepared_table = self.prepare_load_table(table_name)
        except TableNotFound:
            return False
        detected_format = prepared_table.get("table_format")
        return table_format == detected_format
