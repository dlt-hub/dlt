import posixpath
import os
import base64

from types import TracebackType
from typing import Dict, List, Type, Iterable, Iterator, Optional, Tuple, Sequence, cast
from fsspec import AbstractFileSystem
from contextlib import contextmanager

import dlt
from dlt.common import logger, time, json, pendulum
from dlt.common.metrics import LoadJobMetrics
from dlt.common.storages.fsspec_filesystem import glob_files
from dlt.common.typing import DictStrAny
from dlt.common.schema import Schema, TSchemaTables, TTableSchema
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.common.storages import FileStorage, fsspec_from_config
from dlt.common.storages.load_package import (
    LoadJobInfo,
    TPipelineStateDoc,
    load_package as current_load_package,
)
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    FollowupJobRequest,
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
from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.destinations.job_impl import (
    ReferenceFollowupJobRequest,
    FinalizedLoadJob,
    FinalizedLoadJobWithFollowupJobs,
)
from dlt.destinations.impl.filesystem.configuration import FilesystemDestinationClientConfiguration
from dlt.destinations import path_utils
from dlt.destinations.fs_client import FSClientBase

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
        destination_file_name = path_utils.create_path(
            self._job_client.config.layout,
            self._file_name,
            self._job_client.schema.name,
            self._load_id,
            current_datetime=self._job_client.config.current_datetime,
            load_package_timestamp=dlt.current.load_package()["state"]["created_at"],
            extra_placeholders=self._job_client.config.extra_placeholders,
        )
        # pick local filesystem pathlib or posix for buckets
        pathlib = os.path if self.__is_local_filesystem else posixpath
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


class DeltaLoadFilesystemJob(FilesystemLoadJob):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path=file_path)

        # create Arrow dataset from Parquet files
        from dlt.common.libs.pyarrow import pyarrow as pa

        self.file_paths = ReferenceFollowupJobRequest.resolve_references(self._file_path)
        self.arrow_ds = pa.dataset.dataset(self.file_paths)

    def make_remote_path(self) -> str:
        # remote path is table dir - delta will create its file structure inside it
        return self._job_client.get_table_dir(self.load_table_name)

    def run(self) -> None:
        logger.info(f"Will copy file(s) {self.file_paths} to delta table {self.make_remote_url()}")

        from dlt.common.libs.deltalake import write_delta_table, merge_delta_table

        # explicitly check if there is data
        # (https://github.com/delta-io/delta-rs/issues/2686)
        if self.arrow_ds.head(1).num_rows == 0:
            self._create_or_evolve_delta_table()
            return

        with self.arrow_ds.scanner().to_reader() as arrow_rbr:  # RecordBatchReader
            if self._load_table["write_disposition"] == "merge" and self._delta_table is not None:
                assert self._load_table["x-merge-strategy"] in self._job_client.capabilities.supported_merge_strategies  # type: ignore[typeddict-item]
                merge_delta_table(
                    table=self._delta_table,
                    data=arrow_rbr,
                    schema=self._load_table,
                )
            else:
                write_delta_table(
                    table_or_uri=(
                        self.make_remote_url() if self._delta_table is None else self._delta_table
                    ),
                    data=arrow_rbr,
                    write_disposition=self._load_table["write_disposition"],
                    partition_by=self._partition_columns,
                    storage_options=self._storage_options,
                )

    @property
    def _storage_options(self) -> Dict[str, str]:
        from dlt.common.libs.deltalake import _deltalake_storage_options

        return _deltalake_storage_options(self._job_client.config)

    @property
    def _delta_table(self) -> Optional["DeltaTable"]:  # type: ignore[name-defined] # noqa: F821
        from dlt.common.libs.deltalake import try_get_deltatable

        return try_get_deltatable(self.make_remote_url(), storage_options=self._storage_options)

    @property
    def _partition_columns(self) -> List[str]:
        return get_columns_names_with_prop(self._load_table, "partition")

    def _create_or_evolve_delta_table(self) -> None:
        from dlt.common.libs.deltalake import (
            DeltaTable,
            ensure_delta_compatible_arrow_schema,
            _evolve_delta_table_schema,
        )

        if self._delta_table is None:
            DeltaTable.create(
                table_uri=self.make_remote_url(),
                schema=ensure_delta_compatible_arrow_schema(self.arrow_ds.schema),
                mode="overwrite",
                partition_by=self._partition_columns,
                storage_options=self._storage_options,
            )
        else:
            _evolve_delta_table_schema(self._delta_table, self.arrow_ds.schema)


class FilesystemLoadJobWithFollowup(HasFollowupJobs, FilesystemLoadJob):
    def create_followup_jobs(self, final_state: TLoadJobState) -> List[FollowupJobRequest]:
        jobs = super().create_followup_jobs(final_state)
        if self._load_table.get("table_format") == "delta":
            # delta table jobs only require table chain followup jobs
            pass
        elif final_state == "completed":
            ref_job = ReferenceFollowupJobRequest(
                original_file_name=self.file_name(),
                remote_paths=[self._job_client.make_remote_url(self.make_remote_path())],
            )
            jobs.append(ref_job)
        return jobs


class FilesystemClient(FSClientBase, JobClientBase, WithStagingDataset, WithStateSync):
    """filesystem client storing jobs in memory"""

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
            self.dataset_name = self.schema.naming.normalize_table_identifier(
                current_dataset_name + "_staging"
            )
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

    def truncate_tables(self, table_names: List[str]) -> None:
        """Truncate a set of regular tables with given `table_names`"""
        table_dirs = set(self.get_table_dirs(table_names))
        table_prefixes = [self.get_table_prefix(t) for t in table_names]
        for table_dir in table_dirs:
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
        if not self.config.as_staging:
            self._store_current_schema()

        # we assume that expected_update == applied_update so table schemas in dest were not
        # externally changed
        return applied_update

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
        return self.list_files_with_prefixes(table_dir, [table_prefix])

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
        self, table: TTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        # skip the state table, we create a jsonl file in the complete_load step
        # this does not apply to scenarios where we are using filesystem as staging
        # where we want to load the state the regular way
        if table["name"] == self.schema.state_table_name and not self.config.as_staging:
            return FinalizedLoadJob(file_path)
        if table.get("table_format") == "delta":
            import dlt.common.libs.deltalake  # assert dependencies are installed

            # a reference job for a delta table indicates a table chain followup job
            if ReferenceFollowupJobRequest.is_reference_job(file_path):
                return DeltaLoadFilesystemJob(file_path)
            # otherwise just continue
            return FinalizedLoadJobWithFollowupJobs(file_path)

        cls = FilesystemLoadJobWithFollowup if self.config.as_staging else FilesystemLoadJob
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

    def should_load_data_to_staging_dataset(self, table: TTableSchema) -> bool:
        return False

    def should_truncate_table_before_load(self, table: TTableSchema) -> bool:
        return (
            table["write_disposition"] == "replace"
            and not table.get("table_format") == "delta"  # Delta can do a logical replace
        )

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

    def _list_dlt_table_files(self, table_name: str) -> Iterator[Tuple[str, List[str]]]:
        dirname = self.get_table_dir(table_name)
        if not self.fs_client.exists(self.pathlib.join(dirname, INIT_FILE_NAME)):
            raise DestinationUndefinedEntity({"dir": dirname})
        for filepath in self.list_table_files(table_name):
            filename = os.path.splitext(os.path.basename(filepath))[0]
            fileparts = filename.split(FILENAME_SEPARATOR)
            if len(fileparts) != 3:
                continue
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

    def _store_current_state(self, load_id: str) -> None:
        # don't save the state this way when used as staging
        if self.config.as_staging:
            return

        # get state doc from current pipeline
        pipeline_state_doc = current_load_package()["state"].get("pipeline_state")
        if not pipeline_state_doc:
            return

        # get paths
        pipeline_name = pipeline_state_doc["pipeline_name"]
        hash_path = self._get_state_file_name(
            pipeline_name, self.schema.stored_version_hash, load_id
        )

        # write
        self._write_to_json_file(hash_path, cast(DictStrAny, pipeline_state_doc))

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
                state_json["_dlt_load_id"] = load_id
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
        self, version_hash: str = None
    ) -> Optional[StorageSchemaInfo]:
        """Get the schema by supplied hash, falls back to getting the newest version matching the existing schema name"""
        version_hash = self._to_path_safe_string(version_hash)
        # find newest schema for pipeline or by version hash
        selected_path = None
        newest_load_id = "0"
        for filepath, fileparts in self._iter_stored_schema_files():
            if (
                not version_hash
                and fileparts[0] == self.schema.name
                and fileparts[1] > newest_load_id
            ):
                newest_load_id = fileparts[1]
                selected_path = filepath
            elif fileparts[2] == version_hash:
                selected_path = filepath
                break

        if selected_path:
            return StorageSchemaInfo(
                **json.loads(self.fs_client.read_text(selected_path, encoding="utf-8"))
            )

        return None

    def _store_current_schema(self) -> None:
        # check if schema with hash exists
        current_hash = self.schema.stored_version_hash
        if self._get_stored_schema_by_hash_or_newest(current_hash):
            return

        # get paths
        schema_id = str(time.precise_time())
        filepath = self._get_schema_file_name(self.schema.stored_version_hash, schema_id)

        # TODO: duplicate of weaviate implementation, should be abstracted out
        version_info = {
            "version_hash": self.schema.stored_version_hash,
            "schema_name": self.schema.name,
            "version": self.schema.version,
            "engine_version": self.schema.ENGINE_VERSION,
            "inserted_at": pendulum.now(),
            "schema": json.dumps(self.schema.to_dict()),
        }

        # we always keep tabs on what the current schema is
        self._write_to_json_file(filepath, version_info)

    def get_stored_schema(self) -> Optional[StorageSchemaInfo]:
        """Retrieves newest schema from destination storage"""
        return self._get_stored_schema_by_hash_or_newest()

    def get_stored_schema_by_hash(self, version_hash: str) -> Optional[StorageSchemaInfo]:
        return self._get_stored_schema_by_hash_or_newest(version_hash)

    def create_table_chain_completed_followup_jobs(
        self,
        table_chain: Sequence[TTableSchema],
        completed_table_chain_jobs: Optional[Sequence[LoadJobInfo]] = None,
    ) -> List[FollowupJobRequest]:
        assert completed_table_chain_jobs is not None
        jobs = super().create_table_chain_completed_followup_jobs(
            table_chain, completed_table_chain_jobs
        )
        if table_chain[0].get("table_format") == "delta":
            for table in table_chain:
                table_job_paths = [
                    job.file_path
                    for job in completed_table_chain_jobs
                    if job.job_file_info.table_name == table["name"]
                ]
                file_name = FileStorage.get_file_name_from_file_path(table_job_paths[0])
                jobs.append(ReferenceFollowupJobRequest(file_name, table_job_paths))
        return jobs
