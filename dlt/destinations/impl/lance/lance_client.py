from __future__ import annotations

from copy import copy
from types import TracebackType
from typing import (
    Dict,
    List,
    Any,
    Sequence,
    Set,
    Union,
    Tuple,
    Iterable,
    Type,
    Optional,
    TYPE_CHECKING,
)

import lance
from lance import LanceDataset
from lance.namespace import (
    CreateNamespaceRequest,
    DropNamespaceRequest,
    DropTableRequest,
    ListTablesRequest,
    NamespaceExistsRequest,
    TableExistsRequest,
)
from lance_namespace import LanceNamespace
from lancedb.table import LanceTable, _append_vector_columns
from lancedb.embeddings import EmbeddingFunctionConfig, EmbeddingFunctionRegistry
from lancedb.namespace import LanceNamespaceDBConnection

from dlt.common import json, pendulum, logger
from dlt.common.libs.numpy import numpy
from dlt.common.libs.pyarrow import pyarrow as pa
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.exceptions import (
    DestinationUndefinedEntity,
    DestinationTerminalException,
)
from dlt.common.destination.client import (
    FollowupJobRequest,
    JobClientBase,
    PreparedTableSchema,
    WithStateSync,
    StorageSchemaInfo,
    StateInfo,
    LoadJob,
)
from dlt.common.storages import FileStorage
from dlt.common.storages.load_package import LoadJobInfo
from dlt.common.schema import Schema, TSchemaTables
from dlt.common.schema.typing import (
    C_DLT_LOADS_TABLE_LOAD_ID,
    TTableSchemaColumns,
    TColumnSchema,
    TWriteDisposition,
)
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    get_inherited_table_hint,
    is_nested_table,
)
from dlt.common.storages import ParsedLoadJobFileName
from dlt.destinations.impl.lance.configuration import (
    LanceClientConfiguration,
    LanceNamespaceHandle,
    LanceNamespacePool,
)
from dlt.destinations.impl.lance.exceptions import (
    LanceEmbeddingsConfigurationMissing,
    is_lance_undefined_entity_exception,
    raise_destination_error,
)
from dlt.destinations.impl.lance.jobs import LanceCommitLoadJob, LanceLoadJob
from dlt.destinations.job_impl import (
    FinalizedLoadJobWithFollowupJobs,
    ReferenceFollowupJobRequest,
)
from dlt.destinations.impl.lance.lance_adapter import (
    DEFAULT_REMOVE_ORPHANS,
    VECTORIZE_HINT,
    REMOVE_ORPHANS_HINT,
)
from dlt.common.libs.pyarrow import columns_to_arrow, dlt_column_to_arrow_field
from dlt.destinations.impl.lance.utils import _cast_to_target_types
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient

if TYPE_CHECKING:
    from dlt.destinations.impl.lance.sql_client import LanceSQLClient

    NDArray = numpy.ndarray[Any, Any]
else:
    NDArray = numpy.ndarray


class LanceClient(JobClientBase, WithStateSync, WithSqlClient):
    def __init__(
        self,
        schema: Schema,
        config: LanceClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)
        self.config: LanceClientConfiguration = config
        self.type_mapper = self.capabilities.get_type_mapper()
        self.dataset_name = self.config.normalize_dataset_name(self.schema)
        self.embedding_function = (
            self.config.embeddings.create_embedding_function() if self.config.embeddings else None
        )
        self._namespace_handle: Optional[LanceNamespaceHandle] = None
        self._tables_with_jobs: Set[str] = set()
        self._sql_client: SqlClientBase[Any] = None

    def __enter__(self) -> LanceClient:
        return self

    def __exit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType
    ) -> None:
        if self._namespace_handle is not None:
            self.config.namespace_pool.return_handle(self._namespace_handle)
            self._namespace_handle = None

    @property
    def namespace_handle(self) -> LanceNamespaceHandle:
        """Borrows the shared namespace handle on first access, returned in `__exit__`."""
        if self._namespace_handle is None:
            if self.config.namespace_pool is None:
                # hand-built configs that skipped resolution
                self.config.namespace_pool = LanceNamespacePool(self.config)
            self._namespace_handle = self.config.namespace_pool.borrow()
        return self._namespace_handle

    @property
    def namespace(self) -> LanceNamespace:
        return self.namespace_handle.namespace

    @property
    def sql_client_class(self) -> Type[LanceSQLClient]:  # type: ignore[override]
        from dlt.destinations.impl.lance.sql_client import LanceSQLClient

        return LanceSQLClient

    @property
    def sql_client(self) -> SqlClientBase[Any]:
        if not self._sql_client:
            self._sql_client = self.sql_client_class(self)
        return self._sql_client

    @sql_client.setter
    def sql_client(self, client: SqlClientBase[Any]) -> None:
        self._sql_client = client

    def make_namespace_id(self) -> List[str]:
        """Returns namespace `id` for the dataset. Empty (root namespace) when `dataset_name` is
        not set."""
        return [] if self.dataset_name is None else [self.dataset_name]

    @raise_destination_error
    def list_dataset_namespace_tables(self) -> List[str]:
        return self.namespace.list_tables(ListTablesRequest(id=self.make_namespace_id())).tables

    @raise_destination_error
    def create_dataset_namespace(self) -> None:
        """Creates child namespace for dataset in root namespace. No-op for the root namespace
        (`dataset_name` not set) which always exists."""
        if self.dataset_name is None:
            return
        self.namespace.create_namespace(CreateNamespaceRequest(id=self.make_namespace_id()))

    @raise_destination_error
    def drop_dataset_namespace(self) -> None:
        """Drops dataset namespace after removing all its tables"""
        for table in self.list_dataset_namespace_tables():
            self.namespace.drop_table(DropTableRequest(id=self.make_table_id(table)))
        # for the root namespace (`dataset_name` not set) only the tables are dropped
        if self.dataset_name is not None:
            self.namespace.drop_namespace(DropNamespaceRequest(id=self.make_namespace_id()))

    @raise_destination_error
    def dataset_namespace_exists(self) -> bool:
        """Returns True if child namespace for dataset exists in root namespace."""
        # the root namespace (`dataset_name` not set) always exists
        if self.dataset_name is None:
            return True
        try:
            self.namespace.namespace_exists(NamespaceExistsRequest(id=self.make_namespace_id()))
            return True
        except Exception as e:
            if is_lance_undefined_entity_exception(e, self.config.manifest_enabled):
                return False
            raise

    @raise_destination_error
    def create_table(self, table_name: str, schema: pa.Schema) -> None:
        """Creates empty lance dataset from provided PyArrow schema."""
        lance.write_dataset(
            schema.empty_table(),
            namespace_client=self.namespace,
            table_id=self.make_table_id(table_name),
            storage_options=self.namespace_handle.storage_options,
        )

    @raise_destination_error
    def drop_table(self, table_name: str) -> None:
        """Drops table from lance dataset namespace."""
        self.namespace.drop_table(DropTableRequest(id=self.make_table_id(table_name)))

    @raise_destination_error
    def table_exists(self, table_name: str) -> bool:
        try:
            self.namespace.table_exists(TableExistsRequest(id=self.make_table_id(table_name)))
            return True
        except Exception as e:
            if is_lance_undefined_entity_exception(e, self.config.manifest_enabled):
                return False
            raise

    def make_table_id(self, table_name: str) -> List[str]:
        """Returns namespace `table_id` for given table name."""
        return [*self.make_namespace_id(), table_name]

    def get_table_schema(self, table_name: str) -> pa.Schema:
        return self.open_lance_dataset(table_name, branch_name=self.config.branch_name).schema

    def get_table_uri(self, table_name: str) -> str:
        # we don't pass branch here — `uri` always returns base URI
        return self.open_lance_dataset(table_name).uri

    def drop_tables(self, *tables: str, delete_schema: bool = True) -> None:
        """Drops tables from lance dataset namespace and optionally deletes the stored schema."""
        for table_name in tables:
            if self.table_exists(table_name):
                self.drop_table(table_name)
        if delete_schema:
            self._delete_schema_in_storage(self.schema)

    @raise_destination_error
    def _delete_schema_in_storage(self, schema: Schema) -> None:
        """Deletes all stored versions with the same name as `schema`. No-op if table is missing."""
        if not self.table_exists(self.schema.version_table_name):
            return
        col = self.schema.naming.normalize_identifier("schema_name")
        ds = self.open_lance_dataset(
            self.schema.version_table_name, branch_name=self.config.branch_name
        )
        ds.delete(f'`{col}` = "{schema.name}"')

    def drop_storage(self) -> None:
        """Drops dataset namespace and all its tables."""
        if self.dataset_namespace_exists():
            self.drop_dataset_namespace()

    @raise_destination_error
    def truncate_table(self, table_name: str) -> None:
        """Truncates table by deleting all rows in active branch."""
        self.open_lance_dataset(table_name, branch_name=self.config.branch_name).delete("true")

    @raise_destination_error
    def create_branch_if_not_exists(self, table_name: str, branch_name: str) -> None:
        ds = self.open_lance_dataset(table_name)
        if branch_name not in ds.branches.list():
            ds.create_branch(branch_name)

    @raise_destination_error
    def open_lance_dataset(
        self,
        table_name: str,
        branch_name: Optional[str] = None,
        version_number: Optional[int] = None,
    ) -> LanceDataset:
        """Returns lance dataset for given table name.

        Args:
            table_name (str): Name of table to open dataset for.
            branch_name (Optional[str]): Branch to check out. Uses main branch if `None`.
            version_number (Optional[int]): Dataset version to check out. Uses latest if `None`.

        Returns:
            LanceDataset: The dataset checked out at the specified branch and version.
        """
        return lance.dataset(
            namespace_client=self.namespace,
            table_id=self.make_table_id(table_name),
            storage_options=self.namespace_handle.storage_options,
            session=self.namespace_handle.session,
        ).checkout_version((branch_name, version_number))

    def open_lancedb_table(self, table_name: str) -> LanceTable:
        """Returns LanceDB table for given table name.

        This provides access to LanceDB-specific features like vector search.
        """
        # NOTE: the pooled `lance.Session` cannot be shared here, lancedb requires its own
        # session type
        db = LanceNamespaceDBConnection(
            self.namespace, storage_options=self.namespace_handle.storage_options
        )
        # storage options must be repeated per call: connection-level options are not
        # applied when the namespace connection opens the table dataset
        table = db.open_table(
            table_name,
            namespace_path=self.make_namespace_id(),
            storage_options=self.namespace_handle.storage_options,
        )
        # lancedb bug: `LanceTable._dataset_uri` reads the connection `_uri` which namespace
        # connections never set, unlike `_dataset_path` which honors the table location.
        # seed the cached property with the real table uri
        table.__dict__["_dataset_uri"] = self.get_table_uri(table_name)
        return table

    @raise_destination_error
    def _write_records(
        self,
        ds: LanceDataset,
        records: Union[pa.RecordBatchReader, List[Dict[str, Any]]],
        write_disposition: Optional[TWriteDisposition] = "append",
        merge_key: Optional[str] = None,
        when_not_matched_by_source_delete_expr: Optional[str] = None,
    ) -> None:
        if write_disposition in ("append", "skip", "replace"):
            ds.insert(records)
        elif write_disposition == "merge":
            merge_builder = (
                ds.merge_insert(merge_key).when_matched_update_all().when_not_matched_insert_all()
            )
            if when_not_matched_by_source_delete_expr:
                merge_builder = merge_builder.when_not_matched_by_source_delete(
                    when_not_matched_by_source_delete_expr
                )
            merge_builder.execute(records)

    def prepare_records(
        self, records: pa.RecordBatchReader, schema: pa.Schema
    ) -> pa.RecordBatchReader:
        """Adds embedding columns and casts records to the target dataset schema."""
        records = _append_vector_columns(records, schema=schema)
        return _cast_to_target_types(records, schema)

    def write_records(
        self,
        records: Union[pa.RecordBatchReader, List[Dict[str, Any]]],
        table_name: str,
        /,
        *,
        branch_name: Optional[str] = None,
        write_disposition: Optional[TWriteDisposition] = "append",
        merge_key: Optional[str] = None,
        when_not_matched_by_source_delete_expr: Optional[str] = None,
    ) -> None:
        """Inserts records into Lance dataset with automatic embedding computation."""
        ds = self.open_lance_dataset(table_name, branch_name=branch_name)

        if isinstance(records, pa.RecordBatchReader):
            records = self.prepare_records(records, ds.schema)

        self._write_records(
            ds,
            records,
            write_disposition=write_disposition,
            merge_key=merge_key,
            when_not_matched_by_source_delete_expr=when_not_matched_by_source_delete_expr,
        )

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        if not self.is_storage_initialized():
            self.create_dataset_namespace()
        elif truncate_tables:
            for table_name in truncate_tables:
                if (
                    table_name in self._tables_with_jobs
                    and self.prepare_load_table(table_name)["write_disposition"] == "replace"
                ):
                    # replaced atomically by the single Overwrite commit of the table chain.
                    # append tables truncated via refresh still need the truncation
                    continue
                if not self.table_exists(table_name):
                    continue
                self.truncate_table(table_name)

    def is_storage_initialized(self) -> bool:
        return self.dataset_namespace_exists()

    def verify_schema(
        self, only_tables: Iterable[str] = None, new_jobs: Iterable[ParsedLoadJobFileName] = None
    ) -> List[PreparedTableSchema]:
        # tables receiving data files are not truncated in `initialize_storage`
        self._tables_with_jobs = {job.table_name for job in new_jobs or ()}
        loaded_tables = super().verify_schema(only_tables, new_jobs)

        for load_table in loaded_tables:
            # Skip nested tables as they inherit behavior from parent tables
            if is_nested_table(load_table):
                continue

            # Check if this table has orphan removal enabled (either explicitly or via merge strategy)
            remove_orphans = load_table[REMOVE_ORPHANS_HINT]  # type: ignore[literal-required]
            merge_keys = get_columns_names_with_prop(load_table, "merge_key")

            # Validate merge key constraints when orphan removal is enabled
            if remove_orphans and len(merge_keys) > 1:
                raise DestinationTerminalException(
                    "Multiple merge keys are not supported when LanceDB orphan removal is"
                    f" enabled: {merge_keys}"
                )

            # embeddings configuration must be provided if embed columns exist
            if not self.config.embeddings:
                if embed_columns := get_columns_names_with_prop(load_table, VECTORIZE_HINT):
                    raise LanceEmbeddingsConfigurationMissing(load_table["name"], embed_columns)

        return loaded_tables

    def update_stored_schema(
        self,
        only_tables: Iterable[str] = None,
        expected_update: TSchemaTables = None,
        force: bool = False,
    ) -> Optional[TSchemaTables]:
        super().update_stored_schema(only_tables, expected_update, force)
        try:
            schema_info = self.get_stored_schema_by_hash(self.schema.stored_version_hash)
        except DestinationUndefinedEntity:
            schema_info = None

        applied_update: TSchemaTables = {}
        if schema_info is None or force:
            logger.info(
                f"Schema with hash {self.schema.stored_version_hash} "
                "not found in the storage (or update enforced). upgrading"
            )
            applied_update = self._execute_schema_update(
                only_tables, store_schema=schema_info is None
            )
        else:
            logger.debug(
                f"Schema with hash {self.schema.stored_version_hash} "
                f"inserted at {schema_info.inserted_at} found "
                "in storage, no upgrade required"
            )
        return applied_update

    def prepare_load_table(self, table_name: str) -> PreparedTableSchema:
        table = super().prepare_load_table(table_name)

        # inherit missing hint from parent table, or use default
        if REMOVE_ORPHANS_HINT not in table:
            inherited_hint = get_inherited_table_hint(
                self.schema.tables, table_name, REMOVE_ORPHANS_HINT, allow_none=True
            )
            table[REMOVE_ORPHANS_HINT] = (  # type: ignore[literal-required]
                inherited_hint if inherited_hint is not None else DEFAULT_REMOVE_ORPHANS
            )

        return table

    def get_storage_table(self, table_name: str) -> Tuple[bool, TTableSchemaColumns]:
        table_schema: TTableSchemaColumns = {}

        try:
            arrow_schema = self.get_table_schema(table_name)
        except DestinationUndefinedEntity:
            # `open_lance_dataset` already mapped a missing table/namespace to this exception
            return False, table_schema
        except Exception as e:
            if is_lance_undefined_entity_exception(e, self.config.manifest_enabled):
                return False, table_schema
            raise

        for field in arrow_schema:
            name = field.name
            table_schema[name] = {
                "name": name,
                **self.type_mapper.from_destination_type(field.type, None, None),
            }
        return True, table_schema

    def get_storage_tables(
        self, table_names: Iterable[str]
    ) -> Iterable[Tuple[bool, TTableSchemaColumns]]:
        for table_name in table_names:
            # mypy fails to resolve table_schema; ty succeeds
            table_exists, table_schema = self.get_storage_table(table_name)
            yield table_name, table_schema  # type: ignore[misc]

    def make_arrow_table_schema(self, table_name: str) -> pa.Schema:
        """Creates a PyArrow schema for a table, including embedding metadata if configured."""
        columns = self.schema.get_table_columns(table_name)
        arrow_schema = columns_to_arrow(columns, self.capabilities)

        embedding_fields = None
        vector_column = None
        if self.config.embeddings and table_name not in self.schema.dlt_table_names():
            embedding_fields = get_columns_names_with_prop(
                self.schema.get_table(table_name=table_name), VECTORIZE_HINT
            )
            vector_column = self.config.embeddings.vector_column

        if embedding_fields:
            if vector_column not in columns:
                vec_size = self.embedding_function.ndims()
                arrow_schema = arrow_schema.append(
                    pa.field(vector_column, pa.list_(pa.float32(), vec_size))
                )
            else:
                logger.info(
                    f"Lance table `{table_name}` in schema `{self.schema.name}` contains user"
                    f" supplied vector column `{vector_column}`. Arrow column type must fit the"
                    " vector dimensions."
                )

        metadata: Dict[str, bytes] = {}
        if self.embedding_function and embedding_fields:
            registry = EmbeddingFunctionRegistry.get_instance()
            configs = [
                EmbeddingFunctionConfig(
                    source_column=source_column,
                    vector_column=vector_column,
                    function=self.embedding_function,
                )
                for source_column in embedding_fields
            ]
            metadata = registry.get_table_metadata(configs) or {}
            arrow_schema = arrow_schema.with_metadata(metadata)

        return arrow_schema

    @raise_destination_error
    def add_null_columns_to_table(self, table_name: str, new_columns: List[TColumnSchema]) -> None:
        new_fields = [dlt_column_to_arrow_field(col, self.capabilities) for col in new_columns]
        self.open_lance_dataset(table_name, branch_name=self.config.branch_name).add_columns(
            new_fields
        )

    def _execute_schema_update(
        self, only_tables: Iterable[str], store_schema: bool = True
    ) -> TSchemaTables:
        applied_update: TSchemaTables = {}
        for table_name in only_tables or self.schema.tables:
            table_exists = self.table_exists(table_name)

            # create new table if it doesn't exist
            if not table_exists:
                self.create_table(table_name, self.make_arrow_table_schema(table_name))

            # create branch if needed before diffing: a new branch forks from main and inherits
            # its schema, so columns must be read from the branch *after* it exists
            if branch_name := self.config.branch_name:
                self.create_branch_if_not_exists(table_name, branch_name)

            # diff against the destination (branch, if configured): for a new table all columns
            # are new
            existing_columns = self.get_storage_table(table_name)[1] if table_exists else {}
            new_columns = self.schema.get_new_table_columns(
                table_name,
                existing_columns,
                self.capabilities.generates_case_sensitive_identifiers(),
            )

            # add new columns to existing table (on the branch if configured)
            if table_exists and new_columns:
                self.add_null_columns_to_table(table_name, new_columns)

            # record the migration applied to this table (new table or added columns)
            if new_columns:
                partial_table = copy(self.prepare_load_table(table_name))
                partial_table["columns"] = {c["name"]: c for c in new_columns}
                applied_update[table_name] = partial_table

        # skip writing the version row when the schema is already stored (enforced update)
        if store_schema:
            self._update_schema_in_storage(self.schema)
        return applied_update

    def get_stored_state(self, pipeline_name: str) -> Optional[StateInfo]:
        """Retrieves the latest completed state for a pipeline."""

        # normalize column names needed for query / join / sort
        p_load_id = self.schema.naming.normalize_identifier(C_DLT_LOADS_TABLE_LOAD_ID)
        p_dlt_load_id = self.schema.naming.normalize_identifier(
            self.schema.data_item_normalizer.c_dlt_load_id  # type: ignore[attr-defined]
        )
        p_pipeline_name = self.schema.naming.normalize_identifier("pipeline_name")
        p_status = self.schema.naming.normalize_identifier("status")

        # Read the tables into memory as Arrow tables, with pushdown predicates, so we pull as little
        # data into memory as possible.
        state_ds = self.open_lance_dataset(
            self.schema.state_table_name, branch_name=self.config.branch_name
        )
        loads_ds = self.open_lance_dataset(
            self.schema.loads_table_name, branch_name=self.config.branch_name
        )
        state_table = state_ds.scanner(
            filter=f"`{p_pipeline_name}` = '{pipeline_name}'", prefilter=True
        ).to_table()
        loads_table = loads_ds.scanner(filter=f"`{p_status}` = 0", prefilter=True).to_table()

        # Join arrow tables in-memory.
        joined_table: pa.Table = state_table.join(
            loads_table, keys=p_dlt_load_id, right_keys=p_load_id, join_type="inner"
        ).sort_by([(p_dlt_load_id, "descending")])

        if joined_table.num_rows == 0:
            return None

        row = joined_table.take([0]).to_pylist()[0]
        return StateInfo.from_normalized_mapping(row, self.schema.naming)

    def _get_latest_schema(self, filter_: Optional[str] = None) -> Optional[StorageSchemaInfo]:
        try:
            ds = self.open_lance_dataset(
                self.schema.version_table_name, branch_name=self.config.branch_name
            )
        except DestinationUndefinedEntity:
            # version table not created yet (empty storage)
            return None
        table = ds.scanner(filter=filter_, prefilter=True).to_table() if filter_ else ds.to_table()
        rows = table.to_pylist()
        try:
            row = max(rows, key=lambda x: x[self.schema.naming.normalize_identifier("inserted_at")])
        except ValueError:
            return None
        return StorageSchemaInfo.from_normalized_mapping(row, self.schema.naming)

    def get_stored_schema_by_hash(self, schema_hash: str) -> Optional[StorageSchemaInfo]:
        col = self.schema.naming.normalize_identifier("version_hash")
        return self._get_latest_schema(filter_=f'`{col}` = "{schema_hash}"')

    def get_stored_schema(self, schema_name: str = None) -> Optional[StorageSchemaInfo]:
        """Retrieves newest schema from destination storage."""
        if not self.table_exists(self.schema.version_table_name):
            return None
        if schema_name:
            col = self.schema.naming.normalize_identifier("schema_name")
            return self._get_latest_schema(filter_=f'`{col}` = "{schema_name}"')
        return self._get_latest_schema()

    def _update_schema_in_storage(self, schema: Schema) -> None:
        record = {
            "version": schema.version,
            "engine_version": schema.ENGINE_VERSION,
            "inserted_at": pendulum.now(),
            "schema_name": schema.name,
            "version_hash": schema.stored_version_hash,
            "schema": json.dumps(schema.to_dict()),
        }
        records = [{self.schema.naming.normalize_identifier(k): v for k, v in record.items()}]
        write_disposition = self.schema.get_table(self.schema.version_table_name).get(
            "write_disposition"
        )
        self.write_records(
            records,
            self.schema.version_table_name,
            branch_name=self.config.branch_name,
            write_disposition=write_disposition,
        )

    def complete_load(self, load_id: str) -> None:
        record = {
            C_DLT_LOADS_TABLE_LOAD_ID: load_id,
            "schema_name": self.schema.name,
            "status": 0,
            "inserted_at": pendulum.now(),
            "schema_version_hash": self.schema.version_hash,
        }
        records = [{self.schema.naming.normalize_identifier(k): v for k, v in record.items()}]
        write_disposition = self.schema.get_table(self.schema.loads_table_name).get(
            "write_disposition"
        )
        self.write_records(
            records,
            self.schema.loads_table_name,
            branch_name=self.config.branch_name,
            write_disposition=write_disposition,
        )

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        if ReferenceFollowupJobRequest.is_reference_job(file_path):
            return LanceCommitLoadJob(file_path, table, restore=restore)
        if table["write_disposition"] == "merge":
            # all job files of the table merge in one commit in `LanceCommitLoadJob`
            return FinalizedLoadJobWithFollowupJobs(file_path)
        return LanceLoadJob(file_path, table)

    def create_table_chain_completed_followup_jobs(
        self,
        table_chain: Sequence[PreparedTableSchema],
        completed_table_chain_jobs: Optional[Sequence[LoadJobInfo]] = None,
    ) -> List[FollowupJobRequest]:
        assert completed_table_chain_jobs is not None
        jobs = super().create_table_chain_completed_followup_jobs(
            table_chain, completed_table_chain_jobs
        )
        # one commit job per table over all its completed job files
        for table in table_chain:
            file_paths = [
                job.file_path
                for job in completed_table_chain_jobs
                if job.job_file_info.table_name == table["name"]
            ]
            if file_paths:
                file_name = FileStorage.get_file_name_from_file_path(file_paths[0])
                jobs.append(ReferenceFollowupJobRequest(file_name, file_paths))
        return jobs
