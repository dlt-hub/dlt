from copy import copy
from functools import partial
import logging
from types import TracebackType
from typing import (
    Any,
    Callable,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TYPE_CHECKING,
    cast,
)

import lancedb
import lancedb.table
from lancedb.common import DATA
from lancedb.embeddings import TextEmbeddingFunction
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential

from dlt.common import json, pendulum, logger
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.utils import resolve_merge_strategy
from dlt.common.destination.exceptions import (
    DestinationUndefinedEntity,
    DestinationTerminalException,
)
from dlt.common.destination.client import (
    JobClientBase,
    PreparedTableSchema,
    WithStateSync,
    StorageSchemaInfo,
    StateInfo,
    LoadJob,
    FollowupJobRequest,
)
from dlt.common.libs.pyarrow import pyarrow as pa
from dlt.common.schema import Schema, TSchemaTables
from dlt.common.schema.typing import (
    C_DLT_LOADS_TABLE_LOAD_ID,
    TLoaderMergeStrategy,
    TTableSchemaColumns,
    TColumnSchema,
    TTableSchema,
    TWriteDisposition,
)
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    is_nested_table,
)
from dlt.common.storages import FileStorage, LoadJobInfo, ParsedLoadJobFileName
from dlt.destinations.impl.lancedb.configuration import (
    LanceDBClientConfiguration,
)
from dlt.destinations.impl.lance.exceptions import LanceEmbeddingsConfigurationMissing
from dlt.destinations.impl.lancedb.exceptions import (
    LanceDBCommitTagNotApplied,
    lancedb_error,
)
from dlt.destinations.impl.lancedb.jobs import LanceDBLoadJob, LanceDBRemoveOrphansJob
from dlt.destinations.impl.lance.lance_adapter import (
    DEFAULT_REMOVE_ORPHANS,
    REMOVE_ORPHANS_HINT,
    VECTORIZE_HINT,
)
from dlt.destinations.impl.lancedb.schema import (
    add_vector_column,
    make_arrow_table_schema,
    TArrowSchema,
    TArrowField,
)
from dlt.destinations.impl.lancedb.type_mapper import LanceDBTypeMapper
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient

if TYPE_CHECKING:
    from dlt.destinations.impl.lancedb.sql_client import LanceDBSqlClient

ON_BAD_VECTORS = "null"
"""Text that cannot be embedded lands with a null vector, as in the `lance` destination."""


class LanceDBClient(JobClientBase, WithStateSync, WithSqlClient):
    model_func: Optional[TextEmbeddingFunction]
    """The embedder callback used for each chunk, `None` when embeddings are not configured."""
    dataset_name: str

    def __init__(
        self,
        schema: Schema,
        config: LanceDBClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)
        self.config: LanceDBClientConfiguration = config
        self.type_mapper = cast(LanceDBTypeMapper, self.capabilities.get_type_mapper())
        # the dataset is a database of the cluster, so its tables live in that database's root
        self.dataset_name = self.config.normalize_dataset_name(self.schema)
        # loading always reads the latest version, whatever the configured staleness
        self.db_client = self.config.credentials.get_conn(
            self.dataset_name, read_consistency_interval_seconds=0
        )
        self._sql_client: SqlClientBase[Any] = None

        self.model_func = (
            self.config.embeddings.create_embedding_function() if self.config.embeddings else None
        )

    def __enter__(self) -> "LanceDBClient":
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        if self._sql_client:
            self._sql_client.close_connection()

    @property
    def sql_client_class(self) -> Type["LanceDBSqlClient"]:  # type: ignore[override]
        from dlt.destinations.impl.lancedb.sql_client import LanceDBSqlClient

        return LanceDBSqlClient

    @property
    def sql_client(self) -> SqlClientBase[Any]:
        if not self._sql_client:
            self._sql_client = self.sql_client_class(
                self.dataset_name,
                self.config.normalize_staging_dataset_name(self.schema),
                self.config.credentials,
                self.capabilities,
            )
        return self._sql_client

    @sql_client.setter
    def sql_client(self, client: SqlClientBase[Any]) -> None:
        self._sql_client = client

    @staticmethod
    def make_namespace_path() -> List[str]:
        """Returns the root namespace, which holds every table of the dataset."""
        # TODO: when namespaces are fully implemented in flightSQL we can promote this to instance method
        # and enable namespaces again
        return []

    @staticmethod
    def _list_all_pages(list_page: Callable[[Optional[str]], Any], items_attr: str) -> List[str]:
        """Collects `items_attr` of all pages returned by a paginated listing call."""
        items: List[str] = []
        page_token: Optional[str] = None
        while True:
            response = list_page(page_token)
            items.extend(getattr(response, items_attr))
            if not (page_token := response.page_token):
                return items

    @lancedb_error
    def list_table_names(self) -> List[str]:
        """Lists all tables in the dataset namespace."""
        return self._list_all_pages(
            partial(self.db_client.list_tables, self.make_namespace_path()), "tables"
        )

    @lancedb_error
    def list_namespace_names(self) -> List[str]:
        """Lists all namespaces of the database of the dataset."""
        return self._list_all_pages(partial(self.db_client.list_namespaces, []), "namespaces")

    @property
    def sentinel_namespace_path(self) -> List[str]:
        return [self.config.dataset_sentinel_namespace_name]

    @lancedb_error
    def create_dataset(self) -> None:
        """Creates the dataset by creating its sentinel namespace, which creates the database."""
        self.db_client.create_namespace(self.sentinel_namespace_path)

    @lancedb_error
    def drop_dataset(self) -> None:
        """Drops the tables of the dataset and the sentinel that records it as created."""
        existing_tables = self.list_table_names()
        if self.config.credentials.database:
            # a configured database is shared by every dataset and can hold tables of a foreign
            # dataset, so only destination tables that materialize a schema table can be dropped
            to_drop = [name for name in self.schema.tables if name in existing_tables]
            logger.warning(
                "A configured database can hold a foreign dataset, so `dlt` removed only the"
                f" destination tables of the current schema ({len(to_drop)} of"
                f" {len(existing_tables)}) from database `{self.dataset_name}`."
            )
        else:
            to_drop = existing_tables
        for table_name in to_drop:
            self.db_client.drop_table(table_name, namespace_path=self.make_namespace_path())
        # a namespace that still holds tables cannot be dropped, so the sentinel goes last
        if self.config.dataset_sentinel_namespace_name in self.list_namespace_names():
            self.db_client.drop_namespace(self.sentinel_namespace_path)

    def dataset_exists(self) -> bool:
        """Returns True if the sentinel records the dataset as created."""
        return self.config.dataset_sentinel_namespace_name in self.list_namespace_names()

    @lancedb_error
    def create_table(
        self, table_name: str, schema: TArrowSchema, mode: str = "create"
    ) -> "lancedb.table.Table":
        """Creates an empty table in the dataset namespace from the provided PyArrow schema.

        Args:
            table_name: The name of the table to create.
            schema: The table schema to create.
            mode (str): `"create"` raises if the table already exists, `"overwrite"` replaces it.

        Returns:
            lancedb.table.Table: The created table.
        """
        return self.db_client.create_table(
            table_name, schema=schema, mode=mode, namespace_path=self.make_namespace_path()
        )

    @lancedb_error
    def open_table(self, table_name: str) -> "lancedb.table.Table":
        """Opens a table of the dataset namespace at its latest version."""
        return self.db_client.open_table(table_name, namespace_path=self.make_namespace_path())

    def table_exists(self, table_name: str) -> bool:
        return table_name in self.list_table_names()

    def get_table_schema(self, table_name: str) -> TArrowSchema:
        return cast(TArrowSchema, self.open_table(table_name).schema)

    @lancedb_error
    def drop_tables(self, *tables: str, delete_schema: bool = True) -> None:
        """Drops tables of the dataset namespace and optionally deletes the stored schema.

        Args:
            tables: The names of the tables to drop.
            delete_schema: If True, also delete all versions of the current schema from storage.
        """
        if tables:
            namespace_path = self.make_namespace_path()
            existing_tables = self.list_table_names()
            for table_name in tables:
                if table_name in existing_tables:
                    self.db_client.drop_table(table_name, namespace_path=namespace_path)
        if delete_schema:
            self._delete_schema_in_storage(self.schema)

    @lancedb_error
    def _delete_schema_in_storage(self, schema: Schema) -> None:
        """Deletes all stored versions with the same name as `schema`. No-op if table is missing."""
        if not self.table_exists(self.schema.version_table_name):
            return
        p_schema_name = self.schema.naming.normalize_identifier("schema_name")
        self.open_table(self.schema.version_table_name).delete(
            f'`{p_schema_name}` = "{schema.name}"'
        )

    @lancedb_error
    def truncate_table(self, table_name: str) -> None:
        """Truncates the table by deleting all its rows, preserving its schema, tags and history."""
        self.open_table(table_name).delete("true")

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        if not self.is_storage_initialized():
            self.create_dataset()
        elif truncate_tables:
            existing_tables = self.list_table_names()
            for table_name in truncate_tables:
                if table_name in existing_tables:
                    self.truncate_table(table_name)

    def is_storage_initialized(self) -> bool:
        return self.dataset_exists()

    def drop_storage(self) -> None:
        """Drops the tables of the dataset and the sentinel that records it as created."""
        if self.dataset_exists():
            self.drop_dataset()

    @lancedb_error
    def write_records(
        self,
        records: DATA,
        table_name: str,
        /,
        *,
        write_disposition: Optional[TWriteDisposition] = "append",
        merge_key: Optional[str] = None,
        merge_strategy: Optional[TLoaderMergeStrategy] = None,
        remove_orphans: bool = False,
        delete_condition: Optional[str] = None,
    ) -> int:
        """Inserts records into a table of the dataset namespace, computing embeddings server side.

        Args:
            records: The data to be inserted as payload.
            table_name: The name of the table to insert into.
            write_disposition: One of `skip`, `append`, `replace`, `merge`.
            merge_key: Key for update/merge operations.
            merge_strategy: Merge strategy resolved for the table.
            remove_orphans (bool): Whether to remove orphans after insertion (only merge disposition).
            delete_condition (str): SQL filter limiting which rows orphan removal can delete.

        Returns:
            int: Version the write created, which a caller must use instead of reading it back.

        Raises:
            DestinationTerminalException: If the write disposition is unsupported or the records
                do not fit the table schema.
        """
        tbl = self.open_table(table_name)
        try:
            if write_disposition in ("append", "skip", "replace"):
                return int(tbl.add(records, on_bad_vectors=ON_BAD_VECTORS).version)
            elif write_disposition == "merge":
                # LanceDB requires identical schemas for when_not_matched_by_source_delete
                # The incoming arrow schema must match the target table schema (column names,
                # order, and types). Only after 22. does it work with chunks and embeddings
                if self.config.embeddings:
                    records = add_vector_column(
                        records, tbl.schema, self.config.embeddings.vector_column
                    )
                if remove_orphans:
                    tbl.merge_insert(merge_key).when_not_matched_by_source_delete(
                        delete_condition
                    ).execute(records, on_bad_vectors=ON_BAD_VECTORS)
                elif merge_strategy == "insert-only":
                    tbl.merge_insert(merge_key).when_not_matched_insert_all().execute(
                        records, on_bad_vectors=ON_BAD_VECTORS
                    )
                else:
                    tbl.merge_insert(
                        merge_key
                    ).when_matched_update_all().when_not_matched_insert_all().execute(
                        records, on_bad_vectors=ON_BAD_VECTORS
                    )
                # the publishing delete lands after the merge, so its version is the current one
                return self._advance_table_version(tbl)
            else:
                raise DestinationTerminalException(
                    f"Unsupported `{write_disposition=:}` for LanceDB Destination - batch"
                    " failed AND WILL **NOT** BE RETRIED."
                )
        except pa.ArrowInvalid as e:
            raise DestinationTerminalException(
                "Python and Arrow datatype mismatch - batch failed AND WILL **NOT** BE RETRIED."
            ) from e

    @staticmethod
    def _advance_table_version(tbl: "lancedb.table.Table") -> int:
        """Publishes the preceding commit by committing a delete that matches nothing."""
        # a cluster commits merges and column additions without advancing the version, so a cached
        # read keeps serving the version before them. appends advance the version themselves
        return int(tbl.delete("1 = 0").version)

    def list_owned_table_names(self) -> List[str]:
        """Returns the destination tables of the dataset that materialize a schema table."""

        return [name for name in self.list_table_names() if name in self.schema.tables]

    @retry(
        wait=wait_exponential(multiplier=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
        before_sleep=before_sleep_log(logger.LOGGER, logging.WARNING),
    )
    def _tag_table_version(self, table_name: str, tag: str, version: int) -> None:
        """Names `version` of a table with `tag`, retrying because a lost tag is not recoverable."""
        tags = self.open_table(table_name).tags
        try:
            tags.update(tag, version)
        except Exception:
            tags.create(tag, version)

    @lancedb_error
    def _apply_commit_tag(self, tag: str, table_names: Sequence[str]) -> None:
        """Names the current version of each table with `tag`, so the load can be read back whole."""
        for table_name in table_names:
            self._tag_table_version(table_name, tag, self.open_table(table_name).version)

    def verify_schema(
        self, only_tables: Iterable[str] = None, new_jobs: Iterable[ParsedLoadJobFileName] = None
    ) -> List[PreparedTableSchema]:
        loaded_tables = super().verify_schema(only_tables, new_jobs)

        # Verify LanceDB-specific requirements for root tables
        for load_table in loaded_tables:
            # Skip nested tables as they inherit behavior from parent tables
            if is_nested_table(load_table):
                continue

            has_orphan_removal = load_table.get(REMOVE_ORPHANS_HINT, DEFAULT_REMOVE_ORPHANS)
            merge_keys = get_columns_names_with_prop(load_table, "merge_key")
            uses_merge_strategy = load_table.get("write_disposition", "") == "merge"

            # Validate merge key constraints when orphan removal is enabled
            if has_orphan_removal and len(merge_keys) > 1:
                raise DestinationTerminalException(
                    "Multiple merge keys are not supported when LanceDB orphan removal is"
                    f" enabled: {merge_keys}"
                )

            # Check if _dlt_load_id column is required but not present
            requires_dlt_ids = has_orphan_removal and uses_merge_strategy
            if requires_dlt_ids and "_dlt_load_id" not in load_table["columns"].keys():
                raise DestinationTerminalException(
                    "The `_dlt_load_id` column is required for tables with orphan removal or merge"
                    " keys. Enable this by setting"
                    " `NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID=TRUE` or an equivalent in"
                    " config.toml."
                )

            if not self.config.embeddings:
                if embed_columns := get_columns_names_with_prop(load_table, VECTORIZE_HINT):
                    raise LanceEmbeddingsConfigurationMissing(
                        load_table["name"], embed_columns, "lancedb"
                    )

        return loaded_tables

    @lancedb_error
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

    def get_storage_table(self, table_name: str) -> Tuple[bool, TTableSchemaColumns]:
        table_schema: TTableSchemaColumns = {}

        try:
            arrow_schema: TArrowSchema = self.get_table_schema(table_name)
        except DestinationUndefinedEntity:
            return False, table_schema

        field: TArrowField
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

    @lancedb_error
    def add_null_columns_to_table(self, table_name: str, columns: List[TColumnSchema]) -> None:
        """Extends the table schema with nullable columns filled with null values."""
        tbl = self.open_table(table_name)
        tbl.add_columns(
            {
                column["name"]: self.type_mapper.to_null_column_expression(column)
                for column in columns
            }
        )
        self._advance_table_version(tbl)

    def _execute_schema_update(
        self, only_tables: Iterable[str], store_schema: bool = True
    ) -> TSchemaTables:
        applied_update: TSchemaTables = {}
        for table_name in only_tables or self.schema.tables:
            exists, existing_columns = self.get_storage_table(table_name)
            new_columns: List[TColumnSchema] = self.schema.get_new_table_columns(
                table_name,
                existing_columns,
                self.capabilities.generates_case_sensitive_identifiers(),
            )
            logger.info(f"Found {len(new_columns)} updates for {table_name} in {self.schema.name}")
            if new_columns:
                # record the migration applied to this table (new table or added columns)
                partial_table = copy(self.prepare_load_table(table_name))
                partial_table["columns"] = {c["name"]: c for c in new_columns}
                applied_update[table_name] = partial_table
                if exists:
                    self.add_null_columns_to_table(table_name, new_columns)
                else:
                    self.create_table(table_name, self.make_table_arrow_schema(table_name))

        # skip writing the version row when the schema is already stored (enforced update)
        if store_schema:
            self._update_schema_in_storage(self.schema)
        return applied_update

    def make_table_arrow_schema(self, table_name: str) -> TArrowSchema:
        """Creates a PyArrow schema for a table, including embedding metadata if configured."""
        if not self.config.embeddings or table_name in self.schema.dlt_table_names():
            return make_arrow_table_schema(
                table_name, schema=self.schema, type_mapper=self.type_mapper
            )
        return make_arrow_table_schema(
            table_name,
            schema=self.schema,
            type_mapper=self.type_mapper,
            embedding_fields=get_columns_names_with_prop(
                self.schema.get_table(table_name=table_name), VECTORIZE_HINT
            ),
            embedding_model_func=self.model_func,
            embedding_model_dimensions=self.config.embeddings.dimensions,
            vector_field_name=self.config.embeddings.vector_column,
        )

    def _update_schema_in_storage(self, schema: Schema) -> None:
        records = [
            {
                self.schema.naming.normalize_identifier("version"): schema.version,
                self.schema.naming.normalize_identifier("engine_version"): schema.ENGINE_VERSION,
                self.schema.naming.normalize_identifier("inserted_at"): pendulum.now(),
                self.schema.naming.normalize_identifier("schema_name"): schema.name,
                self.schema.naming.normalize_identifier("version_hash"): schema.stored_version_hash,
                self.schema.naming.normalize_identifier("schema"): json.dumps(schema.to_dict()),
            }
        ]
        write_disposition = self.schema.get_table(self.schema.version_table_name).get(
            "write_disposition"
        )
        self.write_records(
            records,
            self.schema.version_table_name,
            write_disposition=write_disposition,
        )

    @lancedb_error
    def get_stored_state(self, pipeline_name: str) -> Optional[StateInfo]:
        """Retrieves the latest completed state for a pipeline."""
        state_table_ = self.open_table(self.schema.state_table_name)
        loads_table_ = self.open_table(self.schema.loads_table_name)

        # normalize property names
        p_load_id = self.schema.naming.normalize_identifier(C_DLT_LOADS_TABLE_LOAD_ID)
        p_dlt_load_id = self.schema.naming.normalize_identifier(
            self.schema.data_item_normalizer.c_dlt_load_id  # type: ignore[attr-defined]
        )
        p_pipeline_name = self.schema.naming.normalize_identifier("pipeline_name")
        p_status = self.schema.naming.normalize_identifier("status")

        # Read the tables into memory as Arrow tables, with pushdown predicates, so we pull as little
        # data into memory as possible.
        state_table = (
            state_table_.search()
            .where(f"`{p_pipeline_name}` = '{pipeline_name}'", prefilter=True)
            .to_arrow()
        )
        loads_table = loads_table_.search().where(f"`{p_status}` = 0", prefilter=True).to_arrow()

        # Join arrow tables in-memory.
        joined_table: pa.Table = state_table.join(
            loads_table, keys=p_dlt_load_id, right_keys=p_load_id, join_type="inner"
        ).sort_by([(p_dlt_load_id, "descending")])

        if joined_table.num_rows == 0:
            return None

        row = joined_table.take([0]).to_pylist()[0]
        return StateInfo.from_normalized_mapping(row, self.schema.naming)

    def _get_latest_schema(self, filter_: Optional[str] = None) -> Optional[StorageSchemaInfo]:
        if not self.table_exists(self.schema.version_table_name):
            # version table not created yet (empty storage)
            return None
        query = self.open_table(self.schema.version_table_name).search()
        if filter_:
            query = query.where(filter_, prefilter=True)
        rows = query.to_list()
        try:
            row = max(rows, key=lambda x: x[self.schema.naming.normalize_identifier("inserted_at")])
        except ValueError:
            return None
        return StorageSchemaInfo.from_normalized_mapping(row, self.schema.naming)

    @lancedb_error
    def get_stored_schema_by_hash(self, schema_hash: str) -> Optional[StorageSchemaInfo]:
        col = self.schema.naming.normalize_identifier("version_hash")
        return self._get_latest_schema(filter_=f'`{col}` = "{schema_hash}"')

    @lancedb_error
    def get_stored_schema(self, schema_name: str = None) -> Optional[StorageSchemaInfo]:
        """Retrieves newest schema from destination storage."""
        if schema_name:
            col = self.schema.naming.normalize_identifier("schema_name")
            return self._get_latest_schema(filter_=f'`{col}` = "{schema_name}"')
        return self._get_latest_schema()

    def complete_load(self, load_id: str) -> None:
        records = [
            {
                self.schema.naming.normalize_identifier(C_DLT_LOADS_TABLE_LOAD_ID): load_id,
                self.schema.naming.normalize_identifier("schema_name"): self.schema.name,
                self.schema.naming.normalize_identifier("status"): 0,
                self.schema.naming.normalize_identifier("inserted_at"): pendulum.now(),
                self.schema.naming.normalize_identifier(
                    "schema_version_hash"
                ): self.schema.version_hash,
            }
        ]
        write_disposition = self.schema.get_table(self.schema.loads_table_name).get(
            "write_disposition"
        )
        loads_table_name = self.schema.loads_table_name
        tag = self.config.commit_tag
        # tag everything holding data before the load is committed, so a tagging failure aborts it
        if tag:
            self._apply_commit_tag(
                tag, [name for name in self.list_owned_table_names() if name != loads_table_name]
            )

        version = self.write_records(
            records,
            loads_table_name,
            write_disposition=write_disposition,
        )

        # the loads row is the commit, so its tag lands after it and `dlt` will not retry this
        if tag:
            try:
                self._tag_table_version(loads_table_name, tag, version)
            except Exception as e:
                raise LanceDBCommitTagNotApplied(
                    tag, loads_table_name, version, self.dataset_name, load_id
                ) from e

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        if ReferenceFollowupJobRequest.is_reference_job(file_path):
            return LanceDBRemoveOrphansJob(file_path)
        else:
            return LanceDBLoadJob(file_path, table)

    def create_table_chain_completed_followup_jobs(
        self,
        table_chain: Sequence[TTableSchema],
        completed_table_chain_jobs: Optional[Sequence[LoadJobInfo]] = None,
    ) -> List[FollowupJobRequest]:
        jobs = super().create_table_chain_completed_followup_jobs(
            table_chain, completed_table_chain_jobs  # type: ignore[arg-type]
        )
        # orphan removal replaces old versions of docs, skip for insert-only
        first_table_in_chain = table_chain[0]
        merge_strategy = resolve_merge_strategy(
            {first_table_in_chain["name"]: first_table_in_chain}, first_table_in_chain
        )
        if (
            first_table_in_chain.get("write_disposition") == "merge"
            and merge_strategy != "insert-only"
            and first_table_in_chain.get(REMOVE_ORPHANS_HINT, DEFAULT_REMOVE_ORPHANS)
        ):
            all_job_paths_ordered = [
                job.file_path
                for table in table_chain
                for job in completed_table_chain_jobs
                if job.job_file_info.table_name == table.get("name")
            ]
            root_table_file_name = FileStorage.get_file_name_from_file_path(
                all_job_paths_ordered[0]
            )
            jobs.append(ReferenceFollowupJobRequest(root_table_file_name, all_job_paths_ordered))
        return jobs
