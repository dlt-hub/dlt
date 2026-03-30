from __future__ import annotations

from types import TracebackType
from typing import (
    Dict,
    List,
    Any,
    Union,
    Tuple,
    Iterable,
    Type,
    Optional,
    TYPE_CHECKING,
)

import lance
import lancedb
import pyarrow as pa
from lance import LanceDataset
from lance.namespace import (
    CreateNamespaceRequest,
    DropNamespaceRequest,
    DropTableRequest,
    ListTablesRequest,
    NamespaceExistsRequest,
    TableExistsRequest,
)
from lancedb.table import LanceTable, _append_vector_columns
from lancedb.embeddings import EmbeddingFunctionConfig, EmbeddingFunctionRegistry

from dlt.common import json, pendulum, logger
from dlt.common.libs.numpy import numpy
from dlt.common.destination import DestinationCapabilitiesContext
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
)
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
)
from dlt.destinations.impl.lance.exceptions import (
    LanceEmbeddingsConfigurationMissing,
    is_lance_undefined_entity_exception,
    raise_destination_error,
)
from dlt.destinations.impl.lance.jobs import LanceLoadJob
from dlt.destinations.impl.lance.lance_adapter import (
    DEFAULT_REMOVE_ORPHANS,
    VECTORIZE_HINT,
    REMOVE_ORPHANS_HINT,
)
from dlt.common.libs.pyarrow import columns_to_arrow, dlt_column_to_arrow_field
from dlt.destinations.impl.lance.utils import _align_schema
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient

if TYPE_CHECKING:
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
        self.namespace = self.config.storage.make_directory_namespace()
        self.embedding_function = (
            self.config.embeddings.create_embedding_function() if self.config.embeddings else None
        )
        self._sql_client: SqlClientBase[Any] = None

    def __enter__(self) -> LanceClient:
        return self

    def __exit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType
    ) -> None:
        pass

    @property
    def sql_client_class(self) -> Type[SqlClientBase[Any]]:
        from dlt.destinations.impl.lance.sql_client import LanceSQLClient

        return LanceSQLClient

    @property
    def sql_client(self) -> SqlClientBase[Any]:
        # inner import because `LanceSQLClient` depends on `duckdb` and is optional
        from dlt.destinations.impl.lance.sql_client import LanceSQLClient

        if not self._sql_client:
            self._sql_client = LanceSQLClient(self)
        return self._sql_client

    @sql_client.setter
    def sql_client(self, client: SqlClientBase[Any]) -> None:
        self._sql_client = client

    @raise_destination_error
    def list_dataset_namespace_tables(self) -> List[str]:
        return self.namespace.list_tables(ListTablesRequest(id=[self.dataset_name])).tables

    @raise_destination_error
    def create_dataset_namespace(self) -> None:
        """Creates child namespace for dataset in root namespace."""
        self.namespace.create_namespace(CreateNamespaceRequest(id=[self.dataset_name]))

    @raise_destination_error
    def drop_dataset_namespace(self) -> None:
        """Drops dataset namespace after removing all its tables."""
        for table in self.list_dataset_namespace_tables():
            self.namespace.drop_table(DropTableRequest(id=[self.dataset_name, table]))
        self.namespace.drop_namespace(DropNamespaceRequest(id=[self.dataset_name]))

    @raise_destination_error
    def dataset_namespace_exists(self) -> bool:
        """Returns True if child namespace for dataset exists in root namespace."""
        try:
            self.namespace.namespace_exists(NamespaceExistsRequest(id=[self.dataset_name]))
            return True
        except Exception as e:
            if is_lance_undefined_entity_exception(e):
                return False
            raise

    @raise_destination_error
    def create_table(self, table_name: str, schema: pa.Schema) -> None:
        """Creates empty lance dataset from provided PyArrow schema."""
        lance.write_dataset(
            schema.empty_table(),
            namespace=self.namespace,
            table_id=self.make_table_id(table_name),
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
            if is_lance_undefined_entity_exception(e):
                return False
            raise

    def make_table_id(self, table_name: str) -> List[str]:
        """Returns namespace `table_id` for given table name."""
        return [self.dataset_name, table_name]

    def get_table_schema(self, table_name: str) -> pa.Schema:
        return self.open_lance_dataset(
            table_name, branch_name=self.config.storage.branch_name
        ).schema

    def get_table_uri(self, table_name: str) -> str:
        # we don't pass branch here — `uri` always returns base URI
        return self.open_lance_dataset(table_name).uri

    def drop_tables(self, *tables: str) -> None:
        """Drops tables from lance dataset namespace."""
        for table_name in tables:
            self.drop_table(table_name)

    def drop_storage(self) -> None:
        """Drops dataset namespace and all its tables."""
        if self.dataset_namespace_exists():
            self.drop_dataset_namespace()

    def truncate_table(self, table_name: str) -> None:
        """Truncates table by deleting all rows in active branch."""
        self.open_lance_dataset(table_name, branch_name=self.config.storage.branch_name).delete(
            "true"
        )

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
            namespace=self.namespace,
            table_id=self.make_table_id(table_name),
        ).checkout_version((branch_name, version_number))

    def open_lancedb_table(self, table_name: str) -> LanceTable:
        """Returns LanceDB table for given table name.

        This provides access to LanceDB-specific features like vector search.
        """
        db = lancedb.connect(
            self.config.storage.bucket_url,
            storage_options=self.config.storage.options,
        )
        return LanceTable.open(db, table_name, location=self.get_table_uri(table_name))

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
            records = _append_vector_columns(records, schema=ds.schema)
            records = _align_schema(records, ds.schema)

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
                if not self.table_exists(table_name):
                    continue
                self.truncate_table(table_name)

    def is_storage_initialized(self) -> bool:
        return self.dataset_namespace_exists()

    def verify_schema(
        self, only_tables: Iterable[str] = None, new_jobs: Iterable[ParsedLoadJobFileName] = None
    ) -> List[PreparedTableSchema]:
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
    ) -> Optional[TSchemaTables]:
        applied_update = super().update_stored_schema(only_tables, expected_update)
        try:
            schema_info = self.get_stored_schema_by_hash(self.schema.stored_version_hash)
        except DestinationUndefinedEntity:
            schema_info = None

        if schema_info is None:
            logger.info(
                f"Schema with hash {self.schema.stored_version_hash} "
                "not found in the storage. upgrading"
            )
            # TODO: return a real updated table schema (like in SQL job client)
            self._execute_schema_update(only_tables)
        else:
            logger.debug(
                f"Schema with hash {self.schema.stored_version_hash} "
                f"inserted at {schema_info.inserted_at} found "
                "in storage, no upgrade required"
            )
        # we assume that expected_update == applied_update so table schemas in dest were not
        # externally changed
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
        except Exception as e:
            if is_lance_undefined_entity_exception(e):
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

    def add_null_columns_to_table(self, table_name: str, new_columns: List[TColumnSchema]) -> None:
        new_fields = [dlt_column_to_arrow_field(col, self.capabilities) for col in new_columns]
        self.open_lance_dataset(
            table_name, branch_name=self.config.storage.branch_name
        ).add_columns(new_fields)

    def _execute_schema_update(self, only_tables: Iterable[str]) -> None:
        for table_name in only_tables or self.schema.tables:
            table_exists = self.table_exists(table_name)

            # create new table if it doesn't exist
            if not table_exists:
                self.create_table(table_name, self.make_arrow_table_schema(table_name))

            # create branch if needed
            if branch_name := self.config.storage.branch_name:
                self.create_branch_if_not_exists(table_name, branch_name)

            # add new columns to existing table (on the branch if configured)
            if table_exists:
                _, existing_columns = self.get_storage_table(table_name)
                new_columns = self.schema.get_new_table_columns(
                    table_name,
                    existing_columns,
                    self.capabilities.generates_case_sensitive_identifiers(),
                )
                if new_columns:
                    self.add_null_columns_to_table(table_name, new_columns)

        self.update_schema_in_storage()

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
            self.schema.state_table_name, branch_name=self.config.storage.branch_name
        )
        loads_ds = self.open_lance_dataset(
            self.schema.loads_table_name, branch_name=self.config.storage.branch_name
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
        ds = self.open_lance_dataset(
            self.schema.version_table_name, branch_name=self.config.storage.branch_name
        )
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

    def update_schema_in_storage(self) -> None:
        record = {
            "version": self.schema.version,
            "engine_version": self.schema.ENGINE_VERSION,
            "inserted_at": pendulum.now(),
            "schema_name": self.schema.name,
            "version_hash": self.schema.stored_version_hash,
            "schema": json.dumps(self.schema.to_dict()),
        }
        records = [{self.schema.naming.normalize_identifier(k): v for k, v in record.items()}]
        write_disposition = self.schema.get_table(self.schema.version_table_name).get(
            "write_disposition"
        )
        self.write_records(
            records,
            self.schema.version_table_name,
            branch_name=self.config.storage.branch_name,
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
            branch_name=self.config.storage.branch_name,
            write_disposition=write_disposition,
        )

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        return LanceLoadJob(file_path, table)
