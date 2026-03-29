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
from pyarrow import ArrowInvalid
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
from lancedb.query import LanceQueryBuilder
from pyarrow import Array, ChunkedArray

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
    lance_error,
)
from dlt.destinations.impl.lance.jobs import LanceLoadJob
from dlt.destinations.impl.lancedb.lancedb_adapter import (
    VECTORIZE_HINT,
    NO_REMOVE_ORPHANS_HINT,
)
from dlt.destinations.impl.lance.schema import (
    make_arrow_field_schema,
    make_arrow_table_schema,
    TArrowSchema,
    TArrowField,
)
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
        self.model_func = (
            self.config.embeddings.create_embedding_function() if self.config.embeddings else None
        )
        self._sql_client: SqlClientBase[Any] = None

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

    def list_tables(self, namespace_name: str) -> List[str]:
        """Lists tables in child namespace."""
        return self.namespace.list_tables(ListTablesRequest(id=[namespace_name])).tables

    def create_namespace(self, name: str) -> None:
        """Creates child namespace in root namespace."""
        self.namespace.create_namespace(CreateNamespaceRequest(id=[name]))

    def drop_namespace(self, name: str) -> None:
        """Drops child namespace after removing all its tables."""
        for table in self.list_tables(name):
            self.namespace.drop_table(DropTableRequest(id=[name, table]))
        self.namespace.drop_namespace(DropNamespaceRequest(id=[name]))

    def namespace_exists(self, name: str) -> bool:
        """Returns True if child namespace exists in root namespace."""
        try:
            self.namespace.namespace_exists(NamespaceExistsRequest(id=[name]))
            return True
        except Exception as e:
            if is_lance_undefined_entity_exception(e):
                return False
            raise

    @lance_error
    def create_table(self, table_name: str, schema: TArrowSchema) -> None:
        """Creates empty lance dataset from provided PyArrow schema."""
        lance.write_dataset(
            schema.empty_table(),
            namespace=self.namespace,
            table_id=self.make_table_id(table_name),
        )

    def drop_table(self, table_name: str) -> None:
        """Drops table from lance dataset namespace."""
        self.namespace.drop_table(DropTableRequest(id=self.make_table_id(table_name)))

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

    def get_table_schema(self, table_name: str) -> TArrowSchema:
        return self.open_lance_dataset(
            table_name, branch_name=self.config.storage.branch_name
        ).schema

    def get_table_uri(self, table_name: str) -> str:
        return self.open_lance_dataset(table_name).uri

    def drop_tables(self, *tables: str) -> None:
        """Drops tables from lance dataset namespace."""
        for table_name in tables:
            self.drop_table(table_name)

    def drop_storage(self) -> None:
        """Drops dataset namespace and all its tables."""
        if self.namespace_exists(self.dataset_name):
            self.drop_namespace(self.dataset_name)

    def truncate_table(self, table_name: str) -> None:
        """Truncates table by deleting all rows in active branch."""
        self.open_lance_dataset(table_name, branch_name=self.config.storage.branch_name).delete(
            "true"
        )

    def create_branch_if_not_exists(self, table_name: str, branch_name: str) -> None:
        ds = self.open_lance_dataset(table_name)
        if branch_name not in ds.branches.list():
            ds.create_branch(branch_name)

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

    def open_lance_table(self, table_name: str) -> LanceTable:
        """Returns LanceDB table for given table name.

        This provides access to LanceDB-specific features like vector search.
        """
        db = lancedb.connect(
            self.config.storage.bucket_url,
            storage_options=self.config.storage.options,
        )
        return LanceTable.open(db, table_name, location=self.get_table_uri(table_name))

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

        try:
            if write_disposition in ("append", "skip", "replace"):
                ds.insert(records)
            elif write_disposition == "merge":
                merge_builder = (
                    ds.merge_insert(merge_key)
                    .when_matched_update_all()
                    .when_not_matched_insert_all()
                )
                if when_not_matched_by_source_delete_expr:
                    merge_builder = merge_builder.when_not_matched_by_source_delete(
                        when_not_matched_by_source_delete_expr
                    )
                merge_builder.execute(records)
            else:
                raise DestinationTerminalException(
                    f"Unsupported `{write_disposition=:}` for Lance Destination - batch"
                    " failed AND WILL **NOT** BE RETRIED."
                )
        except ArrowInvalid as e:
            raise DestinationTerminalException(
                "Python and Arrow datatype mismatch - batch failed AND WILL **NOT** BE RETRIED."
            ) from e

    def query_table(
        self,
        table_name: str,
        query: Union[List[Any], NDArray, Array, ChunkedArray, str, Tuple[Any], None] = None,
    ) -> LanceQueryBuilder:
        """Query a LanceDB table.

        Args:
            table_name: The name of the table to query.
            query: The targeted vector to search for.

        Returns:
            A LanceDB query builder.
        """
        return self.open_lance_table(table_name).search(query=query)

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        if not self.is_storage_initialized():
            self.create_namespace(self.dataset_name)
        elif truncate_tables:
            for table_name in truncate_tables:
                if not self.table_exists(table_name):
                    continue
                self.truncate_table(table_name)

    def is_storage_initialized(self) -> bool:
        return self.namespace_exists(self.dataset_name)

    def verify_schema(
        self, only_tables: Iterable[str] = None, new_jobs: Iterable[ParsedLoadJobFileName] = None
    ) -> List[PreparedTableSchema]:
        loaded_tables = super().verify_schema(only_tables, new_jobs)

        # Verify LanceDB-specific requirements for root tables
        for load_table in loaded_tables:
            # Skip nested tables as they inherit behavior from parent tables
            if is_nested_table(load_table):
                continue

            # Check if this table has orphan removal enabled (either explicitly or via merge strategy)
            should_remove_orphans = LanceLoadJob._should_remove_orphans(load_table)
            merge_keys = get_columns_names_with_prop(load_table, "merge_key")

            # Validate merge key constraints when orphan removal is enabled
            if should_remove_orphans and len(merge_keys) > 1:
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

        # inherit missing hint from parent table, if available
        if NO_REMOVE_ORPHANS_HINT not in table:
            table[NO_REMOVE_ORPHANS_HINT] = get_inherited_table_hint(  # type: ignore[literal-required]
                self.schema.tables, table_name, NO_REMOVE_ORPHANS_HINT, allow_none=True
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

    @lance_error
    def add_null_columns_to_table(self, table_name: str, new_columns: List[TColumnSchema]) -> None:
        new_fields: List[TArrowField] = [
            make_arrow_field_schema(column["name"], column, self.type_mapper)
            for column in new_columns
        ]
        self.open_lance_dataset(
            table_name, branch_name=self.config.storage.branch_name
        ).add_columns(new_fields)

    def _execute_schema_update(self, only_tables: Iterable[str]) -> None:
        for table_name in only_tables or self.schema.tables:
            table_exists = self.table_exists(table_name)

            # create new table if it doesn't exist
            if not table_exists:
                if self.config.embeddings and table_name not in self.schema.dlt_table_names():
                    embedding_fields = get_columns_names_with_prop(
                        self.schema.get_table(table_name=table_name), VECTORIZE_HINT
                    )
                    vector_column = self.config.embeddings.vector_column
                    embedding_model_func = self.model_func
                else:
                    embedding_fields = None
                    vector_column = None
                    embedding_model_func = None

                table_schema: TArrowSchema = make_arrow_table_schema(
                    table_name,
                    schema=self.schema,
                    type_mapper=self.type_mapper,
                    embedding_fields=embedding_fields,
                    embedding_model_func=embedding_model_func,
                    vector_column=vector_column,
                )
                self.create_table(table_name, table_schema)

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

    @lance_error
    def update_schema_in_storage(self) -> None:
        records = [
            {
                self.schema.naming.normalize_identifier("version"): self.schema.version,
                self.schema.naming.normalize_identifier(
                    "engine_version"
                ): self.schema.ENGINE_VERSION,
                self.schema.naming.normalize_identifier("inserted_at"): pendulum.now(),
                self.schema.naming.normalize_identifier("schema_name"): self.schema.name,
                self.schema.naming.normalize_identifier(
                    "version_hash"
                ): self.schema.stored_version_hash,
                self.schema.naming.normalize_identifier("schema"): json.dumps(
                    self.schema.to_dict()
                ),
            }
        ]
        write_disposition = self.schema.get_table(self.schema.version_table_name).get(
            "write_disposition"
        )
        self.write_records(
            records,
            self.schema.version_table_name,
            branch_name=self.config.storage.branch_name,
            write_disposition=write_disposition,
        )

    @lance_error
    def get_stored_state(self, pipeline_name: str) -> Optional[StateInfo]:
        """Retrieves the latest completed state for a pipeline."""

        # normalize property names
        p_load_id = self.schema.naming.normalize_identifier(C_DLT_LOADS_TABLE_LOAD_ID)
        p_dlt_load_id = self.schema.naming.normalize_identifier(
            self.schema.data_item_normalizer.c_dlt_load_id  # type: ignore[attr-defined]
        )
        p_pipeline_name = self.schema.naming.normalize_identifier("pipeline_name")
        p_status = self.schema.naming.normalize_identifier("status")
        p_version = self.schema.naming.normalize_identifier("version")
        p_engine_version = self.schema.naming.normalize_identifier("engine_version")
        p_state = self.schema.naming.normalize_identifier("state")
        p_created_at = self.schema.naming.normalize_identifier("created_at")
        p_version_hash = self.schema.naming.normalize_identifier("version_hash")

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

        state = joined_table.take([0]).to_pylist()[0]
        return StateInfo(
            version=state[p_version],
            engine_version=state[p_engine_version],
            pipeline_name=state[p_pipeline_name],
            state=state[p_state],
            created_at=pendulum.instance(state[p_created_at]),
            version_hash=state[p_version_hash],
            _dlt_load_id=state[p_dlt_load_id],
        )

    @lance_error
    def get_stored_schema_by_hash(self, schema_hash: str) -> Optional[StorageSchemaInfo]:
        ds = self.open_lance_dataset(
            self.schema.version_table_name, branch_name=self.config.storage.branch_name
        )
        p_version_hash = self.schema.naming.normalize_identifier("version_hash")
        p_inserted_at = self.schema.naming.normalize_identifier("inserted_at")
        p_schema_name = self.schema.naming.normalize_identifier("schema_name")
        p_version = self.schema.naming.normalize_identifier("version")
        p_engine_version = self.schema.naming.normalize_identifier("engine_version")
        p_schema = self.schema.naming.normalize_identifier("schema")

        try:
            schemas = (
                ds.scanner(filter=f'`{p_version_hash}` = "{schema_hash}"', prefilter=True)
                .to_table()
                .to_pylist()
            )

            most_recent_schema = sorted(schemas, key=lambda x: x[p_inserted_at], reverse=True)[0]
            return StorageSchemaInfo(
                version_hash=most_recent_schema[p_version_hash],
                schema_name=most_recent_schema[p_schema_name],
                version=most_recent_schema[p_version],
                engine_version=most_recent_schema[p_engine_version],
                inserted_at=most_recent_schema[p_inserted_at],
                schema=most_recent_schema[p_schema],
            )
        except IndexError:
            return None

    @lance_error
    def get_stored_schema(self, schema_name: str = None) -> Optional[StorageSchemaInfo]:
        """Retrieves newest schema from destination storage."""
        if not self.table_exists(self.schema.version_table_name):
            return None

        p_version_hash = self.schema.naming.normalize_identifier("version_hash")
        p_inserted_at = self.schema.naming.normalize_identifier("inserted_at")
        p_schema_name = self.schema.naming.normalize_identifier("schema_name")
        p_version = self.schema.naming.normalize_identifier("version")
        p_engine_version = self.schema.naming.normalize_identifier("engine_version")
        p_schema = self.schema.naming.normalize_identifier("schema")

        try:
            version_dataset = self.open_lance_dataset(
                self.schema.version_table_name, branch_name=self.config.storage.branch_name
            )
            if schema_name:
                schemas = version_dataset.scanner(
                    filter=f'`{p_schema_name}` = "{schema_name}"', prefilter=True
                ).to_table()
            else:
                schemas = version_dataset.to_table()

            most_recent_schema = sorted(
                schemas.to_pylist(), key=lambda x: x[p_inserted_at], reverse=True
            )[0]
            return StorageSchemaInfo(
                version_hash=most_recent_schema[p_version_hash],
                schema_name=most_recent_schema[p_schema_name],
                version=most_recent_schema[p_version],
                engine_version=most_recent_schema[p_engine_version],
                inserted_at=most_recent_schema[p_inserted_at],
                schema=most_recent_schema[p_schema],
            )
        except IndexError:
            return None

    def __exit__(
        self,
        exc_type: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        pass

    def __enter__(self) -> "LanceClient":
        return self

    @lance_error
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
