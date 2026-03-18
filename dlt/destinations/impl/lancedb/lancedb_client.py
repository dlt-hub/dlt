from types import TracebackType
from typing import (
    List,
    Any,
    cast,
    Union,
    Tuple,
    Iterable,
    Type,
    Optional,
    TYPE_CHECKING,
)

import os
import shutil

import lance
import lancedb
import pyarrow as pa
from lance import LanceDataset
from lancedb.embeddings import EmbeddingFunctionRegistry, TextEmbeddingFunction
from lancedb.table import LanceTable
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
    TTableSchema,
)
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    get_inherited_table_hint,
    is_nested_table,
)
from dlt.common.storages import ParsedLoadJobFileName
from dlt.destinations.impl.lancedb.configuration import (
    LanceDBClientConfiguration,
)
from dlt.destinations.impl.lancedb.exceptions import (
    lancedb_error,
)
from dlt.destinations.impl.lancedb.jobs import LanceDBLoadJob
from dlt.destinations.impl.lancedb.lancedb_adapter import (
    VECTORIZE_HINT,
    NO_REMOVE_ORPHANS_HINT,
)
from dlt.destinations.impl.lancedb.schema import (
    make_arrow_field_schema,
    make_arrow_table_schema,
    TArrowSchema,
    NULL_SCHEMA,
    TArrowField,
)
from dlt.destinations.impl.lancedb.utils import (
    _make_lance_table_uri,
    create_empty_lance_dataset,
    set_non_standard_providers_environment_variables,
    write_records,
)
from dlt.destinations.sql_jobs import SqlMergeFollowupJob
from dlt.destinations.type_mapping import TypeMapperImpl
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient

if TYPE_CHECKING:
    NDArray = numpy.ndarray[Any, Any]
else:
    NDArray = numpy.ndarray


class LanceDBClient(JobClientBase, WithStateSync, WithSqlClient):
    """LanceDB destination handler."""

    model_func: TextEmbeddingFunction
    """The embedder callback used for each chunk."""
    dataset_name: str

    def __init__(
        self,
        schema: Schema,
        config: LanceDBClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)
        self.registry = EmbeddingFunctionRegistry.get_instance()
        self.config: LanceDBClientConfiguration = config
        self.type_mapper = self.capabilities.get_type_mapper()
        self.sentinel_table_name = config.sentinel_table_name
        self.dataset_name = self.config.normalize_dataset_name(self.schema)
        self._sql_client: SqlClientBase[Any] = None

        embedding_model_provider = self.config.embedding_model_provider
        embedding_model_host = self.config.embedding_model_provider_host

        # LanceDB doesn't provide a standardized way to set API keys across providers.
        # Some use ENV variables and others allow passing api key as an argument.
        # To account for this, we set provider environment variable as well.
        set_non_standard_providers_environment_variables(
            embedding_model_provider,
            self.config.credentials.embedding_model_provider_api_key,
        )

        self.model_func = self.registry.get(embedding_model_provider).create(
            name=self.config.embedding_model,
            max_retries=self.config.options.max_retries,
            # actually the model func doesnt need the api-key!
            **({"host": embedding_model_host} if embedding_model_host else {}),
        )

    @property
    def sql_client_class(self) -> Type[SqlClientBase[Any]]:
        from dlt.destinations.impl.lancedb.sql_client import LanceDBSQLClient

        return LanceDBSQLClient

    @property
    def sql_client(self) -> SqlClientBase[Any]:
        # inner import because `LanceDBSQLClient` depends on `duckdb` and is optional
        from dlt.destinations.impl.lancedb.sql_client import LanceDBSQLClient

        if not self._sql_client:
            self._sql_client = LanceDBSQLClient(self)
        return self._sql_client

    @sql_client.setter
    def sql_client(self, client: SqlClientBase[Any]) -> None:
        self._sql_client = client

    def make_qualified_table_name(self, table_name: str) -> str:
        return (
            f"{self.dataset_name}{self.config.dataset_separator}{table_name}"
            if self.dataset_name
            else table_name
        )

    def get_table_schema(self, table_name: str) -> TArrowSchema:
        return self._get_lance_dataset(table_name).schema

    @lancedb_error
    def create_table(self, table_name: str, schema: TArrowSchema) -> None:
        """Create a lance dataset from the provided PyArrow schema.

        Args:
            schema: The table schema to create.
            table_name: The name of the table to create.
        """
        create_empty_lance_dataset(schema, uri=self._get_lance_table_uri(table_name))

    def drop_table(self, table_name: str, is_qualified: bool = False) -> None:
        """Drops a LanceDB table.

        Args:
            table_name: The name of the table to drop.
            is_qualified: Whether the provided table name is already qualified.
        """
        uri = self._get_lance_table_uri(table_name, is_qualified=is_qualified)
        shutil.rmtree(uri, ignore_errors=True)

    def drop_tables(self, *tables: str, delete_schema: bool = True) -> None:
        """Drop multiple LanceDB tables.

        Args:
            table_names: The names of the tables to drop.
        """
        for table_name in tables:
            self.drop_table(table_name)

    @lancedb_error
    def drop_storage(self) -> None:
        """Drop the dataset from the LanceDB instance.

        Deletes all tables in the dataset and all data, as well as sentinel table associated with them.

        If the dataset name wasn't provided, it deletes all the tables in the current schema.
        """
        for table_name in self._list_dlt_lance_tables():
            self.drop_table(table_name, is_qualified=True)

    def get_lancedb_table(self, table_name: str) -> LanceTable:
        """Returns a LanceDB table for the specified table."""
        db = self._get_lancedb_connection()
        qualified_table_name = self.make_qualified_table_name(table_name)
        location = self._get_lance_table_uri(table_name)
        return LanceTable.open(db, qualified_table_name, location=location)

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
        return self.get_lancedb_table(table_name).search(query=query)

    @lancedb_error
    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        if not self.is_storage_initialized():
            self._create_sentinel_table()
        elif truncate_tables:
            for table_name in truncate_tables:
                if not self.table_exists(table_name):
                    continue
                schema = self.get_table_schema(table_name)
                self.drop_table(table_name)
                self.create_table(table_name, schema)

    @lancedb_error
    def is_storage_initialized(self) -> bool:
        return self.table_exists(self.sentinel_table_name)

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
            should_remove_orphans = LanceDBLoadJob._should_remove_orphans(load_table)
            merge_keys = get_columns_names_with_prop(load_table, "merge_key")

            # Validate merge key constraints when orphan removal is enabled
            if should_remove_orphans and len(merge_keys) > 1:
                raise DestinationTerminalException(
                    "Multiple merge keys are not supported when LanceDB orphan removal is"
                    f" enabled: {merge_keys}"
                )

        return loaded_tables

    def _create_sentinel_table(self) -> None:
        """Create an empty table to indicate that the storage is initialized."""
        self.create_table(schema=NULL_SCHEMA, table_name=self.sentinel_table_name)

    @lancedb_error
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
        except ValueError:
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
    def extend_lancedb_table_schema(self, table_name: str, field_schemas: List[pa.Field]) -> None:
        """Extend LanceDB table schema with empty columns.

        Args:
        table_name: The name of the table to create the fields on.
        field_schemas: The list of PyArrow Fields to create in the target LanceDB table.
        """
        self._get_lance_dataset(table_name).add_columns(field_schemas)

    def _execute_schema_update(self, only_tables: Iterable[str]) -> None:
        for table_name in only_tables or self.schema.tables:
            exists, existing_columns = self.get_storage_table(table_name)
            new_columns: List[TColumnSchema] = self.schema.get_new_table_columns(
                table_name,
                existing_columns,
                self.capabilities.generates_case_sensitive_identifiers(),
            )
            logger.info(f"Found {len(new_columns)} updates for {table_name} in {self.schema.name}")
            if new_columns:
                if exists:
                    field_schemas: List[TArrowField] = [
                        make_arrow_field_schema(column["name"], column, self.type_mapper)
                        for column in new_columns
                    ]
                    self.extend_lancedb_table_schema(table_name, field_schemas)
                else:
                    if table_name not in self.schema.dlt_table_names():
                        embedding_fields = get_columns_names_with_prop(
                            self.schema.get_table(table_name=table_name), VECTORIZE_HINT
                        )
                        vector_field_name = self.config.vector_field_name
                        embedding_model_func = self.model_func
                        embedding_model_dimensions = self.config.embedding_model_dimensions
                    else:
                        embedding_fields = None
                        vector_field_name = None
                        embedding_model_func = None
                        embedding_model_dimensions = None

                    table_schema: TArrowSchema = make_arrow_table_schema(
                        table_name,
                        schema=self.schema,
                        type_mapper=self.type_mapper,
                        embedding_fields=embedding_fields,
                        embedding_model_func=embedding_model_func,
                        embedding_model_dimensions=embedding_model_dimensions,
                        vector_field_name=vector_field_name,
                    )
                    self.create_table(table_name, table_schema)

        self.update_schema_in_storage()

    @lancedb_error
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
        fq_version_table_name = self.make_qualified_table_name(self.schema.version_table_name)
        write_disposition = self.schema.get_table(self.schema.version_table_name).get(
            "write_disposition"
        )

        write_records(
            records,
            lance_uri=self.config.lance_uri,
            table_name=fq_version_table_name,
            write_disposition=write_disposition,
        )

    @lancedb_error
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
        state_ds = self._get_lance_dataset(self.schema.state_table_name)
        loads_ds = self._get_lance_dataset(self.schema.loads_table_name)
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

    @lancedb_error
    def get_stored_schema_by_hash(self, schema_hash: str) -> Optional[StorageSchemaInfo]:
        ds = self._get_lance_dataset(self.schema.version_table_name)
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

    @lancedb_error
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
            version_dataset = self._get_lance_dataset(self.schema.version_table_name)
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

    def __enter__(self) -> "LanceDBClient":
        return self

    @lancedb_error
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
        fq_loads_table_name = self.make_qualified_table_name(self.schema.loads_table_name)
        write_disposition = self.schema.get_table(self.schema.loads_table_name).get(
            "write_disposition"
        )
        write_records(
            records,
            lance_uri=self.config.lance_uri,
            table_name=fq_loads_table_name,
            write_disposition=write_disposition,
        )

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        return LanceDBLoadJob(file_path, table)

    def table_exists(self, table_name: str) -> bool:
        return os.path.isdir(self._get_lance_table_uri(table_name))

    @property
    def _known_table_names(self) -> List[str]:
        return [table_name for table_name in self.schema.tables] + [self.sentinel_table_name]

    def _get_lancedb_connection(self) -> lancedb.DBConnection:
        return lancedb.connect(self.config.lance_uri)

    def _get_lance_table_uri(self, table_name: str, is_qualified: bool = False) -> str:
        qualified_table_name = (
            table_name if is_qualified else self.make_qualified_table_name(table_name)
        )
        return _make_lance_table_uri(self.config.lance_uri, qualified_table_name)

    def _get_lance_dataset(self, table_name: str) -> LanceDataset:
        """Returns Lance dataset for the specified table name.

        Raises ValueError if dataset does not exist.
        """
        return lance.dataset(uri=self._get_lance_table_uri(table_name))

    def _is_lance_table(self, path: str) -> bool:
        return path.endswith(".lance") and os.path.isdir(path)

    def _list_lance_tables(self) -> List[str]:
        """Lists all Lance table directories in LanceDB directory."""
        uri = self.config.lance_uri
        try:
            entries = os.listdir(uri)
        except FileNotFoundError:  # directory does not exist (yet)
            return []
        return [
            e.removesuffix(".lance") for e in entries if self._is_lance_table(os.path.join(uri, e))
        ]

    def _list_dlt_lance_tables(self) -> List[str]:
        """Lists all dlt-managed Lance tables in LanceDB directory, including the sentinel table."""
        all_tables = self._list_lance_tables()
        if self.dataset_name:
            prefix = f"{self.dataset_name}{self.config.dataset_separator}"
            return [t for t in all_tables if t.startswith(prefix)]
        else:
            return [t for t in all_tables if t in self._known_table_names]
