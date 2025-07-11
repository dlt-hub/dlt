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
    Sequence,
    TYPE_CHECKING,
)

import lancedb
import lancedb.table
import pyarrow as pa
from lancedb.embeddings import EmbeddingFunctionRegistry, TextEmbeddingFunction
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
    FollowupJobRequest,
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
    is_nested_table,
)
from dlt.common.storages import FileStorage, LoadJobInfo, ParsedLoadJobFileName
from dlt.destinations.impl.lancedb.configuration import (
    LanceDBClientConfiguration,
)
from dlt.destinations.impl.lancedb.exceptions import (
    lancedb_error,
)
from dlt.destinations.impl.lancedb.jobs import LanceDBLoadJob, LanceDBRemoveOrphansJob
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
    set_non_standard_providers_environment_variables,
    write_records,
)
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.sql_jobs import SqlMergeFollowupJob
from dlt.destinations.type_mapping import TypeMapperImpl

if TYPE_CHECKING:
    NDArray = numpy.ndarray[Any, Any]
else:
    NDArray = numpy.ndarray


class LanceDBClient(JobClientBase, WithStateSync):
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
        self.db_client = self.config.credentials.get_conn()
        self.type_mapper = self.capabilities.get_type_mapper()
        self.sentinel_table_name = config.sentinel_table_name
        self.dataset_name = self.config.normalize_dataset_name(self.schema)

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
    def sentinel_table(self) -> str:
        return self.make_qualified_table_name(self.sentinel_table_name)

    def make_qualified_table_name(self, table_name: str) -> str:
        return (
            f"{self.dataset_name}{self.config.dataset_separator}{table_name}"
            if self.dataset_name
            else table_name
        )

    def get_table_schema(self, table_name: str) -> TArrowSchema:
        schema_table: "lancedb.table.Table" = self.db_client.open_table(table_name)
        schema_table.checkout_latest()
        schema = schema_table.schema
        return cast(
            TArrowSchema,
            schema,
        )

    @lancedb_error
    def create_table(
        self, table_name: str, schema: TArrowSchema, mode: str = "create"
    ) -> "lancedb.table.Table":
        """Create a LanceDB Table from the provided LanceModel or PyArrow schema.

        Args:
            schema: The table schema to create.
            table_name: The name of the table to create.
            mode (str): The mode to use when creating the table. Can be either "create" or "overwrite".
                By default, if the table already exists, an exception is raised.
                If you want to overwrite the table, use mode="overwrite".
        """
        return self.db_client.create_table(table_name, schema=schema, mode=mode)

    def delete_table(self, table_name: str) -> None:
        """Delete a LanceDB table.

        Args:
            table_name: The name of the table to delete.
        """
        self.db_client.drop_table(table_name)

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
        query_table: "lancedb.table.Table" = self.db_client.open_table(table_name)
        query_table.checkout_latest()
        return query_table.search(query=query)

    @lancedb_error
    def _get_table_names(self) -> List[str]:
        """Return all tables in the dataset, excluding the sentinel table."""
        if self.dataset_name:
            prefix = f"{self.dataset_name}{self.config.dataset_separator}"
            table_names = [
                table_name
                for table_name in self.db_client.table_names()
                if table_name.startswith(prefix)
            ]
        else:
            table_names = self.db_client.table_names()

        return [table_name for table_name in table_names if table_name != self.sentinel_table]

    @lancedb_error
    def drop_storage(self) -> None:
        """Drop the dataset from the LanceDB instance.

        Deletes all tables in the dataset and all data, as well as sentinel table associated with them.

        If the dataset name wasn't provided, it deletes all the tables in the current schema.
        """
        for table_name in self._get_table_names():
            self.db_client.drop_table(table_name)

        self._delete_sentinel_table()

    @lancedb_error
    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        if not self.is_storage_initialized():
            self._create_sentinel_table()
        elif truncate_tables:
            for table_name in truncate_tables:
                fq_table_name = self.make_qualified_table_name(table_name)
                if not self.table_exists(fq_table_name):
                    continue
                schema = self.get_table_schema(fq_table_name)
                self.db_client.drop_table(fq_table_name)
                self.create_table(
                    table_name=fq_table_name,
                    schema=schema,
                )

    @lancedb_error
    def is_storage_initialized(self) -> bool:
        return self.table_exists(self.sentinel_table)

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
            has_orphan_removal = not load_table.get(NO_REMOVE_ORPHANS_HINT)
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

        return loaded_tables

    def _create_sentinel_table(self) -> "lancedb.table.Table":
        """Create an empty table to indicate that the storage is initialized."""
        return self.create_table(schema=NULL_SCHEMA, table_name=self.sentinel_table)

    def _delete_sentinel_table(self) -> None:
        """Delete the sentinel table."""
        self.db_client.drop_table(self.sentinel_table)

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

    def get_storage_table(self, table_name: str) -> Tuple[bool, TTableSchemaColumns]:
        table_schema: TTableSchemaColumns = {}

        try:
            fq_table_name = self.make_qualified_table_name(table_name)

            table: "lancedb.table.Table" = self.db_client.open_table(fq_table_name)
            table.checkout_latest()
            arrow_schema: TArrowSchema = table.schema
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

    @lancedb_error
    def extend_lancedb_table_schema(self, table_name: str, field_schemas: List[pa.Field]) -> None:
        """Extend LanceDB table schema with empty columns.

        Args:
        table_name: The name of the table to create the fields on.
        field_schemas: The list of PyArrow Fields to create in the target LanceDB table.
        """
        table: "lancedb.table.Table" = self.db_client.open_table(table_name)
        table.checkout_latest()

        # will be fillde with null values and column is nullable by default
        table.add_columns(field_schemas)

        # TODO: Update method below doesn't work for bulk NULL assignments, raise with LanceDB developers.
        # table.update(values={field.name: None})

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
                    fq_table_name = self.make_qualified_table_name(table_name)
                    self.extend_lancedb_table_schema(fq_table_name, field_schemas)
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
                    fq_table_name = self.make_qualified_table_name(table_name)
                    self.create_table(fq_table_name, table_schema)

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
            db_client=self.db_client,
            vector_field_name=self.config.vector_field_name,
            table_name=fq_version_table_name,
            write_disposition=write_disposition,
        )

    @lancedb_error
    def get_stored_state(self, pipeline_name: str) -> Optional[StateInfo]:
        """Retrieves the latest completed state for a pipeline."""
        fq_state_table_name = self.make_qualified_table_name(self.schema.state_table_name)
        fq_loads_table_name = self.make_qualified_table_name(self.schema.loads_table_name)

        state_table_: "lancedb.table.Table" = self.db_client.open_table(fq_state_table_name)
        state_table_.checkout_latest()

        loads_table_: "lancedb.table.Table" = self.db_client.open_table(fq_loads_table_name)
        loads_table_.checkout_latest()

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
        fq_version_table_name = self.make_qualified_table_name(self.schema.version_table_name)

        version_table: "lancedb.table.Table" = self.db_client.open_table(fq_version_table_name)
        version_table.checkout_latest()
        p_version_hash = self.schema.naming.normalize_identifier("version_hash")
        p_inserted_at = self.schema.naming.normalize_identifier("inserted_at")
        p_schema_name = self.schema.naming.normalize_identifier("schema_name")
        p_version = self.schema.naming.normalize_identifier("version")
        p_engine_version = self.schema.naming.normalize_identifier("engine_version")
        p_schema = self.schema.naming.normalize_identifier("schema")

        try:
            schemas = (
                version_table.search().where(
                    f'`{p_version_hash}` = "{schema_hash}"', prefilter=True
                )
            ).to_list()

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
        fq_version_table_name = self.make_qualified_table_name(self.schema.version_table_name)

        version_table: "lancedb.table.Table" = self.db_client.open_table(fq_version_table_name)
        version_table.checkout_latest()
        p_version_hash = self.schema.naming.normalize_identifier("version_hash")
        p_inserted_at = self.schema.naming.normalize_identifier("inserted_at")
        p_schema_name = self.schema.naming.normalize_identifier("schema_name")
        p_version = self.schema.naming.normalize_identifier("version")
        p_engine_version = self.schema.naming.normalize_identifier("engine_version")
        p_schema = self.schema.naming.normalize_identifier("schema")

        try:
            query = version_table.search()
            if schema_name:
                query = query.where(f'`{p_schema_name}` = "{schema_name}"', prefilter=True)
            schemas = query.to_list()

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
            db_client=self.db_client,
            vector_field_name=self.config.vector_field_name,
            table_name=fq_loads_table_name,
            write_disposition=write_disposition,
        )

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
        # Orphan removal is only supported for upsert strategy because we need a deterministic key hash.
        first_table_in_chain = table_chain[0]
        if first_table_in_chain.get(
            "write_disposition"
        ) == "merge" and not first_table_in_chain.get(NO_REMOVE_ORPHANS_HINT):
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

    def table_exists(self, table_name: str) -> bool:
        return table_name in self.db_client.table_names()
