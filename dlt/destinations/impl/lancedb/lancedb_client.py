import uuid
from types import TracebackType
from typing import (
    ClassVar,
    List,
    Any,
    cast,
    Union,
    Tuple,
    Iterable,
    Type,
    Optional,
    Dict,
    Sequence,
    TYPE_CHECKING,
)

import lancedb  # type: ignore
import pyarrow as pa
from lancedb import DBConnection
from lancedb.embeddings import EmbeddingFunctionRegistry, TextEmbeddingFunction  # type: ignore
from lancedb.query import LanceQueryBuilder  # type: ignore
from lancedb.table import Table  # type: ignore
from numpy import ndarray
from pyarrow import Array, ChunkedArray, ArrowInvalid

from dlt.common import json, pendulum, logger
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.exceptions import (
    DestinationUndefinedEntity,
    DestinationTransientException,
    DestinationTerminalException,
)
from dlt.common.destination.reference import (
    JobClientBase,
    WithStateSync,
    RunnableLoadJob,
    StorageSchemaInfo,
    StateInfo,
    TLoadJobState,
    LoadJob,
)
from dlt.common.pendulum import timedelta
from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.schema.typing import (
    TColumnType,
    TTableFormat,
    TTableSchemaColumns,
    TWriteDisposition,
)
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.common.storages import FileStorage
from dlt.common.typing import DictStrAny
from dlt.destinations.impl.lancedb.configuration import (
    LanceDBClientConfiguration,
)
from dlt.destinations.impl.lancedb.exceptions import (
    lancedb_error,
)
from dlt.destinations.impl.lancedb.lancedb_adapter import VECTORIZE_HINT
from dlt.destinations.impl.lancedb.schema import (
    make_arrow_field_schema,
    make_arrow_table_schema,
    TArrowSchema,
    NULL_SCHEMA,
    TArrowField,
)
from dlt.destinations.impl.lancedb.utils import (
    list_merge_identifiers,
    generate_uuid,
    set_non_standard_providers_environment_variables,
)
from dlt.destinations.job_impl import FinalizedLoadJobWithFollowupJobs
from dlt.destinations.type_mapping import TypeMapper

if TYPE_CHECKING:
    NDArray = ndarray[Any, Any]
else:
    NDArray = ndarray


TIMESTAMP_PRECISION_TO_UNIT: Dict[int, str] = {0: "s", 3: "ms", 6: "us", 9: "ns"}
UNIT_TO_TIMESTAMP_PRECISION: Dict[str, int] = {v: k for k, v in TIMESTAMP_PRECISION_TO_UNIT.items()}


class LanceDBTypeMapper(TypeMapper):
    sct_to_unbound_dbt = {
        "text": pa.string(),
        "double": pa.float64(),
        "bool": pa.bool_(),
        "bigint": pa.int64(),
        "binary": pa.binary(),
        "date": pa.date32(),
        "complex": pa.string(),
    }

    sct_to_dbt = {}

    dbt_to_sct = {
        pa.string(): "text",
        pa.float64(): "double",
        pa.bool_(): "bool",
        pa.int64(): "bigint",
        pa.binary(): "binary",
        pa.date32(): "date",
    }

    def to_db_decimal_type(
        self, precision: Optional[int], scale: Optional[int]
    ) -> pa.Decimal128Type:
        precision, scale = self.decimal_precision(precision, scale)
        return pa.decimal128(precision, scale)

    def to_db_datetime_type(
        self, precision: Optional[int], table_format: TTableFormat = None
    ) -> pa.TimestampType:
        unit: str = TIMESTAMP_PRECISION_TO_UNIT[self.capabilities.timestamp_precision]
        return pa.timestamp(unit, "UTC")

    def to_db_time_type(
        self, precision: Optional[int], table_format: TTableFormat = None
    ) -> pa.Time64Type:
        unit: str = TIMESTAMP_PRECISION_TO_UNIT[self.capabilities.timestamp_precision]
        return pa.time64(unit)

    def from_db_type(
        self,
        db_type: pa.DataType,
        precision: Optional[int] = None,
        scale: Optional[int] = None,
    ) -> TColumnType:
        if isinstance(db_type, pa.TimestampType):
            return dict(
                data_type="timestamp",
                precision=UNIT_TO_TIMESTAMP_PRECISION[db_type.unit],
                scale=scale,
            )
        if isinstance(db_type, pa.Time64Type):
            return dict(
                data_type="time",
                precision=UNIT_TO_TIMESTAMP_PRECISION[db_type.unit],
                scale=scale,
            )
        if isinstance(db_type, pa.Decimal128Type):
            precision, scale = db_type.precision, db_type.scale
            if (precision, scale) == self.capabilities.wei_precision:
                return cast(TColumnType, dict(data_type="wei"))
            return dict(data_type="decimal", precision=precision, scale=scale)
        return super().from_db_type(db_type, precision, scale)


def upload_batch(
    records: List[DictStrAny],
    /,
    *,
    db_client: DBConnection,
    table_name: str,
    write_disposition: TWriteDisposition,
    id_field_name: Optional[str] = None,
) -> None:
    """Inserts records into a LanceDB table with automatic embedding computation.

    Args:
        records: The data to be inserted as payload.
        db_client: The LanceDB client connection.
        table_name: The name of the table to insert into.
        id_field_name: The name of the ID field for update/merge operations.
        write_disposition: The write disposition - one of 'skip', 'append', 'replace', 'merge'.

    Raises:
        ValueError: If the write disposition is unsupported, or `id_field_name` is not
            provided for update/merge operations.
    """

    try:
        tbl = db_client.open_table(table_name)
        tbl.checkout_latest()
    except FileNotFoundError as e:
        raise DestinationTransientException(
            "Couldn't open lancedb database. Batch WILL BE RETRIED"
        ) from e

    try:
        if write_disposition in ("append", "skip"):
            tbl.add(records)
        elif write_disposition == "replace":
            tbl.add(records, mode="overwrite")
        elif write_disposition == "merge":
            if not id_field_name:
                raise ValueError("To perform a merge update, 'id_field_name' must be specified.")
            tbl.merge_insert(
                id_field_name
            ).when_matched_update_all().when_not_matched_insert_all().execute(records)
        else:
            raise DestinationTerminalException(
                f"Unsupported write disposition {write_disposition} for LanceDB Destination - batch"
                " failed AND WILL **NOT** BE RETRIED."
            )
    except ArrowInvalid as e:
        raise DestinationTerminalException(
            "Python and Arrow datatype mismatch - batch failed AND WILL **NOT** BE RETRIED."
        ) from e


class LanceDBClient(JobClientBase, WithStateSync):
    """LanceDB destination handler."""

    model_func: TextEmbeddingFunction

    def __init__(
        self,
        schema: Schema,
        config: LanceDBClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)
        self.config: LanceDBClientConfiguration = config
        self.db_client: DBConnection = lancedb.connect(
            uri=self.config.credentials.uri,
            api_key=self.config.credentials.api_key,
            read_consistency_interval=timedelta(0),
        )
        self.registry = EmbeddingFunctionRegistry.get_instance()
        self.type_mapper = LanceDBTypeMapper(self.capabilities)
        self.sentinel_table_name = config.sentinel_table_name

        embedding_model_provider = self.config.embedding_model_provider

        # LanceDB doesn't provide a standardized way to set API keys across providers.
        # Some use ENV variables and others allow passing api key as an argument.
        # To account for this, we set provider environment variable as well.
        set_non_standard_providers_environment_variables(
            embedding_model_provider,
            self.config.credentials.embedding_model_provider_api_key,
        )
        # Use the monkey-patched implementation if openai was chosen.
        if embedding_model_provider == "openai":
            from dlt.destinations.impl.lancedb.models import PatchedOpenAIEmbeddings

            self.model_func = PatchedOpenAIEmbeddings(
                max_retries=self.config.options.max_retries,
                api_key=self.config.credentials.api_key,
            )
        else:
            self.model_func = self.registry.get(embedding_model_provider).create(
                name=self.config.embedding_model,
                max_retries=self.config.options.max_retries,
                api_key=self.config.credentials.api_key,
            )

        self.vector_field_name = self.config.vector_field_name
        self.id_field_name = self.config.id_field_name

    @property
    def dataset_name(self) -> str:
        return self.config.normalize_dataset_name(self.schema)

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
        schema_table: Table = self.db_client.open_table(table_name)
        schema_table.checkout_latest()
        schema = schema_table.schema
        return cast(
            TArrowSchema,
            schema,
        )

    @lancedb_error
    def create_table(self, table_name: str, schema: TArrowSchema, mode: str = "create") -> Table:
        """Create a LanceDB Table from the provided LanceModel or PyArrow schema.

        Args:
            schema: The table schema to create.
            table_name: The name of the table to create.
            mode (): The mode to use when creating the table. Can be either "create" or "overwrite".
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
        query_table: Table = self.db_client.open_table(table_name)
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

        If the dataset name was not provided, it deletes all the tables in the current schema.
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

    def _create_sentinel_table(self) -> Table:
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
        super().update_stored_schema(only_tables, expected_update)
        applied_update: TSchemaTables = {}

        try:
            schema_info = self.get_stored_schema_by_hash(self.schema.stored_version_hash)
        except DestinationUndefinedEntity:
            schema_info = None

        if schema_info is None:
            logger.info(
                f"Schema with hash {self.schema.stored_version_hash} "
                "not found in the storage. upgrading"
            )
            self._execute_schema_update(only_tables)
        else:
            logger.info(
                f"Schema with hash {self.schema.stored_version_hash} "
                f"inserted at {schema_info.inserted_at} found "
                "in storage, no upgrade required"
            )
        return applied_update

    def get_storage_table(self, table_name: str) -> Tuple[bool, TTableSchemaColumns]:
        table_schema: TTableSchemaColumns = {}

        try:
            fq_table_name = self.make_qualified_table_name(table_name)

            table: Table = self.db_client.open_table(fq_table_name)
            table.checkout_latest()
            arrow_schema: TArrowSchema = table.schema
        except FileNotFoundError:
            return False, table_schema

        field: TArrowField
        for field in arrow_schema:
            name = self.schema.naming.normalize_identifier(field.name)
            table_schema[name] = {
                "name": name,
                **self.type_mapper.from_db_type(field.type),
            }
        return True, table_schema

    @lancedb_error
    def add_table_fields(
        self, table_name: str, field_schemas: List[TArrowField]
    ) -> Optional[Table]:
        """Add multiple fields to the LanceDB table at once.

        Args:
            table_name: The name of the table to create the fields on.
            field_schemas: The list of fields to create.
        """
        table: Table = self.db_client.open_table(table_name)
        table.checkout_latest()
        arrow_table = table.to_arrow()

        # Check if any of the new fields already exist in the table.
        existing_fields = set(arrow_table.schema.names)
        new_fields = [field for field in field_schemas if field.name not in existing_fields]

        if not new_fields:
            # All fields already present, skip.
            return None

        null_arrays = [pa.nulls(len(arrow_table), type=field.type) for field in new_fields]

        for field, null_array in zip(new_fields, null_arrays):
            arrow_table = arrow_table.append_column(field, null_array)

        try:
            return self.db_client.create_table(table_name, arrow_table, mode="overwrite")
        except OSError:
            # Error occurred while creating the table, skip.
            return None

    def _execute_schema_update(self, only_tables: Iterable[str]) -> None:
        for table_name in only_tables or self.schema.tables:
            exists, existing_columns = self.get_storage_table(table_name)
            new_columns = self.schema.get_new_table_columns(
                table_name,
                existing_columns,
                self.capabilities.generates_case_sensitive_identifiers(),
            )
            embedding_fields: List[str] = get_columns_names_with_prop(
                self.schema.get_table(table_name), VECTORIZE_HINT
            )
            logger.info(f"Found {len(new_columns)} updates for {table_name} in {self.schema.name}")
            if len(new_columns) > 0:
                if exists:
                    field_schemas: List[TArrowField] = [
                        make_arrow_field_schema(column["name"], column, self.type_mapper)
                        for column in new_columns
                    ]
                    fq_table_name = self.make_qualified_table_name(table_name)
                    self.add_table_fields(fq_table_name, field_schemas)
                else:
                    if table_name not in self.schema.dlt_table_names():
                        embedding_fields = get_columns_names_with_prop(
                            self.schema.get_table(table_name=table_name), VECTORIZE_HINT
                        )
                        vector_field_name = self.vector_field_name
                        id_field_name = self.id_field_name
                        embedding_model_func = self.model_func
                        embedding_model_dimensions = self.config.embedding_model_dimensions
                    else:
                        embedding_fields = None
                        vector_field_name = None
                        id_field_name = None
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
                        id_field_name=id_field_name,
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
                self.schema.naming.normalize_identifier("inserted_at"): str(pendulum.now()),
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
        upload_batch(
            records,
            db_client=self.db_client,
            table_name=fq_version_table_name,
            write_disposition=write_disposition,
        )

    @lancedb_error
    def get_stored_state(self, pipeline_name: str) -> Optional[StateInfo]:
        """Retrieves the latest completed state for a pipeline."""
        fq_state_table_name = self.make_qualified_table_name(self.schema.state_table_name)
        fq_loads_table_name = self.make_qualified_table_name(self.schema.loads_table_name)

        state_table_: Table = self.db_client.open_table(fq_state_table_name)
        state_table_.checkout_latest()

        loads_table_: Table = self.db_client.open_table(fq_loads_table_name)
        loads_table_.checkout_latest()

        # normalize property names
        p_load_id = self.schema.naming.normalize_identifier("load_id")
        p_dlt_load_id = self.schema.naming.normalize_identifier("_dlt_load_id")
        p_pipeline_name = self.schema.naming.normalize_identifier("pipeline_name")
        p_status = self.schema.naming.normalize_identifier("status")
        p_version = self.schema.naming.normalize_identifier("version")
        p_engine_version = self.schema.naming.normalize_identifier("engine_version")
        p_state = self.schema.naming.normalize_identifier("state")
        p_created_at = self.schema.naming.normalize_identifier("created_at")
        p_version_hash = self.schema.naming.normalize_identifier("version_hash")

        # Read the tables into memory as Arrow tables, with pushdown predicates, so we pull as less
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

        version_table: Table = self.db_client.open_table(fq_version_table_name)
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

            # LanceDB's ORDER BY clause doesn't seem to work.
            # See https://github.com/dlt-hub/dlt/pull/1375#issuecomment-2171909341
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
    def get_stored_schema(self) -> Optional[StorageSchemaInfo]:
        """Retrieves newest schema from destination storage."""
        fq_version_table_name = self.make_qualified_table_name(self.schema.version_table_name)

        version_table: Table = self.db_client.open_table(fq_version_table_name)
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
                    f'`{p_schema_name}` = "{self.schema.name}"', prefilter=True
                )
            ).to_list()

            # LanceDB's ORDER BY clause doesn't seem to work.
            # See https://github.com/dlt-hub/dlt/pull/1375#issuecomment-2171909341
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
                self.schema.naming.normalize_identifier("load_id"): load_id,
                self.schema.naming.normalize_identifier("schema_name"): self.schema.name,
                self.schema.naming.normalize_identifier("status"): 0,
                self.schema.naming.normalize_identifier("inserted_at"): str(pendulum.now()),
                self.schema.naming.normalize_identifier(
                    "schema_version_hash"
                ): None,  # Payload schema must match the target schema.
            }
        ]
        fq_loads_table_name = self.make_qualified_table_name(self.schema.loads_table_name)
        write_disposition = self.schema.get_table(self.schema.loads_table_name).get(
            "write_disposition"
        )
        upload_batch(
            records,
            db_client=self.db_client,
            table_name=fq_loads_table_name,
            write_disposition=write_disposition,
        )

    def create_load_job(
        self, table: TTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        return LanceDBLoadJob(
            file_path=file_path,
            type_mapper=self.type_mapper,
            model_func=self.model_func,
            fq_table_name=self.make_qualified_table_name(table["name"]),
        )

    def table_exists(self, table_name: str) -> bool:
        return table_name in self.db_client.table_names()


class LanceDBLoadJob(RunnableLoadJob):
    arrow_schema: TArrowSchema

    def __init__(
        self,
        file_path: str,
        type_mapper: LanceDBTypeMapper,
        model_func: TextEmbeddingFunction,
        fq_table_name: str,
    ) -> None:
        super().__init__(file_path)
        self._type_mapper: TypeMapper = type_mapper
        self._fq_table_name: str = fq_table_name
        self._model_func = model_func
        self._job_client: "LanceDBClient" = None

    def run(self) -> None:
        self._db_client: DBConnection = self._job_client.db_client
        self._embedding_model_func: TextEmbeddingFunction = self._model_func
        self._embedding_model_dimensions: int = self._job_client.config.embedding_model_dimensions
        self._id_field_name: str = self._job_client.config.id_field_name

        unique_identifiers: Sequence[str] = list_merge_identifiers(self._load_table)
        write_disposition: TWriteDisposition = cast(
            TWriteDisposition, self._load_table.get("write_disposition", "append")
        )

        with FileStorage.open_zipsafe_ro(self._file_path) as f:
            records: List[DictStrAny] = [json.loads(line) for line in f]

        if self._load_table not in self._schema.dlt_tables():
            for record in records:
                # Add reserved ID fields.
                uuid_id = (
                    generate_uuid(record, unique_identifiers, self._fq_table_name)
                    if unique_identifiers
                    else str(uuid.uuid4())
                )
                record.update({self._id_field_name: uuid_id})

                # LanceDB expects all fields in the target arrow table to be present in the data payload.
                # We add and set these missing fields, that are fields not present in the target schema, to NULL.
                missing_fields = set(self._load_table["columns"]) - set(record)
                for field in missing_fields:
                    record[field] = None

        upload_batch(
            records,
            db_client=self._db_client,
            table_name=self._fq_table_name,
            write_disposition=write_disposition,
            id_field_name=self._id_field_name,
        )
