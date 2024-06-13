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
)

import lancedb  # type: ignore
import pyarrow as pa
from lancedb import DBConnection
from lancedb.embeddings import EmbeddingFunctionRegistry, TextEmbeddingFunction  # type: ignore
from lancedb.query import LanceQueryBuilder  # type: ignore
from lancedb.table import Table  # type: ignore[import-untyped]
from numpy import ndarray
from orjson import JSONDecodeError
from pyarrow import Array, ChunkedArray, ArrowInvalid

from dlt.common import json, pendulum, logger
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.common.destination.reference import (
    JobClientBase,
    WithStateSync,
    LoadJob,
    StorageSchemaInfo,
    StateInfo,
    TLoadJobState,
)
from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.schema.typing import (
    TColumnType,
    TTableFormat,
    TTableSchemaColumns,
    TColumnSchema,
    TWriteDisposition,
)
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.common.storages import FileStorage
from dlt.common.typing import DictStrAny
from dlt.common.utils import without_none
from dlt.destinations.impl.lancedb import capabilities
from dlt.destinations.impl.lancedb.configuration import (
    LanceDBClientConfiguration,
)
from dlt.destinations.impl.lancedb.exceptions import (
    lancedb_error,
    lancedb_batch_error,
    LanceDBBatchError,
)
from dlt.destinations.impl.lancedb.lancedb_adapter import VECTORIZE_HINT
from dlt.destinations.impl.lancedb.schema import (
    arrow_schema_to_dict,
    make_arrow_field_schema,
    make_arrow_table_schema,
    TArrowSchema,
    NULL_SCHEMA,
    TArrowField,
)
from dlt.destinations.impl.lancedb.utils import (
    list_unique_identifiers,
    generate_uuid,
    set_non_standard_providers_environment_variables,
)
from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.type_mapping import TypeMapper


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
        return cast(
            TColumnType,
            without_none(
                dict(
                    data_type=self.dbt_to_sct.get(db_type, "text"),
                    precision=precision,
                    scale=scale,
                )
            ),
        )


@lancedb_batch_error
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
    except FileNotFoundError as e:
        raise LanceDBBatchError(e) from e

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
            raise ValueError(
                f"Unsupported write disposition {write_disposition} for LanceDB Destination."
            )
    except ArrowInvalid as e:
        raise LanceDBBatchError(e) from e
    except Exception:
        raise


class LanceDBClient(JobClientBase, WithStateSync):
    """LanceDB destination handler."""

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: LanceDBClientConfiguration) -> None:
        super().__init__(schema, config)
        self.config: LanceDBClientConfiguration = config
        self.db_client: DBConnection = lancedb.connect(
            uri=self.config.credentials.uri,
            api_key=self.config.credentials.api_key,
        )
        self.registry = EmbeddingFunctionRegistry.get_instance()
        self.type_mapper = LanceDBTypeMapper(self.capabilities)

        # LanceDB doesn't provide a standardized way to set API keys across providers.
        # Some use ENV variables and others allow passing api key as an argument.
        # To account for this, we set provider environment variable as well.
        set_non_standard_providers_environment_variables(
            self.config.embedding_model_provider,
            self.config.credentials.embedding_model_provider_api_key,
        )
        self.model_func: TextEmbeddingFunction = self.registry.get(
            self.config.embedding_model_provider
        ).create(
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
        return self.make_qualified_table_name("dltSentinelTable")

    def make_qualified_table_name(self, table_name: str) -> str:
        return (
            f"{self.dataset_name}{self.config.dataset_separator}{table_name}"
            if self.dataset_name
            else table_name
        )

    def get_table_schema(self, table_name: str) -> TArrowSchema:
        schema = self.db_client.open_table(table_name).schema
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

    def delete_all_tables(self) -> None:
        """Delete all LanceDB tables from the LanceDB instance and all data associated with it."""
        self.db_client.drop_database()

    def query_table(
        self,
        table_name: str,
        query: Union[
            List[Any], ndarray[Any, Any], Array, ChunkedArray, str, Tuple[Any], None
        ] = None,
    ) -> LanceQueryBuilder:
        """Query a LanceDB table.

        Args:
            table_name: The name of the table to query.
            query: The targeted vector to search for.

        Returns:
            A LanceDB query builder.
        """
        return self.db_client.open_table(table_name).search(query=query)

    @lancedb_error
    def drop_storage(self) -> None:
        """Drop the dataset from the LanceDB instance.

        Deletes all tables in the dataset and all data, as well as sentinel table associated with them.

        If the dataset name was not provided, it deletes all the tables in the current schema.
        """
        if self.dataset_name:
            prefix = f"{self.dataset_name}{self.config.dataset_separator}"
            table_names = [
                table_name
                for table_name in self.db_client.table_names()
                if table_name.startswith(prefix)
            ]
        else:
            table_names = self.db_client.table_names()

        for table_name in table_names:
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
            arrow_schema = self.db_client.open_table(fq_table_name).schema
        except FileNotFoundError:
            return False, table_schema
        except Exception:
            raise

        for field_name, field_type in arrow_schema_to_dict(arrow_schema).items():
            schema_c: TColumnSchema = {
                "name": self.schema.naming.normalize_identifier(field_name),
                **self._from_db_type(field_type, None, None),
            }
            table_schema[field_type] = schema_c
        return True, table_schema

    @lancedb_error
    def add_table_field(self, table_name: str, field_schema: TArrowField) -> Optional[Table]:
        """Add a field to the LanceDB table.

        Since arrow tables are immutable, this is done via a staging mechanism.
        The data is stored in-memory in a staging arrow table, evolved then stored
        written over the old table.

        Args:
            table_name: The name of the table to create the field on.
            field_schema: The field to create.
        """
        arrow_table = self.db_client.open_table(table_name).to_arrow()

        # Create an array of null values for the new column.
        null_array = pa.nulls(len(arrow_table), type=field_schema.type)

        # Create staging Table with new column appended.
        stage = arrow_table.append_column(field_schema, null_array)

        try:
            return self.db_client.create_table(table_name, stage, mode="overwrite")
        except OSError:
            # Field already present, skip.
            return None
        except Exception:
            raise

    def _execute_schema_update(self, only_tables: Iterable[str]) -> None:
        for table_name in only_tables or self.schema.tables:
            exists, existing_columns = self.get_storage_table(table_name)
            new_columns = self.schema.get_new_table_columns(table_name, existing_columns)
            embedding_fields: List[str] = get_columns_names_with_prop(
                self.schema.get_table(table_name), VECTORIZE_HINT
            )
            logger.info(f"Found {len(new_columns)} updates for {table_name} in {self.schema.name}")
            if len(new_columns) > 0:
                if exists:
                    for column in new_columns:
                        field_schema: TArrowField = make_arrow_field_schema(
                            column["name"], column, self.type_mapper, embedding_fields
                        )
                        fq_table_name = self.make_qualified_table_name(table_name)
                        self.add_table_field(fq_table_name, field_schema)
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
                "version": self.schema.version,
                "engine_version": self.schema.ENGINE_VERSION,
                "inserted_at": str(pendulum.now()),
                "schema_name": self.schema.name,
                "version_hash": self.schema.stored_version_hash,
                "schema": json.dumps(self.schema.to_dict()),
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
        """Loads compressed state from destination storage by finding a load ID that was completed."""
        fq_state_table_name = self.make_qualified_table_name(self.schema.state_table_name)
        fq_loads_table_name = self.make_qualified_table_name(self.schema.loads_table_name)

        state_records = (
            self.db_client.open_table(fq_state_table_name)
            .search()
            .where(f'pipeline_name = "{pipeline_name}" ORDER BY _dlt_load_id DESC')
            .to_list()
        )
        if len(state_records) == 0:
            return None
        for state in state_records:
            load_id = state["_dlt_load_id"]
            # If there is a load for this state which was successful, return the state.
            if (
                self.db_client.open_table(fq_loads_table_name)
                .search()
                .where(f'load_id = "{load_id}"')
                .limit(1)
                .to_list()
            ):
                state["dlt_load_id"] = state.pop("_dlt_load_id")
                return StateInfo(**{k: v for k, v in state.items() if k in StateInfo._fields})
        return None

    @lancedb_error
    def get_stored_schema_by_hash(self, schema_hash: str) -> Optional[StorageSchemaInfo]:
        fq_version_table_name = self.make_qualified_table_name(self.schema.version_table_name)

        try:
            response = (
                self.db_client.open_table(fq_version_table_name)
                .search()
                .where(f'version_hash = "{schema_hash}" ORDER BY inserted_at DESC')
                .limit(1)
            )
            record = response.to_list()[0]
            return StorageSchemaInfo(**record)
        except IndexError:
            return None

    @lancedb_error
    def get_stored_schema(self) -> Optional[StorageSchemaInfo]:
        """Retrieves newest schema from destination storage."""
        fq_version_table_name = self.make_qualified_table_name(self.schema.version_table_name)

        try:
            response = (
                self.db_client.open_table(fq_version_table_name)
                .search()
                .where(f'schema_name = "{self.schema.name}" ORDER BY inserted_at DESC')
                .limit(1)
            )
            record = response.to_list()[0]
            return StorageSchemaInfo(**record)
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
                "load_id": load_id,
                "schema_name": self.schema.name,
                "status": 0,
                "inserted_at": str(pendulum.now()),
                "schema_version_hash": None,  # Payload schema must match the target schema.
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

    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        return LoadLanceDBJob(
            self.schema,
            table,
            file_path,
            type_mapper=self.type_mapper,
            db_client=self.db_client,
            client_config=self.config,
            model_func=self.model_func,
            fq_table_name=self.make_qualified_table_name(table["name"]),
        )

    def table_exists(self, table_name: str) -> bool:
        return table_name in self.db_client.table_names()

    def _from_db_type(
        self, wt_t: pa.DataType, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_db_type(cast(pa.DataType, wt_t), precision, scale)


class LoadLanceDBJob(LoadJob):
    arrow_schema: TArrowSchema

    def __init__(
        self,
        schema: Schema,
        table_schema: TTableSchema,
        local_path: str,
        type_mapper: LanceDBTypeMapper,
        db_client: DBConnection,
        client_config: LanceDBClientConfiguration,
        model_func: TextEmbeddingFunction,
        fq_table_name: str,
    ) -> None:
        file_name = FileStorage.get_file_name_from_file_path(local_path)
        super().__init__(file_name)
        self.schema: Schema = schema
        self.table_schema: TTableSchema = table_schema
        self.db_client: DBConnection = db_client
        self.type_mapper: TypeMapper = type_mapper
        self.table_name: str = table_schema["name"]
        self.fq_table_name: str = fq_table_name
        self.unique_identifiers: Sequence[str] = list_unique_identifiers(table_schema)
        self.embedding_fields: List[str] = get_columns_names_with_prop(table_schema, VECTORIZE_HINT)
        self.embedding_model_func: TextEmbeddingFunction = model_func
        self.embedding_model_dimensions: int = client_config.embedding_model_dimensions
        self.id_field_name: str = client_config.id_field_name
        self.write_disposition: TWriteDisposition = cast(
            TWriteDisposition, self.table_schema.get("write_disposition", "append")
        )

        records: List[DictStrAny]
        with FileStorage.open_zipsafe_ro(local_path) as f:
            try:
                records = json.load(f)
            except JSONDecodeError:
                # If parsing as a single object fails, try a line at a time.
                records = []
                f.seek(0)
                for line in f:
                    try:
                        json_object = json.loads(line)
                        records.append(json_object)
                    except JSONDecodeError:
                        raise
            except Exception:
                raise

        # Batch load only accepts a list of dicts.
        if not isinstance(records, list) and isinstance(records, dict):
            records = [records]

        if self.table_schema not in self.schema.dlt_tables():
            for record in records:
                # Add reserved ID fields.
                uuid_id = (
                    generate_uuid(record, self.unique_identifiers, self.fq_table_name)
                    if self.unique_identifiers
                    else str(uuid.uuid4())
                )
                record.update({self.id_field_name: uuid_id})

                # LanceDB expects all fields in the target arrow table to be present in the data payload.
                # We add and set these missing fields, that are fields not present in the target schema, to NULL.
                missing_fields = set(self.table_schema["columns"]) - set(record)
                for field in missing_fields:
                    record[field] = None

        upload_batch(
            records,
            db_client=db_client,
            table_name=self.fq_table_name,
            write_disposition=self.write_disposition,
            id_field_name=self.id_field_name,
        )

    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()
