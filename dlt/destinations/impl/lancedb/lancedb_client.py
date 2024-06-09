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
)

import lancedb  # type: ignore
import pyarrow as pa
from lancedb import DBConnection
from lancedb.common import DATA  # type: ignore
from lancedb.embeddings import EmbeddingFunctionRegistry, TextEmbeddingFunction  # type: ignore
from lancedb.pydantic import LanceModel  # type: ignore
from lancedb.query import LanceQueryBuilder  # type: ignore
from lancedb.table import Table  # type: ignore[import-untyped]
from numpy import ndarray
from pyarrow import Array, ChunkedArray
from pydantic import create_model

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
)
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.common.storages import FileStorage
from dlt.common.typing import DictStrAny
from dlt.common.utils import without_none
from dlt.destinations.impl.lancedb import capabilities
from dlt.destinations.impl.lancedb.configuration import (
    LanceDBClientConfiguration,
)
from dlt.destinations.impl.lancedb.exceptions import lancedb_error
from dlt.destinations.impl.lancedb.lancedb_adapter import VECTORIZE_HINT
from dlt.destinations.impl.lancedb.schema import (
    TLanceModel,
    create_template_schema,
    make_fields,
    arrow_schema_to_dict, make_field_schema,
)
from dlt.destinations.impl.lancedb.utils import (
    list_unique_identifiers,
    generate_uuid,
    set_non_standard_providers_environment_variables,
)
from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.type_mapping import TypeMapper


TIMESTAMP_PRECISION_TO_UNIT: Dict[int, str] = {0: "s", 3: "ms", 6: "us", 9: "ns"}
UNIT_TO_TIMESTAMP_PRECISION: Dict[str, int] = {
    v: k for k, v in TIMESTAMP_PRECISION_TO_UNIT.items()
}


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


class NullSchema(LanceModel):
    pass


class VersionSchema(LanceModel):
    version_hash: str
    schema_name: str
    version: int
    engine_version: int
    inserted_at: str
    schema: str


class LoadsSchema(LanceModel):
    load_id: str
    schema_name: str
    status: int
    inserted_at: str


class LanceDBClient(JobClientBase, WithStateSync):
    """LanceDB destination handler."""

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()


    def __init__(self, schema: Schema, config: LanceDBClientConfiguration) -> None:
        super().__init__(schema, config)
        self.config: LanceDBClientConfiguration = config
        self.db_client: DBConnection = lancedb.connect(
            uri=self.config.credentials.uri, api_key=self.config.credentials.api_key
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


    def get_table_schema(self, table_name: str) -> pa.Schema:
        return cast(
            pa.Schema,
            self.db_client.open_table(
                self.make_qualified_table_name(table_name)
            ).schema,
        )


    def get_table_schema_dict(self, table_name: str) -> DictStrAny:
        return arrow_schema_to_dict(
            self.db_client.open_table(self.make_qualified_table_name(table_name)).schema
        )


    @lancedb_error
    def create_table(
        self, table_name: str, schema: Union[pa.Schema, TLanceModel]
    ) -> Table:
        """Create a LanceDB Table from the provided LanceModel or PyArrow schema.

        Args:
            schema: The table schema to create.
            table_name: The name of the table to create.
        """

        # TODO: Add embedding_functions configuration to empty table creation.
        return self.db_client.create_table(
            self.make_qualified_table_name(table_name), schema=schema
        )


    def delete_table(self, table_name: str) -> None:
        """Delete a LanceDB table.

        Args:
            table_name: The name of the table to delete.
        """
        self.db_client.drop_table(self.make_qualified_table_name(table_name))


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
        return self.db_client.open_table(
            self.make_qualified_table_name(table_name)
        ).search(query=query)


    def add_to_table(
        self,
        table_name: str,
        data: DATA,
        mode: str = "append",
        on_bad_vectors: str = "error",
        fill_value: float = 0.0,
    ) -> None:
        """Add more data to the LanceDB Table.

        Args:
            table_name (str): The name of the table to add data to.
            data (DATA): The data to insert into the table.
                         Acceptable types are:
                            - dict or list-of-dict
                            - pandas.DataFrame
                            - pyarrow.Table or pyarrow.RecordBatch
            mode (str): The mode to use when writing the data.
            Valid values are
                "append" and "overwrite".
            on_bad_vectors (str): What to do if any of the vectors are different
                size or contain NaNs.
                One of "error", "drop", "fill".
                Defaults to
                "error".
            fill_value (float): The value to use when filling vectors.
            Only used if
                on_bad_vectors="fill".
                Defaults to 0.0.

        Returns:
            None
        """
        self.db_client.open_table(self.make_qualified_table_name(table_name)).add(
            data, mode, on_bad_vectors, fill_value
        )


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
                self.db_client.drop_table(fq_table_name)
                self.create_table(
                    table_name=table_name,
                    schema=self.get_table_schema(table_name),
                )


    @lancedb_error
    def is_storage_initialized(self) -> bool:
        return self.table_exists(self.sentinel_table)


    def _create_sentinel_table(self) -> None:
        """Create an empty table to indicate that the storage is initialized."""
        self.create_table(
            schema=cast(TLanceModel, NullSchema), table_name=self.sentinel_table
        )


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
            schema_info = self.get_stored_schema_by_hash(
                self.schema.stored_version_hash
            )
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
            table_schema_ = self.get_table_schema_dict(table_name)
        except FileNotFoundError:
            return False, table_schema
        except Exception:
            raise

        for field_name, field_type in table_schema_.items():
            schema_c: TColumnSchema = {
                "name": self.schema.naming.normalize_identifier(field_name),
                **self._from_db_type(field_type, None, None),
            }
            table_schema[field_type] = schema_c
        return True, table_schema

    @lancedb_error
    def add_table_field(self, table_name: str, field_schema: DictStrAny) -> None:
        """Add a field to the LanceDB table.

        Args:
            table_name: The name of the table to create the field on.
            field_schema: The field to create.
        """
        # TODO: Arrow tables are immutable. This is tricky without creating a new table.
        # Perhaps my performing a merge this can work tbl.merge
        # self.db_client.open_table(self.make_qualified_table_name(table_name)).add_columns()

    @lancedb_error
    def _execute_schema_update(self, only_tables: Iterable[str]) -> None:
        for table_name in only_tables or self.schema.tables:
            exists, existing_columns = self.get_storage_table(table_name)
            new_columns = self.schema.get_new_table_columns(
                table_name, existing_columns
            )
            logger.info(
                f"Found {len(new_columns)} updates for {table_name} in {self.schema.name}"
            )
            if new_columns:
                if exists:
                    for column in new_columns:
                        field_schema = make_field_schema(column["name"], column, self.type_mapper)
                        self.add_table_field(table_name, field_schema)
                else:
                    table_schema = self.make_lancedb_table_schema(table_name)
                    self.create_table(table_name, table_schema)
        self.update_schema_in_storage(self.schema)


    @lancedb_error
    def update_schema_in_storage(self, schema: Schema) -> None:
        properties = {
            "version_hash": schema.stored_version_hash,
            "schema_name": schema.name,
            "version": schema.version,
            "engine_version": schema.ENGINE_VERSION,
            "inserted_at": str(pendulum.now()),
            "schema": json.dumps(schema.to_dict()),
        }
        self.create_record(properties, VersionSchema, self.schema.version_table_name)


    @lancedb_error
    def create_record(
        self, record: DictStrAny, lancedb_model: TLanceModel, table_name: str
    ) -> None:
        """Inserts a record into a LanceDB table without a vector.

        Args:
            record (DictStrAny): The data to be inserted as payload.
            table_name (str): The name of the table to insert the record into.
            lancedb_model (LanceModel): Pydantic model to parse records.
        """
        tbl = self.db_client.open_table(self.make_qualified_table_name(table_name))
        tbl.add(lancedb_model(**record))


    def get_stored_state(self, pipeline_name: str) -> Optional[StateInfo]:
        """Loads compressed state from destination storage by finding a load ID that was completed."""
        state_records = (
            self.db_client.open_table(self.schema.state_table_name)
            .search()
            .where(f'pipeline_name = "{pipeline_name}"')
            .to_list()
        )
        if len(state_records) == 0:
            return None
        for state in state_records:
            load_id = state["_dlt_load_id"]
            if (
                self.db_client.open_table(self.schema.loads_table_name)
                    .search()
                    .where(f'load_id = "{load_id}"')
                    .limit(1)
                    .to_list()
            ):
                state["dlt_load_id"] = state.pop("_dlt_load_id")
                return StateInfo(**state)


    def get_stored_schema_by_hash(self, schema_hash: str) -> StorageSchemaInfo:
        try:
            response = (
                self.db_client[
                    self.make_qualified_table_name(self.schema.version_table_name)
                ]
                .search()
                .where(f'version_hash = "{schema_hash}"')
                .limit(1)
            )
            record = response.to_list()[0]
            return StorageSchemaInfo(**record)
        except Exception:
            raise


    def get_stored_schema(self) -> Optional[StorageSchemaInfo]:
        """Retrieves newest schema from destination storage."""
        try:
            response = (
                self.db_client[
                    self.make_qualified_table_name(self.schema.version_table_name)
                ]
                .search()
                .where(f'schema_name = "{self.schema.name}"')
                .limit(1)
            )
            record = response.to_list()[0]
            return StorageSchemaInfo(**record)
        except Exception as e:
            logger.warning(str(e))
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

    def complete_load(self, load_id: str) -> None:
        properties = {
            "load_id": load_id,
            "schema_name": self.schema.name,
            "status": 0,
            "inserted_at": str(pendulum.now()),
        }
        loads_table_name = self.make_qualified_table_name(self.schema.loads_table_name)
        self.create_record(properties, LoadsSchema, loads_table_name)


    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")


    def start_file_load(
        self, table: TTableSchema, file_path: str, load_id: str
    ) -> LoadJob:
        return LoadLanceDBJob(
            self.schema,
            table,
            file_path,
            type_mapper=self.type_mapper,
            db_client=self.db_client,
            client_config=self.config,
            table_name=self.make_qualified_table_name(table["name"]),
            model_func=self.model_func,
        )


    def table_exists(self, table_name: str) -> bool:
        return table_name in self.db_client.table_names()


    def _from_db_type(
        self, wt_t: pa.DataType, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_db_type(cast(pa.DataType, wt_t), precision, scale)


    def make_lancedb_table_schema(self, table_name: str) -> Type[LanceModel]:
        """Creates a LanceDB table schema from dlt table."""
        embedding_fields = get_columns_names_with_prop(
            self.schema.get_table(table_name=table_name), VECTORIZE_HINT
        )

        template_model: TLanceModel = create_template_schema(
            None,
            "vector__" if embedding_fields else None,
            embedding_fields or None,
            self.model_func,
            self.config.embedding_model_dimensions,
        )

        field_types: DictStrAny = {
            k: v
            for d in make_fields(
                self.make_qualified_table_name(table_name),
                schema=self.schema,
                type_mapper=self.type_mapper,
                embedding_fields=embedding_fields or None,
                embedding_model_func=self.model_func,
            )
            for k, v in d.items()
        }

        return create_model(
            table_name,
            __base__=template_model,
            __module__=__name__,
            **field_types,
        )


class LoadLanceDBJob(LoadJob):
    embedding_fields: List[str]
    embedding_model_func: TextEmbeddingFunction


    def __init__(
        self,
        schema: Schema,
        table_schema: TTableSchema,
        local_path: str,
        type_mapper: LanceDBTypeMapper,
        db_client: DBConnection,
        client_config: LanceDBClientConfiguration,
        table_name: str,
        model_func: TextEmbeddingFunction,
    ) -> None:
        file_name = FileStorage.get_file_name_from_file_path(local_path)
        super().__init__(file_name)
        self.schema = schema
        self.db_client = db_client
        self.type_mapper = type_mapper
        self.table_name = table_name
        self.unique_identifiers = list_unique_identifiers(table_schema)
        self.embedding_fields = get_columns_names_with_prop(
            table_schema, VECTORIZE_HINT
        )
        self.embedding_model_func = model_func
        self.embedding_model_dimensions = client_config.embedding_model_dimensions

        # We reserve two field names `id__` and `vector__` to store vector embeddings and record IDs respectively.
        # TODO: Make these field configurable.
        self.vector_field_name = "vector__"
        self.id_field_name = "id__"

        with FileStorage.open_zipsafe_ro(local_path) as f:
            records: List[DictStrAny] = json.load(f)

        for record in records:
            uuid_id = (
                generate_uuid(record, self.unique_identifiers, self.table_name)
                if self.unique_identifiers
                else uuid.uuid4()
            )
            record.update({self.id_field_name: uuid_id})

        template_model: TLanceModel = create_template_schema(
            self.id_field_name,
            self.vector_field_name,
            self.embedding_fields,
            self.embedding_model_func,
            self.embedding_model_dimensions,
        )

        field_types: DictStrAny = {
            k: v
            for d in make_fields(
                self.table_name,
                schema=self.schema,
                type_mapper=self.type_mapper,
                embedding_fields=self.embedding_fields,
                embedding_model_func=self.embedding_model_func,
            )
            for k, v in d.items()
        }

        lance_model: TLanceModel = create_model(
            self.table_name,
            __base__=template_model,
            __module__=__name__,
            **field_types,
        )

        self.upload_data(records, lance_model, table_name)


    def upload_data(
        self, records: List[DictStrAny], lancedb_model: TLanceModel, table_name: str
    ) -> None:
        """Inserts records into a LanceDB table.
        Embeddings are automatically computed according to `lancedb_model`.

        Args:
            records (List[DictStrAny]): The data to be inserted as payload.
            table_name (str): The name of the table to insert the record into.
            lancedb_model (LanceModel): Pydantic model to parse records.
        """
        try:
            tbl = self.db_client.open_table(table_name)
        except FileNotFoundError:
            tbl = self.db_client.create_table(table_name)
        except Exception:
            raise

        parsed_records: List[LanceModel] = [
            lancedb_model(**record) for record in records
        ]

        # Upsert using reserved ID as the key.
        tbl.merge_insert(
            self.id_field_name
        ).when_matched_update_all().when_not_matched_insert_all().execute(
            parsed_records
        )


    def state(self) -> TLoadJobState:
        return "completed"


    def exception(self) -> str:
        raise NotImplementedError()
