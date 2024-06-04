import os
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
from dlt.common.destination.reference import (
    JobClientBase,
    WithStateSync,
    LoadJob,
    StorageSchemaInfo,
    StateInfo,
    TLoadJobState,
)
from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.schema.typing import TColumnType
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.common.storages import FileStorage
from dlt.common.typing import DictStrAny
from dlt.destinations.impl.lancedb import capabilities
from dlt.destinations.impl.lancedb.configuration import (
    LanceDBClientConfiguration,
    TEmbeddingProvider,
)
from dlt.destinations.impl.lancedb.lancedb_adapter import VECTORIZE_HINT
from dlt.destinations.impl.lancedb.schema_conversion import (
    TLanceModel,
    create_template_schema,
    make_fields,
)
from dlt.destinations.impl.lancedb.utils import list_unique_identifiers, generate_uuid
from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.type_mapping import TypeMapper


class LanceDBTypeMapper(TypeMapper):
    sct_to_unbound_dbt = {
        "text": pa.string(),
        "double": pa.float64(),
        "bool": pa.bool_(),
        "timestamp": pa.timestamp("us", "UTC"),
        "bigint": pa.int64(),
        "binary": pa.binary(),
        "decimal": pa.decimal128(38, 18),
        "date": pa.date32(),
        "time": pa.time64("us"),
        "complex": pa.string(),
        "wei": pa.float64(),
    }

    sct_to_dbt = {}

    dbt_to_sct = {
        pa.string(): "text",
        pa.float64(): "double",
        pa.bool_(): "bool",
        pa.timestamp("us", "UTC"): "timestamp",
        pa.int64(): "bigint",
        pa.binary(): "binary",
        pa.decimal128(38, 18): "decimal",
        pa.date32(): "date",
        pa.time64("us"): "time",
    }


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


def set_non_standard_providers_environment_variables(
    embedding_model_provider: TEmbeddingProvider, api_key: Union[str, None]
) -> None:
    os.environ[embedding_model_provider.upper()] = api_key or ""


class LanceDBClient(JobClientBase, WithStateSync):
    """LanceDB destination handler."""

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()
    state_properties: ClassVar[List[str]] = [
        "version",
        "engine_version",
        "pipeline_name",
        "state",
        "created_at",
        "_dlt_load_id",
    ]

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
        # If no dataset name is provided, we still want to create a sentinel table.
        return self.dataset_name or "DltSentinelTable"

    def _make_qualified_table_name(self, table_name: str) -> str:
        return (
            f"{self.dataset_name}{self.config.dataset_separator}{table_name}"
            if self.dataset_name
            else table_name
        )

    def get_table_schema(self, table_name: str) -> pa.Schema:
        return cast(pa.Schema, self.db_client[table_name].schema)

    def _create_table(self, table_name: str, schema: Union[pa.Schema, LanceModel]) -> Table:
        """Create a LanceDB Table from the provided LanceModel or PyArrow schema.

        Args:
            schema: The table schema to create.
            table_name: The name of the table to create.
        """

        # TODO: Add embedding_functions configuration to empty table creation.
        return self.db_client.create_table(table_name, schema=schema)

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
        self.db_client.open_table(table_name).add(data, mode, on_bad_vectors, fill_value)

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

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        if not self.is_storage_initialized():
            self._create_sentinel_table()
        elif truncate_tables:
            for table_name in truncate_tables:
                fq_table_name = self._make_qualified_table_name(table_name)
                if not self._table_exists(fq_table_name):
                    continue
                self.db_client.drop_table(fq_table_name)
                self._create_table(
                    table_name=fq_table_name,
                    schema=self.get_table_schema(fq_table_name),
                )

    def is_storage_initialized(self) -> bool:
        return self._table_exists(self.sentinel_table)

    def _create_sentinel_table(self) -> None:
        """Create an empty table to indicate that the storage is initialized."""
        self._create_table(schema=cast(LanceModel, NullSchema), table_name=self.sentinel_table)

    def _delete_sentinel_table(self) -> None:
        """Delete the sentinel table."""
        self.db_client.drop_table(self.sentinel_table)

    def update_stored_schema(
        self,
        only_tables: Iterable[str] = None,
        expected_update: TSchemaTables = None,
    ) -> Optional[TSchemaTables]:
        super().update_stored_schema(only_tables, expected_update)
        applied_update: TSchemaTables = {}
        schema_info = self.get_stored_schema_by_hash(self.schema.stored_version_hash)
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

    def _update_schema_in_storage(self, schema: Schema) -> None:
        properties = {
            "version_hash": schema.stored_version_hash,
            "schema_name": schema.name,
            "version": schema.version,
            "engine_version": schema.ENGINE_VERSION,
            "inserted_at": str(pendulum.now()),
            "schema": json.dumps(schema.to_dict()),
        }
        version_table_name = self._make_qualified_table_name(self.schema.version_table_name)
        self._create_record(properties, VersionSchema, version_table_name)

    def _create_record(
        self, record: DictStrAny, lancedb_model: TLanceModel, table_name: str
    ) -> None:
        """Inserts a record into a LanceDB table without a vector.

        Args:
            record (DictStrAny): The data to be inserted as payload.
            table_name (str): The name of the table to insert the record into.
            lancedb_model (LanceModel): Pydantic model to parse records.
        """
        try:
            tbl = self.db_client.open_table(self._make_qualified_table_name(table_name))
        except FileNotFoundError:
            tbl = self.db_client.create_table(self._make_qualified_table_name(table_name))
        except Exception:
            raise

        tbl.add(lancedb_model(**record))

    def _execute_schema_update(self, only_tables: Iterable[str]) -> None:
        for table_name in only_tables or self.schema.tables:
            exists = self._table_exists(self._make_qualified_table_name(table_name))
            if not exists:
                self._create_table(
                    self._make_qualified_table_name(table_name),
                    schema=cast(LanceModel, NullSchema),
                )
        self._update_schema_in_storage(self.schema)

    def get_stored_state(self, pipeline_name: str) -> Optional[StateInfo]:
        """Loads compressed state from destination storage by finding a load ID that was completed."""
        while True:
            try:
                state_table_name = self._make_qualified_table_name(self.schema.state_table_name)
                state_records = (
                    self.db_client.open_table(state_table_name)
                    .search()
                    .where(f'pipeline_name = "{pipeline_name}"')
                    .limit(10)
                    .to_list()
                )
                if len(state_records) == 0:
                    return None
                for state in state_records:
                    load_id = state["_dlt_load_id"]
                    loads_table_name = self._make_qualified_table_name(self.schema.loads_table_name)
                    load_records = (
                        self.db_client.open_table(loads_table_name)
                        .search()
                        .where(f'load_id = "{load_id}"')
                        .to_list()
                    )
                    if len(load_records) > 0:
                        state["dlt_load_id"] = state.pop("_dlt_load_id")
                        return StateInfo(**state)
            except Exception:
                raise

    def get_stored_schema_by_hash(self, schema_hash: str) -> StorageSchemaInfo:
        try:
            table_name = self._make_qualified_table_name(self.schema.version_table_name)
            response = (
                self.db_client[table_name]
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
            version_table_name = self._make_qualified_table_name(self.schema.version_table_name)
            response = (
                self.db_client[version_table_name]
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
        pass

    def complete_load(self, load_id: str) -> None:
        properties = {
            "load_id": load_id,
            "schema_name": self.schema.name,
            "status": 0,
            "inserted_at": str(pendulum.now()),
        }
        loads_table_name = self._make_qualified_table_name(self.schema.loads_table_name)
        self._create_record(properties, LoadsSchema, loads_table_name)

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
            table_name=self._make_qualified_table_name(table["name"]),
            model_func=self.model_func,
        )

    def _table_exists(self, table_name: str) -> bool:
        return table_name in self.db_client.table_names()

    def _from_db_type(
        self, wt_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_db_type(wt_t, precision, scale)


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
        self.config = client_config
        self.db_client = db_client
        self.type_mapper = type_mapper
        self.table_name = table_name
        self.table_schema: TTableSchema = table_schema
        self.unique_identifiers = list_unique_identifiers(table_schema)
        self.embedding_fields = get_columns_names_with_prop(table_schema, VECTORIZE_HINT)
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

        parsed_records: List[LanceModel] = [lancedb_model(**record) for record in records]

        # Upsert using reserved ID as the key.
        tbl.merge_insert(
            self.id_field_name
        ).when_matched_update_all().when_not_matched_insert_all().execute(parsed_records)

    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()
