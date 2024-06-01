import uuid
from types import TracebackType
from typing import ClassVar, List, Any, cast, Union, Tuple, Iterable, Type, Optional, Dict, Sequence

import lancedb
import pyarrow as pa
from lancedb import DBConnection
from lancedb.common import DATA
from lancedb.embeddings import EmbeddingFunctionRegistry, TextEmbeddingFunction
from lancedb.pydantic import LanceModel
from lancedb.query import LanceQueryBuilder
from numpy import ndarray
from pyarrow import Array, ChunkedArray

from dlt.common import json, pendulum, logger
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (JobClientBase, WithStateSync, LoadJob, StorageSchemaInfo, StateInfo,
                                              TLoadJobState, )
from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.common.storages import FileStorage
from dlt.common.typing import DictStrAny
from dlt.destinations.impl.lancedb import capabilities
from dlt.destinations.impl.lancedb.configuration import LanceDBClientConfiguration
from dlt.destinations.impl.lancedb.lancedb_adapter import VECTORIZE_HINT
from dlt.destinations.job_impl import EmptyLoadJob


TLanceModel = Type[LanceModel]


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
    state_properties: ClassVar[List[str]] = ["version", "engine_version", "pipeline_name", "state", "created_at", "_dlt_load_id", ]


    def __init__(self, schema: Schema, config: LanceDBClientConfiguration) -> None:
        super().__init__(schema, config)
        self.config: LanceDBClientConfiguration = config
        self.db_client: DBConnection = lancedb.connect(**(dict(self.config.credentials)), **(dict(self.config.options)))
        self.registry = EmbeddingFunctionRegistry.get_instance()
        # We let dlt handles retries.
        self.model_func: TextEmbeddingFunction = self.registry.get(self.config.provider).create(name=self.config.embedding_model, max_retries=1)


    @property
    def dataset_name(self) -> str:
        return self.config.normalize_dataset_name(self.schema)


    @property
    def sentinel_table(self) -> str:
        # If no dataset name is provided, we still want to create a sentinel table.
        return self.dataset_name or "DltSentinelTable"


    def _make_qualified_table_name(self, table_name: str) -> str:
        return f"{self.dataset_name}{self.config.dataset_separator}{table_name}" if self.dataset_name else table_name


    def get_table_schema(self, table_name: str) -> pa.Schema:
        return cast(pa.Schema, self.db_client[table_name].schema)


    def _create_table(self, table_name: str, schema: Union[pa.Schema, LanceModel]) -> None:
        """Create a LanceDB Table from the provided LanceModel or PyArrow schema.

        Args:
            schema: The table schema to create.
            table_name: The name of the table to create.
        """

        self.db_client.create_table(table_name, schema=schema, embedding_functions=self.model_func)


    def delete_table(self, table_name: str) -> None:
        """Delete a LanceDB table.

        Args:
            table_name: The name of the table to delete.
        """
        self.db_client.drop_table(table_name)


    def delete_all_tables(self) -> None:
        """Delete all LanceDB tables from the LanceDB instance and all data associated with it."""
        self.db_client.drop_database()


    def query_table(self, table_name: str, query: Union[List[Any], ndarray[Any, Any], Array, ChunkedArray, str, Tuple[Any], None] = None) -> LanceQueryBuilder:
        """Query a LanceDB table.

        Args:
            table_name: The name of the table to query.
            query: The targeted vector to search for.

        Returns:
            A LanceDB query builder.
        """
        return self.db_client.open_table(table_name).search(query=query)


    def add_to_table(self, table_name: str, data: DATA, mode: str = "append", on_bad_vectors: str = "error", fill_value: float = 0.0) -> None:
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
            table_names = [table_name for table_name in self.db_client.table_names() if table_name.startswith(prefix)]
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
                self._create_table(table_name=fq_table_name, schema=self.get_table_schema(fq_table_name))


    def is_storage_initialized(self) -> bool:
        return self._table_exists(self.sentinel_table)


    def _create_sentinel_table(self) -> None:
        """Create an empty table to indicate that the storage is initialized."""
        self._create_table(schema=cast(LanceModel, NullSchema), table_name=self.sentinel_table)


    def _delete_sentinel_table(self) -> None:
        """Delete the sentinel table."""
        self.db_client.drop_table(self.sentinel_table)


    def update_stored_schema(self, only_tables: Iterable[str] = None, expected_update: TSchemaTables = None, ) -> Optional[TSchemaTables]:
        super().update_stored_schema(only_tables, expected_update)
        applied_update: TSchemaTables = {}
        schema_info = self.get_stored_schema_by_hash(self.schema.stored_version_hash)
        if schema_info is None:
            logger.info(f"Schema with hash {self.schema.stored_version_hash} "
                        "not found in the storage. upgrading")
            self._execute_schema_update(only_tables)
        else:
            logger.info(f"Schema with hash {self.schema.stored_version_hash} "
                        f"inserted at {schema_info.inserted_at} found "
                        "in storage, no upgrade required")
        return applied_update


    def _update_schema_in_storage(self, schema: Schema) -> None:
        properties = {'version_hash': schema.stored_version_hash, 'schema_name': schema.name, 'version': schema.version, 'engine_version': schema.ENGINE_VERSION, 'inserted_at': str(pendulum.now()),
                      'schema': json.dumps(schema.to_dict())}
        version_table_name = self._make_qualified_table_name(self.schema.version_table_name)
        self._create_record(properties, VersionSchema, version_table_name)


    def _create_record(self, record: DictStrAny, lancedb_model: TLanceModel, table_name: str) -> None:
        """Inserts a record into a LanceDB table without a vector.

        Args:
            record (DictStrAny): The data to be inserted as payload.
            table_name (str): The name of the table to insert the record into.
            lancedb_model (LanceModel): Pydantic model to map data onto.
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
                self._create_table(self._make_qualified_table_name(table_name), schema=cast(LanceModel, NullSchema))
        self._update_schema_in_storage(self.schema)


    def get_stored_state(self, pipeline_name: str) -> Optional[StateInfo]:
        """Loads compressed state from destination storage by finding a load ID that was completed."""
        while True:
            try:
                state_table_name = self._make_qualified_table_name(self.schema.state_table_name)
                state_records = self.db_client[state_table_name].search(query_type="fts").where(f"pipeline_name = {pipeline_name}").limit(10).to_list()
                if len(state_records) == 0:
                    return None
                for state in state_records:
                    load_id = state["_dlt_load_id"]
                    loads_table_name = self._make_qualified_table_name(self.schema.loads_table_name)
                    load_records = self.db_client[loads_table_name].search(query_type="fts").where(f"load_id = {load_id}").to_list()
                    if len(load_records) > 0:
                        state["dlt_load_id"] = state.pop("_dlt_load_id")
                        return StateInfo(**state)
            except Exception as e:
                logger.warning(str(e))
                return None


    def get_stored_schema_by_hash(self, schema_hash: str) -> StorageSchemaInfo:
        try:
            table_name = self._make_qualified_table_name(self.schema.version_table_name)
            response = self.db_client[table_name].search(query_type="fts").where(f"version_hash = {schema_hash}").limit(1)
            record = response.to_list()[0]
            return StorageSchemaInfo(**record)
        except Exception as e:
            logger.warning(str(e))
            return None


    def get_stored_schema(self) -> Optional[StorageSchemaInfo]:
        """Retrieves newest schema from destination storage."""
        try:
            version_table_name = self._make_qualified_table_name(self.schema.version_table_name)
            response = self.db_client[version_table_name].search(query_type="fts").where(f"schema_name = {self.schema.name}").limit(1)
            record = response.to_list()[0]
            return StorageSchemaInfo(**record)
        except Exception as e:
            logger.warning(str(e))
            return None


    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        pass


    def __enter__(self) -> "LanceDBClient":
        pass


    def complete_load(self, load_id: str) -> None:
        properties = {"load_id": load_id, "schema_name": self.schema.name, "status": 0, "inserted_at": str(pendulum.now()), }
        loads_table_name = self._make_qualified_table_name(self.schema.loads_table_name)
        self._create_record(properties, LoadsSchema, loads_table_name)


    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")


    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        return LoadLanceDBJob(table, file_path, db_client=self.db_client, client_config=self.config, table_name=self._make_qualified_table_name(table["name"]), model_func=self.model_func)


    def _table_exists(self, table_name: str) -> bool:
        return table_name in self.db_client.table_names()


class LoadLanceDBJob(LoadJob):
    def __init__(self, table_schema: TTableSchema, local_path: str, db_client: DBConnection, client_config: LanceDBClientConfiguration, table_name: str, model_func: TextEmbeddingFunction) -> None:
        file_name = FileStorage.get_file_name_from_file_path(local_path)
        super().__init__(file_name)
        self.config = client_config
        self.db_client = db_client
        self.collection_name = table_name
        self.unique_identifiers = self._list_unique_identifiers(table_schema)
        self.embedding_fields = get_columns_names_with_prop(table_schema, VECTORIZE_HINT)
        self.embedding_model_func = model_func
        self.embedding_model_dimensions = client_config.embedding_model_dimensions

        with FileStorage.open_zipsafe_ro(local_path) as f:
            docs, payloads, ids = [], [], []

            for line in f:
                data = json.loads(line)
                point_id = (self._generate_uuid(data, self.unique_identifiers, self.collection_name) if self.unique_identifiers else uuid.uuid4())
                embedding_doc = self._get_embedding_doc(data)
                payloads.append(data)
                ids.append(point_id)
                docs.append(embedding_doc)

            embedding_model = db_client._get_or_init_model(db_client.embedding_model_name)
            embeddings = list(embedding_model.embed(docs, batch_size=self.config.embedding_batch_size, parallel=self.config.embedding_parallelism, ))
            vector_name = db_client.get_vector_field_name()
            embeddings = [{vector_name: embedding.tolist()} for embedding in embeddings]
            assert len(embeddings) == len(payloads) == len(ids)

            self._upload_data(vectors=embeddings, ids=ids, payloads=payloads)


    def _get_embedding_doc(self, data: Dict[str, Any]) -> str:
        """Returns a document to generate embeddings for.

        Args:
            data (Dict[str, Any]): A dictionary of data to be loaded.

        Returns:
            str: A concatenated string of all the fields intended for embedding.
        """
        return "\n".join(str(data[key]) for key in self.embedding_fields)


    def _list_unique_identifiers(self, table_schema: TTableSchema) -> Sequence[str]:
        """Returns a list of unique identifiers for a table.

        Args:
            table_schema (TTableSchema): a dlt table schema.

        Returns:
            Sequence[str]: A list of unique column identifiers.
        """
        if table_schema.get("write_disposition") == "merge":
            if primary_keys := get_columns_names_with_prop(table_schema, "primary_key"):
                return primary_keys
        return get_columns_names_with_prop(table_schema, "unique")


    def _upload_data(self, ids: Iterable[Any], vectors: Iterable[Any], payloads: Iterable[Any]) -> None:
        """Uploads data to a Qdrant instance in a batch. Supports retries and parallelism.

        Args:
            ids (Iterable[Any]): Point IDs to be uploaded to the collection
            vectors (Iterable[Any]): Embeddings to be uploaded to the collection
            payloads (Iterable[Any]): Payloads to be uploaded to the collection
        """
        self.db_client.upload_collection(self.collection_name, ids=ids, payload=payloads, vectors=vectors, parallel=self.config.upload_parallelism, batch_size=self.config.upload_batch_size,
                                         max_retries=self.config.upload_max_retries, )


    def _generate_uuid(self, data: DictStrAny, unique_identifiers: Sequence[str], collection_name: str) -> str:
        """Generates deterministic UUID. Used for deduplication.

        Args:
            data (Dict[str, Any]): Arbitrary data to generate UUID for.
            unique_identifiers (Sequence[str]): A list of unique identifiers.
            collection_name (str): Qdrant collection name.

        Returns:
            str: A string representation of the genrated UUID
        """
        data_id = "_".join(str(data[key]) for key in unique_identifiers)
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, collection_name + data_id))


    def state(self) -> TLoadJobState:
        return "completed"


    def exception(self) -> str:
        raise NotImplementedError()
