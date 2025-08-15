from types import TracebackType
from typing import Optional, Sequence, List, Dict, Type, Iterable, Any
import threading

from dlt.common import logger
from dlt.common.json import json
from dlt.common.pendulum import pendulum
from dlt.common.schema import Schema, TSchemaTables
from dlt.common.schema.typing import C_DLT_LOAD_ID, C_DLT_LOADS_TABLE_LOAD_ID
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    loads_table,
    normalize_table_identifiers,
    version_table,
)
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.client import (
    PreparedTableSchema,
    TLoadJobState,
    RunnableLoadJob,
    JobClientBase,
    WithStateSync,
    LoadJob,
)
from dlt.common.destination.exceptions import DestinationUndefinedEntity

from dlt.common.storages import FileStorage
from dlt.common.time import precise_time

from dlt.destinations.job_impl import FinalizedLoadJobWithFollowupJobs
from dlt.destinations.job_client_impl import StorageSchemaInfo, StateInfo

from dlt.destinations.utils import get_pipeline_state_query_columns
from dlt.destinations.impl.qdrant.configuration import QdrantClientConfiguration
from dlt.destinations.impl.qdrant.qdrant_adapter import VECTORIZE_HINT

from qdrant_client import QdrantClient as QC, models
from qdrant_client.qdrant_fastembed import uuid
from qdrant_client.http.exceptions import UnexpectedResponse


class QDrantLoadJob(RunnableLoadJob):
    def __init__(
        self,
        file_path: str,
        client_config: QdrantClientConfiguration,
        collection_name: str,
    ) -> None:
        super().__init__(file_path)
        self._collection_name = collection_name
        self._config = client_config
        self._job_client: "QdrantClient" = None

    def run(self) -> None:
        embedding_fields = get_columns_names_with_prop(self._load_table, VECTORIZE_HINT)
        unique_identifiers = self._list_unique_identifiers(self._load_table)
        with FileStorage.open_zipsafe_ro(self._file_path) as f:
            ids: List[str]
            docs, payloads, ids = [], [], []

            for line in f:
                data = json.loads(line)
                point_id = (
                    self._generate_uuid(data, unique_identifiers, self._collection_name)
                    if unique_identifiers
                    else str(uuid.uuid4())
                )
                payloads.append(data)
                ids.append(point_id)
                if len(embedding_fields) > 0:
                    docs.append(self._get_embedding_doc(data, embedding_fields))

            if len(embedding_fields) > 0:
                embedding_model = self._job_client.db_client._get_or_init_model(
                    self._job_client.db_client.embedding_model_name
                )
                embeddings: List[Any] = list(
                    embedding_model.embed(
                        docs,
                        batch_size=self._config.embedding_batch_size,
                        parallel=self._config.embedding_parallelism,
                    )
                )
                vector_name = self._job_client.db_client.get_vector_field_name()
                embeddings = [{vector_name: embedding.tolist()} for embedding in embeddings]
            else:
                embeddings = [{}] * len(ids)
            assert len(embeddings) == len(payloads) == len(ids)

            self._upload_data(vectors=embeddings, ids=ids, payloads=payloads)

    def _get_embedding_doc(self, data: Dict[str, Any], embedding_fields: List[str]) -> str:
        """Returns a document to generate embeddings for.

        Args:
            data (Dict[str, Any]): A dictionary of data to be loaded.

        Returns:
            str: A concatenated string of all the fields intended for embedding.
        """
        doc = "\n".join(str(data[key]) for key in embedding_fields)
        return doc

    def _list_unique_identifiers(self, table_schema: PreparedTableSchema) -> Sequence[str]:
        """Returns a list of unique identifiers for a table.

        Args:
            table_schema (PreparedTableSchema): a dlt table schema.

        Returns:
            Sequence[str]: A list of unique column identifiers.
        """
        if table_schema.get("write_disposition") == "merge":
            primary_keys = get_columns_names_with_prop(table_schema, "primary_key")
            if primary_keys:
                return primary_keys
        return get_columns_names_with_prop(table_schema, "unique")

    def _upload_data(
        self, ids: Iterable[Any], vectors: Iterable[Any], payloads: Iterable[Any]
    ) -> None:
        """Uploads data to a Qdrant instance in a batch. Supports retries and parallelism.

        Args:
            ids (Iterable[Any]): Point IDs to be uploaded to the collection
            vectors (Iterable[Any]): Embeddings to be uploaded to the collection
            payloads (Iterable[Any]): Payloads to be uploaded to the collection
        """
        self._job_client.db_client.upload_collection(
            self._collection_name,
            ids=ids,
            payload=payloads,
            vectors=vectors,
            parallel=self._config.upload_parallelism,
            batch_size=self._config.upload_batch_size,
            max_retries=self._config.upload_max_retries,
        )

    def _generate_uuid(
        self, data: Dict[str, Any], unique_identifiers: Sequence[str], collection_name: str
    ) -> str:
        """Generates deterministic UUID. Used for deduplication.

        Args:
            data (Dict[str, Any]): Arbitrary data to generate UUID for.
            unique_identifiers (Sequence[str]): A list of unique identifiers.
            collection_name (str): Qdrant collection name.

        Returns:
            str: A string representation of the generated UUID
        """
        data_id = "_".join(str(data[key]) for key in unique_identifiers)
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, collection_name + data_id))


class QdrantClient(JobClientBase, WithStateSync):
    """Qdrant Destination Handler"""

    def __init__(
        self,
        schema: Schema,
        config: QdrantClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)
        # get definitions of the dlt tables, normalize column names and keep for later use
        version_table_ = normalize_table_identifiers(version_table(), schema.naming)
        self.version_collection_properties = list(version_table_["columns"].keys())
        loads_table_ = normalize_table_identifiers(loads_table(), schema.naming)
        self.loads_collection_properties = list(loads_table_["columns"].keys())
        state_table_ = normalize_table_identifiers(
            get_pipeline_state_query_columns(), schema.naming
        )
        self.pipeline_state_properties = list(state_table_["columns"].keys())

        self.config: QdrantClientConfiguration = config
        self.db_client: QC = None
        self.model = config.model

    @property
    def dataset_name(self) -> str:
        return self.config.normalize_dataset_name(self.schema)

    @property
    def sentinel_collection(self) -> str:
        return self.dataset_name or "DltSentinelCollection"

    def _make_qualified_collection_name(self, table_name: str) -> str:
        """Generates a qualified collection name.

        Args:
            table_name (str): Name of the table.

        Returns:
            str: The dataset name and table name concatenated with a separator if dataset name is present.
        """
        dataset_separator = self.config.dataset_separator
        return (
            f"{self.dataset_name}{dataset_separator}{table_name}"
            if self.dataset_name
            else table_name
        )

    def _create_collection(self, full_collection_name: str) -> None:
        """Creates a collection in Qdrant.

        Args:
            full_collection_name (str): The name of the collection to be created.
        """

        # Generates config for a named vector according to the selected model.
        # Eg: vector_config={
        #     "fast-bge-small-en": {
        #       "size": 364,
        #       "distance": "Cosine"
        #     },
        # }
        vectors_config = self.db_client.get_fastembed_vector_params()

        self.db_client.create_collection(
            collection_name=full_collection_name, vectors_config=vectors_config
        )
        # TODO: we can use index hints to create indexes on properties or full text
        # self.db_client.create_payload_index(full_collection_name, "_dlt_load_id", field_type="float")

    def _create_point_no_vector(self, obj: Dict[str, Any], collection_name: str) -> None:
        """Inserts a point into a Qdrant collection without a vector.

        Args:
            obj (Dict[str, Any]): The arbitrary data to be inserted as payload.
            collection_name (str): The name of the collection to insert the point into.
        """
        self.db_client.upsert(
            collection_name,
            points=[
                models.PointStruct(
                    id=str(uuid.uuid4()),
                    payload=obj,
                    vector={},
                )
            ],
        )

    def drop_storage(self) -> None:
        """Drop the dataset from the Qdrant instance.

        Deletes all collections in the dataset and all data associated.
        Deletes the sentinel collection.

        If dataset name was not provided, it deletes all the tables in the current schema
        """
        collections = self.db_client.get_collections().collections
        collection_name_list = [collection.name for collection in collections]

        if self.dataset_name:
            prefix = f"{self.dataset_name}{self.config.dataset_separator}"

            for collection_name in collection_name_list:
                if collection_name.startswith(prefix):
                    self.db_client.delete_collection(collection_name)
        else:
            for collection_name in self.schema.tables.keys():
                if collection_name in collection_name_list:
                    self.db_client.delete_collection(collection_name)

        self._delete_sentinel_collection()

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        if not self.is_storage_initialized():
            self._create_sentinel_collection()
        elif truncate_tables:
            for table_name in truncate_tables:
                qualified_table_name = self._make_qualified_collection_name(table_name=table_name)
                if self._collection_exists(qualified_table_name):
                    continue

                self.db_client.delete_collection(qualified_table_name)
                self._create_collection(full_collection_name=qualified_table_name)

    def is_storage_initialized(self) -> bool:
        return self._collection_exists(self.sentinel_collection, qualify_table_name=False)

    def _create_sentinel_collection(self) -> None:
        """Create an empty collection to indicate that the storage is initialized."""
        self._create_collection(full_collection_name=self.sentinel_collection)

    def _delete_sentinel_collection(self) -> None:
        """Delete the sentinel collection."""
        self.db_client.delete_collection(self.sentinel_collection)

    def update_stored_schema(
        self,
        only_tables: Iterable[str] = None,
        expected_update: TSchemaTables = None,
    ) -> Optional[TSchemaTables]:
        applied_update = super().update_stored_schema(only_tables, expected_update)
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
        # we assume that expected_update == applied_update so table schemas in dest were not
        # externally changed
        return applied_update

    def get_stored_state(self, pipeline_name: str) -> Optional[StateInfo]:
        """Loads compressed state from destination storage
        By finding a load id that was completed
        """
        # normalize property names
        p_load_id = self.schema.naming.normalize_identifier(C_DLT_LOADS_TABLE_LOAD_ID)
        p_dlt_load_id = self.schema.naming.normalize_identifier(C_DLT_LOAD_ID)
        p_pipeline_name = self.schema.naming.normalize_identifier("pipeline_name")
        p_created_at = self.schema.naming.normalize_identifier("created_at")

        limit = 10
        start_from = None
        while True:
            try:
                scroll_table_name = self._make_qualified_collection_name(
                    self.schema.state_table_name
                )
                state_records, offset = self.db_client.scroll(
                    scroll_table_name,
                    with_payload=self.pipeline_state_properties,
                    scroll_filter=models.Filter(
                        must=[
                            models.FieldCondition(
                                key=p_pipeline_name, match=models.MatchValue(value=pipeline_name)
                            )
                        ]
                    ),
                    order_by=models.OrderBy(
                        key=p_created_at,
                        direction=models.Direction.DESC,
                        start_from=start_from,
                    ),
                    limit=limit,
                )
                if len(state_records) == 0:
                    return None
                for state_record in state_records:
                    state = state_record.payload
                    start_from = state[p_created_at]
                    load_id = state[p_dlt_load_id]
                    scroll_table_name = self._make_qualified_collection_name(
                        self.schema.loads_table_name
                    )
                    load_records = self.db_client.count(
                        scroll_table_name,
                        exact=True,
                        count_filter=models.Filter(
                            must=[
                                models.FieldCondition(
                                    key=p_load_id, match=models.MatchValue(value=load_id)
                                )
                            ]
                        ),
                    )
                    if load_records.count == 0:
                        continue
                    return StateInfo.from_normalized_mapping(state, self.schema.naming)
            except UnexpectedResponse as e:
                if e.status_code == 404:
                    raise DestinationUndefinedEntity(str(e)) from e
                raise
            except ValueError as e:  # Local qdrant error
                if "not found" in str(e):
                    raise DestinationUndefinedEntity(str(e)) from e
                raise

    def get_stored_schema(self, schema_name: str = None) -> Optional[StorageSchemaInfo]:
        """Retrieves newest schema from destination storage"""
        try:
            scroll_table_name = self._make_qualified_collection_name(self.schema.version_table_name)
            p_schema_name = self.schema.naming.normalize_identifier("schema_name")
            p_inserted_at = self.schema.naming.normalize_identifier("inserted_at")

            name_filter = (
                models.Filter(
                    must=[
                        models.FieldCondition(
                            key=p_schema_name,
                            match=models.MatchValue(value=schema_name),
                        )
                    ]
                )
                if schema_name
                else None
            )

            response = self.db_client.scroll(
                scroll_table_name,
                with_payload=True,
                scroll_filter=name_filter,
                limit=1,
                order_by=models.OrderBy(
                    key=p_inserted_at,
                    direction=models.Direction.DESC,
                ),
            )
            if not response[0]:
                return None
            payload = response[0][0].payload
            return StorageSchemaInfo.from_normalized_mapping(payload, self.schema.naming)
        except UnexpectedResponse as e:
            if e.status_code == 404:
                raise DestinationUndefinedEntity(str(e)) from e
            raise
        except ValueError as e:  # Local qdrant error
            if "not found" in str(e):
                raise DestinationUndefinedEntity(str(e)) from e
            raise

    def get_stored_schema_by_hash(self, schema_hash: str) -> Optional[StorageSchemaInfo]:
        try:
            scroll_table_name = self._make_qualified_collection_name(self.schema.version_table_name)
            p_version_hash = self.schema.naming.normalize_identifier("version_hash")
            response = self.db_client.scroll(
                scroll_table_name,
                with_payload=True,
                scroll_filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key=p_version_hash, match=models.MatchValue(value=schema_hash)
                        )
                    ]
                ),
                limit=1,
            )
            if not response[0]:
                return None
            payload = response[0][0].payload
            return StorageSchemaInfo.from_normalized_mapping(payload, self.schema.naming)
        except UnexpectedResponse as e:
            if e.status_code == 404:
                return None
            raise
        except ValueError as e:  # Local qdrant error
            if "not found" in str(e):
                return None
            raise

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        return QDrantLoadJob(
            file_path,
            client_config=self.config,
            collection_name=self._make_qualified_collection_name(table["name"]),
        )

    def complete_load(self, load_id: str) -> None:
        values = [load_id, self.schema.name, 0, str(pendulum.now()), self.schema.version_hash]
        assert len(values) == len(self.loads_collection_properties)
        properties = {k: v for k, v in zip(self.loads_collection_properties, values)}
        loads_table_name = self._make_qualified_collection_name(self.schema.loads_table_name)
        self._create_point_no_vector(properties, loads_table_name)

    def __enter__(self) -> "QdrantClient":
        self.db_client = self.config.get_client()
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        if self.db_client:
            self.config.close_client(self.db_client)
            self.db_client = None

    def _update_schema_in_storage(self, schema: Schema) -> None:
        schema_str = json.dumps(schema.to_dict())
        values = [
            schema.version,
            schema.ENGINE_VERSION,
            str(pendulum.now().isoformat()),
            schema.name,
            schema.stored_version_hash,
            schema_str,
        ]
        assert len(values) == len(self.version_collection_properties)
        properties = {k: v for k, v in zip(self.version_collection_properties, values)}
        version_table_name = self._make_qualified_collection_name(self.schema.version_table_name)
        self._create_point_no_vector(properties, version_table_name)

    def _execute_schema_update(self, only_tables: Iterable[str]) -> None:
        is_local = self.config.is_local()
        for table_name in only_tables or self.schema.tables:
            exists = self._collection_exists(table_name)

            qualified_collection_name = self._make_qualified_collection_name(table_name)
            # NOTE: there are no property schemas in qdrant so we do not need to alter
            # existing collections
            if not exists:
                self._create_collection(
                    full_collection_name=qualified_collection_name,
                )
                if not is_local:  # Indexes don't work in local Qdrant (trigger log warning)
                    # Create indexes to enable order_by in state and schema tables
                    if table_name == self.schema.state_table_name:
                        self.db_client.create_payload_index(
                            collection_name=qualified_collection_name,
                            field_name=self.schema.naming.normalize_identifier("created_at"),
                            field_schema="datetime",
                        )
                    elif table_name == self.schema.version_table_name:
                        self.db_client.create_payload_index(
                            collection_name=qualified_collection_name,
                            field_name=self.schema.naming.normalize_identifier("inserted_at"),
                            field_schema="datetime",
                        )

        self._update_schema_in_storage(self.schema)

    def _collection_exists(self, table_name: str, qualify_table_name: bool = True) -> bool:
        try:
            table_name = (
                self._make_qualified_collection_name(table_name)
                if qualify_table_name
                else table_name
            )
            self.db_client.get_collection(table_name)
            return True
        except ValueError as e:
            if "not found" in str(e):
                return False
            raise e
        except UnexpectedResponse as e:
            if e.status_code == 404:
                return False
            raise e
