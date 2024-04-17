from types import TracebackType
from typing import ClassVar, Optional, Sequence, List, Dict, Type, Iterable, Any, IO

from dlt.common import logger
from dlt.common.json import json
from dlt.common.pendulum import pendulum
from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import TLoadJobState, LoadJob, JobClientBase, WithStateSync
from dlt.common.storages import FileStorage

from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.job_client_impl import StorageSchemaInfo, StateInfo

from dlt.destinations.impl.qdrant import capabilities
from dlt.destinations.impl.qdrant.configuration import QdrantClientConfiguration
from dlt.destinations.impl.qdrant.qdrant_adapter import VECTORIZE_HINT

from qdrant_client import QdrantClient as QC, models
from qdrant_client.qdrant_fastembed import uuid
from qdrant_client.http.exceptions import UnexpectedResponse


class LoadQdrantJob(LoadJob):
    def __init__(
        self,
        table_schema: TTableSchema,
        local_path: str,
        db_client: QC,
        client_config: QdrantClientConfiguration,
        collection_name: str,
    ) -> None:
        file_name = FileStorage.get_file_name_from_file_path(local_path)
        super().__init__(file_name)
        self.db_client = db_client
        self.collection_name = collection_name
        self.embedding_fields = get_columns_names_with_prop(table_schema, VECTORIZE_HINT)
        self.unique_identifiers = self._list_unique_identifiers(table_schema)
        self.config = client_config

        with FileStorage.open_zipsafe_ro(local_path) as f:
            docs, payloads, ids = [], [], []

            for line in f:
                data = json.loads(line)
                point_id = (
                    self._generate_uuid(data, self.unique_identifiers, self.collection_name)
                    if self.unique_identifiers
                    else uuid.uuid4()
                )
                embedding_doc = self._get_embedding_doc(data)
                payloads.append(data)
                ids.append(point_id)
                docs.append(embedding_doc)

            embedding_model = db_client._get_or_init_model(db_client.embedding_model_name)
            embeddings = list(
                embedding_model.embed(
                    docs,
                    batch_size=self.config.embedding_batch_size,
                    parallel=self.config.embedding_parallelism,
                )
            )
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
        doc = "\n".join(str(data[key]) for key in self.embedding_fields)
        return doc

    def _list_unique_identifiers(self, table_schema: TTableSchema) -> Sequence[str]:
        """Returns a list of unique identifiers for a table.

        Args:
            table_schema (TTableSchema): a dlt table schema.

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
        self.db_client.upload_collection(
            self.collection_name,
            ids=ids,
            payload=payloads,
            vectors=vectors,
            parallel=self.config.upload_parallelism,
            batch_size=self.config.upload_batch_size,
            max_retries=self.config.upload_max_retries,
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
            str: A string representation of the genrated UUID
        """
        data_id = "_".join(str(data[key]) for key in unique_identifiers)
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, collection_name + data_id))

    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()


class QdrantClient(JobClientBase, WithStateSync):
    """Qdrant Destination Handler"""

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()
    state_properties: ClassVar[List[str]] = [
        "version",
        "engine_version",
        "pipeline_name",
        "state",
        "created_at",
        "_dlt_load_id",
    ]

    def __init__(self, schema: Schema, config: QdrantClientConfiguration) -> None:
        super().__init__(schema, config)
        self.config: QdrantClientConfiguration = config
        self.db_client: QC = QdrantClient._create_db_client(config)
        self.model = config.model

    @property
    def dataset_name(self) -> str:
        return self.config.normalize_dataset_name(self.schema)

    @property
    def sentinel_collection(self) -> str:
        return self.dataset_name or "DltSentinelCollection"

    @staticmethod
    def _create_db_client(config: QdrantClientConfiguration) -> QC:
        """Generates a Qdrant client from the 'qdrant_client' package.

        Args:
            config (QdrantClientConfiguration): Credentials and options for the Qdrant client.

        Returns:
            QdrantClient: A Qdrant client instance.
        """
        credentials = dict(config.credentials)
        options = dict(config.options)
        client = QC(**credentials, **options)
        client.set_model(config.model)
        return client

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

    def _create_point(self, obj: Dict[str, Any], collection_name: str) -> None:
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

    def get_stored_state(self, pipeline_name: str) -> Optional[StateInfo]:
        """Loads compressed state from destination storage
        By finding a load id that was completed
        """
        limit = 10
        offset = None
        while True:
            try:
                scroll_table_name = self._make_qualified_collection_name(
                    self.schema.state_table_name
                )
                state_records, offset = self.db_client.scroll(
                    scroll_table_name,
                    with_payload=self.state_properties,
                    scroll_filter=models.Filter(
                        must=[
                            models.FieldCondition(
                                key="pipeline_name", match=models.MatchValue(value=pipeline_name)
                            )
                        ]
                    ),
                    limit=limit,
                    offset=offset,
                )
                if len(state_records) == 0:
                    return None
                for state_record in state_records:
                    state = state_record.payload
                    load_id = state["_dlt_load_id"]
                    scroll_table_name = self._make_qualified_collection_name(
                        self.schema.loads_table_name
                    )
                    load_records = self.db_client.count(
                        scroll_table_name,
                        exact=True,
                        count_filter=models.Filter(
                            must=[
                                models.FieldCondition(
                                    key="load_id", match=models.MatchValue(value=load_id)
                                )
                            ]
                        ),
                    )
                    if load_records.count > 0:
                        state["dlt_load_id"] = state.pop("_dlt_load_id")
                        return StateInfo(**state)
            except Exception:
                return None

    def get_stored_schema(self) -> Optional[StorageSchemaInfo]:
        """Retrieves newest schema from destination storage"""
        try:
            scroll_table_name = self._make_qualified_collection_name(self.schema.version_table_name)
            response = self.db_client.scroll(
                scroll_table_name,
                with_payload=True,
                scroll_filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="schema_name",
                            match=models.MatchValue(value=self.schema.name),
                        )
                    ]
                ),
                limit=1,
            )
            record = response[0][0].payload
            return StorageSchemaInfo(**record)
        except Exception:
            return None

    def get_stored_schema_by_hash(self, schema_hash: str) -> Optional[StorageSchemaInfo]:
        try:
            scroll_table_name = self._make_qualified_collection_name(self.schema.version_table_name)
            response = self.db_client.scroll(
                scroll_table_name,
                with_payload=True,
                scroll_filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="version_hash", match=models.MatchValue(value=schema_hash)
                        )
                    ]
                ),
                limit=1,
            )
            record = response[0][0].payload
            return StorageSchemaInfo(**record)
        except Exception:
            return None

    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        return LoadQdrantJob(
            table,
            file_path,
            db_client=self.db_client,
            client_config=self.config,
            collection_name=self._make_qualified_collection_name(table["name"]),
        )

    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")

    def complete_load(self, load_id: str) -> None:
        properties = {
            "load_id": load_id,
            "schema_name": self.schema.name,
            "status": 0,
            "inserted_at": str(pendulum.now()),
        }
        loads_table_name = self._make_qualified_collection_name(self.schema.loads_table_name)
        self._create_point(properties, loads_table_name)

    def __enter__(self) -> "QdrantClient":
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        pass

    def _update_schema_in_storage(self, schema: Schema) -> None:
        schema_str = json.dumps(schema.to_dict())
        properties = {
            "version_hash": schema.stored_version_hash,
            "schema_name": schema.name,
            "version": schema.version,
            "engine_version": schema.ENGINE_VERSION,
            "inserted_at": str(pendulum.now()),
            "schema": schema_str,
        }
        version_table_name = self._make_qualified_collection_name(self.schema.version_table_name)
        self._create_point(properties, version_table_name)

    def _execute_schema_update(self, only_tables: Iterable[str]) -> None:
        for table_name in only_tables or self.schema.tables:
            exists = self._collection_exists(table_name)

            if not exists:
                self._create_collection(
                    full_collection_name=self._make_qualified_collection_name(table_name)
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
        except UnexpectedResponse as e:
            if e.status_code == 404:
                return False
            raise e
