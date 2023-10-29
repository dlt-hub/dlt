from types import TracebackType
from typing import ClassVar, Optional, Sequence, List, Dict, Type, Iterable, Any, IO

from dlt.common import json, pendulum, logger
from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import TLoadJobState, LoadJob, JobClientBase, WithStateSync
from dlt.common.data_types import TDataType
from dlt.common.storages import FileStorage

from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.job_client_impl import StorageSchemaInfo, StateInfo

from dlt.destinations.qdrant import capabilities
from dlt.destinations.qdrant.configuration import QdrantClientConfiguration
from dlt.destinations.qdrant.qdrant_adapter import VECTORIZE_HINT

from qdrant_client import QdrantClient as QC, models
from qdrant_client.qdrant_fastembed import uuid


class LoadQdrantJob(LoadJob):
    def __init__(
        self,
        table_schema: TTableSchema,
        local_path: str,
        db_client: QC,
        client_config: QdrantClientConfiguration,
        class_name: str,
    ) -> None:
        file_name = FileStorage.get_file_name_from_file_path(local_path)
        super().__init__(file_name)
        self.db_client = db_client
        self.class_name = class_name
        self.embedding_fields = get_columns_names_with_prop(
            table_schema, VECTORIZE_HINT)
        self.unique_identifiers = self._list_unique_identifiers(table_schema)

        with FileStorage.open_zipsafe_ro(local_path) as f:
            docs, payloads, ids = [], [], []

            for line in f:
                data = json.loads(line)
                id = self._generate_uuid(
                    data, self.unique_identifiers, self.class_name) if self.unique_identifiers else uuid.uuid4()
                embedding_doc = self._get_embedding_doc(data)
                payloads.append(data)
                ids.append(id)
                docs.append(embedding_doc)

            embedding_model = db_client._get_or_init_model(
                db_client.embedding_model_name)
            embeddings = list(embedding_model.embed(
                docs, batch_size=32, parallel=0))
            assert len(embeddings) == len(payloads) == len(ids)

            self._upload_data(vectors=embeddings, ids=ids, payloads=payloads)

    def _get_embedding_doc(self, data) -> str:
        doc = "\n".join([str(data[key]) for key in self.embedding_fields])
        return doc

    def _list_unique_identifiers(self, table_schema: TTableSchema) -> Sequence[str]:
        if table_schema.get("write_disposition") == "merge":
            primary_keys = get_columns_names_with_prop(
                table_schema, "primary_key")
            if primary_keys:
                return primary_keys
        return get_columns_names_with_prop(table_schema, "unique")

    def _upload_data(self, ids: Iterable[Any], vectors: Iterable[Any], payloads: Iterable[Any]):
        print("Uploading data into", self.class_name)
        self.db_client.upload_collection(
            self.class_name, ids=ids, payload=payloads, vectors=vectors)

    def _generate_uuid(
        self, data: Dict[str, Any], unique_identifiers: Sequence[str], class_name: str
    ) -> str:
        data_id = "_".join([str(data[key]) for key in unique_identifiers])
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, class_name + data_id))

    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()


class QdrantClient(JobClientBase, WithStateSync):
    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()
    state_properties: ClassVar[List[str]] = [
        "version", "engine_version", "pipeline_name", "state", "created_at", "_dlt_load_id"]

    def __init__(self, schema: Schema, config: QdrantClientConfiguration) -> None:
        super().__init__(schema, config)
        self.config: QdrantClientConfiguration = config
        self.db_client: QC = QdrantClient.create_db_client(config)
        self.model = config.model

    @property
    def dataset_name(self) -> str:
        return self.config.normalize_dataset_name(self.schema)

    @property
    def sentinel_class(self) -> str:
        return self.dataset_name or "DltSentinelCollection"

    @staticmethod
    def create_db_client(config: QdrantClientConfiguration) -> QC:
        credentials = dict(config.credentials)
        options = dict(config.options)
        client = QC(**credentials, **options)
        client.set_model(config.model)
        return client

    def make_qualified_class_name(self, table_name: str) -> str:
        dataset_separator = self.config.dataset_separator
        return f"{self.dataset_name}{dataset_separator}{table_name}" if self.dataset_name else table_name

    def create_class(
        self, full_class_name: Optional[str] = None
    ) -> None:
        print("Creating class with name", full_class_name)
        embeddings_size, distance = self.db_client._get_model_params(
            model_name=self.db_client.embedding_model_name)

        vectors_config = models.VectorParams(
            size=embeddings_size, distance=distance)

        self.db_client.create_collection(
            collection_name=full_class_name, vectors_config=vectors_config)

    def create_point(self, obj: Dict[str, Any], class_name: str) -> None:
        self.db_client.upsert(class_name, points=[
            models.PointStruct(
                id=str(uuid.uuid4()),
                payload=obj,
                vector={},
            )])

    def drop_storage(self) -> None:
        collections = self.db_client.get_collections().collections
        class_name_list = [collection.name
                           for collection in collections]

        if self.dataset_name:
            prefix = f"{self.dataset_name}{self.config.dataset_separator}"

            for class_name in class_name_list:
                if class_name.startswith(prefix):
                    self.db_client.delete_collection(class_name)
        else:
            for class_name in self.schema.tables.keys():
                if class_name in class_name_list:
                    self.db_client.delete_collection(class_name)

        self._delete_sentinel_class()

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        if not self.is_storage_initialized():
            self._create_sentinel_class()
        elif truncate_tables:
            for table_name in truncate_tables:
                qualified_table_name = self.make_qualified_class_name(
                    table_name=table_name)
                try:
                    class_schema = self.db_client.get_collection(
                        collection_name=qualified_table_name)
                except Exception as e:
                    continue

                self.db_client.delete_collection(qualified_table_name)
                self.create_class(full_class_name=qualified_table_name)

    def is_storage_initialized(self) -> bool:
        try:
            self.db_client.get_collection(self.sentinel_class)
        except Exception as e:
            return False
        return True

    def _create_sentinel_class(self) -> None:
        self.create_class(full_class_name=self.sentinel_class)

    def _delete_sentinel_class(self) -> None:
        self.db_client.delete_collection(self.sentinel_class)

    def update_stored_schema(
        self, only_tables: Iterable[str] = None, expected_update: TSchemaTables = None
    ) -> Optional[TSchemaTables]:
        applied_update: TSchemaTables = {}
        try:
            schema_info = self.get_stored_schema_by_hash(
                self.schema.stored_version_hash)
        except Exception:
            schema_info = None
        if schema_info is None:
            logger.info(
                f"Schema with hash {self.schema.stored_version_hash} "
                f"not found in the storage. upgrading"
            )
            self._execute_schema_update(only_tables)
        else:
            logger.info(
                f"Schema with hash {self.schema.stored_version_hash} "
                f"inserted at {schema_info.inserted_at} found "
                f"in storage, no upgrade required"
            )
        return applied_update

    def get_stored_state(self, pipeline_name: str) -> Optional[StateInfo]:
        limit = 10
        offset = None
        while True:
            try:
                scroll_table_name = self.make_qualified_class_name(
                    self.schema.state_table_name)
                state_records, offset = self.db_client.scroll(scroll_table_name, with_payload=self.state_properties, scroll_filter=models.Filter(must=[
                    models.FieldCondition(
                        key="pipeline_name", match=models.MatchValue(value=pipeline_name))
                ]), limit=limit, offset=offset)
                if len(state_records) == 0:
                    return None
                for state_record in state_records:
                    state = state_record.payload
                    load_id = state["_dlt_load_id"]
                    scroll_table_name = self.make_qualified_class_name(
                        self.schema.loads_table_name)
                    load_records = self.db_client.count(scroll_table_name, exact=True, count_filter=models.Filter(
                        must=[models.FieldCondition(
                            key="load_id", match=models.MatchValue(value=load_id)
                        )]
                    ))
                    if load_records.count > 0:
                        state["dlt_load_id"] = state.pop("_dlt_load_id")
                        return StateInfo(**state)
            except Exception as e:
                print("WHen getting state", e)
                return None

    def get_stored_schema(self) -> Optional[StorageSchemaInfo]:
        try:
            scroll_table_name = self.make_qualified_class_name(
                self.schema.version_table_name)
            response = self.db_client.scroll(scroll_table_name, with_payload=True, scroll_filter=models.Filter(
                must=[models.FieldCondition(
                    key="schema_name",
                    match=models.MatchValue(value=self.schema.name),
                )]
            ), limit=1)
            record = response[0][0].payload
            return StorageSchemaInfo(**record)
        except Exception as e:
            return None

    def get_stored_schema_by_hash(self, schema_hash: str) -> Optional[StorageSchemaInfo]:
        try:
            scroll_table_name = self.make_qualified_class_name(
                self.schema.version_table_name)
            response = self.db_client.scroll(scroll_table_name, with_payload=True, scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="version_hash", match=models.MatchValue(value=schema_hash))
                ]

            ), limit=1)
            record = response[0][0].payload
            return StorageSchemaInfo(**record)
        except Exception as e:
            return None

    def start_file_load(
        self, table: TTableSchema, file_path: str, load_id: str
    ) -> LoadJob:
        return LoadQdrantJob(
            table,
            file_path,
            db_client=self.db_client,
            client_config=self.config,
            class_name=self.make_qualified_class_name(table["name"]),
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
        loads_table_name = self.make_qualified_class_name(
            self.schema.loads_table_name)
        self.create_point(properties, loads_table_name)

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
        version_table_name = self.make_qualified_class_name(
            self.schema.version_table_name)
        self.create_point(properties, version_table_name)

    def _execute_schema_update(self, only_tables: Iterable[str]) -> None:

        for table_name in only_tables or self.schema.tables:
            exists = self.table_exists(table_name)

            if not exists:
                self.create_class(
                    full_class_name=self.make_qualified_class_name(table_name)
                )
        self._update_schema_in_storage(self.schema)

    def table_exists(self, table_name: str) -> bool:
        try:
            self.db_client.get_collection(
                self.make_qualified_class_name(table_name))
            return True
        except Exception:
            return False
