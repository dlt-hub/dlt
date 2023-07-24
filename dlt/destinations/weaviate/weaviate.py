from types import TracebackType
from typing import ClassVar, Optional, Sequence, Dict, Type, Iterable, Any
import base64
import binascii
import zlib

import weaviate
from weaviate.util import generate_uuid5

from dlt.common import json, pendulum, logger
from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.schema.typing import VERSION_TABLE_NAME, LOADS_TABLE_NAME
from dlt.common.schema.utils import get_columns_names_with_prop
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    NewLoadJob,
    TLoadJobState,
    LoadJob,
    JobClientBase,
)
from dlt.common.storages import FileStorage
from dlt.destinations.job_impl import EmptyLoadJob
from dlt.destinations.job_client_impl import StorageSchemaInfo

from dlt.destinations.weaviate import capabilities
from dlt.destinations.weaviate.configuration import WeaviateClientConfiguration

DLT_TABLE_PREFIX = "_dlt"


# TODO: move to common
def is_dlt_table(table_name: str) -> bool:
    return table_name.startswith(DLT_TABLE_PREFIX)


def snake_to_camel(snake_str: str) -> str:
    return "".join(x.capitalize() for x in snake_str.split("_"))


def table_name_to_class_name(table_name: str) -> str:
    # Weaviate requires class names to be written with
    # a capital letter first:
    # https://weaviate.io/developers/weaviate/config-refs/schema#class
    # For dlt tables strip the underscore from the name
    # and make it all caps
    # For non dlt tables make the class name camel case
    return (
        snake_to_camel(table_name)
        if not is_dlt_table(table_name)
        else table_name.lstrip("_").upper()
    )


class LoadWeaviateJob(LoadJob):
    def __init__(
        self,
        table_schema: TTableSchema,
        local_path: str,
        db_client: weaviate.Client,
        client_config: WeaviateClientConfiguration,
        load_id: str,
    ) -> None:
        file_name = FileStorage.get_file_name_from_file_path(local_path)
        super().__init__(file_name)

        class_name = table_name_to_class_name(table_schema["name"])

        unique_identifiers = self.list_unique_identifiers(table_schema)

        with db_client.batch(
            batch_size=client_config.weaviate_batch_size,
        ) as batch:
            with FileStorage.open_zipsafe_ro(local_path) as f:
                for line in f:
                    data = json.loads(line)

                    if unique_identifiers:
                        uuid = self.generate_uuid(data, unique_identifiers, class_name)
                    else:
                        uuid = None

                    batch.add_data_object(data, class_name, uuid=uuid)

    def list_unique_identifiers(self, table_schema: TTableSchema) -> Sequence[str]:
        primary_keys = get_columns_names_with_prop(table_schema, "primary_key")
        if primary_keys:
            return primary_keys
        return get_columns_names_with_prop(table_schema, "unique")

    def generate_uuid(
        self, data: Dict[str, Any], unique_identifiers: Sequence[str], class_name: str
    ) -> str:
        data_id = "_".join([str(data[key]) for key in unique_identifiers])
        return generate_uuid5(data_id, class_name)

    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()


class WeaviateClient(JobClientBase):
    """Weaviate client implementation."""

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: WeaviateClientConfiguration) -> None:
        super().__init__(schema, config)
        self.config: WeaviateClientConfiguration = config
        self.db_client = self.create_db_client(config)

    @staticmethod
    def create_db_client(config: WeaviateClientConfiguration) -> weaviate.Client:
        return weaviate.Client(
            url=config.credentials.url,
            auth_client_secret=weaviate.AuthApiKey(api_key=config.credentials.api_key),
            additional_headers=config.credentials.additional_headers,
        )

    def initialize_storage(
        self, staging: bool = False, truncate_tables: Iterable[str] = None
    ) -> None:
        pass

    def is_storage_initialized(self, staging: bool = False) -> bool:
        return True

    def update_storage_schema(
        self,
        staging: bool = False,
        only_tables: Iterable[str] = None,
        expected_update: TSchemaTables = None,
    ) -> Optional[TSchemaTables]:
        # Retrieve the schema from Weaviate
        applied_update: TSchemaTables = {}
        schema_info = self.get_schema_by_hash(self.schema.stored_version_hash)
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

    def _execute_schema_update(self, only_tables: Iterable[str]) -> None:
        for table_name in only_tables or self.schema.tables:
            table = self.schema.tables[table_name]
            class_schema = self.make_weaviate_class_schema(table)

            self.db_client.schema.create_class(class_schema)
        self._update_schema_in_storage(self.schema)

    def get_schema_by_hash(self, schema_hash: str) -> Optional[StorageSchemaInfo]:
        version_class_name = table_name_to_class_name(VERSION_TABLE_NAME)

        try:
            self.db_client.schema.get(version_class_name)
        except weaviate.exceptions.UnexpectedStatusCodeException as e:
            if e.status_code == 404:
                return None
            raise

        properties = [
            "version_hash",
            "schema_name",
            "version",
            "engine_version",
            "inserted_at",
            "schema",
        ]

        response = (
            self.db_client.query.get(version_class_name, properties)
            .with_where(
                {
                    "path": ["version_hash"],
                    "operator": "Equal",
                    "valueString": schema_hash,
                }
            )
            .with_limit(1)
            .do()
        )

        try:
            record = response["data"]["Get"][version_class_name][0]
        except IndexError:
            return None
        return self._decode_schema(record)

    def _decode_schema(self, record: Dict[str, Any]) -> StorageSchemaInfo:
        # XXX: Duplicate code from dlt/destinations/job_client_impl.py
        schema_str = record["schema"]
        try:
            schema_bytes = base64.b64decode(schema_str, validate=True)
            schema_str = zlib.decompress(schema_bytes).decode("utf-8")
        except binascii.Error:
            pass

        return StorageSchemaInfo(
            version_hash=record["version_hash"],
            schema_name=record["schema_name"],
            version=record["version"],
            engine_version=record["engine_version"],
            inserted_at=pendulum.parse(record["inserted_at"]),
            schema=schema_str,
        )

    def make_weaviate_class_schema(self, table: TTableSchema) -> Dict[str, Any]:
        """Creates a Weaviate class schema from a table schema."""
        table_name = table["name"]

        class_name = table_name_to_class_name(table_name)

        if is_dlt_table(table_name):
            return self._make_non_vectorized_class_schema(class_name)

        return self._make_vectorized_class_schema(class_name)

    def _make_vectorized_class_schema(self, class_name: str) -> Dict[str, Any]:
        return {
            "class": class_name,
            "vectorizer": "text2vec-openai",
            "moduleConfig": {
                "text2vec-openai": {
                    "model": "ada",
                    "modelVersion": "002",
                    "type": "text",
                },
            },
        }

    def _make_non_vectorized_class_schema(self, class_name: str) -> Dict[str, Any]:
        return {
            "class": class_name,
            "vectorizer": "none",
            "vectorIndexConfig": {
                "skip": True,
            },
        }

    def start_file_load(
        self, table: TTableSchema, file_path: str, load_id: str
    ) -> LoadJob:
        return LoadWeaviateJob(
            table,
            file_path,
            db_client=self.db_client,
            client_config=self.config,
            load_id=load_id,
        )

    def restore_file_load(self, file_path: str) -> LoadJob:
        return EmptyLoadJob.from_file_path(file_path, "completed")

    def create_merge_job(self, table_chain: Sequence[TTableSchema]) -> NewLoadJob:
        return None

    def complete_load(self, load_id: str) -> None:
        load_table_name = table_name_to_class_name(LOADS_TABLE_NAME)
        properties = {
            "load_id": load_id,
            "schema_name": self.schema.name,
            "status": 0,
            "inserted_at": str(pendulum.now()),
        }
        self.db_client.data_object.create(properties, load_table_name)

    def __enter__(self) -> "WeaviateClient":
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        pass

    def _update_schema_in_storage(self, schema: Schema) -> None:
        now_ts = str(pendulum.now())
        schema_str = json.dumps(schema.to_dict())
        schema_bytes = schema_str.encode("utf-8")
        if len(schema_bytes) > self.capabilities.max_text_data_type_length:
            # compress and to base64
            schema_str = base64.b64encode(zlib.compress(schema_bytes, level=9)).decode(
                "ascii"
            )
        version_class_name = table_name_to_class_name(VERSION_TABLE_NAME)
        properties = {
            "version_hash": schema.stored_version_hash,
            "schema_name": schema.name,
            "version": schema.version,
            "engine_version": schema.ENGINE_VERSION,
            "inserted_at": now_ts,
            "schema": schema_str,
        }

        self.db_client.data_object.create(properties, version_class_name)
