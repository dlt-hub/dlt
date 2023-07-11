from types import TracebackType
from typing import ClassVar, Optional, Sequence, Type, Iterable

import weaviate

from dlt.common import json
from dlt.common.schema import Schema, TTableSchema, TSchemaTables
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    NewLoadJob,
    TLoadJobState,
    LoadJob,
    JobClientBase,
)
from dlt.common.storages import FileStorage
from dlt.destinations.job_impl import EmptyLoadJob

from dlt.destinations.weaviate import capabilities
from dlt.destinations.weaviate.configuration import WeaviateClientConfiguration

DLT_TABLE_PREFIX = "_dlt"


# TODO: move to common
def is_dlt_table(table_name: str) -> bool:
    return table_name.startswith(DLT_TABLE_PREFIX)


def snake_to_camel(snake_str: str) -> str:
    components = snake_str.split("_")
    return "".join(x.capitalize() for x in components)


def table_name_to_class_name(table_name):
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
        table_name: str,
        local_path: str,
        db_client: weaviate.Client,
        client_config: WeaviateClientConfiguration,
        load_id: str,
    ) -> None:
        file_name = FileStorage.get_file_name_from_file_path(local_path)
        super().__init__(file_name)

        class_name = table_name_to_class_name(table_name)

        with db_client.batch(
            batch_size=client_config.weaviate_batch_size,
        ) as batch:
            with FileStorage.open_zipsafe_ro(local_path) as f:
                for line in f:
                    data = json.loads(line)
                    db_client.batch.add_data_object(data, class_name)

    def state(self) -> TLoadJobState:
        return "completed"

    def exception(self) -> str:
        raise NotImplementedError()


class WeaviateClient(JobClientBase):
    """Weaviate client implementation."""

    capabilities: ClassVar[DestinationCapabilitiesContext] = capabilities()

    def __init__(self, schema: Schema, config: WeaviateClientConfiguration) -> None:
        db_client = weaviate.Client(
            url=config.credentials.url,
            auth_client_secret=weaviate.AuthApiKey(api_key=config.credentials.api_key),
            additional_headers=config.credentials.additional_headers,
        )

        super().__init__(schema, config)
        self.config: WeaviateClientConfiguration = config
        self.db_client = db_client

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
        for table_name in self.schema.tables:
            table = self.schema.tables[table_name]
            class_schema = self.make_weaviate_class_schema(table)

            # Todo: check if schema exists (by hash)
            self.db_client.schema.create_class(class_schema)

    def make_weaviate_class_schema(self, table: TTableSchema) -> dict:
        """Creates a Weaviate class schema from a table schema."""
        table_name = table["name"]

        class_name = table_name_to_class_name(table_name)

        if is_dlt_table(table_name):
            return self._make_non_vectorized_class_schema(class_name)

        return self._make_vectorized_class_schema(class_name)

    def _make_vectorized_class_schema(self, class_name: str) -> dict:
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

    def _make_non_vectorized_class_schema(self, class_name: str) -> dict:
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
            table["name"],
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
        pass

    def __enter__(self) -> "WeaviateClient":
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        pass
