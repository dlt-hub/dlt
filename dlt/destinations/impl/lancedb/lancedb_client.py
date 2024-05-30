from types import TracebackType
from typing import ClassVar, List, Any, cast, Union, Tuple, Iterable, Type, Optional

import lancedb
import pyarrow as pa
from lancedb import DBConnection
from lancedb.embeddings import EmbeddingFunctionRegistry
from lancedb.pydantic import LanceModel
from lancedb.query import LanceQueryBuilder
from numpy import ndarray
from pyarrow import Array, ChunkedArray

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (JobClientBase, WithStateSync, LoadJob, StorageSchemaInfo, StateInfo, )
from dlt.common.schema import Schema, TTableSchema
from dlt.destinations.impl.lancedb import capabilities
from dlt.destinations.impl.lancedb.configuration import LanceDBClientConfiguration


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
        self.model_func = self.registry.get(self.config.provider).create(name=self.config.embedding_model, max_retries=1)


    @property
    def dataset_name(self) -> str:
        return self.config.normalize_dataset_name(self.schema)


    @property
    def sentinel_schema(self) -> str:
        # If no dataset name is provided, we still want to create a sentinel schema.
        return self.dataset_name or "DltSentinelSchema"


    def make_qualified_table_name(self, table_name: str) -> str:
        return f"{self.dataset_name}{self.config.dataset_separator}{table_name}" if self.dataset_name else table_name


    def get_table_schema(self, table_name: str) -> pa.Schema:
        return cast(pa.Schema, self.db_client[self.make_qualified_table_name(table_name)].schema)


    def create_table(self, table_name: str, schema: Union[pa.Schema, LanceModel], ) -> None:
        """Create a LanceDB Table from the provided LanceModel or PyArrow schema.

        Args:
            schema: The table schema to create.
            table_name: The name of the table to create.
        """

        self.db_client.create_table(self.make_qualified_table_name(table_name), schema=schema, embedding_functions=self.model_func)


    def delete_table(self, table_name: str) -> None:
        """Delete a LanceDB table.

        Args:
            table_name: The name of the table to delete.
        """
        self.db_client.drop_table(self.make_qualified_table_name(table_name))


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
        return self.db_client.open_table(self.make_qualified_table_name(table_name)).search(query=query)


    def get_stored_state(self, pipeline_name: str) -> Optional[StateInfo]:
        pass


    def get_stored_schema_by_hash(self, version_hash: str) -> StorageSchemaInfo:
        pass


    def get_stored_schema(self) -> Optional[StorageSchemaInfo]:
        pass


    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        pass


    def __enter__(self) -> "JobClientBase":
        pass


    def complete_load(self, load_id: str) -> None:
        pass


    def restore_file_load(self, file_path: str) -> LoadJob:
        pass


    def start_file_load(self, table: TTableSchema, file_path: str, load_id: str) -> LoadJob:
        pass


    def drop_storage(self) -> None:
        pass


    def is_storage_initialized(self) -> bool:
        pass


    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        pass
