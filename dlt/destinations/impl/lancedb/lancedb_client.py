from types import TracebackType
from typing import ClassVar, List, Any, cast, Union, Tuple, Iterable, Type, Optional

import lancedb
import pyarrow as pa
from lancedb import DBConnection
from lancedb.common import DATA
from lancedb.embeddings import EmbeddingFunctionRegistry
from lancedb.pydantic import LanceModel
from lancedb.query import LanceQueryBuilder
from numpy import ndarray
from pyarrow import Array, ChunkedArray

from dlt.common import logger
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.reference import (JobClientBase, WithStateSync, LoadJob, StorageSchemaInfo, StateInfo, )
from dlt.common.schema import Schema, TTableSchema, TSchemaTables, TTableSchemaColumns, TColumnSchema
from dlt.common.schema.typing import TColumnType
from dlt.destinations.impl.lancedb import capabilities
from dlt.destinations.impl.lancedb.configuration import LanceDBClientConfiguration
from dlt.destinations.type_mapping import TypeMapper


class LanceDBTypeMapper(TypeMapper):
    sct_to_unbound_dbt = {
        pa.string(): "text",
        pa.int64(): "int",
        pa.float64(): "number",
        pa.bool_(): "boolean",
        pa.date64(): "date",
        pa.binary(): "blob",
        pa.decimal128(): "text",
    }

    sct_to_dbt = {}

    dbt_to_sct = {
        "text": pa.string(),
        "int": pa.int64(),
        "number": pa.float64(),
        "boolean": pa.bool_(),
        "date": pa.date64(),
        "blob": pa.binary(),
    }


class EmptySchema(LanceModel):
    pass



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
        self.type_mapper = LanceDBTypeMapper(self.capabilities)


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


    def create_table(self, table_name: str, schema: Union[pa.Schema, LanceModel]) -> None:
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
                self.create_table(table_name=fq_table_name, schema=self.get_table_schema(fq_table_name))


    def is_storage_initialized(self) -> bool:
        return self._table_exists(self.sentinel_table)


    def _create_sentinel_table(self) -> None:
        """Create an empty table to indicate that the storage is initialized."""
        self.create_table(schema=cast(LanceModel, EmptySchema), table_name=self.sentinel_table)


    def _delete_sentinel_table(self) -> None:
        """Delete the sentinel table."""
        self.db_client.drop_table(self.sentinel_table)


    def update_stored_schema(self, only_tables: Iterable[str] = None, expected_update: TSchemaTables = None, ) -> Optional[TSchemaTables]:
        super().update_stored_schema(only_tables, expected_update)
        applied_update: TSchemaTables = {}

        try:
            stored_schema = self.get_stored_schema_by_hash(self.schema.stored_version_hash)
        except LanceDBSchemaNotFound:
            stored_schema = None

        if stored_schema is None:
            logger.info(f"Schema with hash {self.schema.stored_version_hash} "
                        "not found in storage. Upgrading schema.")
            self._execute_schema_update(only_tables)
        else:
            logger.info(f"Schema with hash {self.schema.stored_version_hash} "
                        f"found in storage, no upgrade required")

        return applied_update


    def _execute_schema_update(self, only_tables: Iterable[str]) -> None:
        for table_name in only_tables or self.schema.tables:
            if table_exists := self._table_exists(self._make_qualified_table_name(table_name)):
                # Get existing columns
                exists, existing_columns = self.get_table_columns(table_name)

                # Detect new columns to add
                new_columns = self.schema.get_new_table_columns(table_name, existing_columns)

                logger.info(f"Found {len(new_columns)} new columns for table {table_name}")

                # Add the new columns
                for column_name, column_schema in new_columns.items():
                    self.add_table_column(table_name, column_name, column_schema)
            else:
                # Create table if it doesn't exist
                table_schema = self.schema.get_table(table_name)
                self.create_table(table_name, table_schema)

        # Persist updated schema version
        self._update_schema_version_in_storage(self.schema)


    def _execute_schema_update(self, only_tables: Iterable[str]) -> None:
        for table_name in only_tables or self.schema.tables:
            table_exists = self.table_exists(table_name)

            if table_exists:
                # Get existing columns
                existing_columns = self.get_table_columns(table_name)

                # Detect new columns to add
                new_columns = self.schema.get_new_table_columns(table_name, existing_columns)

                logger.info(f"Found {len(new_columns)} new columns for table {table_name}")

                # Add the new columns
                for column_name, column_schema in new_columns.items():
                    self.add_table_column(table_name, column_name, column_schema)
            else:
                # Create table if it doesn't exist
                table_schema = self.schema.get_table(table_name)
                self.create_table(table_name, table_schema)

        # Persist updated schema version
        self._update_schema_version_in_storage(self.schema)


    def get_storage_table(self, table_name: str) -> Tuple[bool, TTableSchemaColumns]:
        table_schema: TTableSchemaColumns = {}

        try:
            table_arrow_schema: pa.Schema = self.get_table_schema(self._make_qualified_table_name(table_name))
        except FileNotFoundError:
            return False, table_schema
        except Exception:
            raise

        # Convert PyArrow schema to dlt table schema.
        for prop in table_arrow_schema["properties"]:
            schema_c: TColumnSchema = {"name": self.schema.naming.normalize_identifier(prop["name"]), **self._from_db_type(prop["dataType"][0], None, None), }
            table_schema[prop["name"]] = schema_c
        return True, table_schema


    def get_stored_state(self, pipeline_name: str) -> Optional[StateInfo]:
        """
        Loads compressed state from destination storage.

        Retrieves the matching stored state a completed load ID. It searches
        for state records in blocks of 10, sorted by descending `_dlt_load_id` which is
        guaranteed to increase over time.
        For each state record found, it looks for a
        corresponding successful load record.
        If a matching successful load is found, the
        state is returned.

        Args:
            pipeline_name (str): The name of the pipeline to retrieve state for.

        Returns:
            Optional[StateInfo]: The state matching a successful load, or None if no
            matching state is found.
        """
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


    def _table_exists(self, table_name: str) -> bool:
        return table_name in self.db_client.table_names()


    def _from_db_type(self, wt_t: str, precision: Optional[int], scale: Optional[int]) -> TColumnType:
        return self.type_mapper.from_db_type(wt_t, precision, scale)
