from typing import Iterable, Optional, Dict, Any, Iterator, Sequence, List, Tuple, IO
from contextlib import suppress
import math

import sqlalchemy as sa

from dlt.common import logger
from dlt.common import pendulum
from dlt.common.destination.reference import (
    JobClientBase,
    LoadJob,
    RunnableLoadJob,
    StorageSchemaInfo,
    StateInfo,
    PreparedTableSchema,
)
from dlt.destinations.job_client_impl import SqlJobClientBase
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.schema import Schema, TTableSchema, TColumnSchema, TSchemaTables
from dlt.common.schema.typing import TColumnType, TTableSchemaColumns
from dlt.common.schema.utils import pipeline_state_table, normalize_table_identifiers
from dlt.common.storages import FileStorage
from dlt.common.json import json, PY_DATETIME_DECODERS
from dlt.destinations.exceptions import DatabaseUndefinedRelation


# from dlt.destinations.impl.sqlalchemy.sql_client import SqlalchemyClient
from dlt.destinations.impl.sqlalchemy.db_api_client import SqlalchemyClient
from dlt.destinations.impl.sqlalchemy.configuration import SqlalchemyClientConfiguration


class SqlalchemyJsonLInsertJob(RunnableLoadJob):
    def __init__(self, file_path: str, table: sa.Table) -> None:
        super().__init__(file_path)
        self._job_client: "SqlalchemyJobClient" = None
        self.table = table

    def _open_load_file(self) -> IO[bytes]:
        return FileStorage.open_zipsafe_ro(self._file_path, "rb")

    def _iter_data_items(self) -> Iterator[Dict[str, Any]]:
        all_cols = {col.name: None for col in self.table.columns}
        with FileStorage.open_zipsafe_ro(self._file_path, "rb") as f:
            for line in f:
                # Decode date/time to py datetime objects. Some drivers have issues with pendulum objects
                for item in json.typed_loadb(line, decoders=PY_DATETIME_DECODERS):
                    # Fill any missing columns in item with None. Bulk insert fails when items have different keys
                    if item.keys() != all_cols.keys():
                        yield {**all_cols, **item}
                    else:
                        yield item

    def _iter_data_item_chunks(self) -> Iterator[Sequence[Dict[str, Any]]]:
        max_rows = self._job_client.capabilities.max_rows_per_insert or math.inf
        # Limit by max query length should not be needed,
        # bulk insert generates an INSERT template with a single VALUES tuple of placeholders
        # If any dialects don't do that we need to check the str length of the query
        # TODO: Max params may not be needed. Limits only apply to placeholders in sql string (mysql/sqlite)
        max_params = self._job_client.capabilities.max_query_parameters or math.inf
        chunk: List[Dict[str, Any]] = []
        params_count = 0
        for item in self._iter_data_items():
            if len(chunk) + 1 == max_rows or params_count + len(item) > max_params:
                # Rotate chunk
                yield chunk
                chunk = []
                params_count = 0
            params_count += len(item)
            chunk.append(item)

        if chunk:
            yield chunk

    def run(self) -> None:
        _sql_client = self._job_client.sql_client

        with _sql_client.begin_transaction():
            for chunk in self._iter_data_item_chunks():
                _sql_client.execute_sql(self.table.insert(), chunk)


class SqlalchemyParquetInsertJob(SqlalchemyJsonLInsertJob):
    def _iter_data_item_chunks(self) -> Iterator[Sequence[Dict[str, Any]]]:
        from dlt.common.libs.pyarrow import ParquetFile

        num_cols = len(self.table.columns)
        max_rows = self._job_client.capabilities.max_rows_per_insert or None
        max_params = self._job_client.capabilities.max_query_parameters or None
        read_limit = None

        with ParquetFile(self._file_path) as reader:
            if max_params is not None:
                read_limit = math.floor(max_params / num_cols)

            if max_rows is not None:
                if read_limit is None:
                    read_limit = max_rows
                else:
                    read_limit = min(read_limit, max_rows)

            if read_limit is None:
                yield reader.read().to_pylist()
                return

            for chunk in reader.iter_batches(batch_size=read_limit):
                yield chunk.to_pylist()


class SqlalchemyJobClient(SqlJobClientBase):
    sql_client: SqlalchemyClient  # type: ignore[assignment]

    def __init__(
        self,
        schema: Schema,
        config: SqlalchemyClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        self.sql_client = SqlalchemyClient(
            config.normalize_dataset_name(schema),
            None,
            config.credentials,
            capabilities,
            engine_args=config.engine_args,
        )

        self.schema = schema
        self.capabilities = capabilities
        self.config = config
        self.type_mapper = self.capabilities.get_type_mapper(self.sql_client.dialect)

    def _to_table_object(self, schema_table: PreparedTableSchema) -> sa.Table:
        existing = self.sql_client.get_existing_table(schema_table["name"])
        if existing is not None:
            existing_col_names = set(col.name for col in existing.columns)
            new_col_names = set(schema_table["columns"])
            # Re-generate the table if columns have changed
            if existing_col_names == new_col_names:
                return existing
        return sa.Table(
            schema_table["name"],
            self.sql_client.metadata,
            *[
                self._to_column_object(col, schema_table)
                for col in schema_table["columns"].values()
            ],
            extend_existing=True,
            schema=self.sql_client.dataset_name,
        )

    def _to_column_object(
        self, schema_column: TColumnSchema, table: PreparedTableSchema
    ) -> sa.Column:
        return sa.Column(
            schema_column["name"],
            self.type_mapper.to_destination_type(schema_column, table),
            nullable=schema_column.get("nullable", True),
            unique=schema_column.get("unique", False),
        )

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        if file_path.endswith(".typed-jsonl"):
            table_obj = self._to_table_object(table)
            return SqlalchemyJsonLInsertJob(file_path, table_obj)
        elif file_path.endswith(".parquet"):
            table_obj = self._to_table_object(table)
            return SqlalchemyParquetInsertJob(file_path, table_obj)
        return None

    def complete_load(self, load_id: str) -> None:
        loads_table = self._to_table_object(self.schema.tables[self.schema.loads_table_name])  # type: ignore[arg-type]
        now_ts = pendulum.now()
        self.sql_client.execute_sql(
            loads_table.insert().values(
                (
                    load_id,
                    self.schema.name,
                    0,
                    now_ts,
                    self.schema.version_hash,
                )
            )
        )

    def _get_table_key(self, name: str, schema: Optional[str]) -> str:
        if schema is None:
            return name
        else:
            return schema + "." + name

    def get_storage_tables(
        self, table_names: Iterable[str]
    ) -> Iterable[Tuple[str, TTableSchemaColumns]]:
        metadata = sa.MetaData()
        for table_name in table_names:
            table_obj = self.sql_client.reflect_table(table_name, metadata)
            if table_obj is None:
                yield table_name, {}
                continue
            yield table_name, {
                col.name: {
                    "name": col.name,
                    "nullable": col.nullable,
                    **self.type_mapper.from_destination_type(col.type, None, None),
                }
                for col in table_obj.columns
            }

    def update_stored_schema(
        self, only_tables: Iterable[str] = None, expected_update: TSchemaTables = None
    ) -> Optional[TSchemaTables]:
        # super().update_stored_schema(only_tables, expected_update)
        JobClientBase.update_stored_schema(self, only_tables, expected_update)

        schema_info = self.get_stored_schema_by_hash(self.schema.stored_version_hash)
        if schema_info is not None:
            logger.info(
                "Schema with hash %s inserted at %s found in storage, no upgrade required",
                self.schema.stored_version_hash,
                schema_info.inserted_at,
            )
            return {}
        else:
            logger.info(
                "Schema with hash %s not found in storage, upgrading",
                self.schema.stored_version_hash,
            )

        # Create all schema tables in metadata
        for table_name in only_tables or self.schema.tables:
            self._to_table_object(self.schema.tables[table_name])  # type: ignore[arg-type]

        schema_update: TSchemaTables = {}
        tables_to_create: List[sa.Table] = []
        columns_to_add: List[sa.Column] = []

        for table_name in only_tables or self.schema.tables:
            table = self.schema.tables[table_name]
            table_obj, new_columns, exists = self.sql_client.compare_storage_table(table["name"])
            if not new_columns:  # Nothing to do, don't create table without columns
                continue
            if not exists:
                tables_to_create.append(table_obj)
            else:
                columns_to_add.extend(new_columns)
            partial_table = self.prepare_load_table(table_name)
            new_column_names = set(col.name for col in new_columns)
            partial_table["columns"] = {
                col_name: col_def
                for col_name, col_def in partial_table["columns"].items()
                if col_name in new_column_names
            }
            schema_update[table_name] = partial_table

        with self.sql_client.begin_transaction():
            for table_obj in tables_to_create:
                self.sql_client.create_table(table_obj)
            self.sql_client.alter_table_add_columns(columns_to_add)
            self._update_schema_in_storage(self.schema)

        return schema_update

    def _delete_schema_in_storage(self, schema: Schema) -> None:
        version_table = schema.tables[schema.version_table_name]
        table_obj = self._to_table_object(version_table)  # type: ignore[arg-type]
        schema_name_col = schema.naming.normalize_identifier("schema_name")
        self.sql_client.execute_sql(
            table_obj.delete().where(table_obj.c[schema_name_col] == schema.name)
        )

    def _update_schema_in_storage(self, schema: Schema) -> None:
        version_table = schema.tables[schema.version_table_name]
        table_obj = self._to_table_object(version_table)  # type: ignore[arg-type]
        schema_str = json.dumps(schema.to_dict())

        schema_mapping = StorageSchemaInfo(
            version=schema.version,
            engine_version=str(schema.ENGINE_VERSION),
            schema_name=schema.name,
            version_hash=schema.stored_version_hash,
            schema=schema_str,
            inserted_at=pendulum.now(),
        ).to_normalized_mapping(schema.naming)

        self.sql_client.execute_sql(table_obj.insert().values(schema_mapping))

    def _get_stored_schema(
        self, version_hash: Optional[str] = None, schema_name: Optional[str] = None
    ) -> Optional[StorageSchemaInfo]:
        version_table = self.schema.tables[self.schema.version_table_name]
        table_obj = self._to_table_object(version_table)  # type: ignore[arg-type]
        with suppress(DatabaseUndefinedRelation):
            q = sa.select(table_obj)
            if version_hash is not None:
                version_hash_col = self.schema.naming.normalize_identifier("version_hash")
                q = q.where(table_obj.c[version_hash_col] == version_hash)
            if schema_name is not None:
                schema_name_col = self.schema.naming.normalize_identifier("schema_name")
                q = q.where(table_obj.c[schema_name_col] == schema_name)
            inserted_at_col = self.schema.naming.normalize_identifier("inserted_at")
            q = q.order_by(table_obj.c[inserted_at_col].desc())
            with self.sql_client.execute_query(q) as cur:
                row = cur.fetchone()
                if row is None:
                    return None

                # TODO: Decode compressed schema str if needed
                return StorageSchemaInfo.from_normalized_mapping(
                    row._mapping, self.schema.naming  # type: ignore[attr-defined]
                )

    def get_stored_schema_by_hash(self, version_hash: str) -> Optional[StorageSchemaInfo]:
        return self._get_stored_schema(version_hash)

    def get_stored_schema(self) -> Optional[StorageSchemaInfo]:
        """Get the latest stored schema"""
        return self._get_stored_schema(schema_name=self.schema.name)

    def get_stored_state(self, pipeline_name: str) -> StateInfo:
        state_table = self.schema.tables.get(
            self.schema.state_table_name
        ) or normalize_table_identifiers(pipeline_state_table(), self.schema.naming)
        state_table_obj = self._to_table_object(state_table)  # type: ignore[arg-type]
        loads_table = self.schema.tables[self.schema.loads_table_name]
        loads_table_obj = self._to_table_object(loads_table)  # type: ignore[arg-type]

        c_load_id, c_dlt_load_id, c_pipeline_name, c_status = map(
            self.schema.naming.normalize_identifier,
            ("load_id", "_dlt_load_id", "pipeline_name", "status"),
        )

        query = (
            sa.select(state_table_obj)
            .join(loads_table_obj, loads_table_obj.c[c_load_id] == state_table_obj.c[c_dlt_load_id])
            .where(
                sa.and_(
                    state_table_obj.c[c_pipeline_name] == pipeline_name,
                    loads_table_obj.c[c_status] == 0,
                )
            )
            .order_by(loads_table_obj.c[c_load_id].desc())
        )

        with self.sql_client.execute_query(query) as cur:
            row = cur.fetchone()
            if not row:
                return None
            mapping = dict(row._mapping)  # type: ignore[attr-defined]

        return StateInfo.from_normalized_mapping(mapping, self.schema.naming)

    def _from_db_type(
        self, db_type: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        raise NotImplementedError()

    def _get_column_def_sql(self, c: TColumnSchema, table_format: TTableSchema = None) -> str:
        raise NotImplementedError()
