from abc import abstractmethod
import base64
import binascii
import contextlib
import datetime  # noqa: 251
from types import TracebackType
from typing import Any, ClassVar, List, NamedTuple, Sequence, Tuple, Type
import zlib

from dlt.common import json, pendulum, logger
from dlt.common.schema.typing import LOADS_TABLE_NAME, VERSION_TABLE_NAME
from dlt.common.storages import FileStorage
from dlt.common.schema import TColumnSchema, Schema, TTableSchemaColumns
from dlt.common.destination import DestinationClientConfiguration, TLoadJobStatus, LoadJob, JobClientBase
from dlt.destinations.exceptions import DatabaseUndefinedRelation

from dlt.destinations.typing import TNativeConn
from dlt.destinations.sql_client import SqlClientBase


class StorageSchemaInfo(NamedTuple):
    version_hash: str
    schema_name: str
    version: int
    engine_version: str
    inserted_at: datetime.datetime
    schema: str


class LoadEmptyJob(LoadJob):
    def __init__(self, file_name: str, status: TLoadJobStatus, exception: str = None) -> None:
        self._status = status
        self._exception = exception
        super().__init__(file_name)

    @classmethod
    def from_file_path(cls, file_path: str, status: TLoadJobStatus, message: str = None) -> "LoadEmptyJob":
        return cls(FileStorage.get_file_name_from_file_path(file_path), status, exception=message)

    def status(self) -> TLoadJobStatus:
        return self._status

    def file_name(self) -> str:
        return self._file_name

    def exception(self) -> str:
        return self._exception


class SqlJobClientBase(JobClientBase):

    VERSION_TABLE_SCHEMA_COLUMNS: ClassVar[str] = "version_hash, schema_name, version, engine_version, inserted_at, schema"

    def __init__(self, schema: Schema, config: DestinationClientConfiguration,  sql_client: SqlClientBase[TNativeConn]) -> None:
        super().__init__(schema, config)
        self.sql_client = sql_client

    def initialize_storage(self) -> None:
        if not self.is_storage_initialized():
            self.sql_client.create_dataset()

    def is_storage_initialized(self) -> bool:
        return self.sql_client.has_dataset()

    def update_storage_schema(self) -> None:
        schema_info = self.get_schema_by_hash(self.schema.stored_version_hash)
        if schema_info is None:
            logger.info(f"Schema with hash {self.schema.stored_version_hash} not found in the storage. upgrading")
            # TODO: make it transactional for databases supporting tx DDL
            updates = self._build_schema_update_sql()
            if len(updates) > 0:
                # execute updates in a single batch
                sql = ";\n".join(updates) + ";"
                self.sql_client.execute_sql(sql)
            self._update_schema_in_storage(self.schema)
        else:
            logger.info(f"Schema with hash {self.schema.stored_version_hash} inserted at {schema_info.inserted_at} found in storage, no upgrade required")

    def complete_load(self, load_id: str) -> None:
        name = self.sql_client.make_qualified_table_name(LOADS_TABLE_NAME)
        now_ts = pendulum.now()
        self.sql_client.execute_sql(
            f"INSERT INTO {name}(load_id, schema_name, status, inserted_at) VALUES(%s, %s, %s, %s);", load_id, self.schema.name, 0, now_ts)

    def __enter__(self) -> "SqlJobClientBase":
        self.sql_client.open_connection()
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        self.sql_client.close_connection()

    @abstractmethod
    def get_storage_table(self, table_name: str) -> Tuple[bool, TTableSchemaColumns]:
        pass

    def get_newest_schema_from_storage(self) -> StorageSchemaInfo:
        name = self.sql_client.make_qualified_table_name(VERSION_TABLE_NAME)
        query = f"SELECT {self.VERSION_TABLE_SCHEMA_COLUMNS} FROM {name} WHERE schema_name = %s ORDER BY inserted_at DESC;"
        return self._row_to_schema_info(query, self.schema.name)

    def get_schema_by_hash(self, version_hash: str) -> StorageSchemaInfo:
        name = self.sql_client.make_qualified_table_name(VERSION_TABLE_NAME)
        query = f"SELECT {self.VERSION_TABLE_SCHEMA_COLUMNS} FROM {name} WHERE version_hash = %s;"
        return self._row_to_schema_info(query, version_hash)

    def _build_schema_update_sql(self) -> List[str]:
        sql_updates = []
        for table_name in self.schema.tables:
            exists, storage_table = self.get_storage_table(table_name)
            new_columns = self._create_table_update(table_name, storage_table)
            if len(new_columns) > 0:
                sql = self._get_table_update_sql(table_name, new_columns, exists)
                sql_updates.append(sql)
        return sql_updates

    @abstractmethod
    def _get_table_update_sql(self, table_name: str, new_columns: Sequence[TColumnSchema], exists: bool) -> str:
        pass

    def _create_table_update(self, table_name: str, storage_table: TTableSchemaColumns) -> Sequence[TColumnSchema]:
        # compare table with stored schema and produce delta
        updates = self.schema.get_new_columns(table_name, storage_table)
        logger.info(f"Found {len(updates)} updates for {table_name} in {self.schema.name}")
        return updates

    def _row_to_schema_info(self, query: str, *args: Any) -> StorageSchemaInfo:
        row: Tuple[Any,...] = None
        # if there's no dataset/schema return none info
        with contextlib.suppress(DatabaseUndefinedRelation):
            with self.sql_client.execute_query(query, *args) as cur:
                row = cur.fetchone()
        if not row:
            return None

        # get schema as string
        schema_str = row[5]
        try:
            schema_bytes = base64.b64decode(schema_str, validate=True)
            schema_str = zlib.decompress(schema_bytes).decode("utf-8")
        except binascii.Error:
            pass

        # make utc datetime
        inserted_at = pendulum.instance(row[4])

        return StorageSchemaInfo(row[0], row[1], row[2], row[3], inserted_at, schema_str)

    def _update_schema_in_storage(self, schema: Schema) -> None:
        now_ts = str(pendulum.now())
        # get schema string or zip
        schema_str = json.dumps(schema.to_dict())
        # TODO: not all databases store data as utf-8 but this exception is mostly for redshift
        schema_bytes = schema_str.encode("utf-8")
        caps = self.capabilities()
        if len(schema_bytes) > caps.max_text_data_type_length:
            # compress and to base64
            schema_str = base64.b64encode(zlib.compress(schema_bytes, level=9)).decode("ascii")
        # insert
        name = self.sql_client.make_qualified_table_name(VERSION_TABLE_NAME)
        # values =  schema.version_hash, schema.name, schema.version, schema.ENGINE_VERSION, str(now_ts), schema_str
        self.sql_client.execute_sql(
            f"INSERT INTO {name}({self.VERSION_TABLE_SCHEMA_COLUMNS}) VALUES (%s, %s, %s, %s, %s, %s);", schema.stored_version_hash, schema.name, schema.version, schema.ENGINE_VERSION, now_ts, schema_str
        )
