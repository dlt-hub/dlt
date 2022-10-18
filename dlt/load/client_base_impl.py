from abc import abstractmethod
from types import TracebackType
from typing import List, Sequence, Type

from dlt.common import pendulum, logger
from dlt.common.storages import FileStorage
from dlt.common.schema import TColumnSchema, Schema, TTableSchemaColumns

from dlt.load.typing import TLoadJobStatus, TNativeConn
from dlt.load.client_base import LoadJob, JobClientBase, SqlClientBase
from dlt.load.configuration import DestinationClientConfiguration
from dlt.load.exceptions import LoadClientSchemaVersionCorrupted


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
    def __init__(self, schema: Schema, config: DestinationClientConfiguration,  sql_client: SqlClientBase[TNativeConn]) -> None:
        super().__init__(schema, config)
        self.sql_client = sql_client

    def update_storage_schema(self) -> None:
        storage_version = self._get_schema_version_from_storage()
        if storage_version < self.schema.stored_version:
            for sql in self._build_schema_update_sql():
                self.sql_client.execute_sql(sql)
            self._update_schema_version(self.schema.stored_version)

    def complete_load(self, load_id: str) -> None:
        name = self.sql_client.make_qualified_table_name(Schema.LOADS_TABLE_NAME)
        now_ts = str(pendulum.now())
        self.sql_client.execute_sql(f"INSERT INTO {name}(load_id, status, inserted_at) VALUES('{load_id}', 0, '{now_ts}');")

    def __enter__(self) -> "SqlJobClientBase":
        self.sql_client.open_connection()
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        self.sql_client.close_connection()

    @abstractmethod
    def _build_schema_update_sql(self) -> List[str]:
        pass

    def _create_table_update(self, table_name: str, storage_table: TTableSchemaColumns) -> Sequence[TColumnSchema]:
        # compare table with stored schema and produce delta
        updates = self.schema.get_new_columns(table_name, storage_table)
        logger.info(f"Found {len(updates)} updates for {table_name} in {self.schema.name}")
        return updates

    def _get_schema_version_from_storage(self) -> int:
        name = self.sql_client.make_qualified_table_name(Schema.VERSION_TABLE_NAME)
        rows = self.sql_client.execute_sql(f"SELECT {Schema.VERSION_COLUMN_NAME} FROM {name} ORDER BY inserted_at DESC LIMIT 1;")
        if len(rows) > 1:
            raise LoadClientSchemaVersionCorrupted(self.sql_client.fully_qualified_dataset_name())
        if len(rows) == 0:
            return 0
        return int(rows[0][0])

    def _update_schema_version(self, new_version: int) -> None:
        now_ts = str(pendulum.now())
        name = self.sql_client.make_qualified_table_name(Schema.VERSION_TABLE_NAME)
        self.sql_client.execute_sql(f"INSERT INTO {name}({Schema.VERSION_COLUMN_NAME}, engine_version, inserted_at) VALUES ({new_version}, {Schema.ENGINE_VERSION}, '{now_ts}');")
