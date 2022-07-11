from abc import ABC, abstractmethod
from types import TracebackType
from typing import Any, Generic, Iterator, List, Tuple, Optional, Sequence, Type, TypeVar, AnyStr, TypedDict
from pathlib import Path

from dlt.common import pendulum, logger
from dlt.common.dataset_writers import TWriterType
from dlt.common.schema import TColumn, Schema, TTableColumns

from dlt.loaders.local_types import LoadJobStatus
from dlt.loaders.exceptions import LoadClientSchemaVersionCorrupted, LoadUnknownTableException

# native connection
TNativeConn = TypeVar("TNativeConn", bound="object")

# type for dbapi cursor
class DBCursor:
    closed: Any
    connection: Any
    query: Any
    description: Tuple[Any, ...]

    def execute(self, query: AnyStr, *args: Any, **kwargs: Any ) -> None:
        ...
    def fetchall(self) -> List[Tuple[Any, ...]]:
        ...
    def fetchmany(self, size: int = ...) -> List[Tuple[Any, ...]]:
        ...
    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        ...


class TJobClientCapabilities(TypedDict):
    writer_type: TWriterType


class LoadJob:
    def __init__(self, file_name: str) -> None:
        """
        File name is also a job id (or job id is deterministically derived) so it must be globally unique
        """
        self._file_name = file_name

    @abstractmethod
    def status(self) -> LoadJobStatus:
        pass

    @abstractmethod
    def file_name(self) -> str:
        pass

    @abstractmethod
    def exception(self) -> str:
        pass


class LoadEmptyJob(LoadJob):
    def __init__(self, file_name: str, status: LoadJobStatus, exception: str = None) -> None:
        self._status = status
        self._exception = exception
        super().__init__(file_name)

    def status(self) -> LoadJobStatus:
        return self._status

    def file_name(self) -> str:
        return self._file_name

    def exception(self) -> str:
        return self._exception


class JobClientBase(ABC):
    def __init__(self, schema: Schema) -> None:
        self.schema = schema

    @abstractmethod
    def initialize_storage(self) -> None:
        pass

    @abstractmethod
    def update_storage_schema(self) -> None:
        pass

    @abstractmethod
    def start_file_load(self, table_name: str, file_path: str) -> LoadJob:
        pass

    @abstractmethod
    def restore_file_load(self, file_path: str) -> LoadJob:
        pass

    @abstractmethod
    def complete_load(self, load_id: str) -> None:
        pass

    @property
    @abstractmethod
    def capabilities(self) -> TJobClientCapabilities:
        pass

    @abstractmethod
    def __enter__(self) -> "JobClientBase":
        pass

    @abstractmethod
    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        pass

    def _get_table_by_name(self, table_name: str, file_name: str) -> TTableColumns:
        try:
            return self.schema.get_table_columns(table_name)
        except KeyError:
            raise LoadUnknownTableException(table_name, file_name)

    @staticmethod
    def get_file_name_from_file_path(file_path: str) -> str:
        return Path(file_path).name

    @staticmethod
    def make_job_with_status(file_path: str, status: LoadJobStatus, message: str = None) -> LoadJob:
        return LoadEmptyJob(JobClientBase.get_file_name_from_file_path(file_path), status, exception=message)

    @staticmethod
    def make_absolute_path(file_path: str) -> str:
        return str(Path(file_path).absolute())


class SqlClientBase(ABC, Generic[TNativeConn]):
    def __init__(self, default_schema_name: str) -> None:
        if not default_schema_name:
            raise ValueError(default_schema_name)
        self._qualified_default_schema_name: str = None
        self._qualified_default_schema_name = self.fully_qualified_schema_name(default_schema_name)

    @abstractmethod
    def open_connection(self) -> None:
        pass

    @abstractmethod
    def close_connection(self) -> None:
        pass

    def __enter__(self) -> "SqlClientBase[TNativeConn]":
        self.open_connection()
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        self.close_connection()

    @abstractmethod
    def native_connection(self) -> TNativeConn:
        pass

    @abstractmethod
    def has_schema(self, qualified_schema_name: str = None) -> bool:
        pass

    @abstractmethod
    def create_schema(self, qualified_schema_name: str = None) -> None:
        pass

    @abstractmethod
    def drop_schema(self, qualified_schema_name: str = None) -> None:
        pass

    @abstractmethod
    def execute_sql(self, sql: AnyStr, *args: Any, **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]:
        pass

    @abstractmethod
    def execute_query(self, query: AnyStr, *args: Any, **kwargs: Any) -> Iterator[DBCursor]:
        pass

    @abstractmethod
    def fully_qualified_schema_name(self, schema_name: str = None) -> str:
        pass

    def fully_qualified_table_name(self, table_name: str) -> str:
        return f"{self.fully_qualified_schema_name()}.{table_name}"


class SqlJobClientBase(JobClientBase):
    def __init__(self, schema: Schema, sql_client: SqlClientBase[TNativeConn]) -> None:
        super().__init__(schema)
        self.sql_client = sql_client

    def update_storage_schema(self) -> None:
        storage_version = self._get_schema_version_from_storage()
        if storage_version < self.schema.schema_version:
            for sql in self._build_schema_update_sql():
                self.sql_client.execute_sql(sql)
            self._update_schema_version(self.schema.schema_version)

    def complete_load(self, load_id: str) -> None:
        name = self.sql_client.fully_qualified_table_name(Schema.LOADS_TABLE_NAME)
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

    def _create_table_update(self, table_name: str, storage_table: TTableColumns) -> Sequence[TColumn]:
        # compare table with stored schema and produce delta
        updates = self.schema.get_schema_update_for(table_name, storage_table)
        logger.info(f"Found {len(updates)} updates for {table_name} in {self.schema.schema_name}")
        return updates

    def _get_schema_version_from_storage(self) -> int:
        name = self.sql_client.fully_qualified_table_name(Schema.VERSION_TABLE_NAME)
        rows = self.sql_client.execute_sql(f"SELECT {Schema.VERSION_COLUMN_NAME} FROM {name} ORDER BY inserted_at DESC LIMIT 1;")
        if len(rows) > 1:
            raise LoadClientSchemaVersionCorrupted(self.sql_client._qualified_default_schema_name)
        if len(rows) == 0:
            return 0
        return int(rows[0][0])

    def _update_schema_version(self, new_version: int) -> None:
        now_ts = str(pendulum.now())
        name = self.sql_client.fully_qualified_table_name(Schema.VERSION_TABLE_NAME)
        self.sql_client.execute_sql(f"INSERT INTO {name}({Schema.VERSION_COLUMN_NAME}, engine_version, inserted_at) VALUES ({new_version}, {Schema.ENGINE_VERSION}, '{now_ts}');")
