from abc import ABC, abstractmethod
from types import TracebackType
from typing import Any, Literal, Sequence, Type, TypeVar, AnyStr
from pathlib import Path

from dlt.common import pendulum, logger
from dlt.common.schema import Column, Schema, Table
# from dlt.common.file_storage import FileStorage

from dlt.loaders.local_types import LoadJobStatus
from dlt.loaders.exceptions import LoadClientSchemaVersionCorrupted, LoadUnknownTableException

# typing for context manager
TClient = TypeVar("TClient", bound="ClientBase")


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


class ClientBase(ABC):
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
    def get_file_load(self, file_path: str) -> LoadJob:
        pass

    @abstractmethod
    def complete_load(self, load_id: str) -> None:
        pass

    @abstractmethod
    def _open_connection(self) -> None:
        pass

    @abstractmethod
    def _close_connection(self) -> None:
        pass


    def __enter__(self: TClient) -> TClient:
        self._open_connection()
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        self._close_connection()

    def _get_table_by_name(self, table_name: str, file_name: str) -> Table:
        try:
            return self.schema.get_table(table_name)
        except KeyError:
            raise LoadUnknownTableException(table_name, file_name)

    @staticmethod
    def get_file_name_from_file_path(file_path: str) -> str:
        return Path(file_path).name

    @staticmethod
    def make_job_with_status(file_path: str, status: LoadJobStatus, message: str = None) -> LoadJob:
        return LoadEmptyJob(ClientBase.get_file_name_from_file_path(file_path), status, exception=message)

    @staticmethod
    def make_absolute_path(file_path: str) -> str:
        return str(Path(file_path).absolute())


class SqlClientBase(ClientBase):
    def __init__(self, schema: Schema) -> None:
        super().__init__(schema)

    def complete_load(self, load_id: str) -> None:
        name = self._to_canonical_table_name(Schema.LOADS_TABLE_NAME)
        now_ts = str(pendulum.now())
        self._execute_sql(f"INSERT INTO {name}(load_id, status, inserted_at) VALUES('{load_id}', 0, '{now_ts}');")

    @abstractmethod
    def _execute_sql(self, query: AnyStr) -> Any:
        pass

    @abstractmethod
    def _to_canonical_schema_name(self) -> str:
        pass

    def _create_table_update(self, table_name: str, storage_table: Table) -> Sequence[Column]:
        # compare table with stored schema and produce delta
        l = self.schema.get_schema_update_for(table_name, storage_table)
        logger.info(f"Found {len(l)} updates for {table_name} in {self.schema.schema_name}")
        return l

    def _to_canonical_table_name(self, table_name: str) -> str:
        return f"{self._to_canonical_schema_name()}.{table_name}"

    def _get_schema_version_from_storage(self) -> int:
        name = self._to_canonical_table_name(Schema.VERSION_TABLE_NAME)
        rows = list(self._execute_sql(f"SELECT {Schema.VERSION_COLUMN_NAME} FROM {name} ORDER BY inserted_at DESC LIMIT 1;"))
        if len(rows) > 1:
            raise LoadClientSchemaVersionCorrupted(self._to_canonical_schema_name())
        if len(rows) == 0:
            return 0
        return int(rows[0][0])

    def _update_schema_version(self, new_version: int) -> None:
        now_ts = str(pendulum.now())
        name = self._to_canonical_table_name(Schema.VERSION_TABLE_NAME)
        self._execute_sql(f"INSERT INTO {name}({Schema.VERSION_COLUMN_NAME}, engine_version, inserted_at) VALUES ({new_version}, {Schema.ENGINE_VERSION}, '{now_ts}');")
