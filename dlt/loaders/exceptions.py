from typing import Sequence
from dlt.common.exceptions import DltException, TerminalException, TransientException

from dlt.loaders.local_types import LoadJobStatus


class LoadException(DltException):
    def __init__(self, msg: str) -> None:
        super().__init__(msg)


class LoadClientTerminalException(LoadException, TerminalException):
    def __init__(self, msg: str) -> None:
        super().__init__(msg)


class LoadClientTransientException(LoadException, TransientException):
    def __init__(self, msg: str) -> None:
        super().__init__(msg)


class LoadClientTerminalInnerException(LoadClientTerminalException):
    def __init__(self, msg: str, inner_exc: Exception) -> None:
        self.inner_exc = inner_exc
        super().__init__(msg)


class LoadClientTransientInnerException(LoadClientTransientException):
    def __init__(self, msg: str, inner_exc: Exception) -> None:
        self.inner_exc = inner_exc
        super().__init__(msg)



class LoadJobNotExistsException(LoadClientTerminalException):
    def __init__(self, job_id: str) -> None:
        super().__init__(f"Job with id/file name {job_id} not found")


class LoadUnknownTableException(LoadClientTerminalException):
    def __init__(self, table_name: str, file_name: str) -> None:
        self.table_name = table_name
        super().__init__(f"Client does not know table {table_name} for load file {file_name}")


class LoadJobInvalidStateTransitionException(LoadClientTerminalException):
    def __init__(self, from_state: LoadJobStatus, to_state: LoadJobStatus) -> None:
        self.from_state = from_state
        self.to_state = to_state
        super().__init__(f"Load job cannot transition form {from_state} to {to_state}")

class LoadJobServerTerminalException(LoadClientTerminalException):
    def __init__(self, file_path: str) -> None:
        super().__init__(f"Job with id/file name {file_path} encountered unrecoverable problem")


class LoadClientSchemaVersionCorrupted(LoadClientTerminalException):
    def __init__(self, dataset_name: str) -> None:
        self.dataset_name = dataset_name
        super().__init__(f"Schema _version table contains too many rows in {dataset_name}")


class LoadClientSchemaWillNotUpdate(LoadClientTerminalException):
    def __init__(self, table_name: str, columns: Sequence[str], msg: str) -> None:
        self.table_name = table_name
        self.columns = columns
        super().__init__(f"Schema for table {table_name} column(s) {columns} will not update: {msg}")


class LoadFileTooBig(LoadClientTerminalException):
    def __init__(self, file_name: str, max_size: int) -> None:
        super().__init__(f"File {file_name} exceedes {max_size} and cannot be loaded. Split the file and try again.")
