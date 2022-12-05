from typing import Sequence
from dlt.common.exceptions import TerminalException, TransientException, DestinationException
from dlt.common.destination import TLoadJobStatus


class DestinationTerminalException(DestinationException, TerminalException):
    pass

class DestinationTransientException(DestinationException, TransientException):
    pass


class DatabaseException(DestinationException):
    def __init__(self, dbapi_exception: Exception) -> None:
        self.dbapi_exception = dbapi_exception
        super().__init__(dbapi_exception)


class DatabaseTerminalException(DestinationTerminalException, DatabaseException):
    def __init__(self, dbapi_exception: Exception) -> None:
        super().__init__(dbapi_exception)


class DatabaseUndefinedRelation(DatabaseTerminalException):
    def __init__(self, dbapi_exception: Exception) -> None:
        super().__init__(dbapi_exception)


class DatabaseTransientException(DestinationTransientException, DatabaseException):
    def __init__(self, dbapi_exception: Exception) -> None:
        super().__init__(dbapi_exception)


class DestinationConnectionError(DestinationTransientException):
    def __init__(self, client_type: str, dataset_name: str, reason: str, inner_exc: Exception) -> None:
        self.client_type = client_type
        self.dataset_name = dataset_name
        self.inner_exc = inner_exc
        super().__init__(f"Connection with {client_type} to dataset name {dataset_name} failed. Please check if you configured the credentials at all and provided the right credentials values. You can be also denied access or your internet connection may be down. The actual reason given is: {reason}")

class LoadClientNotConnected(DestinationTransientException):
    def __init__(self, client_type: str, dataset_name: str) -> None:
        self.client_type = client_type
        self.dataset_name = dataset_name
        super().__init__(f"Connection with {client_type} to dataset {dataset_name} is closed. Open the connection with 'client.open_connection' or with the 'with client:' statement")


class DestinationSchemaWillNotUpdate(DestinationTerminalException):
    def __init__(self, table_name: str, columns: Sequence[str], msg: str) -> None:
        self.table_name = table_name
        self.columns = columns
        super().__init__(f"Schema for table {table_name} column(s) {columns} will not update: {msg}")


class LoadJobNotExistsException(DestinationTerminalException):
    def __init__(self, job_id: str) -> None:
        super().__init__(f"Job with id/file name {job_id} not found")


class LoadJobTerminalException(DestinationTerminalException):
    def __init__(self, file_path: str) -> None:
        super().__init__(f"Job with id/file name {file_path} encountered unrecoverable problem")


class LoadJobUnknownTableException(DestinationTerminalException):
    def __init__(self, table_name: str, file_name: str) -> None:
        self.table_name = table_name
        super().__init__(f"Client does not know table {table_name} for load file {file_name}")


class LoadJobInvalidStateTransitionException(DestinationTerminalException):
    def __init__(self, from_state: TLoadJobStatus, to_state: TLoadJobStatus) -> None:
        self.from_state = from_state
        self.to_state = to_state
        super().__init__(f"Load job cannot transition form {from_state} to {to_state}")


class LoadJobFileTooBig(DestinationTerminalException):
    def __init__(self, file_name: str, max_size: int) -> None:
        super().__init__(f"File {file_name} exceeds {max_size} and cannot be loaded. Split the file and try again.")
