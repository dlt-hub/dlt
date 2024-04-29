from typing import Sequence

from dlt.common.destination.exceptions import (
    DestinationTerminalException,
    DestinationTransientException,
    DestinationUndefinedEntity,
    DestinationException,
)
from dlt.common.destination.reference import TLoadJobState


class DatabaseException(DestinationException):
    def __init__(self, dbapi_exception: Exception) -> None:
        self.dbapi_exception = dbapi_exception
        super().__init__(dbapi_exception)


class DatabaseTerminalException(DestinationTerminalException, DatabaseException):
    def __init__(self, dbapi_exception: Exception) -> None:
        super().__init__(dbapi_exception)


class DatabaseUndefinedRelation(DestinationUndefinedEntity, DatabaseException):
    def __init__(self, dbapi_exception: Exception) -> None:
        super().__init__(dbapi_exception)


class DatabaseTransientException(DestinationTransientException, DatabaseException):
    def __init__(self, dbapi_exception: Exception) -> None:
        super().__init__(dbapi_exception)


class DestinationConnectionError(DestinationTransientException):
    def __init__(
        self, client_type: str, dataset_name: str, reason: str, inner_exc: Exception
    ) -> None:
        self.client_type = client_type
        self.dataset_name = dataset_name
        self.inner_exc = inner_exc
        super().__init__(
            f"Connection with {client_type} to dataset name {dataset_name} failed. Please check if"
            " you configured the credentials at all and provided the right credentials values. You"
            " can be also denied access or your internet connection may be down. The actual reason"
            f" given is: {reason}"
        )


class LoadClientNotConnected(DestinationTransientException):
    def __init__(self, client_type: str, dataset_name: str) -> None:
        self.client_type = client_type
        self.dataset_name = dataset_name
        super().__init__(
            f"Connection with {client_type} to dataset {dataset_name} is closed. Open the"
            " connection with 'client.open_connection' or with the 'with client:' statement"
        )


class DestinationSchemaWillNotUpdate(DestinationTerminalException):
    def __init__(self, table_name: str, columns: Sequence[str], msg: str) -> None:
        self.table_name = table_name
        self.columns = columns
        super().__init__(
            f"Schema for table {table_name} column(s) {columns} will not update: {msg}"
        )


class LoadJobNotExistsException(DestinationTerminalException):
    def __init__(self, job_id: str) -> None:
        super().__init__(f"Job with id/file name {job_id} not found")


class LoadJobTerminalException(DestinationTerminalException):
    def __init__(self, file_path: str, message: str) -> None:
        super().__init__(
            f"Job with id/file name {file_path} encountered unrecoverable problem: {message}"
        )


class LoadJobInvalidStateTransitionException(DestinationTerminalException):
    def __init__(self, from_state: TLoadJobState, to_state: TLoadJobState) -> None:
        self.from_state = from_state
        self.to_state = to_state
        super().__init__(f"Load job cannot transition form {from_state} to {to_state}")


class LoadJobFileTooBig(DestinationTerminalException):
    def __init__(self, file_name: str, max_size: int) -> None:
        super().__init__(
            f"File {file_name} exceeds {max_size} and cannot be loaded. Split the file and try"
            " again."
        )


class MergeDispositionException(DestinationTerminalException):
    def __init__(
        self, dataset_name: str, staging_dataset_name: str, tables: Sequence[str], reason: str
    ) -> None:
        self.dataset_name = dataset_name
        self.staging_dataset_name = staging_dataset_name
        self.tables = tables
        self.reason = reason
        msg = (
            f"Merge sql job for dataset name {dataset_name}, staging dataset name"
            f" {staging_dataset_name} COULD NOT BE GENERATED. Merge will not be performed. "
        )
        msg += (
            f"Data for the following tables ({tables}) is loaded to staging dataset. You may need"
            " to write your own materialization. The reason is:\n"
        )
        msg += reason
        super().__init__(msg)


class InvalidFilesystemLayout(DestinationTerminalException):
    def __init__(
        self,
        layout: str,
        expected_placeholders: Sequence[str],
        extra_placeholders: Sequence[str],
        invalid_placeholders: Sequence[str],
        unused_placeholders: Sequence[str],
    ) -> None:
        self.invalid_placeholders = invalid_placeholders
        self.extra_placeholders = extra_placeholders
        self.expected_placeholders = expected_placeholders
        self.unused_placeholders = unused_placeholders
        self.layout = layout

        message = (
            f"Layout '{layout}' expected {', '.join(expected_placeholders)} placeholders."
            f"Missing placeholders: {', '.join(invalid_placeholders)}."
        )

        if extra_placeholders:
            message += f"Extra placeholders specified: {', '.join(extra_placeholders)}."

        if unused_placeholders:
            message += f"Unused placeholders: {', '.join(unused_placeholders)}."

        super().__init__(message)


class InvalidPlaceholderCallback(DestinationTransientException):
    def __init__(self, callback_name: str) -> None:
        self.callback_name = callback_name
        super().__init__(
            f"Invalid placeholder callback: {callback_name}, please make sure it can"
            " accept parameters the following `schema name`, `table name`,"
            " `load_id`, `file_id` and an `extension`",
        )


class CantExtractTablePrefix(DestinationTerminalException):
    def __init__(self, layout: str, details: str) -> None:
        msg = f"Cannot extract unique table prefix in layout '{layout}'. "
        msg += details
        msg += "An example of valid layout: {table_name}/{load_id}.{file_id}.{ext}"
        super().__init__(msg)
