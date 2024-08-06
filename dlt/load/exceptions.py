from typing import Sequence
from dlt.common.destination.exceptions import (
    DestinationTerminalException,
    DestinationTransientException,
)


class LoadClientJobException(Exception):
    load_id: str
    job_id: str


class LoadClientJobFailed(DestinationTerminalException, LoadClientJobException):
    def __init__(self, load_id: str, job_id: str, failed_message: str) -> None:
        self.load_id = load_id
        self.job_id = job_id
        self.failed_message = failed_message
        super().__init__(
            f"Job for {job_id} failed terminally in load {load_id} with message {failed_message}."
            " The package is aborted and cannot be retried."
        )


class LoadClientJobRetry(DestinationTransientException, LoadClientJobException):
    def __init__(
        self, load_id: str, job_id: str, retry_count: int, max_retry_count: int, retry_message: str
    ) -> None:
        self.load_id = load_id
        self.job_id = job_id
        self.retry_count = retry_count
        self.max_retry_count = max_retry_count
        self.retry_message = retry_message
        super().__init__(
            f"Job for {job_id} had {retry_count} retries which a multiple of {max_retry_count}."
            " Exiting retry loop. You can still rerun the load package to retry this job."
            f" Last failure message was {retry_message}"
        )


class LoadClientUnsupportedFileFormats(DestinationTerminalException):
    def __init__(
        self, file_format: str, supported_file_format: Sequence[str], file_path: str
    ) -> None:
        self.file_format = file_format
        self.supported_types = supported_file_format
        self.file_path = file_path
        super().__init__(
            f"Loader does not support writer {file_format} in  file {file_path}. Supported writers:"
            f" {supported_file_format}"
        )


class LoadClientUnsupportedWriteDisposition(DestinationTerminalException):
    def __init__(self, table_name: str, write_disposition: str, file_name: str) -> None:
        self.table_name = table_name
        self.write_disposition = write_disposition
        self.file_name = file_name
        super().__init__(
            f"Loader does not support {write_disposition} in table {table_name} when loading file"
            f" {file_name}"
        )


class FollowupJobCreationFailedException(DestinationTransientException):
    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"Failed to create followup job for job with id {job_id}")


class TableChainFollowupJobCreationFailedException(DestinationTransientException):
    def __init__(self, root_table_name: str) -> None:
        self.root_table_name = root_table_name
        super().__init__(
            "Failed creating table chain followup jobs for table chain with root table"
            f" {root_table_name}."
        )
