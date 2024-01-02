from typing import Sequence
from dlt.destinations.exceptions import DestinationTerminalException, DestinationTransientException


# class LoadException(DltException):
#     def __init__(self, msg: str) -> None:
#         super().__init__(msg)


class LoadClientJobFailed(DestinationTerminalException):
    def __init__(self, load_id: str, job_id: str, failed_message: str) -> None:
        self.load_id = load_id
        self.job_id = job_id
        self.failed_message = failed_message
        super().__init__(
            f"Job for {job_id} failed terminally in load {load_id} with message {failed_message}."
            " The package is aborted and cannot be retried."
        )


class LoadClientJobRetry(DestinationTransientException):
    def __init__(self, load_id: str, job_id: str, retry_count: int, max_retry_count: int) -> None:
        self.load_id = load_id
        self.job_id = job_id
        self.retry_count = retry_count
        self.max_retry_count = max_retry_count
        super().__init__(
            f"Job for {job_id} had {retry_count} retries which a multiple of {max_retry_count}."
            " Exiting retry loop. You can still rerun the load package to retry this job."
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
