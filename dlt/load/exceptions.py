from typing import Sequence
from dlt.common.exceptions import DltException
from dlt.destinations.exceptions import DestinationTerminalException


# class LoadException(DltException):
#     def __init__(self, msg: str) -> None:
#         super().__init__(msg)


class LoadClientJobFailed(DestinationTerminalException):
    def __init__(self, load_id: str, job_id: str, failed_message: str) -> None:
        self.load_id = load_id
        self.job_id = job_id
        self.failed_message = failed_message
        super().__init__(f"Job for {job_id} failed terminally in load {load_id} with message {failed_message}")


class LoadClientUnsupportedFileFormats(DestinationTerminalException):
    def __init__(self, file_format: str, supported_file_format: Sequence[str], file_path: str) -> None:
        self.file_format = file_format
        self.supported_types = supported_file_format
        self.file_path = file_path
        super().__init__(f"Loader does not support writer {file_format} in  file {file_path}. Supported writers: {supported_file_format}")


class LoadClientUnsupportedWriteDisposition(DestinationTerminalException):
    def __init__(self, table_name: str, write_disposition: str, file_name: str) -> None:
        self.table_name = table_name
        self.write_disposition = write_disposition
        self.file_name = file_name
        super().__init__(f"Loader does not support {write_disposition} in table {table_name} when loading file {file_name}")
