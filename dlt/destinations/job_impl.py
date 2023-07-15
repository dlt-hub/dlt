import os
import tempfile  # noqa: 251

from dlt.common.storages import FileStorage

from dlt.common.destination.reference import NewLoadJob, FollowupJob, TLoadJobState, LoadJob
from dlt.common.storages.load_storage import ParsedLoadJobFileName

class EmptyLoadJobWithoutFollowup(LoadJob):
    def __init__(self, file_name: str, status: TLoadJobState, exception: str = None) -> None:
        self._status = status
        self._exception = exception
        super().__init__(file_name)

    @classmethod
    def from_file_path(cls, file_path: str, status: TLoadJobState, message: str = None) -> "EmptyLoadJobWithoutFollowup":
        return cls(FileStorage.get_file_name_from_file_path(file_path), status, exception=message)

    def state(self) -> TLoadJobState:
        return self._status

    def exception(self) -> str:
        return self._exception


class EmptyLoadJob(EmptyLoadJobWithoutFollowup, FollowupJob):
    pass


class NewLoadJobImpl(EmptyLoadJobWithoutFollowup, NewLoadJob):
    def _save_text_file(self, data: str) -> None:
        temp_file = os.path.join(tempfile.gettempdir(), self._file_name)
        with open(temp_file, "w", encoding="utf-8") as f:
            f.write(data)
        self._new_file_path = temp_file

    def new_file_path(self) -> str:
        """Path to a newly created temporary job file"""
        return self._new_file_path

class NewReferenceJob(NewLoadJobImpl):

    def __init__(self, file_name: str, status: TLoadJobState, exception: str = None, remote_path: str = None) -> None:
        file_name = os.path.splitext(file_name)[0] + ".reference"
        super().__init__(file_name, status, exception)
        self._remote_path = remote_path
        self._save_text_file(remote_path)

    @staticmethod
    def is_reference_job(file_path: str) -> bool:
        return os.path.splitext(file_path)[1][1:] == "reference"

    @staticmethod
    def resolve_reference(file_path: str) -> str:
        with open(file_path, "r+", encoding="utf-8") as f:
            # Reading from a file
            return f.read()
