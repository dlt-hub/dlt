import os
import tempfile
import shutil
from pathlib import Path
from typing import IO, Any, List

from dlt.common.utils import encoding_for_mode


class FileStorage:
    def __init__(self,
                 storage_path: str,
                 file_type: str = "t",
                 makedirs: bool = False) -> None:
        # make it absolute path
        self.storage_path = os.path.join(os.path.realpath(storage_path), '')
        self.file_type = file_type
        if makedirs:
            os.makedirs(storage_path, exist_ok=True)

    @classmethod
    def from_file(cls, file_path: str, file_type: str = "t",) -> "FileStorage":
        return cls(os.path.dirname(file_path), file_type)

    def save(self, relative_path: str, data: Any) -> str:
        return self.save_atomic(self.storage_path, relative_path, data, file_type=self.file_type)

    @staticmethod
    def save_atomic(storage_path: str, relative_path: str, data: Any, file_type: str = "t") -> str:
        mode = "w" + file_type
        with tempfile.NamedTemporaryFile(dir=storage_path, mode=mode, delete=False, encoding=encoding_for_mode(mode)) as f:
            tmp_path = f.name
            f.write(data)
        try:
            dest_path = os.path.join(storage_path, relative_path)
            # os.rename reverts to os.replace on posix. on windows this operation is not atomic!
            os.replace(tmp_path, dest_path)
            return dest_path
        except Exception:
            if os.path.isfile(tmp_path):
                os.remove(tmp_path)
            raise

    def load(self, relative_path: str) -> Any:
        # raises on file not existing
        with self.open_file(relative_path) as text_file:
            return text_file.read()

    def delete(self, relative_path: str) -> None:
        file_path = self._make_path(relative_path)
        if os.path.isfile(file_path):
            os.remove(file_path)
        else:
            raise FileNotFoundError(file_path)

    def delete_folder(self, relative_path: str, recursively: bool = False) -> None:
        folder_path = self._make_path(relative_path)
        if os.path.isdir(folder_path):
            if recursively:
                shutil.rmtree(folder_path)
            else:
                os.rmdir(folder_path)
        else:
            raise NotADirectoryError(folder_path)

    def open_file(self, realtive_path: str, mode: str = "r") -> IO[Any]:
        mode = mode + self.file_type
        return open(self._make_path(realtive_path), mode, encoding=encoding_for_mode(mode))

    def open_temp(self, delete: bool = False, mode: str = "w", file_type: str = None) -> IO[Any]:
        mode = mode + file_type or self.file_type
        return tempfile.NamedTemporaryFile(dir=self.storage_path, mode=mode, delete=delete, encoding=encoding_for_mode(mode))

    def has_file(self, relative_path: str) -> bool:
        return os.path.isfile(self._make_path(relative_path))

    def has_folder(self, relative_path: str) -> bool:
        return os.path.isdir(self._make_path(relative_path))

    def list_folder_files(self, relative_path: str, to_root: bool = True) -> List[str]:
        scan_path = self._make_path(relative_path)
        if to_root:
            # list files in relative path, returning paths relative to storage root
            return [os.path.join(relative_path, e.name) for e in os.scandir(scan_path) if e.is_file()]
        else:
            # or to the folder
            return [e.name for e in os.scandir(scan_path) if e.is_file()]

    def list_folder_dirs(self, relative_path: str, to_root: bool = True) -> List[str]:
        # list content of relative path, returning paths relative to storage root
        scan_path = self._make_path(relative_path)
        if to_root:
            # list folders in relative path, returning paths relative to storage root
            return [os.path.join(relative_path, e.name) for e in os.scandir(scan_path) if e.is_dir()]
        else:
            # or to the folder
            return [e.name for e in os.scandir(scan_path) if e.is_dir()]

    def create_folder(self, relative_path: str, exists_ok: bool = False) -> None:
        os.makedirs(self._make_path(relative_path), exist_ok=exists_ok)

    def copy_cross_storage_atomically(self, dest_volume_root: str, dest_relative_path: str, source_path: str, dest_name: str) -> None:
        external_tmp_file = tempfile.mktemp(dir=dest_volume_root)
        # first copy to temp file
        shutil.copy(self._make_path(source_path), external_tmp_file)
        # then rename to dest name
        external_dest = os.path.join(dest_volume_root, dest_relative_path, dest_name)
        try:
            os.rename(external_tmp_file, external_dest)
        except Exception:
            if os.path.isfile(external_tmp_file):
                os.remove(external_tmp_file)
            raise

    def atomic_rename(self, from_relative_path: str, to_relative_path: str) -> None:
        os.rename(
            self._make_path(from_relative_path),
            self._make_path(to_relative_path)
        )

    def in_storage(self, path: str) -> bool:
        file = os.path.realpath(path)
        # return true, if the common prefix of both is equal to directory
        # e.g. /a/b/c/d.rst and directory is /a/b, the common prefix is /a/b
        return os.path.commonprefix([file, self.storage_path]) == self.storage_path

    def to_relative_path(self, path: str) -> str:
        if not self.in_storage(path):
            raise ValueError(path)
        return os.path.relpath(path, start=self.storage_path)

    def get_file_stem(self, path: str) ->  str:
        return Path(os.path.basename(path)).stem

    def get_file_name(self, path: str) ->  str:
        return Path(path).name

    def _make_path(self, relative_path: str) -> str:
        return os.path.join(self.storage_path, relative_path)
