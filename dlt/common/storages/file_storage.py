import gzip
import os
import re
import stat
import errno
import tempfile
import shutil
import pathvalidate
from typing import IO, Any, Optional, List, cast, overload
from dlt.common.typing import AnyFun

from dlt.common.utils import encoding_for_mode, uniq_id


FILE_COMPONENT_INVALID_CHARACTERS = re.compile(r"[.%{}]")

class FileStorage:
    def __init__(self,
                 storage_path: str,
                 file_type: str = "t",
                 makedirs: bool = False) -> None:
        # make it absolute path
        self.storage_path = os.path.realpath(storage_path)  # os.path.join(, '')
        self.file_type = file_type
        if makedirs:
            os.makedirs(storage_path, exist_ok=True)

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

    @staticmethod
    def copy_atomic(source_file_path: str, dest_folder_path: str) -> str:
        file_name = os.path.basename(source_file_path)
        dest_file_path = os.path.join(dest_folder_path, file_name)
        try:
            os.rename(source_file_path, dest_file_path)
        except OSError:
            # copy to local temp file
            dest_temp_file = os.path.join(dest_folder_path, uniq_id())
            try:
                shutil.copyfile(source_file_path, dest_temp_file)
                os.rename(dest_temp_file, dest_file_path)
                os.unlink(source_file_path)
            except Exception:
                if os.path.isfile(dest_temp_file):
                    os.remove(dest_temp_file)
                raise
        return dest_file_path

    def load(self, relative_path: str) -> Any:
        # raises on file not existing
        with self.open_file(relative_path) as text_file:
            return text_file.read()

    def delete(self, relative_path: str) -> None:
        file_path = self.make_full_path(relative_path)
        if os.path.isfile(file_path):
            os.remove(file_path)
        else:
            raise FileNotFoundError(file_path)

    def delete_folder(self, relative_path: str, recursively: bool = False, delete_ro: bool = False) -> None:
        folder_path = self.make_full_path(relative_path)
        if os.path.isdir(folder_path):
            if recursively:
                if delete_ro:
                    del_ro = self.rmtree_del_ro
                else:
                    del_ro = None
                shutil.rmtree(folder_path, onerror=del_ro)
            else:
                os.rmdir(folder_path)
        else:
            raise NotADirectoryError(folder_path)

    def open_file(self, relative_path: str, mode: str = "r") -> IO[Any]:
        if "b" not in mode and "t" not in mode:
            mode = mode + self.file_type
        if "r" in mode:
            return FileStorage.open_zipsafe_ro(self.make_full_path(relative_path), mode)
        return open(self.make_full_path(relative_path), mode, encoding=encoding_for_mode(mode))

    def open_temp(self, delete: bool = False, mode: str = "w", file_type: str = None) -> IO[Any]:
        mode = mode + file_type or self.file_type
        return tempfile.NamedTemporaryFile(dir=self.storage_path, mode=mode, delete=delete, encoding=encoding_for_mode(mode))

    def has_file(self, relative_path: str) -> bool:
        return os.path.isfile(self.make_full_path(relative_path))

    def has_folder(self, relative_path: str) -> bool:
        return os.path.isdir(self.make_full_path(relative_path))

    def list_folder_files(self, relative_path: str, to_root: bool = True) -> List[str]:
        """List all files in ``relative_path`` folder

        Args:
            relative_path (str): A path to folder, relative to storage root
            to_root (bool, optional): If True returns paths to files in relation to root, if False, returns just file names. Defaults to True.

        Returns:
            List[str]: A list of file names with optional path as per ``to_root`` parameter
        """
        scan_path = self.make_full_path(relative_path)
        if to_root:
            # list files in relative path, returning paths relative to storage root
            return [os.path.join(relative_path, e.name) for e in os.scandir(scan_path) if e.is_file()]
        else:
            # or to the folder
            return [e.name for e in os.scandir(scan_path) if e.is_file()]

    def list_folder_dirs(self, relative_path: str, to_root: bool = True) -> List[str]:
        # list content of relative path, returning paths relative to storage root
        scan_path = self.make_full_path(relative_path)
        if to_root:
            # list folders in relative path, returning paths relative to storage root
            return [os.path.join(relative_path, e.name) for e in os.scandir(scan_path) if e.is_dir()]
        else:
            # or to the folder
            return [e.name for e in os.scandir(scan_path) if e.is_dir()]

    def create_folder(self, relative_path: str, exists_ok: bool = False) -> None:
        os.makedirs(self.make_full_path(relative_path), exist_ok=exists_ok)

    def link_hard(self, from_relative_path: str, to_relative_path: str) -> None:
        # note: some interesting stuff on links https://lightrun.com/answers/conan-io-conan-research-investigate-symlinks-and-hard-links
        os.link(
            self.make_full_path(from_relative_path),
            self.make_full_path(to_relative_path)
        )

    def atomic_rename(self, from_relative_path: str, to_relative_path: str) -> None:
        """Renames a path using os.rename which is atomic on POSIX, Windows and NFS v4.

        Method falls back to non-atomic method in following cases:
        1. On Windows when destination file exists
        2. If underlying file system does not support atomic rename
        3. All buckets mapped with FUSE are not atomic
        """

        os.rename(
            self.make_full_path(from_relative_path),
            self.make_full_path(to_relative_path)
        )

    def rename_tree(self, from_relative_path: str, to_relative_path: str) -> None:
        """Renames a tree using os.rename if possible making it atomic

        If we get 'too many open files': in that case `rename_tree_files is used
        """

        try:
            self.atomic_rename(from_relative_path, to_relative_path)
            return
        except OSError as ex:
            if ex.errno != errno.EMFILE:
                raise
        self.rename_tree_files(from_relative_path, to_relative_path)

    def rename_tree_files(self, from_relative_path: str, to_relative_path: str) -> None:
        """Renames files in a tree recursively using os.rename."""
        from_path = self.make_full_path(from_relative_path)
        to_path = self.make_full_path(to_relative_path)

        if not os.path.isdir(from_path):
            raise ValueError(f"{from_path} is not a directory")

        # make sure the destination directory exists
        os.makedirs(to_path, exist_ok=True)

        # move files and directories from source to destination starting from leafs
        for root, _, files in os.walk(from_path, topdown=False):
            to_root = root.replace(from_path, to_path, 1)
            os.makedirs(to_root, exist_ok=True)

            for name in files:
                file_from_path = os.path.join(root, name)
                os.rename(file_from_path, os.path.join(to_root, name))

            if not os.listdir(root):
                os.rmdir(root)

    def atomic_import(self, external_file_path: str, to_folder: str) -> str:
        """Moves a file at `external_file_path` into the `to_folder` effectively importing file into storage"""
        return self.to_relative_path(FileStorage.copy_atomic(external_file_path, self.make_full_path(to_folder)))
        # file_name = FileStorage.get_file_name_from_file_path(external_path)
        # os.rename(external_path, os.path.join(self.make_full_path(to_folder), file_name))

    def in_storage(self, path: str) -> bool:
        assert path is not None
        # all paths are relative to root
        if not os.path.isabs(path):
            path = os.path.join(self.storage_path, path)
        file = os.path.realpath(path)
        # return true, if the common prefix of both is equal to directory
        # e.g. /a/b/c/d.rst and directory is /a/b, the common prefix is /a/b
        return os.path.commonprefix([file, self.storage_path]) == self.storage_path

    def to_relative_path(self, path: str) -> str:
        if path == "":
            return ""
        if not self.in_storage(path):
            raise ValueError(path)
        if not os.path.isabs(path):
            path = os.path.realpath(os.path.join(self.storage_path, path))
        # for abs paths find the relative
        return os.path.relpath(path, start=self.storage_path)

    def make_full_path(self, path: str) -> str:
        # try to make a relative path if paths are absolute or overlapping
        path = self.to_relative_path(path)
        # then assume that it is a path relative to storage root
        return os.path.realpath(os.path.join(self.storage_path, path))

    def from_wd_to_relative_path(self, wd_relative_path: str) -> str:
        path = os.path.realpath(wd_relative_path)
        return self.to_relative_path(path)

    def from_relative_path_to_wd(self, relative_path: str) -> str:
        return os.path.relpath(self.make_full_path(relative_path), start=".")

    @staticmethod
    def get_file_name_from_file_path(file_path: str) -> str:
        return os.path.basename(file_path)

    @staticmethod
    def validate_file_name_component(name: str) -> None:
        # Universal platform bans several characters allowed in POSIX ie. | < \ or "COM1" :)
        pathvalidate.validate_filename(name, platform="Universal")
        # component cannot contain "."
        if FILE_COMPONENT_INVALID_CHARACTERS.search(name):
            raise pathvalidate.error.InvalidCharError(description="Component name cannot contain the following characters: . % { }")

    @staticmethod
    def rmtree_del_ro(action: AnyFun, name: str, exc: Any) -> Any:
        if action is os.unlink or action is os.remove or action is os.rmdir:
            os.chmod(name, stat.S_IWRITE)
            if os.path.isdir(name):
                os.rmdir(name)
            else:
                os.remove(name)

    @staticmethod
    def open_zipsafe_ro(path: str, mode: str = "r", **kwargs: Any) -> IO[Any]:
        """Opens a file using gzip.open if it is a gzip file, otherwise uses open."""
        assert "r" in mode, "FileStorage.open_zipsafe_ro only supports read modes"
        encoding = kwargs.pop("encoding", encoding_for_mode(mode))
        origmode = str(mode)
        try:
            if encoding is not None and mode == "r":
                mode += "t"  # gzip requires text mode explicitly to use encoding
            f = gzip.open(path, mode, encoding=encoding, **kwargs)
            # Force gzip to read the first few bytes and check the magic number
            f.read(2), f.seek(0)
            return cast(IO[Any], f)
        except (gzip.BadGzipFile, OSError):
            return open(path, origmode, encoding=encoding, **kwargs)


    @staticmethod
    def is_gzipped(path: str) -> bool:
        """Checks if file under path is gzipped by reading a header"""
        try:
            with gzip.open(path, "rt", encoding="utf-8") as f:
                # Force gzip to read the first few bytes and check the magic number
                f.read(2)
                return True
        except (gzip.BadGzipFile, OSError):
            return False
