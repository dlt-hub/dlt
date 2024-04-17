import posixpath
from typing import Any, Dict, List, Literal, Optional, Tuple

from dlt.common.json import json
from dlt.common.configuration.specs import GcpCredentials, GcpOAuthCredentials
from dlt.common.exceptions import MissingDependencyException

from fsspec.spec import AbstractFileSystem, AbstractBufferedFile

try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ModuleNotFoundError:
    raise MissingDependencyException("GoogleDriveFileSystem", ["google-api-python-client"])

try:
    from google.auth.credentials import AnonymousCredentials
except ModuleNotFoundError:
    raise MissingDependencyException(
        "GoogleDriveFileSystem", ["google-auth-httplib2 google-auth-oauthlib"]
    )

SCOPES = {
    "full_control": "https://www.googleapis.com/auth/drive",
    "read_only": "https://www.googleapis.com/auth/drive.readonly",
}


DIR_MIME_TYPE = "application/vnd.google-apps.folder"
FILE_INFO_FIELDS = ",".join(
    [
        "name",
        "id",
        "size",
        "description",
        "mimeType",
        "version",
        "createdTime",
        "modifiedTime",
        "capabilities",
    ]
)


DEFAULT_BLOCK_SIZE = 5 * 2**20
# maps (parent id, child_name) into file id
FILE_ID_CACHE: Dict[Tuple[str, str], str] = {}


class GoogleDriveFileSystem(AbstractFileSystem):
    protocol = "gdrive"
    root_marker = ""

    def __init__(
        self,
        credentials: GcpCredentials = None,
        trash_delete: bool = True,
        access: Optional[Literal["full_control", "read_only"]] = "full_control",
        spaces: Optional[Literal["drive", "appDataFolder", "photos"]] = "drive",
        **kwargs: Any,
    ):
        """Google Drive as a file-system.

        The gdrive url has following format: gdrive://<root_file_id/<file_path>
        Where <root_file_id> is a file id of the folder where the <file_path> is present.

        Google Drive provides consistency when file ids are used. Changes are reflected immediately.
        In case of listings (ls) the consistency is eventual. Changes are reflected with a delay.
        As ls is used to retrieve file id from file name, we will be unable to build consistent filesystem
        with google drive API. Use this with care.

        Based on original fsspec Google Drive implementation: https://github.com/fsspec/gdrivefs

        Args:
            credentials (GcpCredentials): Google Service credentials. If not provided, anonymous credentials
                are used
            trash_delete (bool): If True sends files to trash on rm. If False, deletes permanently.
                Note that permanent delete is not available for shared drives
            access (Optional[Literal["full_control", "read_only"]]):
                One of "full_control", "read_only".
            spaces (Optional[Literal["drive", "appDataFolder", "photos"]]):
                Category of files to search, can be 'drive', 'appDataFolder' and 'photos'.
                Of these, only the first is general.
            **kwargs:
                Passed to the parent.
        """
        super().__init__(**kwargs)
        self.trash_delete = trash_delete
        self.access = access
        self.scopes = [SCOPES[access]]
        self.credentials = credentials
        self.spaces = spaces
        self.connect()

    def connect(self) -> None:
        """Connect to Google Drive."""

        if self.credentials:
            if isinstance(self.credentials, GcpOAuthCredentials):
                self.credentials.auth(self.scopes)
            cred = self.credentials.to_native_credentials()
        else:
            cred = AnonymousCredentials()

        srv = build("drive", "v3", credentials=cred)
        self.service = srv.files()

    def mkdir(self, path: str, create_parents: Optional[bool] = True) -> None:
        """Create a directory.

        Args:
            path (str): The directory to create.
            create_parents (Optional[bool]):
                Whether to create parent directories if they don't exist.
                Defaults to True.
        """
        # if there are more than two components, create parents (first component is root id, second - path being created)
        if create_parents and len(path.split("/")) > 2:
            self.makedirs(self._parent(path), exist_ok=True)
        parent_id = self.path_to_file_id(self._parent(path))
        name = path.rstrip("/").rsplit("/", 1)[-1]
        meta = {
            "name": name,
            "mimeType": DIR_MIME_TYPE,
            "parents": [parent_id],
        }
        file = self.service.create(body=meta, supportsAllDrives=True).execute()
        # cache the new dir
        FILE_ID_CACHE[(parent_id, name)] = file["id"]
        try:
            self.invalidate_cache(parent_id)
        except Exception:
            # allow for parent to not exist as valid path
            pass

    def makedirs(self, path: str, exist_ok: Optional[bool] = True) -> None:
        """Create a directory and all its parent components.

        Args:
            path (str): The directory to create.
            exist_ok (Optional[bool]): Whether to raise an error if the directory already exists.
                Defaults to True.
        """
        if self.isdir(path):
            if exist_ok:
                return
            else:
                raise FileExistsError(path)
        # if there are more than two components, create parents (first component is root id, second - path being created)
        if len(path.split("/")) > 2:
            self.makedirs(self._parent(path), exist_ok=True)
        self.mkdir(path, create_parents=False)

    def _is_path_root_id(self, path: str) -> bool:
        """Checks if path is root id"""
        return len(path.split("/")) == 1

    def _delete(self, file_id: str) -> None:
        """Delete a file.

        Args:
            file_id (str): The ID of the file to delete.
        """
        self.service.delete(fileId=file_id, supportsAllDrives=True).execute()

    def _trash(self, file_id: str) -> None:
        """Sends file to trash.

        Args:
            file_id (str): The ID of the file to trash.
        """
        file_metadata = {"trashed": True}
        self.service.update(fileId=file_id, supportsAllDrives=True, body=file_metadata).execute()

    def rm(
        self, path: str, recursive: Optional[bool] = True, maxdepth: Optional[int] = None
    ) -> None:
        """Remove files or directories.

        Args:
            path (str): The file or directory to remove.
            recursive (Optional[bool]): Whether to remove directories recursively.
                Defaults to True.
            maxdepth (Optional[int]): The maximum depth to remove directories.
        """
        if recursive is False and self.isdir(path) and self.ls(path):
            raise ValueError("Attempt to delete non-empty folder")
        file_id = self.path_to_file_id(path)
        if self.trash_delete:
            self._trash(file_id)
        else:
            self._delete(file_id)
        self.invalidate_cache(file_id)
        try:
            if not self._is_path_root_id(path):
                parent_id = self.path_to_file_id(self._parent(path))
                # drop file id if exists
                FILE_ID_CACHE.pop((parent_id, path.split("/")[-1]), None)
                self.invalidate_cache(parent_id)
        except Exception:
            # allow for parent to not exist as valid path
            pass

    def invalidate_cache(self, file_id: str) -> None:
        # remove listing from cache by file id
        self.dircache.pop(file_id, None)

    def rmdir(self, path: str) -> None:
        """Remove a directory.

        Args:
            path (str): The directory to remove.
        """
        if not self.isdir(path):
            raise ValueError("Path is not a directory")
        self.rm(path, recursive=False)

    def info(self, path: str, **kwargs: Any) -> Dict[str, Any]:
        file_id = self.path_to_file_id(path)
        return self._info_by_id(file_id, self._parent(path))

    def _info_by_id(self, file_id: str, path_prefix: Optional[str] = None) -> Dict[str, Any]:
        response = self.service.get(
            fileId=file_id,
            fields=FILE_INFO_FIELDS,
            supportsAllDrives=True,
        ).execute()
        return self._file_info_from_response(response, path_prefix)

    def export(self, path: str, mime_type: str) -> Any:
        """Convert a Google-native file to other format and download

        mime_type is something like "text/plain"
        """
        file_id = self.path_to_file_id(path)
        return self.service.export(
            fileId=file_id, mimeType=mime_type, supportsAllDrives=True
        ).execute()

    def ls(self, path: str, detail: Optional[bool] = False, refresh: Optional[bool] = False) -> Any:
        """List files in a directory.

        Args:
            path (str): The directory to list.
            detail (Optional[bool]): Whether to return detailed file information.
                Defaults to False.

        Returns:
            Any: Files in the directory data.
        """
        file_id = self.path_to_file_id(path)
        files = self._list_directory_by_id(file_id, dir_path=path)

        if detail:
            return files
        else:
            return [f["name"] for f in files]

    def _list_directory_by_id(
        self, dir_file_id: str, dir_path: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List files in a directory by ID.

        Args:
            dir_file_id (str): The ID of the directory to list.
            path_prefix (Optional[str]): The path prefix to use.

        Returns:
            List[Dict[str, Any]]: A list of files in the directory.
        """
        # use file_id for caching
        files = self.dircache.get(dir_file_id)
        if files is not None:
            return files  # type: ignore

        all_files = []
        page_token = None
        all_fields = "nextPageToken, files(%s)" % FILE_INFO_FIELDS
        query = f"'{dir_file_id}' in parents "

        while True:
            response = self.service.list(
                q=query,
                spaces=self.spaces,
                fields=all_fields,
                pageToken=page_token,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                corpora="allDrives",
            ).execute()

            for f in response.get("files", []):
                all_files.append(self._file_info_from_response(f, dir_path))

            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break

        self.dircache[dir_file_id] = all_files
        return all_files

    def path_to_file_id(
        self, path: str, parent_id: Optional[str] = None, parent_path: str = ""
    ) -> str:
        """Get the file ID from a path.

        Args:
            path (str): The path to get the file ID from.
            parent_id (Optional[str]): The parent directory id to search.
            parent_path (Optional[str]): Path corresponding to parent id

        Returns:
            str: The file ID.
        """
        items = path.strip("/").split("/", 3)
        if not parent_id:
            # must have root id
            if len(items) == 0:
                raise ValueError(
                    "Google drive path must start with folder root id ie."
                    " gdrive://15eC3e5MNew2XAIefWNlG8VlEa0ISnnaG/..."
                )
            if len(items) == 1:
                return items[0]  #  only root id present
            parent_id, file_name = items[:2]
            descendants = items[2:]
            parent_path = parent_id
        else:
            if len(items) == 0 or len(path) == 0:
                return parent_id
            file_name = items[0]
            descendants = items[1:]

        # use cached file ids
        top_file_id = FILE_ID_CACHE.get((parent_id, file_name))
        if not top_file_id:
            top_file_id = self._find_file_id_in_dir(file_name, parent_id, parent_path)
            FILE_ID_CACHE[(parent_id, file_name)] = top_file_id
        if not descendants:
            return top_file_id
        else:
            sub_path = posixpath.join(*descendants)
            return self.path_to_file_id(
                sub_path, parent_id=top_file_id, parent_path=posixpath.join(parent_path, file_name)
            )

    def _find_file_id_in_dir(self, file_name: str, dir_file_id: str, dir_path: str) -> Any:
        """Get the file ID of a file with a given name in a directory.

        Args:
            child_name (str): The name of the child to get the file ID of.
            dir_file_id (str): The ID of the directory to search.
            dir_path (str): Path corresponding to dir_file_id

        Returns:
            str: The file ID of the file_name.
        """
        all_children = self._list_directory_by_id(dir_file_id, dir_path=dir_path)
        possible_children = []
        for child in all_children:
            if child["name"].strip("/").split("/")[-1] == file_name:
                possible_children.append(child["id"])

        if len(possible_children) == 0:
            raise FileNotFoundError(f"Directory {dir_file_id} has no child named {file_name}")
        if len(possible_children) == 1:
            return possible_children[0]
        else:
            raise KeyError(
                f"Directory {dir_file_id} has more than one "
                f"child named {file_name}. Unable to resolve path "
                "to file_id."
            )

    def _open(self, path: str, mode: Optional[str] = "rb", **kwargs: Any) -> "GoogleDriveFile":
        """Open a file.

        Args:
            path (str): The file to open.
            mode (Optional[str]): The mode to open the file in.
                Defaults to "rb".
            **kwargs: Passed to the parent.

        Returns:
            GoogleDriveFile: The opened file.
        """
        return GoogleDriveFile(self, path, mode=mode, **kwargs)

    @staticmethod
    def _file_info_from_response(file: Dict[str, Any], path_prefix: str = None) -> Dict[str, Any]:
        """Create fsspec compatible file info"""
        ftype = "directory" if file.get("mimeType") == DIR_MIME_TYPE else "file"
        if path_prefix:
            name = posixpath.join(path_prefix, file["name"])
        else:
            name = file["name"]

        info = {"name": name, "size": int(file.get("size", 0)), "type": ftype}
        file.update(info)
        return file


class GoogleDriveFile(AbstractBufferedFile):
    def __init__(
        self,
        fs: GoogleDriveFileSystem,
        path: str,
        mode: Optional[str] = "rb",
        block_size: Optional[int] = DEFAULT_BLOCK_SIZE,
        autocommit: Optional[bool] = True,
        **kwargs: Any,
    ):
        """A Google Drive file.

        Args:
            fs (AbstractFileSystem): The file system to open the file from.
            path (str): The file to open.
            mode (Optional[str]): The mode to open the file in.
                Defaults to "rb".
            block_size (Optional[str]): The block size to use.
                Defaults to DEFAULT_BLOCK_SIZE.
            autocommit (Optional[bool]): Whether to automatically commit the file.
                Defaults to True.
            **kwargs: Passed to the parent.
        """
        self.root_file_id, _ = path.split("/", 1)
        super().__init__(fs, path, mode, block_size, autocommit=autocommit, **kwargs)
        self.fs = fs
        # try to get file_id
        try:
            self.file_id = fs.path_to_file_id(path)
        except FileNotFoundError:
            if "r" in mode:
                raise
            self.file_id = None
        self.parent_id: str = None
        self.location = None

    def _fetch_range(self, start: Optional[int] = None, end: Optional[int] = None) -> Any:
        """Read data from Google Drive.

        Args:
            start (Optional[int]): The start of the range to read.
            end (Optional[int]): The end of the range to read.

        Returns:
            Any: The data read from Google Drive.
        """

        if start is not None or end is not None:
            start = start or 0
            end = end or 0
            head = {"Range": "bytes=%i-%i" % (start, end - 1)}
        else:
            head = {}
        try:
            files_service = self.fs.service
            media_obj = files_service.get_media(fileId=self.file_id)
            media_obj.headers.update(head)
            data = media_obj.execute()
            return data
        except HttpError as e:
            # TODO : doc says server might send everything if range is outside
            if "not satisfiable" in str(e):
                return b""
            raise

    def _upload_chunk(self, final: Optional[bool] = False) -> bool:
        """Write one part of a multi-block file upload.

        Args:
            final (Optional[bool]): Whether to finalize the file.
                Defaults to False.

        Returns:
            bool: Whether the upload was successful.
        """
        self.buffer.seek(0)
        data = self.buffer.getvalue()
        head = {}
        length = len(data)
        if final and self.autocommit:
            if length:
                part = "%i-%i" % (self.offset, self.offset + length - 1)
                head["Content-Range"] = "bytes %s/%i" % (part, self.offset + length)
            else:
                # closing when buffer is empty
                head["Content-Range"] = "bytes */%i" % self.offset
                data = None
        else:
            head["Content-Range"] = "bytes %i-%i/*" % (self.offset, self.offset + length - 1)
        head.update({"Content-Type": "application/octet-stream", "Content-Length": str(length)})
        req = self.fs.service._http.request
        head, body = req(self.location, method="PUT", body=data, headers=head)
        status = int(head["status"])
        # TODO: raise an exception similar to google api wrapper
        assert status < 400, "Init upload failed"
        if status in [200, 201]:
            # server thinks we are finished, this should happen
            # only when closing
            file_meta = json.loads(body.decode())
            self.file_id = file_meta["id"]
            # file_parent_id = file_meta["parents"][0]
            file_name = file_meta["name"]
            FILE_ID_CACHE[(self.parent_id, file_name)] = self.file_id
            # TODO: invalidate listing cache
        elif "range" in head:
            assert status == 308
        else:
            raise IOError
        return True

    def commit(self) -> None:
        """If not auto-committing, finalize the file."""
        self.autocommit = True
        self._upload_chunk(final=True)

    def _initiate_upload(self) -> None:
        """Create a multi-upload."""
        parent_path = self.fs._parent(self.path)
        if parent_path == self.root_file_id:
            self.parent_id = self.root_file_id
        else:
            self.parent_id = self.fs.path_to_file_id(self.fs._parent(self.path))
        head = {"Content-Type": "application/json; charset=UTF-8"}
        query = "https://www.googleapis.com/upload/drive/v3/files"
        if self.file_id:
            query += "/" + self.file_id
            body = {}
        else:
            # TODO: infer mime type from extension
            body = {"name": self.path.rsplit("/", 1)[-1], "parents": [self.parent_id]}
        query += "?uploadType=resumable&supportsAllDrives=true"
        req = self.fs.service._http.request
        head, body = req(
            query,
            method="PATCH" if self.file_id else "POST",
            headers=head,
            body=json.dumps(body).encode("utf-8"),
        )
        assert int(head["status"]) < 400, "Init upload failed"
        self.location = head["location"]  # type: ignore

    def discard(self) -> None:
        """Cancel in-progress multi-upload."""
        if self.location is None:
            return
        # uid = re.findall("upload_id=([^&=?]+)", self.location)
        # # TODO: there's no gcfs property, I could never work
        # head, _ = self.gcsfs._call(
        #     "DELETE",
        #     "https://www.googleapis.com/upload/drive/v3/files",
        #     params={"uploadType": "resumable", "upload_id": uid},
        # )
        # assert int(head["status"]) < 400, "Cancel upload failed"
