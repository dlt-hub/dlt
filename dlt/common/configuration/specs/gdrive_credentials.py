import re
import json
import os
from urllib.parse import urlparse, parse_qs

from fsspec import register_implementation
from fsspec.spec import AbstractFileSystem, AbstractBufferedFile
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.credentials import AnonymousCredentials
from google.oauth2 import service_account
import pydata_google_auth


scope_dict = {
    "full_control": "https://www.googleapis.com/auth/drive",
    "read_only": "https://www.googleapis.com/auth/drive.readonly",
}


DIR_MIME_TYPE = "application/vnd.google-apps.folder"
fields = ",".join(
    [
        "name",
        "id",
        "size",
        "description",
        "trashed",
        "mimeType",
        "version",
        "createdTime",
        "modifiedTime",
        "capabilities",
    ]
)


def _normalize_path(prefix, name):
    raw_prefix = prefix.strip("/")
    return "/" + "/".join([raw_prefix, name])


def _finfo_from_response(f, path_prefix=None):
    # strictly speaking, other types might be capable of having children,
    # such as packages
    ftype = "directory" if f.get("mimeType") == DIR_MIME_TYPE else "file"
    if path_prefix:
        name = _normalize_path(path_prefix, f["name"])
    else:
        name = f["name"]
    info = {"name": "/" + name, "size": int(f.get("size", 0)), "type": ftype}
    f.update(info)
    return f


class GoogleDriveFileSystem(AbstractFileSystem):
    protocol = "gdrive"
    root_marker = ""

    def __init__(
        self,
        root_file_id=None,
        token="browser",
        access="full_control",
        spaces="drive",
        creds=None,
        **kwargs,
    ):
        """
        Access to dgrive as a file-system

        :param root_file_id: str or None
            If you have a share, drive or folder ID to treat as the FS root, enter
            it here. Otherwise, you will get your default drive
        :param token: str
            One of "anon", "browser", "cache", "service_account". Using "browser" will prompt a URL to
            be put in a browser, and cache the response for future use with token="cache".
            "browser" will remove any previously cached token file if it exists.
        :param access: str
            One of "full_control", "read_only
        :param spaces:
            Category of files to search, can be  'drive', 'appDataFolder' and 'photos'.
            Of these, only the first is general
        :param creds: None or dict
            Required just for "service_account" token, a dict containing the service account
            credentials obtainend in GCP console. The dict content is the same as the json file
            downloaded from GCP console. More details can be found here:
            https://cloud.google.com/iam/docs/service-account-creds#key-types
            This credential can be usful when integrating with other GCP services, and when you
            don't want the user to be prompted to authenticate.
            The files need to be shared with the service account email address, that can be found
            in the json file.
        :param kwargs:
            Passed to parent
        """
        super().__init__(**kwargs)
        self.access = access
        self.scopes = [scope_dict[access]]
        self.token = token
        self.spaces = spaces
        self.root_file_id = root_file_id or "root"
        self.creds = creds
        self.connect(method="cache")

    @staticmethod
    def _get_kwargs_from_urls(path):
        parsed = urlparse(path)
        query = parse_qs(parsed.query)
        if "RootId" in query:
            return {"root_file_id": query["RootId"][0]}
        return {}

    def connect(self, method=None):
        from google.oauth2.credentials import Credentials

        SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]

        if os.path.exists("token.json"):
            cred = Credentials.from_authorized_user_file("token.json", SCOPES)
        elif method == "browser":
            cred = self._connect_browser()
        elif method == "cache":
            cred = self._connect_cache()
        elif method == "anon":
            cred = AnonymousCredentials()
        elif method is "service_account":
            cred = self._connect_service_account()
        else:
            raise ValueError(f"Invalid connection method `{method}`.")
        srv = build("drive", "v3", credentials=cred)

        self._drives = srv.drives()
        self.service = srv.files()

    def _connect_browser(self):
        try:
            os.remove(pydata_google_auth.cache.READ_WRITE._path)
        except OSError:
            pass
        return self._connect_cache()

    def _connect_cache(self):
        return pydata_google_auth.get_user_credentials(self.scopes, use_local_webserver=True)

    def _connect_service_account(self):
        return service_account.Credentials.from_service_account_info(
            info=self.creds, scopes=self.scopes
        )

    @property
    def drives(self):
        if self._drives is not None:
            return self._drives.list().execute()["drives"]
        else:
            return []

    def mkdir(self, path, create_parents=True, **kwargs):
        if create_parents and self._parent(path):
            self.makedirs(self._parent(path), exist_ok=True)
        parent_id = self.path_to_file_id(self._parent(path))
        meta = {
            "name": path.rstrip("/").rsplit("/", 1)[-1],
            "mimeType": DIR_MIME_TYPE,
            "parents": [parent_id],
        }
        self.service.create(body=meta).execute()
        self.invalidate_cache(self._parent(path))

    def makedirs(self, path, exist_ok=True):
        if self.isdir(path):
            if exist_ok:
                return
            else:
                raise FileExistsError(path)
        if self._parent(path):
            self.makedirs(self._parent(path), exist_ok=True)
        self.mkdir(path, create_parents=False)

    def _delete(self, file_id):
        self.service.delete(fileId=file_id).execute()

    def rm(self, path, recursive=True, maxdepth=None):
        if recursive is False and self.isdir(path) and self.ls(path):
            raise ValueError("Attempt to delete non-empty folder")
        self._delete(self.path_to_file_id(path))
        self.invalidate_cache(path)
        self.invalidate_cache(self._parent(path))

    def rmdir(self, path):
        if not self.isdir(path):
            raise ValueError("Path is not a directory")
        self.rm(path, recursive=False)

    def _info_by_id(self, file_id, path_prefix=None):
        response = self.service.get(
            fileId=file_id,
            fields=fields,
        ).execute()
        return _finfo_from_response(response, path_prefix)

    def export(self, path, mime_type):
        """Convert a google-native file to other format and download

        mime_type is something like "text/plain"
        """
        file_id = self.path_to_file_id(path)
        return self.service.export(fileId=file_id, mimeType=mime_type).execute()

    def ls(self, path, detail=False, trashed=False):
        path = self._strip_protocol(path)
        if path in [None, "/"]:
            path = ""

        files = self._ls_from_cache(path)
        if not files:
            if path == "":
                file_id = self.root_file_id
            else:
                file_id = self.path_to_file_id(path, trashed=trashed)
            files = self._list_directory_by_id(file_id, trashed=trashed, path_prefix=path)
            if files:
                self.dircache[path] = files
            else:
                file_id = self.path_to_file_id(path, trashed=trashed)
                files = [self._info_by_id(file_id)]

        if detail:
            return files
        else:
            return sorted([f["name"] for f in files])

    def _list_directory_by_id(self, file_id, trashed=False, path_prefix=None):
        all_files = []
        page_token = None
        afields = "nextPageToken, files(%s)" % fields
        query = f"'{file_id}' in parents  "
        if not trashed:
            query += "and trashed = false "
        while True:
            response = self.service.list(
                q=query, spaces=self.spaces, fields=afields, pageToken=page_token
            ).execute()
            for f in response.get("files", []):
                all_files.append(_finfo_from_response(f, path_prefix))

            more = response.get("incompleteSearch", False)
            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break

        return all_files

    def path_to_file_id(self, path, parent=None, trashed=False):
        items = path.strip("/").split("/")
        if path in ["", "/", "root", self.root_file_id]:
            return self.root_file_id
        if parent is None:
            parent = self.root_file_id
        top_file_id = self._get_directory_child_by_name(items[0], parent, trashed=trashed)
        if len(items) == 1:
            return top_file_id
        else:
            sub_path = "/".join(items[1:])
            return self.path_to_file_id(sub_path, parent=top_file_id, trashed=trashed)

    def _get_directory_child_by_name(self, child_name, directory_file_id, trashed=False):
        all_children = self._list_directory_by_id(directory_file_id, trashed=trashed)
        possible_children = []
        for child in all_children:
            if child["name"] == child_name:
                possible_children.append(child["id"])
        if len(possible_children) == 0:
            raise FileNotFoundError(
                f"Directory {directory_file_id} has no child named {child_name}"
            )
        if len(possible_children) == 1:
            return possible_children[0]
        else:
            raise KeyError(
                f"Directory {directory_file_id} has more than one "
                f"child named {child_name}. Unable to resolve path "
                "to file_id."
            )

    def _open(self, path, mode="rb", **kwargs):
        return GoogleDriveFile(self, path, mode=mode, **kwargs)


DEFAULT_BLOCK_SIZE = 5 * 2**20


class GoogleDriveFile(AbstractBufferedFile):
    def __init__(
        self, fs, path, mode="rb", block_size=DEFAULT_BLOCK_SIZE, autocommit=True, **kwargs
    ):
        """
        Open a file.

        Parameters
        ----------
        fs: instance of GoogleDriveFileSystem
        mode: str
            Normal file modes. Currently only 'wb' amd 'rb'.
        block_size: int
            Buffer size for reading or writing (default 5MB)
        """
        super().__init__(fs, path, mode, block_size, autocommit=autocommit, **kwargs)

        if mode == "wb":
            self.location = None
        else:
            self.file_id = fs.path_to_file_id(path)

    def _fetch_range(self, start=None, end=None):
        """Get data from Google Drive

        start, end : None or integers
            if not both None, fetch only given range
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

    def _upload_chunk(self, final=False):
        """Write one part of a multi-block file upload

        Parameters
        ----------
        final: bool
            Complete and commit upload
        """
        self.buffer.seek(0)
        data = self.buffer.getvalue()
        head = {}
        l = len(data)
        if final and self.autocommit:
            if l:
                part = "%i-%i" % (self.offset, self.offset + l - 1)
                head["Content-Range"] = "bytes %s/%i" % (part, self.offset + l)
            else:
                # closing when buffer is empty
                head["Content-Range"] = "bytes */%i" % self.offset
                data = None
        else:
            head["Content-Range"] = "bytes %i-%i/*" % (self.offset, self.offset + l - 1)
        head.update({"Content-Type": "application/octet-stream", "Content-Length": str(l)})
        req = self.fs.service._http.request
        head, body = req(self.location, method="PUT", body=data, headers=head)
        status = int(head["status"])
        assert status < 400, "Init upload failed"
        if status in [200, 201]:
            # server thinks we are finished, this should happen
            # only when closing
            self.file_id = json.loads(body.decode())["id"]
        elif "range" in head:
            assert status == 308
        else:
            raise IOError
        return True

    def commit(self):
        """If not auto-committing, finalize file"""
        self.autocommit = True
        self._upload_chunk(final=True)

    def _initiate_upload(self):
        """Create multi-upload"""
        parent_id = self.fs.path_to_file_id(self.fs._parent(self.path))
        head = {"Content-Type": "application/json; charset=UTF-8"}
        # also allows description, MIME type, version, thumbnail...
        body = json.dumps({"name": self.path.rsplit("/", 1)[-1], "parents": [parent_id]}).encode()
        req = self.fs.service._http.request
        # TODO : this creates a new file. If the file exists, you should
        #   update it by getting the ID and using PATCH, else you get two
        #   identically-named files
        r = req(
            "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable",
            method="POST",
            headers=head,
            body=body,
        )
        head = r[0]
        assert int(head["status"]) < 400, "Init upload failed"
        self.location = r[0]["location"]

    def discard(self):
        """Cancel in-progress multi-upload"""
        if self.location is None:
            return
        uid = re.findall("upload_id=([^&=?]+)", self.location)
        head, _ = self.gcsfs._call(
            "DELETE",
            "https://www.googleapis.com/upload/drive/v3/files",
            params={"uploadType": "resumable", "upload_id": uid},
        )
        assert int(head["status"]) < 400, "Cancel upload failed"


register_implementation("gdrive", GoogleDriveFileSystem, "GoogleDriveFileSystem")
