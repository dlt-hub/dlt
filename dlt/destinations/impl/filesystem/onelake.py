from dlt.destinations.impl.filesystem.filesystem import FilesystemClient


class OneLakeFilesystemClient(FilesystemClient):
    def _probe_path(self, path: str) -> str:
        # OneLake rejects directory HEAD probes with a trailing slash.
        return path.rstrip(self.pathlib.sep) or path

    def _exists(self, path: str) -> bool:
        return self.fs_client.exists(self._probe_path(path))  # type: ignore[no-any-return]

    def _isdir(self, path: str) -> bool:
        return self.fs_client.isdir(self._probe_path(path))  # type: ignore[no-any-return]
