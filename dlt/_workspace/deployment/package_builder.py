import base64
import hashlib
import os
import tarfile
from io import BytesIO
from pathlib import Path
from typing import BinaryIO, List, Tuple

import yaml

from dlt.common import logger
from dlt.common.storages.load_package import create_load_id

from dlt._workspace.deployment.file_selector import BaseFileSelector
from dlt._workspace.deployment.manifest import DEPLOYMENT_ENGINE_VERSION
from dlt._workspace.deployment.typing import (
    TDeploymentFileItem,
    TFilesManifest,
)

from dlt._workspace._workspace_context import WorkspaceRunContext


DEFAULT_DEPLOYMENT_FILES_FOLDER = "files"
DEFAULT_MANIFEST_FILE_NAME = "manifest.yaml"
DEFAULT_DEPLOYMENT_PACKAGE_LAYOUT = "deployment-{timestamp}.tar.gz"


class _HashingReader:
    """File wrapper that accumulates a sha3_256 as tarfile reads it."""

    def __init__(self, fileobj: BinaryIO) -> None:
        self._f = fileobj
        self._h = hashlib.sha3_256()

    def read(self, size: int = -1) -> bytes:
        chunk = self._f.read(size)
        self._h.update(chunk)
        return chunk

    def digest_b64(self) -> str:
        return base64.b64encode(self._h.digest()).decode("ascii")


def _symlink_target_inside_root(abs_path: Path, workspace_root: Path) -> bool:
    """True iff `abs_path` resolves to an existing path under `workspace_root`."""
    # strict=True raises on missing targets (FileNotFoundError) and cycles (RuntimeError)
    try:
        resolved = abs_path.resolve(strict=True)
    except (FileNotFoundError, RuntimeError):
        return False
    try:
        return resolved.is_relative_to(workspace_root)
    except ValueError:
        return False


def compute_package_content_hash(manifest: TFilesManifest) -> str:
    """Content hash of a deployment package. `manifest["files"]` must be pre-sorted."""
    h = hashlib.sha3_256()
    for item in manifest["files"]:
        h.update(item["relative_path"].encode("utf-8"))
        h.update(item["sha3_256"].encode("ascii"))
    return base64.b64encode(h.digest()).decode("ascii")


class PackageBuilder:
    """Builds gzipped deployment package from file selectors"""

    def __init__(self, context: WorkspaceRunContext):
        self.run_context: WorkspaceRunContext = context

    def write_package_to_stream(
        self, file_selector: BaseFileSelector, output_stream: BinaryIO
    ) -> str:
        """Write deployment package to output stream, return content hash"""
        manifest_files: List[TDeploymentFileItem] = []
        # needed to check symlink target
        workspace_root: Path = None

        with tarfile.open(fileobj=output_stream, mode="w|gz") as tar:
            for abs_path, rel_path in file_selector:
                posix_path = rel_path.as_posix()
                arcname = f"{DEFAULT_DEPLOYMENT_FILES_FOLDER}/{posix_path}"

                if abs_path.is_symlink():
                    # resolve workspace root on demand
                    if not workspace_root:
                        workspace_root = Path(self.run_context.run_dir).resolve()
                    if not _symlink_target_inside_root(abs_path, workspace_root):
                        logger.warning(
                            "Skipping symlink %r in deployment: target %r does"
                            " not resolve inside the workspace root",
                            posix_path,
                            os.readlink(abs_path),
                        )
                        continue
                    # tar.add reads os.readlink only, never the target content
                    tar.add(str(abs_path), arcname=arcname, recursive=False)
                    linkname = os.readlink(abs_path)
                    # hash the link target string (not its content) so retargeting
                    # the symlink always changes the package content hash
                    linkname_digest = base64.b64encode(
                        hashlib.sha3_256(linkname.encode("utf-8")).digest()
                    ).decode("ascii")
                    manifest_files.append(
                        {
                            "relative_path": posix_path,
                            "size_in_bytes": 0,
                            "sha3_256": linkname_digest,
                            "linkname": linkname,
                        }
                    )
                    continue

                st = abs_path.stat()
                info = tarfile.TarInfo(name=arcname)
                info.size = st.st_size
                info.mtime = int(st.st_mtime)
                info.mode = st.st_mode & 0o7777
                with abs_path.open("rb") as f:
                    reader = _HashingReader(f)
                    tar.addfile(info, reader)
                manifest_files.append(
                    {
                        "relative_path": posix_path,
                        "size_in_bytes": st.st_size,
                        "sha3_256": reader.digest_b64(),
                    }
                )

            # os.scandir is platform-dependent (PEP 471); sort for cross-OS stable hashes
            manifest_files.sort(key=lambda x: x["relative_path"])

            manifest: TFilesManifest = {
                "engine_version": DEPLOYMENT_ENGINE_VERSION,
                "files": manifest_files,
            }
            manifest_yaml = yaml.dump(
                manifest, allow_unicode=True, default_flow_style=False, sort_keys=False
            ).encode("utf-8")
            manifest_info = tarfile.TarInfo(name=DEFAULT_MANIFEST_FILE_NAME)
            manifest_info.size = len(manifest_yaml)
            tar.addfile(manifest_info, BytesIO(manifest_yaml))

        return compute_package_content_hash(manifest)

    def build_package(self, file_selector: BaseFileSelector) -> Tuple[Path, str]:
        """Create deployment package file, return (path, content_hash)"""
        package_name = DEFAULT_DEPLOYMENT_PACKAGE_LAYOUT.format(timestamp=create_load_id())
        package_path = Path(self.run_context.get_data_entity(package_name))

        with open(package_path, "w+b") as f:
            content_hash = self.write_package_to_stream(file_selector, f)

        return package_path, content_hash
