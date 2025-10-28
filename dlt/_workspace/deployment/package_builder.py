from io import BytesIO
from typing import Tuple, BinaryIO, List
from pathlib import Path
import tarfile
import yaml

from dlt.common.time import precise_time
from dlt.common.utils import digest256_tar_stream

from dlt._workspace.deployment.file_selector import WorkspaceFileSelector
from dlt._workspace.deployment.manifest import (
    TDeploymentFileItem,
    TDeploymentManifest,
    DEPLOYMENT_ENGINE_VERSION,
)

from dlt._workspace._workspace_context import WorkspaceRunContext


DEFAULT_DEPLOYMENT_FILES_FOLDER = "files"
DEFAULT_MANIFEST_FILE_NAME = "manifest.yaml"
DEFAULT_DEPLOYMENT_PACKAGE_LAYOUT = "deployment-{timestamp}.tar.gz"


class DeploymentPackageBuilder:
    """Builds gzipped deployment package from file selectors"""

    def __init__(self, context: WorkspaceRunContext):
        self.run_context: WorkspaceRunContext = context

    def write_package_to_stream(
        self, file_selector: WorkspaceFileSelector, output_stream: BinaryIO
    ) -> str:
        """Write deployment package to output stream, return content hash"""
        manifest_files: List[TDeploymentFileItem] = []

        # Add files to the archive
        with tarfile.open(fileobj=output_stream, mode="w|gz") as tar:
            for file_path in file_selector:
                full_path = self.run_context.run_dir / file_path
                # Use POSIX paths for tar archives (cross-platform compatibility)
                posix_path = file_path.as_posix()
                tar.add(
                    full_path,
                    arcname=f"{DEFAULT_DEPLOYMENT_FILES_FOLDER}/{posix_path}",
                    recursive=False,
                )
                manifest_files.append(
                    {
                        "relative_path": posix_path,
                        "size_in_bytes": full_path.stat().st_size,
                    }
                )
            # Create and add manifest with file metadata at the end
            # NOTE: Sort files in manifest because os.scandir(), which the file selector's pathspec.util.iter_tree_files() relies on,
            # yields files in a system-dependent order (https://peps.python.org/pep-0471/#os-scandir).
            manifest: TDeploymentManifest = {
                "engine_version": DEPLOYMENT_ENGINE_VERSION,
                "files": sorted(manifest_files, key=lambda x: x["relative_path"]),
            }
            manifest_yaml = yaml.dump(
                manifest, allow_unicode=True, default_flow_style=False, sort_keys=False
            ).encode("utf-8")
            manifest_info = tarfile.TarInfo(name=DEFAULT_MANIFEST_FILE_NAME)
            manifest_info.size = len(manifest_yaml)
            tar.addfile(manifest_info, BytesIO(manifest_yaml))

        return digest256_tar_stream(output_stream)

    def build_package(self, file_selector: WorkspaceFileSelector) -> Tuple[Path, str]:
        """Create deployment package file, return (path, content_hash)"""
        package_name = DEFAULT_DEPLOYMENT_PACKAGE_LAYOUT.format(timestamp=str(precise_time()))
        package_path = Path(self.run_context.get_data_entity(package_name))

        with open(package_path, "w+b") as f:
            content_hash = self.write_package_to_stream(file_selector, f)

        return package_path, content_hash
