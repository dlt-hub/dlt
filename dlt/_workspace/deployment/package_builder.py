from io import BytesIO
from typing import Tuple, BinaryIO, List
from pathlib import Path
import tarfile
import yaml

from dlt.common.time import precise_time
from dlt.common.utils import digest256_file_stream

from dlt._workspace.deployment.file_selector import FileSelector
from dlt._workspace.deployment.manifest import (
    TDeploymentFileItem,
    TDeploymentManifest,
    DEPLOYMENT_ENGINE_VERSION,
)

DEFAULT_DEPLOYMENT_FILES_FOLDER = "files"
DEFAULT_MANIFEST_FILE_NAME = "manifest.yaml"
DEFAULT_DEPLOYMENT_PACKAGE_LAYOUT = "deployment-{timestamp}.tar.gz"


class DeploymentPackageBuilder:
    """Builds gzipped deployment package from file selectors"""

    def __init__(self, workspace_root: Path):
        self.workspace_root = Path(workspace_root)

    def build_package_to_stream(self, file_selector: FileSelector, output_stream: BinaryIO) -> str:
        """Build deployment package to stream and return content hash"""
        manifest_files: List[TDeploymentFileItem] = []

        # Add files to the archive
        with tarfile.open(fileobj=output_stream, mode="w|gz") as tar:
            for file_path in file_selector.iter_files():
                full_path = self.workspace_root / file_path
                tar.add(
                    full_path,
                    arcname=f"{DEFAULT_DEPLOYMENT_FILES_FOLDER}/{file_path}",
                    recursive=False,
                )
                manifest_files.append(
                    {
                        "relative_path": str(file_path),
                        "size_in_bytes": full_path.stat().st_size,
                    }
                )

            # Create and add manifest with file metadata at the end
            manifest: TDeploymentManifest = {
                "engine_version": DEPLOYMENT_ENGINE_VERSION,
                "files": manifest_files,
            }
            manifest_yaml = yaml.dump(
                manifest, allow_unicode=True, default_flow_style=False, sort_keys=False
            ).encode("utf-8")
            manifest_info = tarfile.TarInfo(name=DEFAULT_MANIFEST_FILE_NAME)
            manifest_info.size = len(manifest_yaml)
            tar.addfile(manifest_info, BytesIO(manifest_yaml))

        return digest256_file_stream(output_stream)

    def build_package(self, file_selector: FileSelector) -> Tuple[Path, str]:
        """Build deployment package and returns (package_path, content_hash)"""
        package_path = Path(DEFAULT_DEPLOYMENT_PACKAGE_LAYOUT.format(timestamp=str(precise_time())))

        with open(package_path, "wb") as f:
            content_hash = self.build_package_to_stream(file_selector, f)

        return package_path, content_hash
