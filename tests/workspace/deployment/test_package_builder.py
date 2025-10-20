import os
import tarfile
import yaml
from io import BytesIO
import time

from dlt._workspace.deployment.package_builder import (
    DeploymentPackageBuilder,
    DEFAULT_DEPLOYMENT_FILES_FOLDER,
    DEFAULT_MANIFEST_FILE_NAME,
)
from dlt._workspace.deployment.file_selector import WorkspaceFileSelector
from dlt._workspace.deployment.manifest import DEPLOYMENT_ENGINE_VERSION

from tests.workspace.utils import isolated_workspace, WORKSPACE_CASES_DIR


def test_write_package_to_stream() -> None:
    """Test building deployment package to a stream and verify structure."""

    run_dir = os.path.join(WORKSPACE_CASES_DIR, "default")
    with isolated_workspace(run_dir, "test_write_package_to_stream") as ctx:
        builder = DeploymentPackageBuilder(ctx)
        selector = WorkspaceFileSelector(ctx, ignore_file=".ignorefile")

        stream = BytesIO()
        content_hash = builder.write_package_to_stream(selector, stream)

        assert content_hash
        assert len(content_hash) == 44  # sha3_256 base64 string

        expected_workspace_files = [
            "additional_exclude/empty_file.py",
            "ducklake_pipeline.py",
            ".ignorefile",
        ]

        # Verify tar.gz structure
        stream.seek(0)
        with tarfile.open(fileobj=stream, mode="r:gz") as tar:
            members = tar.getnames()

            # Tar contains files under "files/" prefix + manifest
            assert DEFAULT_MANIFEST_FILE_NAME in members
            tar_files = [m for m in members if m.startswith(DEFAULT_DEPLOYMENT_FILES_FOLDER)]
            assert set(tar_files) == {
                f"{DEFAULT_DEPLOYMENT_FILES_FOLDER}/{f}" for f in expected_workspace_files
            }

            # Verify manifest structure
            manifest_member = tar.extractfile(DEFAULT_MANIFEST_FILE_NAME)
            manifest = yaml.safe_load(manifest_member)

            assert manifest["engine_version"] == DEPLOYMENT_ENGINE_VERSION
            assert all(
                "relative_path" in file_item and "size_in_bytes" in file_item
                for file_item in manifest["files"]
            )

            # Manifest has workspace-relative paths (no "files/" prefix)
            manifest_paths = [f["relative_path"] for f in manifest["files"]]
            assert set(manifest_paths) == set(expected_workspace_files)


def test_build_package() -> None:
    """Test that deployment packages are content-addressable with reproducible hashes."""

    run_dir = os.path.join(WORKSPACE_CASES_DIR, "default")
    with isolated_workspace(run_dir, "test_package_builder") as ctx:
        builder = DeploymentPackageBuilder(ctx)
        selector = WorkspaceFileSelector(ctx)

        package_path, content_hash = builder.build_package(selector)
        assert str(package_path).startswith(f"{ctx.data_dir}/deployment-")
        assert len(content_hash) == 44  # sha3_256 base64 string

        time.sleep(0.2)

        package_path_2, content_hash_2 = builder.build_package(selector)

        assert package_path != package_path_2
        assert content_hash == content_hash_2
