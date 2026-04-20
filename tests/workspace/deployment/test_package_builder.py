import base64
import hashlib
import logging
import os
import sys
import tarfile
import time
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple, cast

import pytest
import yaml

from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.deployment.file_selector import BaseFileSelector, WorkspaceFileSelector
from dlt._workspace.deployment.manifest import DEPLOYMENT_ENGINE_VERSION
from dlt._workspace.deployment.package_builder import (
    DEFAULT_DEPLOYMENT_FILES_FOLDER,
    DEFAULT_MANIFEST_FILE_NAME,
    PackageBuilder,
    compute_package_content_hash,
)
from dlt._workspace.deployment.typing import TFilesManifest

from tests.workspace.utils import isolated_workspace


class _FakeContext:
    """Minimal run_context stand-in for PackageBuilder — only run_dir is read."""

    def __init__(self, run_dir: Path) -> None:
        self.run_dir = str(run_dir)


def _builder(run_dir: Path) -> PackageBuilder:
    return PackageBuilder(cast(WorkspaceRunContext, _FakeContext(run_dir)))


class _ListSelector(BaseFileSelector):
    """File selector that yields a fixed list of (abs_path, rel_path) tuples."""

    def __init__(self, items: List[Tuple[Path, Path]]) -> None:
        self._items = list(items)

    def __iter__(self) -> Iterator[Tuple[Path, Path]]:
        return iter(self._items)


def _sha3_b64(data: bytes) -> str:
    return base64.b64encode(hashlib.sha3_256(data).digest()).decode("ascii")


def _read_manifest(buf: BytesIO) -> Dict[str, Any]:
    buf.seek(0)
    with tarfile.open(fileobj=buf, mode="r:gz") as tar:
        return yaml.safe_load(tar.extractfile(DEFAULT_MANIFEST_FILE_NAME))


def _tar_members(buf: BytesIO) -> Dict[str, tarfile.TarInfo]:
    buf.seek(0)
    with tarfile.open(fileobj=buf, mode="r:gz") as tar:
        return {m.name: m for m in tar.getmembers()}


@pytest.fixture
def dlt_logger_propagates() -> Iterator[None]:
    dlt_logger = logging.getLogger("dlt")
    previous = dlt_logger.propagate
    dlt_logger.propagate = True
    try:
        yield
    finally:
        dlt_logger.propagate = previous


def test_write_package_to_stream() -> None:
    """Test building deployment package to a stream and verify structure."""

    with isolated_workspace("default") as ctx:
        builder = PackageBuilder(ctx)
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
            for item in manifest["files"]:
                assert "relative_path" in item
                assert "size_in_bytes" in item
                # regular files get sha3_256 that matches the disk bytes
                assert "sha3_256" in item
                disk = (Path(ctx.run_dir) / item["relative_path"]).read_bytes()
                assert item["sha3_256"] == _sha3_b64(disk)

            # Manifest has workspace-relative paths (no "files/" prefix)
            manifest_paths = [f["relative_path"] for f in manifest["files"]]
            assert set(manifest_paths) == set(expected_workspace_files)


def test_build_package() -> None:
    """Test that deployment packages are content-addressable with reproducible hashes."""

    with isolated_workspace("default") as ctx:
        builder = PackageBuilder(ctx)
        selector = WorkspaceFileSelector(ctx)

        package_path, content_hash = builder.build_package(selector)
        assert str(package_path).startswith(f"{ctx.data_dir}{os.sep}deployment-")
        assert len(content_hash) == 44  # sha3_256 base64 string

        # Sleep ensures tarballs have different timestamps in their metadata; the rolling
        # content hash is derived from file identity, not tar bytes, so it stays stable.
        time.sleep(0.2)

        package_path_2, content_hash_2 = builder.build_package(selector)

        assert package_path != package_path_2
        assert content_hash == content_hash_2


def test_manifest_files_are_sorted() -> None:
    """Test that hash is independent of file iteration order."""
    with isolated_workspace("default") as ctx:
        builder = PackageBuilder(ctx)
        selector = WorkspaceFileSelector(ctx)

        hash1 = builder.write_package_to_stream(selector, BytesIO())

        original_order = list(selector)
        reversed_order = list(reversed(original_order))
        assert original_order != reversed_order

        # Imitate different iteration order
        class ReversedSelector(WorkspaceFileSelector):
            def __init__(self, files):
                self.files = files

            def __iter__(self):
                return iter(self.files)

        hash2 = builder.write_package_to_stream(ReversedSelector(reversed_order), BytesIO())

        assert hash1 == hash2


def test_inside_root_symlink_preserved(tmp_path: Path) -> None:
    (tmp_path / "real.txt").write_bytes(b"hello\n")
    os.symlink("real.txt", tmp_path / "link.txt")

    items = [
        (tmp_path / "real.txt", Path("real.txt")),
        (tmp_path / "link.txt", Path("link.txt")),
    ]
    builder = _builder(tmp_path)
    buf = BytesIO()
    builder.write_package_to_stream(_ListSelector(items), buf)

    members = _tar_members(buf)
    assert members[f"{DEFAULT_DEPLOYMENT_FILES_FOLDER}/real.txt"].isfile()
    link_member = members[f"{DEFAULT_DEPLOYMENT_FILES_FOLDER}/link.txt"]
    assert link_member.issym()
    assert link_member.linkname == "real.txt"

    manifest = _read_manifest(buf)
    link_entry = next(f for f in manifest["files"] if f["relative_path"] == "link.txt")
    assert link_entry["linkname"] == "real.txt"
    assert "sha3_256" not in link_entry
    assert link_entry["size_in_bytes"] == 0
    real_entry = next(f for f in manifest["files"] if f["relative_path"] == "real.txt")
    assert real_entry["sha3_256"] == _sha3_b64(b"hello\n")


def test_up_dir_symlink_still_inside_is_preserved(tmp_path: Path) -> None:
    (tmp_path / "real.txt").write_bytes(b"hello\n")
    (tmp_path / "subdir").mkdir()
    # use os.path.join so the separator is native; Windows' CreateSymbolicLinkW
    # rejects forward-slash `..` targets with WinError 123
    os.symlink(os.path.join("..", "real.txt"), tmp_path / "subdir" / "link.txt")

    items = [
        (tmp_path / "real.txt", Path("real.txt")),
        (tmp_path / "subdir" / "link.txt", Path("subdir/link.txt")),
    ]
    builder = _builder(tmp_path)
    buf = BytesIO()
    builder.write_package_to_stream(_ListSelector(items), buf)

    manifest = _read_manifest(buf)
    link_entry = next(f for f in manifest["files"] if f["relative_path"] == "subdir/link.txt")
    assert link_entry["linkname"] == "../real.txt"


def test_outside_root_symlink_skipped(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    dlt_logger_propagates: None,
) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "external.txt").write_bytes(b"external\n")

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "keep.txt").write_bytes(b"keep\n")
    os.symlink(str(outside / "external.txt"), workspace / "escape.txt")

    items = [
        (workspace / "keep.txt", Path("keep.txt")),
        (workspace / "escape.txt", Path("escape.txt")),
    ]
    builder = _builder(workspace)
    buf = BytesIO()
    with caplog.at_level(logging.WARNING, logger="dlt"):
        builder.write_package_to_stream(_ListSelector(items), buf)

    members = _tar_members(buf)
    assert f"{DEFAULT_DEPLOYMENT_FILES_FOLDER}/escape.txt" not in members
    assert f"{DEFAULT_DEPLOYMENT_FILES_FOLDER}/keep.txt" in members

    manifest = _read_manifest(buf)
    assert not any(f["relative_path"] == "escape.txt" for f in manifest["files"])

    assert any("escape.txt" in rec.getMessage() for rec in caplog.records)


@pytest.mark.parametrize(
    "variant",
    [
        "relative_escape",
        "dangling",
        pytest.param(
            "cyclic",
            marks=pytest.mark.skipif(
                sys.platform == "win32",
                reason=(
                    "Windows refuses to create cyclic symlinks "
                    "(CreateSymbolicLinkW raises ERROR_CANT_RESOLVE_FILENAME)"
                ),
            ),
        ),
    ],
)
def test_invalid_symlink_variants_skipped(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    dlt_logger_propagates: None,
    variant: str,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "keep.txt").write_bytes(b"keep\n")

    if variant == "relative_escape":
        (workspace / "subdir").mkdir()
        # native separator — Windows rejects forward-slash `..` symlink targets
        os.symlink(
            os.path.join("..", "..", "not_in_workspace.txt"),
            workspace / "subdir" / "bad.txt",
        )
        bad_abs = workspace / "subdir" / "bad.txt"
        bad_rel = Path("subdir/bad.txt")
    elif variant == "dangling":
        os.symlink("nonexistent.txt", workspace / "bad.txt")
        bad_abs = workspace / "bad.txt"
        bad_rel = Path("bad.txt")
    else:  # cyclic
        os.symlink("b.txt", workspace / "a.txt")
        os.symlink("a.txt", workspace / "b.txt")
        bad_abs = workspace / "a.txt"
        bad_rel = Path("a.txt")

    items = [
        (workspace / "keep.txt", Path("keep.txt")),
        (bad_abs, bad_rel),
    ]
    builder = _builder(workspace)
    buf = BytesIO()
    with caplog.at_level(logging.WARNING, logger="dlt"):
        builder.write_package_to_stream(_ListSelector(items), buf)

    members = _tar_members(buf)
    assert f"{DEFAULT_DEPLOYMENT_FILES_FOLDER}/{bad_rel.as_posix()}" not in members
    assert any(bad_rel.as_posix() in rec.getMessage() for rec in caplog.records)


def test_content_hash_reacts_to_rename(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_bytes(b"same bytes\n")
    builder = _builder(tmp_path)

    h_a = builder.write_package_to_stream(
        _ListSelector([(tmp_path / "a.txt", Path("a.txt"))]), BytesIO()
    )
    # same abs_path but different rel_path → rename
    h_b = builder.write_package_to_stream(
        _ListSelector([(tmp_path / "a.txt", Path("renamed.txt"))]), BytesIO()
    )
    assert h_a != h_b


def test_content_hash_ignores_mtime_change(tmp_path: Path) -> None:
    f = tmp_path / "a.txt"
    f.write_bytes(b"same bytes\n")
    builder = _builder(tmp_path)
    selector = _ListSelector([(f, Path("a.txt"))])

    h1 = builder.write_package_to_stream(selector, BytesIO())
    os.utime(f, (1, 1))  # epoch mtime — bound to differ from first build
    h2 = builder.write_package_to_stream(selector, BytesIO())

    assert h1 == h2


def test_compute_package_content_hash_matches_builder(tmp_path: Path) -> None:
    (tmp_path / "a.txt").write_bytes(b"one\n")
    (tmp_path / "b.txt").write_bytes(b"two\n")
    os.symlink("a.txt", tmp_path / "link.txt")

    items = [
        (tmp_path / "a.txt", Path("a.txt")),
        (tmp_path / "b.txt", Path("b.txt")),
        (tmp_path / "link.txt", Path("link.txt")),
    ]
    buf = BytesIO()
    content_hash = _builder(tmp_path).write_package_to_stream(_ListSelector(items), buf)

    # recompute from the persisted manifest without touching the tar bytes
    manifest = cast(TFilesManifest, _read_manifest(buf))
    assert compute_package_content_hash(manifest) == content_hash


def test_content_hash_reacts_to_symlink_target_change(tmp_path: Path) -> None:
    (tmp_path / "t1.txt").write_bytes(b"one\n")
    (tmp_path / "t2.txt").write_bytes(b"two\n")
    builder = _builder(tmp_path)

    link = tmp_path / "link.txt"
    os.symlink("t1.txt", link)
    items = [
        (tmp_path / "t1.txt", Path("t1.txt")),
        (tmp_path / "t2.txt", Path("t2.txt")),
        (link, Path("link.txt")),
    ]
    h1 = builder.write_package_to_stream(_ListSelector(items), BytesIO())

    link.unlink()
    os.symlink("t2.txt", link)
    h2 = builder.write_package_to_stream(_ListSelector(items), BytesIO())

    assert h1 != h2
