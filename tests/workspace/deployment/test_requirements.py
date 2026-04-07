import json
import subprocess
from pathlib import Path
from typing import Dict, List, Set
from unittest.mock import patch

import pytest
from packaging.requirements import Requirement

from dlt._workspace.deployment.requirements import (
    MAIN_GROUP,
    WorkspaceRequirementsError,
    export_workspace_requirements,
    get_dlt_requirement_spec,
)

from tests.workspace.utils import isolated_workspace


_SHUTIL_WHICH = "dlt._workspace.deployment.requirements.shutil.which"


def _uv_lock(run_dir: str) -> None:
    """Generate uv.lock inside an isolated workspace copy."""
    subprocess.run(["uv", "lock"], cwd=run_dir, check=True, capture_output=True)


def test_get_dlt_requirement_spec() -> None:
    spec = get_dlt_requirement_spec()
    req = Requirement(spec)
    assert req.name == "dlt"
    assert spec.startswith("dlt==") or spec.startswith("dlt @ ")


def test_empty_workspace_auto_injects_dlt() -> None:
    with isolated_workspace("deps_none") as ctx:
        result = export_workspace_requirements(Path(ctx.run_dir))
    assert result == {MAIN_GROUP: [get_dlt_requirement_spec()]}


def test_empty_workspace_with_custom_defaults() -> None:
    with isolated_workspace("deps_none") as ctx:
        result = export_workspace_requirements(
            Path(ctx.run_dir), default_dependencies=["s3fs", "botocore"]
        )
    # sorted result: botocore, dlt==..., s3fs
    assert list(result.keys()) == [MAIN_GROUP]
    main = result[MAIN_GROUP]
    assert main == sorted(main)
    assert "s3fs" in main
    assert "botocore" in main
    assert any(Requirement(s).name == "dlt" for s in main)


def test_empty_workspace_caller_dlt_not_duplicated() -> None:
    with isolated_workspace("deps_none") as ctx:
        result = export_workspace_requirements(
            Path(ctx.run_dir), default_dependencies=["dlt[duckdb]>=1.0"]
        )
    dlt_entries = [s for s in result[MAIN_GROUP] if Requirement(s).name == "dlt"]
    assert len(dlt_entries) == 1
    assert "duckdb" in Requirement(dlt_entries[0]).extras


def test_empty_workspace_uses_module_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "dlt._workspace.deployment.requirements.DEFAULT_REQUIREMENTS",
        ["botocore"],
    )
    with isolated_workspace("deps_none") as ctx:
        result = export_workspace_requirements(Path(ctx.run_dir))
    main = result[MAIN_GROUP]
    assert "botocore" in main
    assert any(Requirement(s).name == "dlt" for s in main)


@pytest.mark.parametrize(
    "fixture_name, expected",
    [
        (
            "deps_pyproject",
            {
                "main": ["packaging>=23.0", "tomlkit>=0.12"],
                "dev": ["pytest>=7.0"],
                "gpu": ["numpy>=1.24"],
            },
        ),
        ("deps_pyproject_minimal", {"main": ["requests>=2.0"]}),
    ],
    ids=["with-groups", "minimal"],
)
def test_pyproject_no_lock(fixture_name: str, expected: Dict[str, List[str]]) -> None:
    with isolated_workspace(fixture_name) as ctx:
        result = export_workspace_requirements(Path(ctx.run_dir))
    assert result == expected


def test_pyproject_with_lock_resolves_all_groups() -> None:
    with isolated_workspace("deps_pyproject") as ctx:
        _uv_lock(ctx.run_dir)
        result = export_workspace_requirements(Path(ctx.run_dir))

    assert set(result.keys()) == {"main", "dev", "gpu"}

    # every spec must be pinned, and free of hashes / local paths
    for group, specs in result.items():
        assert specs, f"group {group!r} is empty"
        for spec in specs:
            assert "==" in spec, f"unpinned spec in {group!r}: {spec}"
            assert "file://" not in spec, f"local path in {group!r}: {spec}"
            assert "sha256:" not in spec, f"hash leaked into {group!r}: {spec}"

    # named packages landed in the right groups
    main_names = {Requirement(s).name for s in result["main"]}
    assert {"tomlkit", "packaging"}.issubset(main_names)

    dev_names = {Requirement(s).name for s in result["dev"]}
    assert "pytest" in dev_names

    gpu_names = {Requirement(s).name for s in result["gpu"]}
    assert "numpy" in gpu_names


def test_pyproject_lock_out_of_sync_raises() -> None:
    with isolated_workspace("deps_pyproject") as ctx:
        # empty lockfile is guaranteed to be out of sync
        (Path(ctx.run_dir) / "uv.lock").write_text("")
        with pytest.raises(WorkspaceRequirementsError):
            export_workspace_requirements(Path(ctx.run_dir))


@pytest.mark.parametrize(
    "fixture_name, required_names",
    [
        ("deps_requirements_txt", {"dlt", "requests", "pydantic"}),
        ("deps_requirements_in", {"dlt", "s3fs"}),
    ],
    ids=["txt", "in"],
)
def test_requirements_file_resolved_with_uv(fixture_name: str, required_names: Set[str]) -> None:
    with isolated_workspace(fixture_name) as ctx:
        result = export_workspace_requirements(Path(ctx.run_dir))

    assert list(result.keys()) == [MAIN_GROUP]
    specs = result[MAIN_GROUP]
    assert specs
    # every spec is pinned by uv pip compile --universal
    for spec in specs:
        assert "==" in spec, f"unpinned spec: {spec}"

    resolved_names = {Requirement(s).name for s in specs}
    assert required_names.issubset(resolved_names)


def test_requirements_txt_fallback_without_uv() -> None:
    with isolated_workspace("deps_requirements_txt") as ctx:
        with patch(_SHUTIL_WHICH, return_value=None):
            result = export_workspace_requirements(Path(ctx.run_dir))
    # parsed as authored (sorted, normalized through Requirement())
    assert result == {MAIN_GROUP: ["dlt>=1.0", "pydantic", "requests==2.31.0"]}


def test_requirements_fallback_drops_flag_lines(tmp_path: Path) -> None:
    # marker so `isolated_workspace` is not needed — this targets the pure parser
    (tmp_path / ".dlt").mkdir()
    (tmp_path / ".dlt" / ".workspace").touch()
    (tmp_path / "requirements.txt").write_text(
        "# a leading comment\n"
        "\n"
        "-e .\n"
        "-r other.txt\n"
        "--index-url https://example.com/simple\n"
        "requests>=2.0  # inline comment\n"
        "pydantic \\\n"
        ">=2.0\n"
    )
    with patch(_SHUTIL_WHICH, return_value=None):
        result = export_workspace_requirements(tmp_path)
    # both real specs survive; flags/comments dropped; line-continuation joined
    assert result == {MAIN_GROUP: ["pydantic>=2.0", "requests>=2.0"]}


def test_pyproject_with_lock_missing_uv_raises_friendly_error() -> None:
    with isolated_workspace("deps_pyproject") as ctx:
        _uv_lock(ctx.run_dir)
        with patch(_SHUTIL_WHICH, return_value=None):
            with pytest.raises(WorkspaceRequirementsError) as exc_info:
                export_workspace_requirements(Path(ctx.run_dir))
    message = str(exc_info.value)
    assert "astral.sh/uv/install" in message
    assert "docs.astral.sh/uv" in message


def test_output_is_json_serializable_and_deterministic() -> None:
    with isolated_workspace("deps_pyproject") as ctx:
        _uv_lock(ctx.run_dir)
        first = export_workspace_requirements(Path(ctx.run_dir))
        second = export_workspace_requirements(Path(ctx.run_dir))

    assert first == second
    # JSON round-trip must not lose anything
    assert json.loads(json.dumps(first)) == first
    # top-level keys sorted
    assert list(first.keys()) == sorted(first.keys())
    # each group's specs sorted
    for specs in first.values():
        assert specs == sorted(specs)
