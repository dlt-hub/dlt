import io
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Set
from unittest.mock import patch

import pytest
from packaging.requirements import Requirement

from dlt._workspace.deployment.launchers import (
    LAUNCHER_DASHBOARD,
    LAUNCHER_JOB,
    LAUNCHER_MARIMO,
    LAUNCHER_MCP,
    LAUNCHER_MODULE,
    LAUNCHER_STREAMLIT,
)
from dlt._workspace.deployment.manifest import default_dashboard_job
from dlt._workspace.deployment.requirements import (
    MAIN_GROUP,
    REQUIREMENTS_ENGINE_VERSION,
    WorkspaceRequirementsError,
    build_dashboard_group,
    build_launcher_requirements,
    default_requirements_manifest,
    export_workspace_requirements,
    get_dlt_requirement_spec,
    load_requirements,
    migrate_requirements,
    python_version,
    save_requirements,
)
from dlt._workspace.deployment.typing import DASHBOARD_JOB_REF

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


def test_empty_workspace_dlt_in_every_launcher() -> None:
    with isolated_workspace("deps_none") as ctx:
        result = export_workspace_requirements(Path(ctx.run_dir))
    assert result["engine_version"] == REQUIREMENTS_ENGINE_VERSION
    assert result["default_groups"] == [MAIN_GROUP]
    # main is empty — no dep files in the workspace
    assert result["groups"][MAIN_GROUP] == []
    # dlt got injected into every launcher entry (including dashboard)
    dlt_spec = get_dlt_requirement_spec()
    for launcher, specs in result["launcher_requirements"].items():
        assert dlt_spec in specs, f"dlt missing from {launcher!r}"


@pytest.mark.parametrize(
    "fixture_name, expected_user_groups",
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
def test_pyproject_no_lock(fixture_name: str, expected_user_groups: Dict[str, List[str]]) -> None:
    with isolated_workspace(fixture_name) as ctx:
        result = export_workspace_requirements(Path(ctx.run_dir))
    assert result["engine_version"] == REQUIREMENTS_ENGINE_VERSION
    assert result["default_groups"] == [MAIN_GROUP]
    for name, specs in expected_user_groups.items():
        assert result["groups"][name] == specs
    assert DASHBOARD_JOB_REF in result["groups"]


def test_pyproject_with_lock_resolves_all_groups() -> None:
    with isolated_workspace("deps_pyproject") as ctx:
        _uv_lock(ctx.run_dir)
        result = export_workspace_requirements(Path(ctx.run_dir))

    groups = result["groups"]
    user_groups = {k: v for k, v in groups.items() if k != DASHBOARD_JOB_REF}
    assert set(user_groups.keys()) == {"main", "dev", "gpu"}

    # every user spec must be pinned, and free of hashes / local paths
    for group, specs in user_groups.items():
        assert specs, f"group {group!r} is empty"
        for spec in specs:
            assert "==" in spec, f"unpinned spec in {group!r}: {spec}"
            assert "file://" not in spec, f"local path in {group!r}: {spec}"
            assert "sha256:" not in spec, f"hash leaked into {group!r}: {spec}"

    # named packages landed in the right groups
    main_names = {Requirement(s).name for s in groups["main"]}
    assert {"tomlkit", "packaging"}.issubset(main_names)

    dev_names = {Requirement(s).name for s in groups["dev"]}
    assert "pytest" in dev_names

    gpu_names = {Requirement(s).name for s in groups["gpu"]}
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

    user_group_names = [k for k in result["groups"] if k != DASHBOARD_JOB_REF]
    assert user_group_names == [MAIN_GROUP]
    specs = result["groups"][MAIN_GROUP]
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
    assert result["groups"][MAIN_GROUP] == ["dlt>=1.0", "pydantic", "requests==2.31.0"]


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
    assert result["groups"][MAIN_GROUP] == ["pydantic>=2.0", "requests>=2.0"]


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
    # group keys sorted
    groups = first["groups"]
    assert list(groups.keys()) == sorted(groups.keys())
    # each group's specs sorted
    for specs in groups.values():
        assert specs == sorted(specs)


def test_export_default_groups_override() -> None:
    with isolated_workspace("deps_pyproject") as ctx:
        result = export_workspace_requirements(Path(ctx.run_dir), default_groups=["main", "gpu"])
    assert result["default_groups"] == ["main", "gpu"]
    # override does not affect the `groups` map itself
    assert "main" in result["groups"]
    assert "gpu" in result["groups"]


def test_save_load_roundtrip() -> None:
    with isolated_workspace("deps_pyproject") as ctx:
        original = export_workspace_requirements(Path(ctx.run_dir))

    buf = io.BytesIO()
    save_requirements(original, buf)
    buf.seek(0)
    restored = load_requirements(buf)

    assert restored == original
    assert restored["engine_version"] == REQUIREMENTS_ENGINE_VERSION
    assert restored["default_groups"] == [MAIN_GROUP]
    assert MAIN_GROUP in restored["groups"]


def test_migrate_requirements_same_version_is_noop() -> None:
    manifest = {
        "engine_version": REQUIREMENTS_ENGINE_VERSION,
        "default_groups": [MAIN_GROUP],
        "groups": {MAIN_GROUP: ["dlt==1.0.0"]},
    }
    result = migrate_requirements(
        manifest, REQUIREMENTS_ENGINE_VERSION, REQUIREMENTS_ENGINE_VERSION
    )
    assert result == manifest


def test_migrate_requirements_unknown_path_raises() -> None:
    with pytest.raises(ValueError, match="no requirements migration path"):
        migrate_requirements({}, 99, REQUIREMENTS_ENGINE_VERSION)


def test_load_unknown_engine_version_raises() -> None:
    data = json.dumps(
        {
            "engine_version": 99,
            "default_groups": [MAIN_GROUP],
            "groups": {MAIN_GROUP: ["dlt==1.0.0"]},
        }
    ).encode("utf-8")
    with pytest.raises(WorkspaceRequirementsError, match="migration path"):
        load_requirements(io.BytesIO(data))


def test_load_invalid_shape_raises_validation_error() -> None:
    # missing required `groups` field — other required fields present
    data = json.dumps(
        {
            "engine_version": REQUIREMENTS_ENGINE_VERSION,
            "python_version": python_version(),
            "default_groups": [MAIN_GROUP],
            "launcher_requirements": {"": []},
        }
    ).encode("utf-8")
    with pytest.raises(WorkspaceRequirementsError, match="invalid requirements manifest"):
        load_requirements(io.BytesIO(data))


def test_launcher_requirements_shape() -> None:
    lreq = build_launcher_requirements()
    assert set(lreq.keys()) == {
        LAUNCHER_JOB,
        LAUNCHER_MODULE,
        LAUNCHER_MARIMO,
        LAUNCHER_MCP,
        LAUNCHER_STREAMLIT,
        LAUNCHER_DASHBOARD,
    }
    # dlt is NOT in the bare launcher dict — it's injected conditionally
    # by export_workspace_requirements / default_requirements_manifest
    assert lreq[LAUNCHER_JOB] == ["botocore", "s3fs"]
    assert lreq[LAUNCHER_MODULE] == ["botocore", "s3fs"]
    assert lreq[LAUNCHER_MARIMO] == ["marimo", "uvicorn"]
    assert lreq[LAUNCHER_MCP] == ["fastmcp", "uvicorn"]
    assert lreq[LAUNCHER_STREAMLIT] == ["streamlit"]
    # dashboard is empty — its extras travel via the DASHBOARD_JOB_REF group
    assert lreq[LAUNCHER_DASHBOARD] == []


def test_interactive_launchers_omit_botocore_and_s3fs() -> None:
    lreq = build_launcher_requirements()
    for launcher in (LAUNCHER_MARIMO, LAUNCHER_MCP, LAUNCHER_STREAMLIT, LAUNCHER_DASHBOARD):
        assert "botocore" not in lreq[launcher]
        assert "s3fs" not in lreq[launcher]


def test_export_injects_dlt_when_absent_from_default_group() -> None:
    # deps_pyproject declares packaging+tomlkit in main; no dlt there
    with isolated_workspace("deps_pyproject") as ctx:
        result = export_workspace_requirements(Path(ctx.run_dir))
    dlt_spec = get_dlt_requirement_spec()
    for specs in result["launcher_requirements"].values():
        assert dlt_spec in specs


def test_export_skips_dlt_injection_when_present_in_default_group() -> None:
    # deps_requirements_txt fixture already lists dlt>=1.0 in main
    with isolated_workspace("deps_requirements_txt") as ctx:
        with patch(_SHUTIL_WHICH, return_value=None):
            result = export_workspace_requirements(Path(ctx.run_dir))
    for specs in result["launcher_requirements"].values():
        assert not any(Requirement(s).name == "dlt" for s in specs)


def test_dashboard_group_always_present() -> None:
    dashboard_specs = build_dashboard_group()
    assert dashboard_specs == [
        "botocore",
        "ibis-framework",
        "marimo",
        "numpy",
        "pandas",
        "pyarrow",
        "s3fs",
        "uvicorn",
    ]

    # present in every export path
    with isolated_workspace("deps_none") as ctx:
        result = export_workspace_requirements(Path(ctx.run_dir))
    assert result["groups"][DASHBOARD_JOB_REF] == dashboard_specs


def test_default_dashboard_job_declares_dashboard_group() -> None:
    job = default_dashboard_job()
    assert job["require"]["dependency_groups"] == [DASHBOARD_JOB_REF]
    # matches the group injected by export_workspace_requirements
    with isolated_workspace("deps_none") as ctx:
        result = export_workspace_requirements(Path(ctx.run_dir))
    for group in job["require"]["dependency_groups"]:
        assert group in result["groups"]


def test_default_requirements_manifest_shape() -> None:
    manifest = default_requirements_manifest()
    assert manifest["engine_version"] == REQUIREMENTS_ENGINE_VERSION
    assert manifest["default_groups"] == [MAIN_GROUP]
    # empty main + dashboard group
    assert manifest["groups"] == {
        MAIN_GROUP: [],
        DASHBOARD_JOB_REF: build_dashboard_group(),
    }
    # dlt injected into every launcher entry
    dlt_spec = get_dlt_requirement_spec()
    lreq = manifest["launcher_requirements"]
    for specs in lreq.values():
        assert dlt_spec in specs
        assert specs == sorted(specs)
    # batch launchers still carry botocore/s3fs; interactive don't
    assert "botocore" in lreq[LAUNCHER_JOB]
    assert "s3fs" in lreq[LAUNCHER_JOB]
    assert "botocore" not in lreq[LAUNCHER_MARIMO]
    # dashboard has dlt only (extras come from DASHBOARD_JOB_REF group)
    assert lreq[LAUNCHER_DASHBOARD] == [dlt_spec]


def test_default_requirements_manifest_is_save_load_stable() -> None:
    original = default_requirements_manifest()
    buf = io.BytesIO()
    save_requirements(original, buf)
    buf.seek(0)
    assert load_requirements(buf) == original


@pytest.mark.parametrize(
    "spec",
    [
        "dlt",
        "dlt==1.14.0",
        "dlt>=1.0,<2.0",
        "dlt[workspace]",
        "dlt[workspace]==1.14.0",
        "dlt @ https://github.com/dlt-hub/dlt/archive/refs/heads/devel.zip",
        (
            "dlt[workspace] @"
            " https://github.com/dlt-hub/dlt/archive/refs/heads/feat/workspace-manifest-concept.zip"
        ),
        "dlt[workspace,providers] @ https://example.com/dlt.zip",
        "  dlt[workspace] @ https://example.com/dlt.zip  ",
    ],
)
def test_contains_package_recognizes_dlt_in_every_form(spec: str) -> None:
    from dlt._workspace.deployment.requirements import _contains_package

    assert _contains_package([spec], "dlt") is True


@pytest.mark.parametrize(
    "spec, needle, expected",
    [
        ("ibis-framework>=12", "ibis-framework", True),
        ("ibis_framework>=12", "ibis-framework", True),  # PEP 503 separator norm
        ("ibis.framework>=12", "ibis-framework", True),
        ("pytz==2024.1", "ibis-framework", False),
        ("", "dlt", False),
        ("# comment", "dlt", False),
    ],
)
def test_contains_package_name_normalization(spec: str, needle: str, expected: bool) -> None:
    from dlt._workspace.deployment.requirements import _contains_package

    assert _contains_package([spec], needle) is expected


def test_python_version_shape() -> None:
    version = python_version()
    assert re.fullmatch(r"\d+\.\d+", version)
    assert version == f"{sys.version_info.major}.{sys.version_info.minor}"


def test_export_includes_python_version() -> None:
    with isolated_workspace("deps_pyproject") as ctx:
        result = export_workspace_requirements(Path(ctx.run_dir))
    assert result["python_version"] == python_version()


def test_default_manifest_includes_python_version() -> None:
    manifest = default_requirements_manifest()
    assert manifest["python_version"] == python_version()
