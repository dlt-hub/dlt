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
    AGENT_LOOP_GROUP_PREFIX,
    LAUNCHER_AGENT,
    LAUNCHER_DASHBOARD,
    LAUNCHER_JOB,
    LAUNCHER_MARIMO,
    LAUNCHER_MCP,
    LAUNCHER_MODULE,
    LAUNCHER_STREAMLIT,
    LOOP_PYDANTIC_AI,
)
from dlt._workspace.cli.dlthub._init_command import init_dlthub_workspace
from dlt._workspace.cli.dlthub.utils import fetch_init_plan
from dlt._workspace.deployment.manifest import default_dashboard_job
from dlt._workspace.deployment.requirements import (
    MAIN_GROUP,
    PRE_TRACKING_DLT_INSTALL_SPEC,
    REQUIREMENTS_ENGINE_VERSION,
    WorkspaceRequirementsError,
    _BASE_LAUNCHER_SPECS,
    build_agent_loop_groups,
    build_dashboard_group,
    build_launcher_requirements,
    default_requirements_manifest,
    export_workspace_requirements,
    get_dlt_requirement_spec,
    get_pkg_install_spec,
    load_requirements,
    migrate_requirements,
    python_version,
    save_requirements,
    _collect_package_names,
    _normalize_name,
    _is_index_option,
    _parse_compile_output,
    _parse_uv_lock_sources,
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
    # nothing to resolve from
    assert result["resolution"] is None
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
    user_groups = {
        k: v
        for k, v in groups.items()
        if k != DASHBOARD_JOB_REF and not k.startswith(AGENT_LOOP_GROUP_PREFIX)
    }
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

    user_group_names = [
        k
        for k in result["groups"]
        if k != DASHBOARD_JOB_REF and not k.startswith(AGENT_LOOP_GROUP_PREFIX)
    ]
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
    # index options are kept as written; nothing is resolved, so no sources
    assert result["resolution"]["index_options"] == ["--index-url https://example.com/simple"]
    assert result["resolution"]["package_sources"] == {}
    assert result["resolution"]["exported_from"] == "requirements.txt"


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
        _uv_lock(ctx.run_dir)
        original = export_workspace_requirements(Path(ctx.run_dir))

    buf = io.BytesIO()
    save_requirements(original, buf)
    buf.seek(0)
    restored = load_requirements(buf)

    assert restored == original
    assert restored["engine_version"] == REQUIREMENTS_ENGINE_VERSION
    assert restored["default_groups"] == [MAIN_GROUP]
    assert MAIN_GROUP in restored["groups"]
    assert restored["resolution"]["package_sources"]


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


def test_migrate_requirements_v1_backfills_dlt_version() -> None:
    """Engine-1 manifests (no dlt_version) load at the current engine with the 1.28.0 default."""
    data = json.dumps(
        {
            "engine_version": 1,
            "python_version": "3.11",
            "default_groups": [MAIN_GROUP],
            "groups": {MAIN_GROUP: ["dlt==1.0.0"]},
            "launcher_requirements": {},
        }
    ).encode("utf-8")
    manifest = load_requirements(io.BytesIO(data))
    assert manifest["engine_version"] == REQUIREMENTS_ENGINE_VERSION
    assert manifest["dlt_version"] == PRE_TRACKING_DLT_INSTALL_SPEC
    assert manifest["dlt_version"]["version"] == "1.28.0"
    assert manifest["resolution"] is None


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


def test_load_unknown_package_source_kind_raises() -> None:
    manifest = default_requirements_manifest()
    manifest["resolution"] = {
        "exported_from": "uv.lock",
        "package_sources": {"pkg": [{"kind": "bogus", "location": "x"}]},  # type: ignore[typeddict-item]
        "index_options": [],
    }
    buf = io.BytesIO()
    save_requirements(manifest, buf)
    buf.seek(0)
    with pytest.raises(WorkspaceRequirementsError, match="invalid requirements manifest"):
        load_requirements(buf)


def test_launcher_requirements_shape() -> None:
    lreq = build_launcher_requirements()
    assert set(lreq.keys()) == {
        LAUNCHER_JOB,
        LAUNCHER_MODULE,
        LAUNCHER_MARIMO,
        LAUNCHER_MCP,
        LAUNCHER_STREAMLIT,
        LAUNCHER_DASHBOARD,
        LAUNCHER_AGENT,
    }
    # every launcher gets the base specs
    for specs in lreq.values():
        for base in _BASE_LAUNCHER_SPECS:
            assert base in specs
        assert specs == sorted(specs)
    # dlt is NOT in the bare launcher dict — it's injected conditionally
    # by export_workspace_requirements / default_requirements_manifest
    assert lreq[LAUNCHER_JOB] == sorted(set(["dlt[s3]"] + _BASE_LAUNCHER_SPECS))
    assert lreq[LAUNCHER_MODULE] == sorted(set(["dlt[s3]"] + _BASE_LAUNCHER_SPECS))
    # loop packages live in agent-loop-* groups, not here
    assert lreq[LAUNCHER_AGENT] == sorted(set(["dlt[s3]"] + _BASE_LAUNCHER_SPECS))
    assert lreq[LAUNCHER_MARIMO] == sorted(set(["marimo", "uvicorn"] + _BASE_LAUNCHER_SPECS))
    assert lreq[LAUNCHER_MCP] == sorted(set(["fastmcp", "uvicorn"] + _BASE_LAUNCHER_SPECS))
    assert lreq[LAUNCHER_STREAMLIT] == sorted(set(["streamlit"] + _BASE_LAUNCHER_SPECS))
    assert lreq[LAUNCHER_DASHBOARD] == sorted(_BASE_LAUNCHER_SPECS)


def test_interactive_launchers_omit_artifact_storage_deps() -> None:
    lreq = build_launcher_requirements()
    for launcher in (LAUNCHER_MARIMO, LAUNCHER_MCP, LAUNCHER_STREAMLIT, LAUNCHER_DASHBOARD):
        per_launcher = [s for s in lreq[launcher] if s not in _BASE_LAUNCHER_SPECS]
        assert "dlt[s3]" not in per_launcher


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
        assert get_dlt_requirement_spec() not in specs


def test_dashboard_group_always_present() -> None:
    dashboard_specs = build_dashboard_group()
    # dashboard runner gate (marimo, pyarrow, ibis-framework) + dlt's s3 extra for artifacts
    assert dashboard_specs == sorted(["dlt[s3]", "ibis-framework", "marimo", "pyarrow"])

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
    # dlt version + source captured at export time
    assert manifest["dlt_version"] == get_pkg_install_spec("dlt")
    assert manifest["dlt_version"]["version"]
    assert manifest["default_groups"] == [MAIN_GROUP]
    # empty main + dashboard group + one group per built-in agent loop
    assert manifest["groups"] == {
        MAIN_GROUP: [],
        DASHBOARD_JOB_REF: build_dashboard_group(),
        **build_agent_loop_groups(),
    }
    # dlt injected into every launcher entry
    dlt_spec = get_dlt_requirement_spec()
    lreq = manifest["launcher_requirements"]
    for specs in lreq.values():
        assert dlt_spec in specs
        assert specs == sorted(specs)
    # base specs in every launcher
    for base in _BASE_LAUNCHER_SPECS:
        for specs in lreq.values():
            assert base in specs
    # batch launchers carry dlt's s3 extra; interactive don't
    assert "dlt[s3]" in lreq[LAUNCHER_JOB]
    assert "dlt[s3]" not in lreq[LAUNCHER_MARIMO]
    # dashboard has base specs + dlt (extras come from DASHBOARD_JOB_REF group)
    assert lreq[LAUNCHER_DASHBOARD] == sorted(set(_BASE_LAUNCHER_SPECS + [dlt_spec]))


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
        "dlt[hub]",
        "dlt[hub]==1.14.0",
        "dlt @ https://github.com/dlt-hub/dlt/archive/refs/heads/devel.zip",
        (
            "dlt[hub] @"
            " https://github.com/dlt-hub/dlt/archive/refs/heads/feat/workspace-manifest-concept.zip"
        ),
        "dlt[workspace,providers] @ https://example.com/dlt.zip",
        "  dlt[hub] @ https://example.com/dlt.zip  ",
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


@pytest.mark.parametrize(
    "spec, names, kept",
    [
        ("s3fs", {"s3fs"}, False),
        ("s3fs==2024.1", {"s3fs"}, False),
        ("ibis-framework[duckdb]", {"ibis-framework"}, False),
        ("ibis_framework>=12", {"ibis-framework"}, False),
        ('s3fs ; python_version > "3.8"', {"s3fs"}, False),
        ("Pandas", {"pandas"}, False),
        ("marimo", {"s3fs"}, True),
        ("croniter", set(), True),
        ("", {"s3fs"}, True),
        ("# comment", {"s3fs"}, True),
        ("dlt[s3]", {"dlt"}, True),
        ("dlt[s3]", {"dlt[s3]"}, False),
    ],
    ids=[
        "bare-name",
        "pinned",
        "extras",
        "underscore-separator",
        "marker",
        "case-insensitive",
        "no-match",
        "empty-set",
        "empty-line",
        "comment-line",
        "dlt-extra-kept-by-plain-dlt",
        "dlt-extra-pruned-by-its-token",
    ],
)
def test_prune_specs_normalization(spec: str, names: Set[str], kept: bool) -> None:
    from dlt._workspace.deployment.requirements import _prune_specs

    assert _prune_specs([spec], names) == ([spec] if kept else [])


def test_export_prunes_launcher_specs_against_default_group() -> None:
    # deps_requirements_in declares dlt + s3fs in main
    with isolated_workspace("deps_requirements_in") as ctx:
        with patch(_SHUTIL_WHICH, return_value=None):
            result = export_workspace_requirements(Path(ctx.run_dir))
    lreq = result["launcher_requirements"]
    for launcher in (LAUNCHER_JOB, LAUNCHER_MODULE):
        # the user's own s3fs stands in for dlt's s3 extra
        assert "dlt[s3]" not in lreq[launcher]
    # dlt in main — no dlt spec injected anywhere
    for specs in lreq.values():
        assert get_dlt_requirement_spec() not in specs
        assert specs == sorted(specs)
    # dashboard group pruned against main too
    dashboard = result["groups"][DASHBOARD_JOB_REF]
    assert "dlt[s3]" not in dashboard
    assert "marimo" in dashboard
    assert "pyarrow" in dashboard


def test_export_prunes_dashboard_group_with_normalized_names() -> None:
    with isolated_workspace("deps_none") as ctx:
        Path(ctx.run_dir, "requirements.txt").write_text(
            "ibis_framework>=12\nPandas\nnumpy>=1.24\n"
        )
        with patch(_SHUTIL_WHICH, return_value=None):
            result = export_workspace_requirements(Path(ctx.run_dir))
    dashboard = result["groups"][DASHBOARD_JOB_REF]
    assert "ibis-framework" not in dashboard
    # rest of the synthesized group survives
    for kept in ("marimo", "pyarrow", "dlt[s3]"):
        assert kept in dashboard
    # launcher lists share none of these names — untouched except dlt injection
    dlt_spec = get_dlt_requirement_spec()
    assert result["launcher_requirements"][LAUNCHER_JOB] == sorted(
        set(["dlt[s3]", dlt_spec] + _BASE_LAUNCHER_SPECS)
    )


@pytest.mark.parametrize(
    "default_groups, pyarrow_pruned",
    [(None, False), (["main", "extras"], True)],
    ids=["main-only", "main-and-extras"],
)
def test_export_prunes_only_against_default_groups(
    default_groups: List[str], pyarrow_pruned: bool
) -> None:
    with isolated_workspace("deps_none") as ctx:
        Path(ctx.run_dir, "pyproject.toml").write_text(
            "[project]\n"
            'name = "deps-groups"\n'
            'version = "0.0.1"\n'
            'dependencies = ["requests>=2.0"]\n'
            "\n"
            "[dependency-groups]\n"
            'extras = ["pyarrow>=16.0.0"]\n'
        )
        result = export_workspace_requirements(Path(ctx.run_dir), default_groups=default_groups)
    dashboard = result["groups"][DASHBOARD_JOB_REF]
    assert ("pyarrow" not in dashboard) is pyarrow_pruned
    # user groups are never modified by pruning
    assert result["groups"]["extras"] == ["pyarrow>=16.0.0"]


@pytest.mark.parametrize(
    "spec, expected",
    [
        ("dlt>=1.0", {"dlt"}),
        ("dlt[hub]==1.2", {"dlt", "dlt[hub]"}),
        ("dlt[Hub , providers]>=1.0", {"dlt", "dlt[hub]", "dlt[providers]"}),
        ("dlt[hub] @ https://example.com/dlt.zip", {"dlt", "dlt[hub]"}),
    ],
    ids=["no-extras", "single-extra", "multiple-extras-normalized", "extras-with-direct-ref"],
)
def test_collect_package_names_extras_tokens(spec: str, expected: Set[str]) -> None:
    assert _collect_package_names([spec]) == expected


@pytest.mark.parametrize(
    "spec, pruned, kept, dlt_injected",
    [
        ("dlt[hub]>=1.0", {"dlthub"}, {"dlt[s3]"}, False),
        ("dlthub>=0.1", {"dlthub"}, {"dlt[s3]"}, True),
        ("dlthub-client", set(), {"dlthub", "dlt[s3]"}, True),
        ("dlt>=1.0", set(), {"dlthub", "dlt[s3]"}, False),
        ("dlt[s3]>=1.0", {"dlt[s3]"}, {"dlthub"}, False),
    ],
    ids=["dlt-hub-extra", "dlthub", "dlthub-client", "plain-dlt", "dlt-s3-extra"],
)
def test_export_prunes_implied_packages(
    spec: str, pruned: Set[str], kept: Set[str], dlt_injected: bool
) -> None:
    """`dlt[hub]` pulls dlthub and `dlt[s3]` the artifact deps; plain dlt pulls neither."""
    with isolated_workspace("deps_none") as ctx:
        Path(ctx.run_dir, "requirements.txt").write_text(f"{spec}\n")
        with patch(_SHUTIL_WHICH, return_value=None):
            result = export_workspace_requirements(Path(ctx.run_dir))
    job_specs = result["launcher_requirements"][LAUNCHER_JOB]
    for name in pruned:
        assert name not in job_specs
    for name in kept:
        assert name in job_specs
    has_dlt = get_dlt_requirement_spec() in job_specs
    assert has_dlt is dlt_injected


def test_export_scaffolded_workspace_prunes_all_launcher_specs() -> None:
    """A `dlthub init` workspace declares every launcher dep except streamlit."""
    with isolated_workspace("deps_none") as ctx:
        plan = fetch_init_plan(ctx.run_dir, dependencies="pyproject")
        init_dlthub_workspace(plan)
        result = export_workspace_requirements(Path(ctx.run_dir))
    for launcher, specs in result["launcher_requirements"].items():
        if launcher == LAUNCHER_STREAMLIT:
            assert specs == ["streamlit"]
        else:
            assert specs == [], f"{launcher!r} not fully pruned: {specs}"
    assert result["groups"][DASHBOARD_JOB_REF] == []


def test_export_prunes_marker_guarded_specs() -> None:
    """Marker-guarded user specs count toward the prune set — name-level only."""
    with isolated_workspace("deps_none") as ctx:
        Path(ctx.run_dir, "requirements.txt").write_text('s3fs ; python_version >= "3.8"\n')
        with patch(_SHUTIL_WHICH, return_value=None):
            result = export_workspace_requirements(Path(ctx.run_dir))
    assert "dlt[s3]" not in result["launcher_requirements"][LAUNCHER_JOB]
    assert "dlt[s3]" not in result["groups"][DASHBOARD_JOB_REF]


def test_pydantic_ai_loop_group_has_a_floor() -> None:
    """Without a floor, a conflict with the workspace's pins walks the resolver back to 0.2.4."""
    (spec,) = build_agent_loop_groups()[f"{AGENT_LOOP_GROUP_PREFIX}{LOOP_PYDANTIC_AI}"]
    assert Requirement(spec).specifier.contains("2.36.0")
    assert not Requirement(spec).specifier.contains("2.35.3")


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


def _make_dist(direct_url_payload):
    """Return a fake `importlib.metadata.Distribution`-like object."""
    from unittest.mock import MagicMock

    dist = MagicMock()
    dist.metadata = {"Version": "9.9.9"}
    if direct_url_payload is None:
        dist.read_text.return_value = None
    else:
        dist.read_text.return_value = json.dumps(direct_url_payload)
    return dist


def _patch_distribution(monkeypatch: pytest.MonkeyPatch, payload) -> None:
    monkeypatch.setattr(
        "dlt._workspace.deployment.requirements.importlib.metadata.distribution",
        lambda _name: _make_dist(payload),
    )


def test_get_pkg_install_spec_pypi(monkeypatch: pytest.MonkeyPatch) -> None:
    """No `direct_url.json` -> pypi install with version from metadata."""
    from dlt._workspace.deployment.requirements import get_pkg_install_spec

    _patch_distribution(monkeypatch, None)
    spec = get_pkg_install_spec("dlt", extras=["hub"])
    assert spec["mode"] == "pypi"
    assert spec["version"] == "9.9.9"
    assert spec["extras"] == ["hub"]
    assert "path" not in spec and "git_url" not in spec


def test_get_pkg_install_spec_editable(monkeypatch: pytest.MonkeyPatch) -> None:
    from dlt._workspace.deployment.requirements import get_pkg_install_spec

    _patch_distribution(
        monkeypatch,
        {"url": "file:///abs/path", "dir_info": {"editable": True}},
    )
    spec = get_pkg_install_spec("dlthub-client")
    assert spec["mode"] == "editable"
    assert spec["path"] == "/abs/path"


def test_get_pkg_install_spec_path_non_editable(monkeypatch: pytest.MonkeyPatch) -> None:
    from dlt._workspace.deployment.requirements import get_pkg_install_spec

    _patch_distribution(
        monkeypatch,
        {"url": "file:///abs/path", "dir_info": {"editable": False}},
    )
    spec = get_pkg_install_spec("dlthub")
    assert spec["mode"] == "path"
    assert spec["path"] == "/abs/path"


def test_get_pkg_install_spec_git(monkeypatch: pytest.MonkeyPatch) -> None:
    from dlt._workspace.deployment.requirements import get_pkg_install_spec

    _patch_distribution(
        monkeypatch,
        {
            "url": "https://github.com/dlt-hub/dlt.git",
            "vcs_info": {"vcs": "git", "commit_id": "abc123"},
        },
    )
    spec = get_pkg_install_spec("dlt")
    assert spec["mode"] == "git"
    assert spec["git_url"] == "https://github.com/dlt-hub/dlt.git"
    assert spec["git_rev"] == "abc123"


@pytest.mark.parametrize(
    "url,extra_block",
    [
        (
            "https://github.com/dlt-hub/dlt/archive/refs/heads/main.zip?v=13",
            {"archive_info": {"hashes": {"sha256": "deadbeef"}}},
        ),
        ("https://files.pythonhosted.org/packages/dlt-1.0.0-py3-none-any.whl", {}),
    ],
    ids=["github-archive-with-query", "https-wheel-no-archive-info"],
)
def test_get_pkg_install_spec_archive(
    monkeypatch: pytest.MonkeyPatch, url: str, extra_block: Dict[str, object]
) -> None:
    """`pip install https://...zip|whl` with or without `archive_info` becomes mode=archive."""
    from dlt._workspace.deployment.requirements import get_pkg_install_spec

    payload = {"url": url, **extra_block}
    _patch_distribution(monkeypatch, payload)
    spec = get_pkg_install_spec("dlt")
    assert spec["mode"] == "archive"
    assert spec["archive_url"] == url


def _spec(name, mode, **extra):
    """Build a minimal `TInstallSpec` dict for render-helper tests."""
    base = {"name": name, "extras": [], "version": "9.9.9", "mode": mode}
    base.update(extra)
    return base


@pytest.mark.parametrize(
    "spec,for_deployment,expected",
    [
        (_spec("dlt", "pypi"), True, "dlt==9.9.9"),
        (_spec("dlt", "pypi", extras=["hub"]), True, "dlt[hub]==9.9.9"),
        (_spec("dlt", "editable", path="/abs"), True, "dlt==9.9.9"),
        (_spec("dlt", "editable", path="/abs"), False, "dlt==9.9.9"),
        (_spec("dlt", "path", path="/abs"), True, "dlt @ file:///abs"),
        (_spec("dlt", "path", path="/abs"), False, "dlt==9.9.9"),
        (
            _spec("dlt", "git", git_url="https://example.com/dlt.git", git_rev="abc"),
            True,
            "dlt @ git+https://example.com/dlt.git@abc",
        ),
        (
            _spec("dlt", "git", git_url="https://example.com/dlt.git", git_rev="abc"),
            False,
            "dlt==9.9.9",
        ),
        (
            _spec(
                "dlt",
                "archive",
                archive_url="https://github.com/dlt-hub/dlt/archive/main.zip?v=13",
            ),
            True,
            "dlt @ https://github.com/dlt-hub/dlt/archive/main.zip?v=13",
        ),
        (
            _spec(
                "dlt",
                "archive",
                extras=["hub"],
                archive_url="https://github.com/dlt-hub/dlt/archive/main.zip?v=13",
            ),
            False,
            "dlt[hub] @ https://github.com/dlt-hub/dlt/archive/main.zip?v=13",
        ),
    ],
    ids=[
        "pypi-deployment",
        "pypi-with-extras",
        "editable-deployment",
        "editable-scaffold",
        "path-deployment-direct-ref",
        "path-scaffold-version-pin",
        "git-deployment-direct-ref",
        "git-scaffold-version-pin",
        "archive-deployment-direct-ref",
        "archive-scaffold-direct-ref-with-extras",
    ],
)
def test_render_pep508(spec, for_deployment, expected) -> None:
    from dlt._workspace.deployment.requirements import render_pep508

    assert render_pep508(spec, for_deployment=for_deployment) == expected


@pytest.mark.parametrize(
    "spec,expected",
    [
        (_spec("dlt", "pypi"), None),
        (_spec("dlt", "path", path="/abs"), {"path": "/abs"}),
        (_spec("dlt", "editable", path="/abs"), {"path": "/abs", "editable": True}),
        (
            _spec("dlt", "git", git_url="https://x/dlt.git", git_rev="abc"),
            {"git": "https://x/dlt.git", "rev": "abc"},
        ),
        (
            _spec("dlt", "git", git_url="https://x/dlt.git"),
            {"git": "https://x/dlt.git"},
        ),
        # archive direct refs go inline in dependencies — no override needed
        (_spec("dlt", "archive", archive_url="https://x/dlt.zip"), None),
    ],
    ids=["pypi-none", "path", "editable", "git-with-rev", "git-no-rev", "archive-none"],
)
def test_render_uv_source(spec, expected) -> None:
    from dlt._workspace.deployment.requirements import render_uv_source

    assert render_uv_source(spec) == expected


@pytest.mark.parametrize(
    "spec,expected",
    [
        (_spec("dlt", "pypi"), ["dlt==9.9.9"]),
        (_spec("dlt", "pypi", extras=["hub"]), ["dlt[hub]==9.9.9"]),
        (_spec("dlt", "editable", path="/abs"), ["-e /abs"]),
        (_spec("dlt", "path", path="/abs"), ["dlt @ file:///abs"]),
        (
            _spec("dlt", "git", git_url="https://x/dlt.git", git_rev="abc"),
            ["dlt @ git+https://x/dlt.git@abc"],
        ),
        (
            _spec("dlt", "archive", archive_url="https://x/dlt.zip?v=13"),
            ["dlt @ https://x/dlt.zip?v=13"],
        ),
    ],
    ids=[
        "pypi",
        "pypi-with-extras",
        "editable-dash-e",
        "path-direct-ref",
        "git",
        "archive-direct-ref",
    ],
)
def test_render_requirements_lines(spec, expected) -> None:
    from dlt._workspace.deployment.requirements import render_requirements_lines

    assert render_requirements_lines(spec) == expected


def test_get_workspace_install_specs_includes_dlt_with_hub_extra() -> None:
    from dlt._workspace.deployment.requirements import get_workspace_install_specs

    specs = get_workspace_install_specs()
    # dlt is always present (we're running inside it)
    dlt_spec = next((s for s in specs if s["name"] == "dlt"), None)
    assert dlt_spec is not None
    assert dlt_spec["extras"] == ["hub"]


def test_get_workspace_install_specs_skips_uninstalled_packages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When dlthub / dlthub-client aren't installed, they're omitted (PyPI-via-extra fallback)."""
    from dlt._workspace.deployment import requirements as req_mod

    real_dist = req_mod.importlib.metadata.distribution

    def _missing_for_hubs(name: str):
        if name in ("dlthub", "dlthub-client"):
            raise req_mod.importlib.metadata.PackageNotFoundError(name)
        return real_dist(name)

    monkeypatch.setattr(req_mod.importlib.metadata, "distribution", _missing_for_hubs)
    specs = req_mod.get_workspace_install_specs()
    names = {s["name"] for s in specs}
    assert names == {"dlt"}


_SYNTHETIC_UV_LOCK = """
version = 1
requires-python = ">=3.9"

[[package]]
name = "public-pkg"
version = "1.0.0"
source = { registry = "https://pypi.org/simple" }

[[package]]
name = "Private_Pkg"
version = "2.0.0"
source = { registry = "https://acme.example.com/simple" }

[[package]]
name = "git-pkg"
version = "3.0.0"
source = { git = "https://github.com/acme/git-pkg.git?rev=main#abc123" }

[[package]]
name = "url-pkg"
version = "4.0.0"
source = { url = "https://example.com/url_pkg-4.0.0-py3-none-any.whl" }

[[package]]
name = "path-pkg"
version = "4.1.0"
source = { path = "dist/path_pkg-4.1.0-py3-none-any.whl" }

[[package]]
name = "dir-pkg"
version = "5.0.0"
source = { directory = "vendor/dir-pkg" }

[[package]]
name = "editable-pkg"
version = "6.0.0"
source = { editable = "." }

[[package]]
name = "the-workspace"
version = "0.1.0"
source = { virtual = "." }

[[package]]
name = "no-source-pkg"
version = "7.0.0"

[[package]]
name = "torch"
version = "2.5.1"
source = { registry = "https://download.pytorch.org/whl/cpu" }
resolution-markers = ["sys_platform == 'linux'"]

[[package]]
name = "torch"
version = "2.5.1"
source = { registry = "https://pypi.org/simple" }
resolution-markers = ["sys_platform == 'darwin'"]
"""


def test_parse_uv_lock_sources_all_kinds(tmp_path: Path) -> None:
    lock_path = tmp_path / "uv.lock"
    lock_path.write_text(_SYNTHETIC_UV_LOCK)
    sources = _parse_uv_lock_sources(lock_path)

    # a private index is reported as a registry with its own URL — that is the only
    # signal distinguishing it from PyPI, the specs are byte-identical
    assert sources["public-pkg"] == [{"kind": "registry", "location": "https://pypi.org/simple"}]
    assert sources["private-pkg"] == [
        {"kind": "registry", "location": "https://acme.example.com/simple"}
    ]
    assert sources["git-pkg"] == [
        {"kind": "git", "location": "https://github.com/acme/git-pkg.git?rev=main#abc123"}
    ]
    assert sources["url-pkg"] == [
        {"kind": "url", "location": "https://example.com/url_pkg-4.0.0-py3-none-any.whl"}
    ]
    assert sources["path-pkg"] == [
        {"kind": "path", "location": "dist/path_pkg-4.1.0-py3-none-any.whl"}
    ]
    assert sources["dir-pkg"] == [{"kind": "directory", "location": "vendor/dir-pkg"}]
    assert sources["editable-pkg"] == [{"kind": "editable", "location": "."}]
    # virtual carries no location
    assert sources["the-workspace"] == [{"kind": "virtual"}]
    # a package without a source block is not reported at all
    assert "no-source-pkg" not in sources
    # a package locked per platform keeps every entry with its markers, in lock order
    assert sources["torch"] == [
        {
            "kind": "registry",
            "location": "https://download.pytorch.org/whl/cpu",
            "markers": ["sys_platform == 'linux'"],
        },
        {
            "kind": "registry",
            "location": "https://pypi.org/simple",
            "markers": ["sys_platform == 'darwin'"],
        },
    ]
    assert list(sources.keys()) == sorted(sources.keys())


def test_parse_uv_lock_sources_unparsable_raises(tmp_path: Path) -> None:
    lock_path = tmp_path / "uv.lock"
    lock_path.write_text("[[package]\nname = ")
    with pytest.raises(WorkspaceRequirementsError, match="Failed to parse"):
        _parse_uv_lock_sources(lock_path)


def test_pyproject_with_lock_exports_package_sources() -> None:
    with isolated_workspace("deps_pyproject") as ctx:
        _uv_lock(ctx.run_dir)
        result = export_workspace_requirements(Path(ctx.run_dir))

    sources = result["resolution"]["package_sources"]
    # every spec exported from the lock has a source; names join on PEP 503 normalization.
    # the dashboard and agent loop groups are built in, not resolved from the lock
    builtin_groups = {DASHBOARD_JOB_REF, *build_agent_loop_groups()}
    for group, specs in result["groups"].items():
        if group in builtin_groups:
            continue
        for spec in specs:
            name = _normalize_name(Requirement(spec).name)
            assert name in sources, f"{name} from {group!r} missing a source"
    assert sources["tomlkit"] == [{"kind": "registry", "location": "https://pypi.org/simple"}]
    # the workspace project itself is not published anywhere
    assert [s["kind"] for s in sources["test-workspace-deps"]] in (["virtual"], ["editable"])
    assert result["resolution"]["exported_from"] == "uv.lock"
    assert result["resolution"]["index_options"] == []


def test_pyproject_without_lock_exports_no_package_sources() -> None:
    with isolated_workspace("deps_pyproject") as ctx:
        result = export_workspace_requirements(Path(ctx.run_dir))
    assert result["resolution"]["package_sources"] == {}
    assert result["resolution"]["index_options"] == []
    assert result["resolution"]["exported_from"] == "pyproject.toml"


@pytest.mark.parametrize(
    "fixture_name, file_name",
    [
        ("deps_requirements_txt", "requirements.txt"),
        ("deps_requirements_in", "requirements.in"),
    ],
    ids=["txt", "in"],
)
def test_requirements_file_exports_index_sources(fixture_name: str, file_name: str) -> None:
    with isolated_workspace(fixture_name) as ctx:
        result = export_workspace_requirements(Path(ctx.run_dir))

    assert result["resolution"]["exported_from"] == file_name
    # uv always emits the effective default index
    assert result["resolution"]["index_options"] == ["--index-url https://pypi.org/simple"]
    # every compiled spec resolved from an index has that index as its source; a package
    # compiled per Python version has one entry per marker
    sources = result["resolution"]["package_sources"]
    for spec in result["groups"][MAIN_GROUP]:
        name = _normalize_name(Requirement(spec).name)
        assert {(e["kind"], e["location"]) for e in sources[name]} == {
            ("registry", "https://pypi.org/simple")
        }


def test_requirements_file_keeps_index_placeholders(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / ".dlt").mkdir()
    (tmp_path / ".dlt" / ".workspace").touch()
    (tmp_path / "links").mkdir()
    (tmp_path / "requirements.txt").write_text(
        "--index-url https://${PYPI_HOST}/simple\n--find-links ./links\nsix\n"
    )
    # uv expands the placeholder to resolve, so it must point at a reachable index
    monkeypatch.setenv("PYPI_HOST", "pypi.org")
    result = export_workspace_requirements(tmp_path)

    # index options keep the placeholder, so no secret is written into the manifest
    assert result["resolution"]["index_options"] == [
        "--index-url https://${PYPI_HOST}/simple",
        "--find-links ./links",
    ]
    # the source records the index the package was actually resolved from
    assert result["resolution"]["package_sources"]["six"] == [
        {"kind": "registry", "location": "https://pypi.org/simple"}
    ]


# shape of `uv pip compile --universal --no-annotate --emit-index-annotation --emit-index-url
# --emit-find-links` output
_COMPILE_OUTPUT = """--index-url https://pypi.org/simple
--extra-index-url https://${IDX_USER}:${IDX_PASS}@acme.example.com/simple
--find-links ./links

-e file:///workspace/vendor/local-pkg
iniconfig @ git+https://github.com/pytest-dev/iniconfig@93f5930e668c0d1ddf4597e38dd0dea4e2665e7a
Private_Pkg==2.0.0
    # from https://acme.example.com/simple
six==1.16.0 ; python_full_version < '3.10'
    # from https://pypi.org/simple
six==1.17.0 ; python_full_version >= '3.10'
    # from https://pypi.org/simple
"""


def test_parse_compile_output() -> None:
    compiled = _parse_compile_output(_COMPILE_OUTPUT)

    assert compiled.index_options == [
        "--index-url https://pypi.org/simple",
        "--extra-index-url https://${IDX_USER}:${IDX_PASS}@acme.example.com/simple",
        "--find-links ./links",
    ]
    # index lines are not specs; everything else is, sorted
    assert compiled.specs == sorted(
        [
            "-e file:///workspace/vendor/local-pkg",
            (
                "iniconfig @"
                " git+https://github.com/pytest-dev/iniconfig@93f5930e668c0d1ddf4597e38dd0dea4e2665e7a"
            ),
            "Private_Pkg==2.0.0",
            "six==1.16.0 ; python_full_version < '3.10'",
            "six==1.17.0 ; python_full_version >= '3.10'",
        ]
    )
    assert compiled.package_sources == {
        # a private index is told apart from PyPI only by its URL
        "private-pkg": [{"kind": "registry", "location": "https://acme.example.com/simple"}],
        # a package compiled per environment keeps each entry with its marker
        "six": [
            {
                "kind": "registry",
                "location": "https://pypi.org/simple",
                "markers": ['python_full_version < "3.10"'],
            },
            {
                "kind": "registry",
                "location": "https://pypi.org/simple",
                "markers": ['python_full_version >= "3.10"'],
            },
        ],
    }
    # direct references and editables carry their source in the spec, not as an annotation
    assert "iniconfig" not in compiled.package_sources
    assert "local-pkg" not in compiled.package_sources


@pytest.mark.parametrize(
    "line, is_option",
    [
        ("--index-url https://pypi.org/simple", True),
        ("--index-url=https://pypi.org/simple", True),
        ("-i https://pypi.org/simple", True),
        ("--extra-index-url https://acme.example.com/simple", True),
        ("--find-links ./links", True),
        ("-f ./links", True),
        ("-e ./local-pkg", False),
        ("-r other.txt", False),
        ("--index-strategy unsafe-best-match", False),
        ("requests==2.31.0", False),
    ],
    ids=[
        "index-url",
        "index-url-equals",
        "index-url-short",
        "extra-index-url",
        "find-links",
        "find-links-short",
        "editable",
        "include",
        "other-option",
        "spec",
    ],
)
def test_is_index_option(line: str, is_option: bool) -> None:
    assert _is_index_option(line) is is_option


@pytest.mark.parametrize("from_engine", [1, 2])
def test_migrate_requirements_to_engine_3_adds_resolution(from_engine: int) -> None:
    manifest = {
        "engine_version": from_engine,
        "python_version": python_version(),
        "default_groups": [MAIN_GROUP],
        "groups": {MAIN_GROUP: ["dlt==1.0.0"]},
        "launcher_requirements": {LAUNCHER_JOB: ["dlt==1.0.0"]},
    }
    if from_engine >= 2:
        manifest["dlt_version"] = dict(PRE_TRACKING_DLT_INSTALL_SPEC)
    result = migrate_requirements(manifest, from_engine, 3)
    assert result["engine_version"] == 3
    assert result["resolution"] is None
    assert result["dlt_version"] == PRE_TRACKING_DLT_INSTALL_SPEC
    assert result["groups"] == {MAIN_GROUP: ["dlt==1.0.0"]}
