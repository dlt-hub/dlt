import io
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Literal, Set, get_args
from unittest.mock import patch

import pytest
from packaging.requirements import Requirement

import dlt

from dlt._workspace.deployment.launchers import (
    AGENT_LOOP_GROUP_PREFIX,
    LAUNCHER_AGENT,
    LAUNCHER_DASHBOARD,
    LAUNCHER_JOB,
    LAUNCHER_MARIMO,
    LAUNCHER_MCP,
    LAUNCHER_MODULE,
    LAUNCHER_STREAMLIT,
    LOOP_CLAUDE_AGENT_SDK,
    LOOP_PYDANTIC_AI,
    agent_loop_group,
)
from dlt._workspace.cli.dlthub._init_command import init_dlthub_workspace
from dlt._workspace.cli.dlthub.utils import fetch_init_plan
from dlt._workspace.deployment.manifest import default_dashboard_job
from dlt._workspace.deployment.requirements import (
    MAIN_GROUP,
    PRE_TRACKING_DLT_INSTALL_SPEC,
    REQUIREMENTS_ENGINE_VERSION,
    WorkspaceRequirementsError,
    dlt_extra_specs,
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
    _add_transitive_names,
    _collect_package_names,
)
from dlt._workspace.deployment.typing import DASHBOARD_JOB_REF, TWorkspaceRequirementsManifest

from tests.workspace.utils import isolated_workspace


_SHUTIL_WHICH = "dlt._workspace.deployment.requirements.shutil.which"
_HUB = "dlt[hub]"
_S3_NAMES = ["botocore", "s3fs"]

TDepsSource = Literal[
    "requirements-declared",
    "requirements-resolved",
    "pyproject-declared",
    "pyproject-resolved",
]
"""Workspace deps taken as declared, or resolved by uv: pip compile or uv.lock."""


def _get_names(specs: List[str]) -> List[str]:
    return sorted(Requirement(s).name for s in specs)


def _run_uv_lock(run_dir: str) -> None:
    """Generate uv.lock inside an isolated workspace copy."""
    subprocess.run(["uv", "lock"], cwd=run_dir, check=True, capture_output=True)


# this checkout of dlt, so extras resolve with the bounds under test rather than PyPI's
_DLT_CHECKOUT_SPEC = f"dlt @ {Path(dlt.__file__).parents[1].as_uri()}"


def _run_uv_compile(specs: List[str], tmp_path: Path) -> subprocess.CompletedProcess[str]:
    """Resolve `specs` together the way the runtime installs a run's requirements."""
    requirements = tmp_path / "requirements.in"
    requirements.write_text("".join(f"{s}\n" for s in specs))
    return subprocess.run(
        [
            "uv",
            "pip",
            "compile",
            "--python-version",
            python_version(),
            "--no-header",
            requirements.name,
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )


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
        _run_uv_lock(ctx.run_dir)
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


def test_pyproject_with_lock_missing_uv_raises_friendly_error() -> None:
    with isolated_workspace("deps_pyproject") as ctx:
        _run_uv_lock(ctx.run_dir)
        with patch(_SHUTIL_WHICH, return_value=None):
            with pytest.raises(WorkspaceRequirementsError) as exc_info:
                export_workspace_requirements(Path(ctx.run_dir))
    message = str(exc_info.value)
    assert "astral.sh/uv/install" in message
    assert "docs.astral.sh/uv" in message


def test_output_is_json_serializable_and_deterministic() -> None:
    with isolated_workspace("deps_pyproject") as ctx:
        _run_uv_lock(ctx.run_dir)
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


def test_migrate_requirements_v1_backfills_dlt_version() -> None:
    """Engine-1 manifests (no dlt_version) load as engine 2 with the 1.28.0 default."""
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
        LAUNCHER_AGENT,
    }
    # every launcher gets dlt's hub extra, so dlthub follows whichever dlt resolves
    for specs in lreq.values():
        assert _HUB in specs
        assert not {"dlthub", "dlthub-client"} & set(_get_names(specs))
        assert specs == sorted(specs)
    # dlt itself is NOT in the bare launcher dict — it's injected conditionally
    # by export_workspace_requirements / default_requirements_manifest
    for launcher in (LAUNCHER_JOB, LAUNCHER_MODULE, LAUNCHER_AGENT):
        # loop packages live in agent-loop-* groups, not here
        assert lreq[launcher] == sorted([*dlt_extra_specs("s3").values(), _HUB])
    assert _get_names(lreq[LAUNCHER_MARIMO]) == ["dlt", "marimo", "uvicorn"]
    assert _get_names(lreq[LAUNCHER_MCP]) == ["dlt", "fastmcp", "uvicorn"]
    assert _get_names(lreq[LAUNCHER_STREAMLIT]) == ["dlt", "streamlit"]
    assert lreq[LAUNCHER_DASHBOARD] == [_HUB]


def test_interactive_launchers_omit_artifact_storage_deps() -> None:
    lreq = build_launcher_requirements()
    for launcher in (LAUNCHER_MARIMO, LAUNCHER_MCP, LAUNCHER_STREAMLIT, LAUNCHER_DASHBOARD):
        assert not set(_S3_NAMES) & set(_get_names(lreq[launcher]))


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
    # dashboard runner packages (marimo, pyarrow, ibis-framework) + dlt's s3 extra for artifacts
    assert _get_names(dashboard_specs) == sorted(
        ["ibis-framework", "marimo", "pyarrow"] + _S3_NAMES
    )
    assert set(dlt_extra_specs("parquet").values()) <= set(dashboard_specs)
    assert set(dlt_extra_specs("s3").values()) <= set(dashboard_specs)

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
    for specs in lreq.values():
        assert _HUB in specs
    # batch launchers carry dlt's s3 extra; interactive don't
    s3 = set(dlt_extra_specs("s3").values())
    assert s3 <= set(lreq[LAUNCHER_JOB])
    assert not s3 & set(lreq[LAUNCHER_MARIMO])
    # dashboard has dlt's hub extra + dlt (extras come from DASHBOARD_JOB_REF group)
    assert lreq[LAUNCHER_DASHBOARD] == sorted([_HUB, dlt_spec])


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
def test_collect_package_names_recognizes_dlt_in_every_form(spec: str) -> None:
    assert "dlt" in _collect_package_names([spec])


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
        ("dlt[hub]", {"dlt"}, True),
        ("dlt[hub]", {"dlthub"}, False),
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
        "dlt-hub-kept-by-plain-dlt",
        "dlt-hub-pruned-by-dlthub",
    ],
)
def test_prune_specs_normalization(spec: str, names: Set[str], kept: bool) -> None:
    from dlt._workspace.deployment.requirements import _prune_specs

    assert _prune_specs([spec], names) == ([spec] if kept else [])


def test_export_keeps_workspace_deps_and_prunes_ours() -> None:
    loop_group = agent_loop_group(LOOP_PYDANTIC_AI)
    with isolated_workspace("deps_none") as ctx:
        Path(ctx.run_dir, "pyproject.toml").write_text(
            "[project]\n"
            'name = "deps-kept"\n'
            'version = "0.0.1"\n'
            'dependencies = ["s3fs==2023.6.0", "marimo==0.10.0"]\n'
            "\n"
            "[dependency-groups]\n"
            f'{loop_group} = ["pydantic-ai-slim==2.20.0"]\n'
        )
        result = export_workspace_requirements(Path(ctx.run_dir))
    groups = result["groups"]
    # the workspace's specs and groups are exported as declared, also below our floors
    assert groups[MAIN_GROUP] == ["marimo==0.10.0", "s3fs==2023.6.0"]
    assert groups[loop_group] == ["pydantic-ai-slim==2.20.0"]
    # ours are dropped where the workspace declares the package or one that installs it
    lreq = result["launcher_requirements"]
    for launcher in (LAUNCHER_JOB, LAUNCHER_MODULE, LAUNCHER_AGENT):
        assert not set(_S3_NAMES) & set(_get_names(lreq[launcher]))
    assert not {"marimo", "uvicorn"} & set(_get_names(lreq[LAUNCHER_MARIMO]))
    assert _get_names(groups[DASHBOARD_JOB_REF]) == ["ibis-framework", "pyarrow"]
    # a loop group the workspace does not declare is added as usual
    claude_group = agent_loop_group(LOOP_CLAUDE_AGENT_SDK)
    assert groups[claude_group] == build_agent_loop_groups()[claude_group]


def test_export_prunes_dashboard_group_with_normalized_names() -> None:
    with isolated_workspace("deps_none") as ctx:
        Path(ctx.run_dir, "requirements.txt").write_text(
            "ibis_framework>=12\nPandas\nnumpy>=1.24\n"
        )
        with patch(_SHUTIL_WHICH, return_value=None):
            result = export_workspace_requirements(Path(ctx.run_dir))
    dashboard = _get_names(result["groups"][DASHBOARD_JOB_REF])
    assert "ibis-framework" not in dashboard
    # rest of the synthesized group survives
    assert dashboard == sorted(["marimo", "pyarrow"] + _S3_NAMES)
    # launcher lists share none of these names — untouched except dlt injection
    dlt_spec = get_dlt_requirement_spec()
    assert result["launcher_requirements"][LAUNCHER_JOB] == sorted(
        [*dlt_extra_specs("s3").values(), _HUB, dlt_spec]
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
    dashboard = _get_names(result["groups"][DASHBOARD_JOB_REF])
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
def test_collect_package_names_extras(spec: str, expected: Set[str]) -> None:
    assert _collect_package_names([spec]) == expected


def _export_with_deps(specs: List[str], source: TDepsSource) -> TWorkspaceRequirementsManifest:
    """Exports a workspace whose main dependencies are `specs`, read from `source`."""
    deps_file, resolve = source.split("-")
    with isolated_workspace("deps_none") as ctx:
        run_dir = Path(ctx.run_dir)
        if deps_file == "pyproject":
            deps = ", ".join(f'"{s}"' for s in specs)
            (run_dir / "pyproject.toml").write_text(
                '[project]\nname = "deps-case"\nversion = "0.0.1"\n'
                f'requires-python = ">={python_version()}"\ndependencies = [{deps}]\n'
            )
            if resolve == "resolved":
                _run_uv_lock(ctx.run_dir)
            return export_workspace_requirements(run_dir)
        (run_dir / "requirements.txt").write_text("".join(f"{s}\n" for s in specs))
        if resolve == "resolved":
            return export_workspace_requirements(run_dir)
        # without uv on PATH the requirements file is taken as declared
        with patch(_SHUTIL_WHICH, return_value=None):
            return export_workspace_requirements(run_dir)


def _collect_keys(specs: List[str]) -> Set[str]:
    """Package names in `specs`, plus a `name[extra]` key per declared extra."""
    keys: Set[str] = set()
    for s in specs:
        req = Requirement(s)
        keys.add(req.name)
        keys.update(f"{req.name}[{extra}]" for extra in req.extras)
    return keys


def _get_spec_key(spec: str) -> str:
    """Name of a spec the export adds, `dlt[extra]` for a dlt extra."""
    req = Requirement(spec)
    return f"{req.name}[{','.join(sorted(req.extras))}]" if req.name == "dlt" else req.name


@pytest.mark.parametrize("source", get_args(TDepsSource))
@pytest.mark.parametrize(
    "specs, skipped, dlt_injected",
    [
        (["requests>=2.0"], set(), True),
        (["dlt>=1.0"], set(), False),
        (["dlt[hub]>=1.0"], {_HUB}, False),
        (["dlthub"], {_HUB}, True),
        (["dlthub-client"], set(), True),
        (["dlt[s3]>=1.0"], {"botocore", "s3fs"}, False),
        (["dlt[parquet]>=1.0"], {"pyarrow"}, False),
        (["botocore>=1.28"], {"botocore"}, True),
        (["s3fs>=2024.1"], {"botocore", "s3fs"}, True),
        (["dlt[hub,s3]>=1.0"], {_HUB, "botocore", "s3fs"}, False),
        (["pydantic-ai>=2.33.0"], {"pydantic-ai-slim"}, True),
        (["marimo>=0.14.5"], {"marimo", "uvicorn"}, True),
        (
            [
                "ibis-framework>=12.0.0",
                "streamlit>=1.50.0",
                "claude-agent-sdk>=0.2.126",
                "pydantic-ai-slim>=2.33.0",
            ],
            {"ibis-framework", "streamlit", "claude-agent-sdk", "pydantic-ai-slim"},
            True,
        ),
    ],
    ids=[
        "unrelated",
        "plain-dlt",
        "dlt-hub-extra",
        "dlthub-alone",
        "dlthub-client-alone",
        "dlt-s3-extra",
        "dlt-parquet-extra",
        "botocore-alone",
        "s3fs-with-botocore",
        "dlt-hub-and-s3-extras",
        "pydantic-ai-meta-package",
        "marimo-with-uvicorn",
        "floored-packages-direct",
    ],
)
def test_export_skips_specs_the_workspace_declares(
    specs: List[str], skipped: Set[str], dlt_injected: bool, source: TDepsSource
) -> None:
    result = _export_with_deps(specs, source)
    main = result["groups"][MAIN_GROUP]
    # uv-resolved sources pin every spec and flatten extras, declared ones keep them unchanged
    declared = source.endswith("-declared")
    assert not declared or all(s in main for s in specs)
    assert declared or all("==" in s and not Requirement(s).extras for s in main)

    # a resolved main also lists transitive packages, which skip our spec for them as well
    main_keys = _collect_keys(main)
    built = {
        **build_launcher_requirements(),
        DASHBOARD_JOB_REF: build_dashboard_group(),
        **build_agent_loop_groups(),
    }
    exported = {**result["groups"], **result["launcher_requirements"]}
    dlt_spec = get_dlt_requirement_spec()
    for name, ours in built.items():
        expected = [s for s in ours if _get_spec_key(s) not in skipped | main_keys]
        assert [s for s in exported[name] if s != dlt_spec] == expected, name
    for launcher_specs in result["launcher_requirements"].values():
        assert (dlt_spec in launcher_specs) is dlt_injected


@pytest.mark.parametrize(
    "names, added",
    [
        ({"dlt", "dlt[hub]"}, {"dlthub", "dlthub-client"}),
        ({"dlt", "dlt[s3]"}, {"botocore", "s3fs"}),
        ({"dlt", "dlt[not-an-extra]"}, set()),
        ({"s3fs"}, {"botocore"}),
        ({"pydantic-ai"}, {"pydantic-ai-slim"}),
        ({"marimo"}, {"uvicorn"}),
        ({"dlt"}, set()),
    ],
    ids=[
        "dlt-hub-extra",
        "dlt-s3-extra",
        "unknown-dlt-extra",
        "s3fs",
        "pydantic-ai",
        "marimo",
        "plain-dlt",
    ],
)
def test_add_transitive_names(names: Set[str], added: Set[str]) -> None:
    expanded = set(names)
    _add_transitive_names(expanded)
    assert expanded - names == added


def test_export_scaffolded_workspace_prunes_all_launcher_specs() -> None:
    """A `dlthub init` workspace declares every launcher dep except streamlit."""
    with isolated_workspace("deps_none") as ctx:
        plan = fetch_init_plan(ctx.run_dir, dependencies="pyproject")
        init_dlthub_workspace(plan)
        result = export_workspace_requirements(Path(ctx.run_dir))
    for launcher, specs in result["launcher_requirements"].items():
        if launcher == LAUNCHER_STREAMLIT:
            assert _get_names(specs) == ["streamlit"]
        else:
            assert specs == [], f"{launcher!r} not fully pruned: {specs}"
    assert result["groups"][DASHBOARD_JOB_REF] == []


def test_export_prunes_marker_guarded_specs() -> None:
    """Marker-guarded user specs count toward the prune set — name-level only."""
    with isolated_workspace("deps_none") as ctx:
        Path(ctx.run_dir, "requirements.txt").write_text('s3fs ; python_version >= "3.8"\n')
        with patch(_SHUTIL_WHICH, return_value=None):
            result = export_workspace_requirements(Path(ctx.run_dir))
    assert "s3fs" not in _get_names(result["launcher_requirements"][LAUNCHER_JOB])
    assert "s3fs" not in _get_names(result["groups"][DASHBOARD_JOB_REF])


def _get_added_specs() -> List[str]:
    return [
        *(s for specs in build_launcher_requirements().values() for s in specs),
        *build_dashboard_group(),
        *(s for specs in build_agent_loop_groups().values() for s in specs),
    ]


def test_added_specs_have_lower_bounds() -> None:
    # an unbounded spec lets a conflict walk the resolver back to an ancient release. the dlt
    # extra is bounded by whichever dlt resolves
    unbounded = []
    for s in _get_added_specs():
        req = Requirement(s)
        if req.name == "dlt" and req.extras and not req.specifier:
            continue
        if not any(c.operator in (">=", ">", "==", "~=") for c in req.specifier):
            unbounded.append(s)
    assert unbounded == []


def test_added_specs_allow_no_prereleases() -> None:
    # a direct spec naming a prerelease makes uv install prereleases of that package
    assert [s for s in _get_added_specs() if Requirement(s).specifier.prereleases] == []


def _resolve_export(main_specs: List[str], tmp_path: Path) -> subprocess.CompletedProcess[str]:
    """Resolves a workspace's main group together with every spec the export adds."""
    # the union is a superset of what any single run installs, this checkout stands in for dlt
    result = _export_with_deps(main_specs, "requirements-declared")
    dlt_spec = get_dlt_requirement_spec()
    added = {
        s
        for specs in [
            *result["launcher_requirements"].values(),
            result["groups"][DASHBOARD_JOB_REF],
            *(result["groups"][g] for g in build_agent_loop_groups()),
        ]
        for s in specs
        if s != dlt_spec
    }
    return _run_uv_compile(
        [*result["groups"][MAIN_GROUP], *sorted(added), _DLT_CHECKOUT_SPEC], tmp_path
    )


@pytest.mark.parametrize(
    "main_specs",
    [
        [_DLT_CHECKOUT_SPEC, "dlthub==0.29.0"],
        ["dlthub==0.29.0"],
        [_DLT_CHECKOUT_SPEC, "s3fs==2023.6.0"],
        [_DLT_CHECKOUT_SPEC, "marimo==0.10.0"],
        [_DLT_CHECKOUT_SPEC, "pydantic-ai-slim==2.20.0"],
        [_DLT_CHECKOUT_SPEC, "pydantic-ai==1.0.0"],
    ],
    ids=[
        "old-dlthub",
        "old-dlthub-dlt-injected",
        "old-s3fs",
        "old-marimo",
        "old-pydantic-ai-slim",
        "old-pydantic-ai",
    ],
)
def test_export_resolves_with_workspace_pins(main_specs: List[str], tmp_path: Path) -> None:
    # a package the workspace pins resolves at that version next to what the export adds
    result = _resolve_export(main_specs, tmp_path)
    assert result.returncode == 0, result.stderr
    resolved = {line.strip() for line in result.stdout.splitlines()}
    assert {s for s in main_specs if "==" in s} <= resolved


@pytest.mark.parametrize(
    "main_specs, needs",
    [
        ([_DLT_CHECKOUT_SPEC, "anthropic==0.75.0"], "pydantic-ai-slim"),
        ([_DLT_CHECKOUT_SPEC, "botocore==1.27.0"], "s3fs"),
        ([_DLT_CHECKOUT_SPEC, "dlthub-client==0.28.0"], "dlthub-client"),
    ],
    ids=["anthropic-before-1", "botocore-before-1.28", "dlthub-client-before-0.28.3"],
)
def test_export_fails_on_pins_below_floors(
    main_specs: List[str], needs: str, tmp_path: Path
) -> None:
    # a pin our floors exclude stops the resolve instead of walking a package back
    result = _resolve_export(main_specs, tmp_path)
    assert result.returncode != 0
    assert needs in result.stderr


def test_dlt_extra_specs() -> None:
    s3 = dlt_extra_specs("s3")
    assert set(s3) == set(_S3_NAMES)
    assert not Requirement(s3["s3fs"]).specifier.contains("0.4.2")
    with pytest.raises(WorkspaceRequirementsError, match="no `not-an-extra` extra"):
        dlt_extra_specs("not-an-extra")


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


def _make_install_spec(name, mode, **extra):
    """Build a minimal `TInstallSpec` dict for render-helper tests."""
    base = {"name": name, "extras": [], "version": "9.9.9", "mode": mode}
    base.update(extra)
    return base


@pytest.mark.parametrize(
    "spec,for_deployment,expected",
    [
        (_make_install_spec("dlt", "pypi"), True, "dlt==9.9.9"),
        (_make_install_spec("dlt", "pypi", extras=["hub"]), True, "dlt[hub]==9.9.9"),
        (_make_install_spec("dlt", "editable", path="/abs"), True, "dlt==9.9.9"),
        (_make_install_spec("dlt", "editable", path="/abs"), False, "dlt==9.9.9"),
        (_make_install_spec("dlt", "path", path="/abs"), True, "dlt @ file:///abs"),
        (_make_install_spec("dlt", "path", path="/abs"), False, "dlt==9.9.9"),
        (
            _make_install_spec("dlt", "git", git_url="https://example.com/dlt.git", git_rev="abc"),
            True,
            "dlt @ git+https://example.com/dlt.git@abc",
        ),
        (
            _make_install_spec("dlt", "git", git_url="https://example.com/dlt.git", git_rev="abc"),
            False,
            "dlt==9.9.9",
        ),
        (
            _make_install_spec(
                "dlt",
                "archive",
                archive_url="https://github.com/dlt-hub/dlt/archive/main.zip?v=13",
            ),
            True,
            "dlt @ https://github.com/dlt-hub/dlt/archive/main.zip?v=13",
        ),
        (
            _make_install_spec(
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
        (_make_install_spec("dlt", "pypi"), None),
        (_make_install_spec("dlt", "path", path="/abs"), {"path": "/abs"}),
        (_make_install_spec("dlt", "editable", path="/abs"), {"path": "/abs", "editable": True}),
        (
            _make_install_spec("dlt", "git", git_url="https://x/dlt.git", git_rev="abc"),
            {"git": "https://x/dlt.git", "rev": "abc"},
        ),
        (
            _make_install_spec("dlt", "git", git_url="https://x/dlt.git"),
            {"git": "https://x/dlt.git"},
        ),
        # archive direct refs go inline in dependencies — no override needed
        (_make_install_spec("dlt", "archive", archive_url="https://x/dlt.zip"), None),
    ],
    ids=["pypi-none", "path", "editable", "git-with-rev", "git-no-rev", "archive-none"],
)
def test_render_uv_source(spec, expected) -> None:
    from dlt._workspace.deployment.requirements import render_uv_source

    assert render_uv_source(spec) == expected


@pytest.mark.parametrize(
    "spec,expected",
    [
        (_make_install_spec("dlt", "pypi"), ["dlt==9.9.9"]),
        (_make_install_spec("dlt", "pypi", extras=["hub"]), ["dlt[hub]==9.9.9"]),
        (_make_install_spec("dlt", "editable", path="/abs"), ["-e /abs"]),
        (_make_install_spec("dlt", "path", path="/abs"), ["dlt @ file:///abs"]),
        (
            _make_install_spec("dlt", "git", git_url="https://x/dlt.git", git_rev="abc"),
            ["dlt @ git+https://x/dlt.git@abc"],
        ),
        (
            _make_install_spec("dlt", "archive", archive_url="https://x/dlt.zip?v=13"),
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

    def _get_distribution_without_hubs(name: str):
        if name in ("dlthub", "dlthub-client"):
            raise req_mod.importlib.metadata.PackageNotFoundError(name)
        return real_dist(name)

    monkeypatch.setattr(req_mod.importlib.metadata, "distribution", _get_distribution_without_hubs)
    specs = req_mod.get_workspace_install_specs()
    names = {s["name"] for s in specs}
    assert names == {"dlt"}
