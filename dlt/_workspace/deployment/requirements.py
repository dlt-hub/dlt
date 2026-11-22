"""Export, save, load, and migrate `TWorkspaceRequirementsManifest` — the wire format for workspace dependencies shipped to the runtime.
Supports requirements.txt and requirements.in. If uv is installed, it also supports pyproject with
dependency groups and uv/PEP lock files. This includes the lock at the root of a uv workspace that
the dlt workspace is a member of.
poetry/PDM support can be easily added.
"""

import importlib.metadata
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, BinaryIO, Dict, Final, List, Optional, Sequence, Set, Tuple
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname

import tomlkit
from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

from dlt.common import json, logger
from dlt.common.exceptions import DictValidationException
from dlt.common.typing import DictStrAny
from dlt.common.validation import validate_dict
from dlt.version import DLT_PKG_NAME, get_extra_requirements

DLTHUB_PKG_NAME = "dlthub"
DLTHUB_CLIENT_PKG_NAME = "dlthub-client"

from dlt._workspace.deployment.launchers import (
    BUILTIN_AGENT_LOOPS,
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
from dlt._workspace.deployment.typing import (
    DASHBOARD_JOB_REF,
    MAIN_GROUP,
    REQUIREMENTS_ENGINE_VERSION,
    TDependencyChange,
    TInstallMode,
    TInstallSpec,
    TWorkspaceRequirementsManifest,
)


PYPROJECT_TOML = "pyproject.toml"
REQUIREMENTS_TXT = "requirements.txt"
REQUIREMENTS_IN = "requirements.in"
UV_LOCK = "uv.lock"

__all__ = [
    "DLTHUB_CLIENT_PKG_NAME",
    "DLTHUB_PKG_NAME",
    "MAIN_GROUP",
    "REQUIREMENTS_ENGINE_VERSION",
    "TDependencyChange",
    "TInstallSpec",
    "TInstallMode",
    "TWorkspaceRequirementsManifest",
    "WorkspaceRequirementsError",
    "add_pyproject_dependencies",
    "append_lines",
    "build_dashboard_group",
    "build_launcher_requirements",
    "default_requirements_manifest",
    "dlt_extra_specs",
    "export_workspace_requirements",
    "floored_spec",
    "get_dlt_requirement_spec",
    "get_file_url_path",
    "get_pkg_install_spec",
    "get_workspace_install_specs",
    "is_uv_available",
    "load_requirements",
    "merge_requirements_txt",
    "migrate_requirements",
    "plan_dependency_changes",
    "plan_dependency_file_changes",
    "python_version",
    "read_pyproject_dependencies",
    "read_requirements_file",
    "render_pep508",
    "render_pyproject",
    "render_requirements_lines",
    "render_uv_source",
    "run_uv",
    "save_requirements",
]


_UV_MISSING_MESSAGE = (
    "`uv` is required to export dependencies from this workspace but was not found"
    " on PATH.\n\n"
    "uv is a fast drop-in replacement for pip + pip-tools + virtualenv + pyenv,"
    " written in Rust. It resolves and installs Python dependencies 10-100x faster"
    " than pip and ships as a single static binary — no Python bootstrap needed.\n\n"
    "Install it with one of:\n"
    "    curl -LsSf https://astral.sh/uv/install.sh | sh                 # Linux / macOS\n"
    '    powershell -c "irm https://astral.sh/uv/install.ps1 | iex"      # Windows\n'
    "    brew install uv                                                 # macOS (Homebrew)\n"
    "    pipx install uv                                                 # cross-platform\n\n"
    "Docs: https://docs.astral.sh/uv/"
)


class WorkspaceRequirementsError(Exception):
    """Raised when a workspace's dependency files cannot be exported."""


def get_dlt_requirement_spec() -> str:
    """PEP 508 spec for the currently installed dlt distribution, deployment-mode."""
    return render_pep508(get_pkg_install_spec(DLT_PKG_NAME), for_deployment=True)


def get_pkg_install_spec(pkg_name: str, *, extras: Optional[List[str]] = None) -> TInstallSpec:
    """Classify the install of `pkg_name` as pypi / path / editable / git."""
    # raises importlib.metadata.PackageNotFoundError if the package isn't installed
    dist = importlib.metadata.distribution(pkg_name)
    version = dist.metadata["Version"] or ""
    spec: TInstallSpec = {
        "name": pkg_name,
        "extras": list(extras or []),
        "version": version,
        "mode": "pypi",
    }
    direct_url_text = dist.read_text("direct_url.json")
    if not direct_url_text:
        return spec
    info = json.loads(direct_url_text)
    url = info.get("url") or ""
    vcs_info = info.get("vcs_info") or {}
    dir_info = info.get("dir_info") or {}

    if vcs_info.get("vcs") == "git":
        spec["mode"] = "git"
        spec["git_url"] = url
        commit = vcs_info.get("commit_id")
        if commit:
            spec["git_rev"] = commit
        return spec

    # both editable and plain local installs use file:// URIs
    if url.startswith("file://"):
        spec["mode"] = "editable" if dir_info.get("editable") else "path"
        spec["path"] = get_file_url_path(url)
        return spec

    # http(s) archive — `pip install https://.../pkg.zip` (incl. github archive URLs)
    if info.get("archive_info") is not None or url.startswith(("http://", "https://")):
        spec["mode"] = "archive"
        spec["archive_url"] = url
        return spec

    # unknown direct_url variant — fall back to pypi version pin
    return spec


def get_file_url_path(url: str) -> str:
    """The local path of a `file://` URL, decoded. A UNC URL (`file://server/share/pkg`) keeps its
    host."""
    parsed = urlparse(url)
    if parsed.netloc and parsed.netloc != "localhost":
        unc_path = f"//{parsed.netloc}{parsed.path}"
        # python 3.14 `url2pathname` refuses a remote host on posix, which has no UNC paths
        return url2pathname(unc_path) if os.name == "nt" else unquote(unc_path)
    return url2pathname(parsed.path)


def _name_with_extras(spec: TInstallSpec) -> str:
    if not spec["extras"]:
        return spec["name"]
    return f"{spec['name']}[{','.join(spec['extras'])}]"


def render_pep508(spec: TInstallSpec, *, for_deployment: bool) -> str:
    """PEP 508 line for `[project.dependencies]` / top-of-`requirements.txt`.

    Args:
        spec: Install spec to render.
        for_deployment: When True, render a portable line (editable becomes a
            version pin; path/git become PEP 508 direct refs). When False (used
            for scaffolding), emit a version pin so `[tool.uv.sources]` can carry
            the override.
    """
    name_extras = _name_with_extras(spec)
    mode = spec["mode"]
    version = spec["version"]
    if mode == "pypi":
        return f"{name_extras}=={version}" if version else name_extras
    if mode == "editable":
        # editable can't be expressed in PEP 508 — both modes fall back to the pin
        return f"{name_extras}=={version}" if version else name_extras
    if mode == "path":
        if for_deployment:
            return f"{name_extras} @ {Path(spec['path']).as_uri()}"
        return f"{name_extras}=={version}" if version else name_extras
    if mode == "archive":
        # PEP 508 direct ref; portable to pip + uv with no source-override needed
        return f"{name_extras} @ {spec['archive_url']}"
    # git
    git_url = spec.get("git_url", "")
    git_rev = spec.get("git_rev")
    if for_deployment or not version:
        ref = f"git+{git_url}@{git_rev}" if git_rev else f"git+{git_url}"
        return f"{name_extras} @ {ref}"
    return f"{name_extras}=={version}"


def render_uv_source(spec: TInstallSpec) -> Optional[Dict[str, Any]]:
    """`[tool.uv.sources][<name>]` value, or `None` when a direct ref in deps suffices."""
    # archives carry the URL inline in `dependencies` (PEP 508 direct ref) — no override
    mode = spec["mode"]
    if mode in ("pypi", "archive"):
        return None
    if mode == "path":
        return {"path": spec["path"]}
    if mode == "editable":
        return {"path": spec["path"], "editable": True}
    # git
    out: Dict[str, Any] = {"git": spec.get("git_url", "")}
    rev = spec.get("git_rev")
    if rev:
        out["rev"] = rev
    return out


def render_requirements_lines(spec: TInstallSpec) -> List[str]:
    """Lines for `requirements.txt` reproducing `spec`. Editable becomes `-e <path>`."""
    name_extras = _name_with_extras(spec)
    mode = spec["mode"]
    version = spec["version"]
    if mode == "pypi":
        return [f"{name_extras}=={version}"] if version else [name_extras]
    if mode == "editable":
        return [f"-e {spec['path']}"]
    if mode == "path":
        return [f"{name_extras} @ {Path(spec['path']).as_uri()}"]
    if mode == "archive":
        return [f"{name_extras} @ {spec['archive_url']}"]
    # git
    git_url = spec.get("git_url", "")
    git_rev = spec.get("git_rev")
    ref = f"git+{git_url}@{git_rev}" if git_rev else f"git+{git_url}"
    return [f"{name_extras} @ {ref}"]


def render_pyproject(
    name: str,
    deps: Sequence[str],
    uv_sources: Dict[str, Dict[str, Any]],
    requires_python: Optional[str] = None,
) -> str:
    """A `pyproject.toml` that declares `deps`, with `[tool.uv.sources]` when `uv_sources` is not
    empty."""
    doc = tomlkit.document()
    project = tomlkit.table()
    project["name"] = name
    project["version"] = "0.0.1"
    project["description"] = "A dlthub workspace"
    if requires_python:
        project["requires-python"] = requires_python
    deps_array = tomlkit.array()
    for d in deps:
        deps_array.append(d)
    deps_array.multiline(True)
    project["dependencies"] = deps_array
    doc["project"] = project

    if uv_sources:
        sources_table = tomlkit.table()
        for pkg, src in uv_sources.items():
            entry = tomlkit.inline_table()
            for k, v in src.items():
                entry[k] = v
            sources_table[pkg] = entry
        uv_table = tomlkit.table()
        uv_table["sources"] = sources_table
        tool_table = tomlkit.table()
        tool_table["uv"] = uv_table
        doc["tool"] = tool_table

    return tomlkit.dumps(doc)


def get_workspace_install_specs() -> List[TInstallSpec]:
    """Specs for `dlt[hub]` + `dlthub` + `dlthub-client`, skipping ones not installed."""
    # dlt is always present (it's how this code is running) and tagged with [hub] so
    # the scaffold's transitive resolution pulls dlthub / dlthub-client. The other two
    # are only listed when locally installed — that's the signal we need an override.
    specs: List[TInstallSpec] = [get_pkg_install_spec(DLT_PKG_NAME, extras=["hub"])]
    for pkg in (DLTHUB_PKG_NAME, DLTHUB_CLIENT_PKG_NAME):
        try:
            specs.append(get_pkg_install_spec(pkg))
        except importlib.metadata.PackageNotFoundError:
            continue
    return specs


def python_version() -> str:
    """Current interpreter's `major.minor` version, e.g. `"3.12"`."""
    return f"{sys.version_info.major}.{sys.version_info.minor}"


def dlt_extra_specs(extra: str) -> Dict[str, str]:
    """Specs of the installed dlt's `extra` by package name, with dlt's own bounds.

    Raises:
        WorkspaceRequirementsError: The installed dlt declares nothing under `extra`.
    """
    specs = {name: str(req) for name, req in get_extra_requirements(extra).items()}
    if not specs:
        raise WorkspaceRequirementsError(f"installed {DLT_PKG_NAME} declares no `{extra}` extra")
    return specs


# relaxed floors: rarely conflicting with a workspace's pins, never resolving ancient releases
_FLOORS: Final[Dict[str, str]] = {
    # 0.2.126 is the first release whose ResultMessage carries terminal_reason
    "claude-agent-sdk": ">=0.2.126",
    "fastmcp": ">=3.1.0",
    "ibis-framework": ">=12.0.0",
    "marimo": ">=0.14.5",
    # 2.45.0 is the first release whose mcp module imports httpx2 instead of undeclared httpx
    "pydantic-ai-slim": ">=2.45.0",
    "streamlit": ">=1.50.0",
    "uvicorn": ">=0.46.0",
}


def floored_spec(name: str, *, extras: Sequence[str] = ()) -> str:
    """PEP 508 spec for `name` with its relaxed floor."""
    extras_part = f"[{','.join(extras)}]" if extras else ""
    return f"{name}{extras_part}{_FLOORS[name]}"


def build_launcher_requirements() -> Dict[str, List[str]]:
    """Per-launcher mandatory specs. dlt is injected separately at build time."""
    s3 = list(dlt_extra_specs("s3").values())
    # the bounds of dlt's hub extra keep dlthub compatible with the installed dlt
    dlthub = dlt_extra_specs("hub")[DLTHUB_PKG_NAME]
    per_launcher: Dict[str, List[str]] = {
        LAUNCHER_JOB: [*s3, dlthub],
        LAUNCHER_MODULE: [*s3, dlthub],
        # loop packages are per-job, so they live in `agent-loop-*` groups instead
        LAUNCHER_AGENT: s3,
        LAUNCHER_MARIMO: [floored_spec("marimo"), floored_spec("uvicorn"), dlthub],
        LAUNCHER_MCP: [floored_spec("fastmcp"), floored_spec("uvicorn"), dlthub],
        LAUNCHER_STREAMLIT: [floored_spec("streamlit"), dlthub],
        LAUNCHER_DASHBOARD: [dlthub],
    }
    return {k: sorted(v) for k, v in per_launcher.items()}


def build_dashboard_group() -> List[str]:
    """Specs for the `DASHBOARD_JOB_REF` group.

    Matches the packages the dashboard runner requires plus dlt's `s3` extra for artifact
    access; the launcher baseline (dlthub, dlt) comes from `launcher_requirements`.
    """
    return sorted(
        [floored_spec("ibis-framework"), floored_spec("marimo")]
        + list(dlt_extra_specs("parquet").values())
        + list(dlt_extra_specs("s3").values())
    )


def build_agent_loop_groups() -> Dict[str, List[str]]:
    """Specs for each built-in agent loop, keyed by its requirements group name."""
    loop_specs: Dict[str, List[str]] = {
        LOOP_PYDANTIC_AI: [
            floored_spec(
                "pydantic-ai-slim", extras=["anthropic", "openai", "google", "mcp", "spec"]
            )
        ],
        LOOP_CLAUDE_AGENT_SDK: [floored_spec("claude-agent-sdk")],
    }
    return {agent_loop_group(loop): sorted(loop_specs[loop]) for loop in BUILTIN_AGENT_LOOPS}


def _add_agent_loop_groups(groups: Dict[str, List[str]], default_names: Set[str]) -> None:
    """Adds the built-in loop groups, leaving a group the workspace already declares alone."""
    for name, specs in build_agent_loop_groups().items():
        if name not in groups:
            groups[name] = _prune_specs(specs, default_names)


def _inject_dlt_into_launchers(launcher_requirements: Dict[str, List[str]]) -> None:
    dlt_spec = get_dlt_requirement_spec()
    for launcher in launcher_requirements:
        launcher_requirements[launcher] = sorted(launcher_requirements[launcher] + [dlt_spec])


def default_requirements_manifest() -> TWorkspaceRequirementsManifest:
    """Minimal manifest: empty `main`, dashboard and agent loop groups, launcher specs with dlt."""
    launcher_requirements = build_launcher_requirements()
    _inject_dlt_into_launchers(launcher_requirements)
    groups: Dict[str, List[str]] = {MAIN_GROUP: [], DASHBOARD_JOB_REF: build_dashboard_group()}
    groups.update(build_agent_loop_groups())
    return {
        "engine_version": REQUIREMENTS_ENGINE_VERSION,
        "python_version": python_version(),
        "dlt_version": get_pkg_install_spec(DLT_PKG_NAME),
        "default_groups": [MAIN_GROUP],
        "groups": groups,
        "launcher_requirements": launcher_requirements,
    }


def export_workspace_requirements(
    workspace_root: Path,
    default_groups: Optional[List[str]] = None,
) -> TWorkspaceRequirementsManifest:
    """Export a workspace's dependencies as a `TWorkspaceRequirementsManifest`.

    dlt exports a `pyproject.toml` from `uv.lock` next to it. Without that lock, dlt uses the lock
    at the root of the uv workspace that the dlt workspace belongs to. Without either lock, dlt
    exports the declared dependencies. If no default group names dlt, the installed dlt spec is
    injected into the launcher baseline so every job gets it.

    Args:
        workspace_root (Path): Workspace directory.
        default_groups (Optional[List[str]]): Manifest-level `default_groups`.
            Defaults to `["main"]`.

    Returns:
        TWorkspaceRequirementsManifest: Always contains a `main` entry in `groups`.

    Raises:
        WorkspaceRequirementsError: `uv.lock` out of sync, `uv` failure, parse error, or a
            dependency on another member of the uv workspace.
    """
    pyproject_path = workspace_root / PYPROJECT_TOML
    uv_lock_path = workspace_root / UV_LOCK
    requirements_txt_path = workspace_root / REQUIREMENTS_TXT
    requirements_in_path = workspace_root / REQUIREMENTS_IN

    dlt_version = get_pkg_install_spec(DLT_PKG_NAME)
    # detection order: pyproject -> requirements.txt -> requirements.in -> empty main
    resolved = False
    if pyproject_path.exists():
        groups, resolved = _export_from_pyproject(workspace_root, pyproject_path, uv_lock_path)
    elif requirements_txt_path.exists() or requirements_in_path.exists():
        file_path = (
            requirements_txt_path if requirements_txt_path.exists() else requirements_in_path
        )
        main, compiled = _compile_requirements_file(workspace_root, file_path)
        groups = {MAIN_GROUP: main}
        if compiled:
            dlt_version = _get_deployed_dlt_spec(main, dlt_version, workspace_root, file_path.name)
    else:
        groups = {MAIN_GROUP: []}

    resolved_default_groups = list(default_groups) if default_groups else [MAIN_GROUP]
    # names installed by default groups — always present at runtime, safe to prune against
    default_names: Set[str] = set()
    for name in resolved_default_groups:
        default_names.update(_collect_package_names(groups.get(name, [])))
    _add_transitive_names(default_names)
    if resolved:
        _prune_resolved_groups(groups, resolved_default_groups, default_names)

    groups[DASHBOARD_JOB_REF] = _prune_specs(build_dashboard_group(), default_names)
    _add_agent_loop_groups(groups, default_names)

    launcher_requirements = build_launcher_requirements()
    for launcher, specs in launcher_requirements.items():
        launcher_requirements[launcher] = _prune_specs(specs, default_names)
    # inject dlt into every launcher entry unless a default group already names it
    if _normalize_name(DLT_PKG_NAME) not in default_names:
        _inject_dlt_into_launchers(launcher_requirements)

    return {
        "engine_version": REQUIREMENTS_ENGINE_VERSION,
        "python_version": python_version(),
        "dlt_version": dlt_version,
        "default_groups": resolved_default_groups,
        "groups": dict(sorted(groups.items())),
        "launcher_requirements": launcher_requirements,
    }


PRE_TRACKING_DLT_INSTALL_SPEC: TInstallSpec = {
    "name": DLT_PKG_NAME,
    "extras": [],
    "version": "1.28.0",
    "mode": "pypi",
}
"""dlt install assumed for engine-1 manifests, which predate dlt-version tracking."""


def migrate_requirements(
    manifest_dict: DictStrAny, from_engine: int, to_engine: int
) -> TWorkspaceRequirementsManifest:
    """Migrate a requirements manifest dict between engine versions, in place."""
    if from_engine == to_engine:
        return manifest_dict  # type: ignore[return-value]
    if from_engine == 1 and to_engine > 1:
        # engine 2 adds dlt_version; engine-1 manifests predate it, assume 1.28.0
        manifest_dict.setdefault("dlt_version", dict(PRE_TRACKING_DLT_INSTALL_SPEC))
        from_engine = 2
    if from_engine != to_engine:
        raise ValueError(f"no requirements migration path from engine {from_engine} to {to_engine}")
    manifest_dict["engine_version"] = to_engine
    return manifest_dict  # type: ignore[return-value]


def save_requirements(req: TWorkspaceRequirementsManifest, f: BinaryIO) -> None:
    """Serialize a requirements manifest as typed JSON."""
    f.write(json.typed_dumpb(req))


def load_requirements(f: BinaryIO) -> TWorkspaceRequirementsManifest:
    """Read, migrate, and validate a requirements manifest."""
    data = f.read()
    manifest_dict: DictStrAny = json.typed_loadb(data)
    engine_version = manifest_dict.get("engine_version", 1)
    try:
        manifest = migrate_requirements(manifest_dict, engine_version, REQUIREMENTS_ENGINE_VERSION)
    except ValueError as ex:
        raise WorkspaceRequirementsError(str(ex)) from ex
    try:
        validate_dict(TWorkspaceRequirementsManifest, manifest, ".")
    except DictValidationException as ex:
        raise WorkspaceRequirementsError(f"invalid requirements manifest: {ex}") from ex
    return manifest


def _prune_resolved_groups(
    groups: Dict[str, List[str]], default_groups: Sequence[str], default_names: Set[str]
) -> None:
    """Drops from each resolved group what the default groups install: the runtime installs a
    group on top of them."""
    default_specs = {spec for name in default_groups for spec in groups.get(name, [])}
    for name, specs in groups.items():
        if name not in default_groups:
            # an editable or a path install has no leading name, so it matches as written
            groups[name] = [s for s in _prune_specs(specs, default_names) if s not in default_specs]


def _export_from_pyproject(
    workspace_root: Path, pyproject_path: Path, uv_lock_path: Path
) -> Tuple[Dict[str, List[str]], bool]:
    """The dependency groups of `pyproject_path`, and whether they come resolved from a lock."""
    try:
        doc = tomlkit.parse(pyproject_path.read_text(encoding="utf-8"))
    except Exception as ex:
        raise WorkspaceRequirementsError(f"Failed to parse {pyproject_path}: {ex}") from ex

    group_names = _dependency_group_names(doc)

    if uv_lock_path.exists():
        # only uv interprets its lock faithfully, so this path has no fallback
        _require_uv()
        # verify the lock is in sync — non-mutating
        _run_uv_on_lock(["lock", "--check"], workspace_root)
        return _export_locked(workspace_root, group_names, []), True

    workspace_member = _get_uv_workspace_member(workspace_root, doc)
    if workspace_member is not None:
        member, uv_workspace_root = workspace_member
        # validates the lock at the root of the uv workspace
        _run_uv_on_lock(["lock", "--check"], workspace_root)
        _check_no_workspace_member_dependencies(workspace_root, member, uv_workspace_root)
        return (
            _export_locked(
                workspace_root, group_names, ["--package", member, "--no-emit-workspace"]
            ),
            True,
        )

    # no lock — parse declarations directly
    project = doc.get("project", {}) or {}
    main_deps = list(project.get("dependencies", []) or [])
    result = {MAIN_GROUP: _parse_dep_list(main_deps)}

    groups = doc.get("dependency-groups", {}) or {}
    for name in group_names:
        result[name] = _parse_dep_list(list(groups.get(name, []) or []))
    return dict(sorted(result.items())), False


_UV_EXPORT_ARGS = [
    "export",
    "--no-hashes",
    "--no-emit-project",
    "--no-default-groups",
    "--format",
    "requirements.txt",
]


def _export_locked(
    workspace_root: Path, group_names: Sequence[str], extra_args: Sequence[str]
) -> Dict[str, List[str]]:
    """Exports the main dependencies and each of `group_names` from the lock with `uv export`,
    passing `extra_args` to every call."""
    result: Dict[str, List[str]] = {
        MAIN_GROUP: _parse_uv_output(
            _run_uv_on_lock([*_UV_EXPORT_ARGS, *extra_args], workspace_root)
        )
    }
    for name in group_names:
        result[name] = _parse_uv_output(
            _run_uv_on_lock([*_UV_EXPORT_ARGS, "--only-group", name, *extra_args], workspace_root)
        )
    return dict(sorted(result.items()))


def _run_uv_on_lock(args: List[str], cwd: Path) -> str:
    """Runs uv on the lock without the uv settings that skip its checks or update it."""
    # with `UV_FROZEN`, `uv lock --check` passes a stale lock
    env = {k: v for k, v in os.environ.items() if k not in ("UV_FROZEN", "UV_LOCKED", "UV_NO_SYNC")}
    return run_uv(args, cwd, env)


def _get_uv_workspace_member(
    workspace_root: Path, pyproject_doc: Any
) -> Optional[Tuple[str, Path]]:
    """The project name in `workspace_root` and the root of its uv workspace, when that root
    holds `uv.lock`. `None` when the project is not a member of such a uv workspace, or when uv
    cannot tell."""
    name = (pyproject_doc.get("project", {}) or {}).get("name")
    if not name or not is_uv_available():
        return None
    try:
        root = Path(run_uv(["workspace", "dir"], cwd=workspace_root).strip())
        if root.resolve() == workspace_root.resolve() or not (root / UV_LOCK).exists():
            return None
        member_dir = Path(
            run_uv(["workspace", "dir", "--package", str(name)], cwd=workspace_root).strip()
        )
    except WorkspaceRequirementsError:
        # an older uv without `workspace dir` or no such member
        return None
    if member_dir.resolve() != workspace_root.resolve():
        return None
    return str(name), root


def _check_no_workspace_member_dependencies(
    workspace_root: Path, member: str, uv_workspace_root: Path
) -> None:
    """Raises when `member` depends on other members of its uv workspace: the deployment ships
    only the dlt workspace and cannot install them."""
    args = [*_UV_EXPORT_ARGS, "--package", member, "--all-groups"]
    with_members = _parse_uv_output(_run_uv_on_lock(args, workspace_root))
    without_members = set(
        _parse_uv_output(_run_uv_on_lock([*args, "--no-emit-workspace"], workspace_root))
    )
    other_members = [line for line in with_members if line not in without_members]
    if other_members:
        raise WorkspaceRequirementsError(
            f"`{member}` depends on other members of the uv workspace in `{uv_workspace_root}`:"
            f" {', '.join(other_members)}. A deployment ships only the dlt workspace in"
            f" `{workspace_root}` and cannot install them. Publish them as packages. Or move"
            " their code into the dlt workspace."
        )


def _compile_requirements_file(workspace_root: Path, file_path: Path) -> Tuple[List[str], bool]:
    """A `requirements.txt` / `requirements.in` file as sorted PEP 508 specs, and whether uv
    resolved them."""
    # with uv: `uv pip compile --universal` for a resolved, platform-independent
    # lockset. without uv: pure-Python parse, specs returned as authored
    if is_uv_available():
        stdout = run_uv(
            [
                "pip",
                "compile",
                "--universal",
                "--no-header",
                "--no-annotate",
                file_path.name,
            ],
            cwd=workspace_root,
        )
        return _parse_uv_output(stdout), True
    return read_requirements_file(file_path), False


def _get_deployed_dlt_spec(
    main: Sequence[str], installed: TInstallSpec, workspace_root: Path, file_name: str
) -> TInstallSpec:
    """The dlt that the resolved `main` installs. Warns when it is not the installed dlt."""
    for line in main:
        # an editable line names its path only
        if line.startswith("-e "):
            if _is_installed_path(line[3:].strip(), installed, workspace_root):
                return installed
            continue
        requirement = _parse_requirements_line(line)
        if requirement is None or canonicalize_name(requirement.name) != DLT_PKG_NAME:
            continue
        if requirement.url:
            if not _is_installed_source(requirement.url, installed, workspace_root):
                logger.warning(
                    f"`{file_name}` installs dlt from `{requirement.url}`, but dlt"
                    f" {installed['version']} is installed. dlt cannot tell the version that the"
                    " deployment runs. To deploy the installed dlt, pin it in"
                    f" `{file_name}`."
                )
            return installed
        pinned = next((s.version for s in requirement.specifier if s.operator == "=="), None)
        if pinned is None:
            return installed
        if pinned != installed["version"]:
            logger.warning(
                f"`{file_name}` resolves dlt {pinned}, but dlt {installed['version']} is installed."
                f" The deployment runs dlt {pinned}. To deploy the installed dlt, pin it in"
                f" `{file_name}`."
            )
        return {"name": DLT_PKG_NAME, "extras": [], "version": pinned, "mode": "pypi"}
    # without dlt in main, the launchers get the installed dlt
    return installed


def _is_installed_source(url: str, installed: TInstallSpec, workspace_root: Path) -> bool:
    """Whether the direct reference `url` points at the source of the `installed` dlt."""
    if url.startswith("file:"):
        return _is_installed_path(get_file_url_path(url), installed, workspace_root)
    if installed["mode"] == "archive":
        return url == installed.get("archive_url")
    if installed["mode"] == "git":
        return url in (
            f"git+{installed.get('git_url')}@{installed.get('git_rev')}",
            f"git+{installed.get('git_url')}",
        )
    return False


def _is_installed_path(path: str, installed: TInstallSpec, workspace_root: Path) -> bool:
    if installed["mode"] not in ("path", "editable"):
        return False
    resolved = os.path.realpath(os.path.join(workspace_root, path))
    return os.path.normcase(resolved) == os.path.normcase(os.path.realpath(installed["path"]))


def read_pyproject_dependencies(pyproject_path: Path) -> List[str]:
    """`[project.dependencies]` of a `pyproject.toml` as sorted PEP 508 specs."""
    try:
        doc = tomlkit.parse(pyproject_path.read_text(encoding="utf-8"))
    except Exception as ex:
        raise WorkspaceRequirementsError(f"Failed to parse {pyproject_path}: {ex}") from ex
    project = doc.get("project", {}) or {}
    return _parse_dep_list(list(project.get("dependencies", []) or []))


def read_requirements_file(file_path: Path) -> List[str]:
    """Parse a user-authored `requirements.txt` / `.in` file to sorted PEP 508 specs."""
    # does not resolve
    text = file_path.read_text(encoding="utf-8")
    # join line continuations
    text = text.replace("\\\n", "")
    specs: Set[str] = set()
    for line in text.splitlines():
        requirement = _parse_requirements_line(line)
        if requirement is not None:
            specs.add(str(requirement))
    return sorted(specs)


def _parse_requirements_line(line: str) -> Optional[Requirement]:
    """The requirement that a `requirements.txt` line declares, `None` for comments, flag lines
    (`-e`, `-r`, `--index-url`) and lines that are not PEP 508. Options of the requirement, such
    as `--hash`, are dropped."""
    line = line.split(" #", 1)[0].split(" --", 1)[0].strip()
    if not line or line.startswith(("#", "-")):
        return None
    try:
        return Requirement(line)
    except InvalidRequirement:
        return None


def append_lines(text: str, lines: Sequence[str], comment: str) -> str:
    """Appends `lines` to `text` below the `comment` line, separated by an empty line."""
    if not lines:
        return text
    if text and not text.endswith("\n"):
        text += "\n"
    separator = "\n" if text else ""
    return text + separator + comment + "\n" + "\n".join(lines) + "\n"


def merge_requirements_txt(
    existing: str, changes: Sequence[TDependencyChange], comment: str
) -> str:
    """Applies `changes` to the content of a `requirements.txt`. An upgrade replaces the line of
    its package in place. An added package goes below the `comment` line at the end."""
    lines = existing.splitlines()
    present = {line.strip() for line in lines}
    additions: List[str] = []
    for change in changes:
        if change["op"] == "upgrade":
            for i, line in enumerate(lines):
                requirement = _parse_requirements_line(line)
                if requirement and canonicalize_name(requirement.name) == change["name"]:
                    lines[i] = change["spec"]
                    break
            else:
                additions.append(change["spec"])
        elif change["spec"] not in present:
            additions.append(change["spec"])
    merged = "\n".join(lines) + "\n" if lines else ""
    return append_lines(merged, additions, comment)


def plan_dependency_file_changes(
    file_path: Path, desired: Sequence[str], workspace_specs: Sequence[TInstallSpec]
) -> List[TDependencyChange]:
    """Plans the changes that make the `pyproject.toml` or `requirements.txt` at `file_path` meet
    the `desired` specs. In `requirements.txt`, an editable install of `workspace_specs` is
    written as `-e <path>`."""
    if file_path.name == PYPROJECT_TOML:
        return plan_dependency_changes(read_pyproject_dependencies(file_path), desired)
    changes = plan_dependency_changes(read_requirements_file(file_path), desired)
    lines: Dict[str, str] = {
        canonicalize_name(s["name"]): render_requirements_lines(s)[0] for s in workspace_specs
    }
    for change in changes:
        line = lines.get(change["name"], "")
        if line.startswith("-e ") and change["op"] != "newer":
            change["spec"] = line
    # the reader skips `-e` lines, so an editable install already listed looks missing
    present = {line.strip() for line in file_path.read_text(encoding="utf-8").splitlines()}
    return [c for c in changes if not (c["op"] == "add" and c["spec"] in present)]


def add_pyproject_dependencies(
    changes: Sequence[TDependencyChange], uv_sources: Dict[str, Dict[str, Any]], project_dir: Path
) -> None:
    """Writes `changes` into the `pyproject.toml` in `project_dir` with `uv add`, without locking or
    syncing. A package in `uv_sources` keeps its source in `[tool.uv.sources]`."""
    if not changes:
        return
    # `--frozen` already skips the sync, and uv refuses `--frozen` together with these settings
    env = {k: v for k, v in os.environ.items() if k not in ("UV_NO_SYNC", "UV_LOCKED")}
    run_uv(["add", "--frozen", *(change["spec"] for change in changes)], project_dir, env)
    for change in changes:
        source = uv_sources.get(change["name"])
        if source is None:
            continue
        # uv writes the source and keeps the spec added before
        if "git" in source:
            target = ["git+" + source["git"]]
            if source.get("rev"):
                target += ["--rev", source["rev"]]
        elif source.get("editable"):
            target = ["--editable", source["path"]]
        else:
            target = [source["path"]]
        extras = [arg for e in sorted(Requirement(change["spec"]).extras) for arg in ("--extra", e)]
        run_uv(["add", "--frozen", *target, *extras], project_dir, env)


def _dependency_group_names(pyproject_doc: Any) -> List[str]:
    """Sorted keys of `[dependency-groups]`, or `[]` if absent."""
    groups = pyproject_doc.get("dependency-groups", {}) or {}
    return sorted(str(k) for k in groups.keys())


def _parse_uv_output(text: str) -> List[str]:
    """Deduplicate and sort the lines of a `uv export` / `uv pip compile` blob."""
    # callers always pass --no-header --no-annotate --no-hashes (or equivalent),
    # so the input is already canonical PEP 508 — just drop blanks/comments
    return sorted(
        {
            line.strip()
            for line in text.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
    )


# PEP 508 leading identifier with optional extras. a regex rather than Requirement() because a
# resolved export carries hundreds of specs and full parsing is ~45x slower
_LEADING_NAME_RE = re.compile(
    r"^\s*(?P<name>[A-Za-z0-9][A-Za-z0-9_.\-]*)\s*(?:\[(?P<extras>[^\]]*)\])?"
)
# PEP 503 normalization: lowercase, runs of `-_.` collapsed to single `-`
_PEP503_SEP_RE = re.compile(r"[-_.]+")


def _normalize_name(name: str) -> str:
    return _PEP503_SEP_RE.sub("-", name.lower())


# packages a workspace dependency installs on its own, which make our spec for them redundant
_TRANSITIVE_DEPENDENCIES: Dict[str, List[str]] = {
    "s3fs": ["botocore"],
    "pydantic-ai": ["pydantic-ai-slim"],
    "marimo": ["uvicorn"],
    "fastmcp": ["uvicorn"],
}


def _collect_package_names(specs: Sequence[str]) -> Set[str]:
    """PEP 503 normalized names of `specs`, plus a `name[extra]` entry per declared extra."""
    names: Set[str] = set()
    for s in specs:
        m = _LEADING_NAME_RE.match(s)
        if not m:
            continue
        name = _normalize_name(m.group("name"))
        names.add(name)
        for extra in (m.group("extras") or "").split(","):
            if extra.strip():
                names.add(f"{name}[{_normalize_name(extra.strip())}]")
    return names


def _add_transitive_names(names: Set[str]) -> None:
    """Adds the names that packages in `names` install on their own."""
    # a resolved export already lists the packages of a dlt extra, a declared `dlt[extra]` does not
    extra_prefix = f"{DLT_PKG_NAME}["
    for entry in [n for n in names if n.startswith(extra_prefix)]:
        names.update(get_extra_requirements(entry[len(extra_prefix) : -1]))
    for name, transitive in _TRANSITIVE_DEPENDENCIES.items():
        if name in names:
            names.update(transitive)


def _prune_specs(specs: List[str], names: Set[str]) -> List[str]:
    """Drop specs whose PEP 503 normalized name is in `names`, preserving order."""
    pruned: List[str] = []
    for s in specs:
        m = _LEADING_NAME_RE.match(s)
        if m is None or _normalize_name(m.group("name")) not in names:
            pruned.append(s)
    return pruned


def _parse_dep_list(entries: Sequence[Any]) -> List[str]:
    """Normalize a `[project.dependencies]` / `[dependency-groups].<name>` list.

    Skips non-string entries (PEP 735 `include-group` directives) and any
    entries that fail to parse as PEP 508.
    """
    specs: List[str] = []
    for raw in entries:
        if not isinstance(raw, str):
            continue
        s = raw.strip()
        if not s:
            continue
        try:
            req = Requirement(s)
        except Exception:
            continue
        specs.append(str(req))
    return sorted(set(specs))


def is_uv_available() -> bool:
    """Return `True` if the `uv` binary is on PATH."""
    return shutil.which("uv") is not None


def _require_uv() -> None:
    """Raise a user-friendly `WorkspaceRequirementsError` when `uv` is missing."""
    if not is_uv_available():
        raise WorkspaceRequirementsError(_UV_MISSING_MESSAGE)


def run_uv(args: List[str], cwd: Path, env: Optional[Dict[str, str]] = None) -> str:
    """Invoke `uv` with the given args, returning stdout on success."""
    try:
        proc = subprocess.run(
            ["uv", *args],
            cwd=str(cwd),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as ex:
        raise WorkspaceRequirementsError(_UV_MISSING_MESSAGE) from ex
    if proc.returncode != 0:
        raise WorkspaceRequirementsError(
            f"`uv {' '.join(args)}` failed with exit code {proc.returncode}:\n{proc.stderr}"
        )
    return proc.stdout


# clauses whose version a spec pins or starts at
_LOWER_BOUND_OPERATORS = ("==", "===", ">=", "~=", ">")


def _get_lower_bound(requirement: Requirement) -> Optional[Version]:
    """The version that `requirement` pins or starts at, `None` when it has none."""
    for specifier in requirement.specifier:
        if specifier.operator in _LOWER_BOUND_OPERATORS:
            try:
                return Version(specifier.version)
            except InvalidVersion:
                # a wildcard pin such as `==1.*`
                return None
    return None


def plan_dependency_changes(
    declared: Sequence[str], desired: Sequence[str]
) -> List[TDependencyChange]:
    """Plans the changes that make the `declared` specs of a project meet the `desired` ones.

    The plan adds a missing spec. It upgrades a declared spec that excludes the desired version
    or lacks a desired extra, and keeps its other extras. It keeps a declared newer version and
    never downgrades it. It leaves a spec that admits the desired version as is.
    """
    declared_by_name: Dict[str, Tuple[Requirement, str]] = {}
    for spec in declared:
        try:
            requirement = Requirement(spec)
        except InvalidRequirement:
            continue
        declared_by_name[canonicalize_name(requirement.name)] = (requirement, spec)

    changes: List[TDependencyChange] = []
    for spec in desired:
        wanted = Requirement(spec)
        name = canonicalize_name(wanted.name)
        if name not in declared_by_name:
            changes.append({"name": name, "op": "add", "spec": spec})
            continue
        have, declared_spec = declared_by_name[name]
        lacks_extras = not wanted.extras <= have.extras
        if wanted.url:
            # a dev install (editable, path or git) replaces what is declared
            if have.url != wanted.url or lacks_extras:
                changes.append(_make_upgrade(name, wanted, have, declared_spec))
            continue
        if have.url:
            # the project installs it from its own source
            continue
        wanted_version = _get_lower_bound(wanted)
        excludes = wanted_version is not None and not have.specifier.contains(
            wanted_version, prereleases=True
        )
        have_version = _get_lower_bound(have)
        is_newer = bool(wanted_version and have_version and have_version > wanted_version)
        if excludes and not is_newer:
            changes.append(_make_upgrade(name, wanted, have, declared_spec))
        elif lacks_extras:
            # the declared version stays, only the extras are added
            changes.append(_make_upgrade(name, have, wanted, declared_spec))
        elif excludes:
            changes.append({"name": name, "op": "newer", "spec": spec, "declared": declared_spec})
    return changes


def _make_upgrade(
    name: str, kept: Requirement, other: Requirement, declared_spec: str
) -> TDependencyChange:
    """An upgrade to the version of `kept` with the extras of both and a marker of either."""
    upgraded = Requirement(str(kept))
    upgraded.extras = kept.extras | other.extras
    if upgraded.marker is None:
        upgraded.marker = other.marker
    return {"name": name, "op": "upgrade", "spec": str(upgraded), "declared": declared_spec}
