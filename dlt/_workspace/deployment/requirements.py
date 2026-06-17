"""Export, save, load, and migrate `TWorkspaceRequirementsManifest` — the wire format for workspace dependencies shipped to the runtime.
Supports requirements.txt and requirements.in. If uv installed - supports pyproject with dependency groups and uv/PEP lock files.
poetry/PDM support can be easily added.
"""

import importlib.metadata
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, BinaryIO, Dict, List, Literal, Optional, Sequence, Set

import tomlkit
from packaging.requirements import Requirement

from dlt.common import json
from dlt.common.exceptions import DictValidationException
from dlt.common.typing import DictStrAny, NotRequired, TypedDict
from dlt.common.validation import validate_dict
from dlt.version import DLT_PKG_NAME

DLTHUB_PKG_NAME = "dlthub"
DLTHUB_CLIENT_PKG_NAME = "dlthub-client"

from dlt._workspace.deployment.launchers import (
    LAUNCHER_DASHBOARD,
    LAUNCHER_JOB,
    LAUNCHER_MARIMO,
    LAUNCHER_MCP,
    LAUNCHER_MODULE,
    LAUNCHER_STREAMLIT,
)
from dlt._workspace.deployment.typing import (
    DASHBOARD_JOB_REF,
    MAIN_GROUP,
    REQUIREMENTS_ENGINE_VERSION,
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
    "TInstallSpec",
    "TInstallMode",
    "TWorkspaceRequirementsManifest",
    "WorkspaceRequirementsError",
    "build_dashboard_group",
    "build_launcher_requirements",
    "default_requirements_manifest",
    "export_workspace_requirements",
    "get_dlt_requirement_spec",
    "get_pkg_install_spec",
    "get_workspace_install_specs",
    "load_requirements",
    "migrate_requirements",
    "python_version",
    "render_pep508",
    "render_requirements_lines",
    "render_uv_source",
    "save_requirements",
]


TInstallMode = Literal["pypi", "path", "editable", "git", "archive"]


class TInstallSpec(TypedDict):
    """How a Python package is installed, derived from PEP 610 `direct_url.json`."""

    name: str
    extras: List[str]
    version: str
    mode: TInstallMode
    path: NotRequired[str]
    git_url: NotRequired[str]
    git_rev: NotRequired[str]
    archive_url: NotRequired[str]


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
        spec["path"] = url[len("file://") :]
        return spec

    # http(s) archive — `pip install https://.../pkg.zip` (incl. github archive URLs)
    if info.get("archive_info") is not None or url.startswith(("http://", "https://")):
        spec["mode"] = "archive"
        spec["archive_url"] = url
        return spec

    # unknown direct_url variant — fall back to pypi version pin
    return spec


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
            return f"{name_extras} @ file://{spec['path']}"
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
        return [f"{name_extras} @ file://{spec['path']}"]
    if mode == "archive":
        return [f"{name_extras} @ {spec['archive_url']}"]
    # git
    git_url = spec.get("git_url", "")
    git_rev = spec.get("git_rev")
    ref = f"git+{git_url}@{git_rev}" if git_rev else f"git+{git_url}"
    return [f"{name_extras} @ {ref}"]


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


_BASE_LAUNCHER_SPECS: List[str] = ["croniter", "dlthub"]
"""Specs added to every launcher group and the dashboard group."""


def build_launcher_requirements() -> Dict[str, List[str]]:
    """Per-launcher mandatory specs. dlt is injected separately at build time."""
    per_launcher: Dict[str, List[str]] = {
        LAUNCHER_JOB: ["botocore", "s3fs"],
        LAUNCHER_MODULE: ["botocore", "s3fs"],
        LAUNCHER_MARIMO: ["marimo", "uvicorn"],
        LAUNCHER_MCP: ["fastmcp", "uvicorn"],
        LAUNCHER_STREAMLIT: ["streamlit"],
        LAUNCHER_DASHBOARD: [],
    }
    return {k: sorted(set(v + _BASE_LAUNCHER_SPECS)) for k, v in per_launcher.items()}


def build_dashboard_group() -> List[str]:
    """Specs for the `DASHBOARD_JOB_REF` group.

    Matches the dashboard runner's dependency gate plus `s3fs` for artifact access;
    the launcher baseline (croniter, dlthub, dlt) comes from `launcher_requirements`.
    """
    return sorted(["ibis-framework", "marimo", "pyarrow", "s3fs"])


def _inject_dlt_into_launchers(launcher_requirements: Dict[str, List[str]]) -> None:
    dlt_spec = get_dlt_requirement_spec()
    for launcher in launcher_requirements:
        launcher_requirements[launcher] = sorted(launcher_requirements[launcher] + [dlt_spec])


def default_requirements_manifest() -> TWorkspaceRequirementsManifest:
    """Minimal manifest: empty `main`, dashboard group, launcher specs with dlt injected."""
    launcher_requirements = build_launcher_requirements()
    _inject_dlt_into_launchers(launcher_requirements)
    return {
        "engine_version": REQUIREMENTS_ENGINE_VERSION,
        "python_version": python_version(),
        "default_groups": [MAIN_GROUP],
        "groups": {MAIN_GROUP: [], DASHBOARD_JOB_REF: build_dashboard_group()},
        "launcher_requirements": launcher_requirements,
    }


def export_workspace_requirements(
    workspace_root: Path,
    default_groups: Optional[List[str]] = None,
) -> TWorkspaceRequirementsManifest:
    """Export a workspace's dependencies as a `TWorkspaceRequirementsManifest`.

    If no default group names dlt, the installed dlt spec is injected into the
    launcher baseline so every job gets it.

    Args:
        workspace_root (Path): Workspace directory.
        default_groups (Optional[List[str]]): Manifest-level `default_groups`.
            Defaults to `["main"]`.

    Returns:
        TWorkspaceRequirementsManifest: Always contains a `main` entry in `groups`.

    Raises:
        WorkspaceRequirementsError: `uv.lock` out of sync, `uv` failure, or parse error.
    """
    pyproject_path = workspace_root / PYPROJECT_TOML
    uv_lock_path = workspace_root / UV_LOCK
    requirements_txt_path = workspace_root / REQUIREMENTS_TXT
    requirements_in_path = workspace_root / REQUIREMENTS_IN

    # detection order: pyproject -> requirements.txt -> requirements.in -> empty main
    if pyproject_path.exists():
        groups = _export_from_pyproject(workspace_root, pyproject_path, uv_lock_path)
    elif requirements_txt_path.exists():
        groups = {MAIN_GROUP: _compile_requirements_file(workspace_root, requirements_txt_path)}
    elif requirements_in_path.exists():
        groups = {MAIN_GROUP: _compile_requirements_file(workspace_root, requirements_in_path)}
    else:
        groups = {MAIN_GROUP: []}

    resolved_default_groups = list(default_groups) if default_groups else [MAIN_GROUP]
    # names installed by default groups — always present at runtime, safe to prune against
    default_names: Set[str] = set()
    for name in resolved_default_groups:
        default_names.update(_collect_package_names(groups.get(name, [])))
    _expand_implied_names(default_names)

    groups[DASHBOARD_JOB_REF] = _prune_specs(build_dashboard_group(), default_names)

    launcher_requirements = build_launcher_requirements()
    for launcher, specs in launcher_requirements.items():
        launcher_requirements[launcher] = _prune_specs(specs, default_names)
    # inject dlt into every launcher entry unless a default group already names it
    if _normalize_name(DLT_PKG_NAME) not in default_names:
        _inject_dlt_into_launchers(launcher_requirements)

    return {
        "engine_version": REQUIREMENTS_ENGINE_VERSION,
        "python_version": python_version(),
        "default_groups": resolved_default_groups,
        "groups": dict(sorted(groups.items())),
        "launcher_requirements": launcher_requirements,
    }


def migrate_requirements(
    manifest_dict: DictStrAny, from_engine: int, to_engine: int
) -> TWorkspaceRequirementsManifest:
    """Migrate a requirements manifest dict between engine versions."""
    if from_engine == to_engine:
        return manifest_dict  # type: ignore[return-value]
    raise ValueError(f"no requirements migration path from engine {from_engine} to {to_engine}")


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


def _export_from_pyproject(
    workspace_root: Path, pyproject_path: Path, uv_lock_path: Path
) -> Dict[str, List[str]]:
    try:
        doc = tomlkit.parse(pyproject_path.read_text(encoding="utf-8"))
    except Exception as ex:
        raise WorkspaceRequirementsError(f"Failed to parse {pyproject_path}: {ex}") from ex

    group_names = _dependency_group_names(doc)

    if uv_lock_path.exists():
        # lockfile is binary-ish YAML that only uv can faithfully interpret,
        # so this path has no fallback — require uv upfront.
        _require_uv()
        # verify the lock is in sync — non-mutating
        _run_uv(["lock", "--check"], cwd=workspace_root)
        result: Dict[str, List[str]] = {}
        result[MAIN_GROUP] = _parse_uv_output(
            _run_uv(
                [
                    "export",
                    "--no-hashes",
                    "--no-emit-project",
                    "--no-default-groups",
                    "--format",
                    "requirements.txt",
                ],
                cwd=workspace_root,
            )
        )
        for name in group_names:
            result[name] = _parse_uv_output(
                _run_uv(
                    [
                        "export",
                        "--no-hashes",
                        "--no-emit-project",
                        "--no-default-groups",
                        "--only-group",
                        name,
                        "--format",
                        "requirements.txt",
                    ],
                    cwd=workspace_root,
                )
            )
        return dict(sorted(result.items()))

    # no lock — parse declarations directly
    project = doc.get("project", {}) or {}
    main_deps = list(project.get("dependencies", []) or [])
    result = {MAIN_GROUP: _parse_dep_list(main_deps)}

    groups = doc.get("dependency-groups", {}) or {}
    for name in group_names:
        result[name] = _parse_dep_list(list(groups.get(name, []) or []))
    return dict(sorted(result.items()))


def _compile_requirements_file(workspace_root: Path, file_path: Path) -> List[str]:
    """Export a `requirements.txt` / `requirements.in` file as sorted PEP 508 specs."""
    # with uv: `uv pip compile --universal` for a resolved, platform-independent
    # lockset. without uv: pure-Python parse, specs returned as authored
    if is_uv_available():
        stdout = _run_uv(
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
        return _parse_uv_output(stdout)
    return _parse_requirements_file(file_path)


def _parse_requirements_file(file_path: Path) -> List[str]:
    """Parse a user-authored `requirements.txt` / `.in` file to sorted PEP 508 specs."""
    # does not resolve; flag lines (-e, -r, --index-url) are skipped
    text = file_path.read_text(encoding="utf-8")
    # join line continuations
    text = text.replace("\\\n", "")
    specs: List[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        # flag lines: -e, -r, --index-url, etc.
        if line.startswith("-"):
            continue
        # strip inline comments
        if " #" in line:
            line = line.split(" #", 1)[0].strip()
        if not line:
            continue
        try:
            req = Requirement(line)
        except Exception:
            continue
        specs.append(str(req))
    return sorted(set(specs))


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


# PEP 508 leading identifier: first letter/digit then letters/digits/_/-/., optional extras
_LEADING_NAME_RE = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9_.\-]*)\s*(?:\[([^\]]*)\])?")
# PEP 503 normalization: lowercase, runs of `-_.` collapsed to single `-`
_PEP503_SEP_RE = re.compile(r"[-_.]+")


def _normalize_name(name: str) -> str:
    return _PEP503_SEP_RE.sub("-", name.lower())


def _contains_package(specs: Sequence[str], pkg_name: str) -> bool:
    """Check whether any PEP 508 spec in `specs` names `pkg_name` (PEP 503 normalized)."""
    target = _normalize_name(pkg_name)
    for s in specs:
        m = _LEADING_NAME_RE.match(s)
        if m and _normalize_name(m.group(1)) == target:
            return True
    return False


_IMPLIED_NAMES: Dict[str, List[str]] = {
    f"{DLT_PKG_NAME}[hub]": [DLTHUB_PKG_NAME, "croniter"],
    DLTHUB_CLIENT_PKG_NAME: ["croniter"],
    "s3fs": ["botocore"],
    "marimo": ["uvicorn"],
    "fastmcp": ["uvicorn"],
}
"""Launcher specs pulled in transitively when the key package (or extra) is installed."""


def _collect_package_names(specs: Sequence[str]) -> Set[str]:
    """PEP 503 normalized names of `specs`; extras add one `name[extra]` token each."""
    names: Set[str] = set()
    for s in specs:
        m = _LEADING_NAME_RE.match(s)
        if not m:
            continue
        name = _normalize_name(m.group(1))
        names.add(name)
        extras = m.group(2)
        if extras:
            for extra in extras.split(","):
                extra = extra.strip()
                if extra:
                    names.add(f"{name}[{_normalize_name(extra)}]")
    return names


def _expand_implied_names(names: Set[str]) -> None:
    """Add names pulled in transitively by meta-packages in `names`."""
    for implying, implied in _IMPLIED_NAMES.items():
        if implying in names:
            names.update(implied)


def _prune_specs(specs: List[str], names: Set[str]) -> List[str]:
    """Drop specs whose PEP 503 normalized name is in `names`, preserving order."""
    pruned: List[str] = []
    for s in specs:
        m = _LEADING_NAME_RE.match(s)
        if m and _normalize_name(m.group(1)) in names:
            continue
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


def _run_uv(args: List[str], cwd: Path) -> str:
    """Invoke `uv` with the given args, returning stdout on success."""
    try:
        proc = subprocess.run(
            ["uv", *args],
            cwd=str(cwd),
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
