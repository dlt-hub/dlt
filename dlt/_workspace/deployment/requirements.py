"""Export, save, load, and migrate `TWorkspaceRequirementsManifest` — the
wire format for workspace dependencies shipped to the runtime."""

import importlib.metadata
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, BinaryIO, Dict, List, Optional, Sequence

import tomlkit
from packaging.requirements import Requirement

from dlt.common import json
from dlt.common.exceptions import DictValidationException
from dlt.common.typing import DictStrAny
from dlt.common.validation import validate_dict
from dlt.version import DLT_PKG_NAME

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
    "MAIN_GROUP",
    "REQUIREMENTS_ENGINE_VERSION",
    "TWorkspaceRequirementsManifest",
    "WorkspaceRequirementsError",
    "build_dashboard_group",
    "build_launcher_requirements",
    "default_requirements_manifest",
    "export_workspace_requirements",
    "get_dlt_requirement_spec",
    "load_requirements",
    "migrate_requirements",
    "python_version",
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
    """Build a PEP 508 spec for the currently installed dlt distribution.

    Uses PEP 610 `direct_url.json` when dlt was installed from a URL (branch
    zip, git) so the spec survives on a remote runner; falls back to a
    `dlt==<version>` pin for ordinary index installs. Mirrors the runtime
    vault's repackager so manifests stay consistent with what runs on Modal.
    """
    dist = importlib.metadata.distribution(DLT_PKG_NAME)
    direct_url_text = dist.read_text("direct_url.json")
    if direct_url_text:
        info = json.loads(direct_url_text)
        # editable local installs (`file://` + `dir_info.editable=True`) can't
        # be resolved on a remote runner — fall back to a version pin in that case
        dir_info = info.get("dir_info") or {}
        if not dir_info.get("editable"):
            return f"{DLT_PKG_NAME} @ {info['url']}"
    return f"{DLT_PKG_NAME}=={dist.metadata['Version']}"


def python_version() -> str:
    """Current interpreter's `major.minor` version, e.g. `"3.12"`."""
    return f"{sys.version_info.major}.{sys.version_info.minor}"


def build_launcher_requirements() -> Dict[str, List[str]]:
    """Per-launcher mandatory specs. dlt is injected separately at build time."""
    # only batch launchers (job, module) pull botocore/s3fs; dashboard extras
    # travel via the DASHBOARD_JOB_REF group and leave the entry empty
    return {
        LAUNCHER_JOB: ["botocore", "s3fs"],
        LAUNCHER_MODULE: ["botocore", "s3fs"],
        LAUNCHER_MARIMO: ["marimo", "uvicorn"],
        LAUNCHER_MCP: ["fastmcp", "uvicorn"],
        LAUNCHER_STREAMLIT: ["streamlit"],
        LAUNCHER_DASHBOARD: [],
    }


def build_dashboard_group() -> List[str]:
    """Specs for the `DASHBOARD_JOB_REF` group."""
    return [
        "botocore",
        "ibis-framework",
        "marimo",
        "numpy",
        "pandas",
        "pyarrow",
        "s3fs",
        "uvicorn",
    ]


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
    workspace_root = Path(workspace_root)

    pyproject_path = workspace_root / PYPROJECT_TOML
    uv_lock_path = workspace_root / UV_LOCK
    requirements_txt_path = workspace_root / REQUIREMENTS_TXT
    requirements_in_path = workspace_root / REQUIREMENTS_IN

    # detection order: pyproject → requirements.txt → requirements.in → empty main
    if pyproject_path.exists():
        groups = _export_from_pyproject(workspace_root, pyproject_path, uv_lock_path)
    elif requirements_txt_path.exists():
        groups = {MAIN_GROUP: _compile_requirements_file(workspace_root, requirements_txt_path)}
    elif requirements_in_path.exists():
        groups = {MAIN_GROUP: _compile_requirements_file(workspace_root, requirements_in_path)}
    else:
        groups = {MAIN_GROUP: []}

    groups[DASHBOARD_JOB_REF] = build_dashboard_group()

    resolved_default_groups = list(default_groups) if default_groups else [MAIN_GROUP]

    launcher_requirements = build_launcher_requirements()
    # inject dlt into every launcher entry unless a default group already names it
    default_has_dlt = any(
        _contains_package(groups.get(name, []), DLT_PKG_NAME) for name in resolved_default_groups
    )
    if not default_has_dlt:
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
    """Export a `requirements.txt` / `requirements.in` file as sorted PEP 508 specs.

    When `uv` is available, runs `uv pip compile --universal` for a fully
    resolved, platform-independent lockset. When `uv` is missing, falls back
    to a pure-Python parse of the file — no resolution, specs are returned
    as authored.
    """
    if _is_uv_available():
        stdout = _run_uv(
            [
                "pip",
                "compile",
                "--universal",
                "--no-header",
                "--no-annotate",
                "-o",
                "-",
                file_path.name,
            ],
            cwd=workspace_root,
        )
        return _parse_uv_output(stdout)
    return _parse_requirements_file(file_path)


def _parse_requirements_file(file_path: Path) -> List[str]:
    """Parse a user-authored `requirements.txt` / `.in` file to sorted PEP 508 specs.

    Handles comments, blank lines, line continuations, and skips flag lines
    (`-e`, `-r`, `--index-url`, ...). Does **not** resolve — returns specs
    as authored, normalized through `packaging.requirements.Requirement`.
    """
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
    """Deduplicate and sort the lines of a `uv export` / `uv pip compile` blob.

    We pass `--no-header --no-annotate --no-hashes` / equivalent flags so uv's
    output is already canonical PEP 508 — one spec per line, no flags, no
    continuations. We only need to drop blanks/comments and return a stable
    sorted set.
    """
    return sorted(
        {
            line.strip()
            for line in text.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
    )


# PEP 508 leading identifier: first letter/digit then letters/digits/_/-/.
_LEADING_NAME_RE = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9_.\-]*)")
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


def _is_uv_available() -> bool:
    """Return `True` if the `uv` binary is on PATH."""
    return shutil.which("uv") is not None


def _require_uv() -> None:
    """Raise a user-friendly `WorkspaceRequirementsError` when `uv` is missing."""
    if not _is_uv_available():
        raise WorkspaceRequirementsError(_UV_MISSING_MESSAGE)


def _run_uv(args: List[str], cwd: Path) -> str:
    """Invoke `uv` with the given args, returning stdout on success.

    Callers are expected to have checked `_is_uv_available()` first; the
    `FileNotFoundError` branch is a last-resort safety net.
    """
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
