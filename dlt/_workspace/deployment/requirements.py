"""Export a workspace's Python dependencies as a platform-independent
group-keyed dict that can be embedded in a deployment manifest.

The output shape is `Dict[str, List[str]]` where keys are dependency group
names (`main` + any PEP 735 `[dependency-groups]` entries) and values are
sorted lists of PEP 508 requirement specs. Downstream runtime code (Modal)
composes an image from `result["main"]` plus each group named in a job's
`TRequireSpec.extras`.
"""

import importlib.metadata
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import tomlkit
from packaging.requirements import Requirement

from dlt.common import json
from dlt.version import DLT_PKG_NAME


PYPROJECT_TOML = "pyproject.toml"
REQUIREMENTS_TXT = "requirements.txt"
REQUIREMENTS_IN = "requirements.in"
UV_LOCK = "uv.lock"
MAIN_GROUP = "main"

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

DEFAULT_REQUIREMENTS: List[str] = []
"""Extra packages for the `main` group when a workspace has no dep management
files. Tests can append to this list or pass `default_dependencies=` per call.
The locally installed dlt is **always** added to the fallback — resolved via
`get_dlt_requirement_spec()` — so callers don't need to repeat it here."""


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


def export_workspace_requirements(
    workspace_root: Path,
    default_dependencies: Optional[List[str]] = None,
) -> Dict[str, List[str]]:
    """Export a workspace's Python dependencies as a platform-independent,
    group-keyed dict.

    Detection order (first match wins):
      1. `pyproject.toml` (+ optional `uv.lock`)
      2. `requirements.txt` or `requirements.in` (uniformly via `uv pip compile --universal`)
      3. Nothing → `{MAIN_GROUP: default_dependencies or DEFAULT_REQUIREMENTS}`
         with the locally installed dlt spec always appended if missing.

    Args:
        workspace_root (Path): Directory to scan for dependency files.
        default_dependencies (Optional[List[str]]): Fallback for the `main`
            group when no dep management files exist. When `None`, the
            module-level `DEFAULT_REQUIREMENTS` is used. The locally
            installed dlt is always added to the result if not already
            present.

    Returns:
        Dict[str, List[str]]: `{group_name: [pep508_spec, ...]}`. Always
        contains a `main` group. Keys beyond `main` come from
        `[dependency-groups]` in `pyproject.toml`. Both the top-level keys
        and the spec lists are sorted so the result is byte-stable.

    Raises:
        WorkspaceRequirementsError: If `uv.lock` is out of sync with
            `pyproject.toml`, a `uv` subprocess fails, or a file cannot be
            parsed.
    """
    workspace_root = Path(workspace_root)

    pyproject_path = workspace_root / PYPROJECT_TOML
    uv_lock_path = workspace_root / UV_LOCK
    requirements_txt_path = workspace_root / REQUIREMENTS_TXT
    requirements_in_path = workspace_root / REQUIREMENTS_IN

    if pyproject_path.exists():
        return _export_from_pyproject(workspace_root, pyproject_path, uv_lock_path)

    if requirements_txt_path.exists():
        return {MAIN_GROUP: _compile_requirements_file(workspace_root, requirements_txt_path)}

    if requirements_in_path.exists():
        return {MAIN_GROUP: _compile_requirements_file(workspace_root, requirements_in_path)}

    defaults = list(
        default_dependencies if default_dependencies is not None else DEFAULT_REQUIREMENTS
    )
    if not _contains_package(defaults, DLT_PKG_NAME):
        defaults.append(get_dlt_requirement_spec())
    return {MAIN_GROUP: sorted(set(defaults))}


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


def _contains_package(specs: Sequence[str], pkg_name: str) -> bool:
    """Check whether any PEP 508 spec in `specs` names `pkg_name` (PEP 503 normalized)."""
    target = pkg_name.lower().replace("-", "_").replace(".", "_")
    for s in specs:
        try:
            name = Requirement(s).name
        except Exception:
            continue
        if name.lower().replace("-", "_").replace(".", "_") == target:
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
