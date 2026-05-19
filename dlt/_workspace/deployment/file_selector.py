from typing import Iterable, Iterator, Optional, List, Tuple, TYPE_CHECKING
from pathlib import Path


if TYPE_CHECKING:
    from pathspec import PathSpec


from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.profile import LOCAL_PROFILES


# fallback ignore patterns used when no ignore file is found in the workspace
DEFAULT_IGNORES: List[str] = [
    "__pycache__/",
    "*.py[cod]",
    ".venv/",
    "venv/",
    "dist/",
    "build/",
    "*.egg-info/",
    ".mypy_cache/",
    ".ruff_cache/",
    ".pytest_cache/",
    ".coverage",
    "htmlcov/",
    "*.so",
    ".DS_Store",
    ".env",
]


class BaseFileSelector(Iterable[Tuple[Path, Path]]):
    """
    Base class for file selectors. For every file yields 2 paths: absolute path in the filesystem
    and relative path of the file in the resulting tarball
    """

    pass


class WorkspaceFileSelector(BaseFileSelector):
    """Iterates files in workspace respecting ignore patterns and excluding workspace internals.

    Uses gitignore-style patterns from a configurable ignore file (default .gitignore). Additional
    patterns can be provided as relative paths from workspace root. Settings directory is always excluded.
    """

    def __init__(
        self,
        context: WorkspaceRunContext,
        additional_excludes: Optional[List[str]] = None,
        ignore_file: str = ".gitignore",
    ) -> None:
        self.root_path: Path = Path(context.run_dir).resolve()
        self.settings_dir: Path = Path(context.settings_dir).resolve()
        self.ignore_file: str = ignore_file
        self.ignore_file_found: bool = False
        self.ignore_spec: "PathSpec" = self._build_pathspec(additional_excludes or [])

    def _build_pathspec(self, additional_excludes: List[str]) -> "PathSpec":
        """Build PathSpec from ignore file + defaults + additional excludes"""
        from pathspec import PathSpec

        patterns: List[str] = [f"{self.settings_dir.relative_to(self.root_path)}/"]

        # load ignore file if exists, otherwise fall back to default ignores
        ignore_path = self.root_path / self.ignore_file
        if ignore_path.exists():
            with ignore_path.open("r", encoding="utf-8") as f:
                patterns.extend(f.read().splitlines())
            self.ignore_file_found = True
        else:
            patterns.extend(DEFAULT_IGNORES)

        # Add caller-provided excludes
        patterns.extend(additional_excludes)

        return PathSpec.from_lines("gitignore", patterns)

    def __iter__(self) -> Iterator[Tuple[Path, Path]]:
        """Yield paths of files eligible for deployment"""
        from pathspec.util import iter_tree_files

        root_path = Path(self.root_path)
        for file_path in iter_tree_files(self.root_path):
            if not self.ignore_spec.match_file(file_path):
                yield root_path / file_path, Path(file_path)


class ConfigurationFileSelector(BaseFileSelector):
    """Iterates top-level config/secrets TOMLs from the workspace settings dir."""

    def __init__(
        self,
        context: WorkspaceRunContext,
        local_profiles: Optional[List[str]] = None,
    ) -> None:
        self.settings_dir: Path = Path(context.settings_dir).resolve()
        # files belonging to local-only profiles (`dev`, `tests` by default) are excluded
        if local_profiles is None:
            local_profiles = LOCAL_PROFILES
        # filter out files starting with ie. "dev"
        self._excluded_prefixes: Tuple[str, ...] = tuple(f"{p}." for p in local_profiles)

    def __iter__(self) -> Iterator[Tuple[Path, Path]]:
        """Yield paths of config and secrets files (flat, profile-filtered)."""
        if not self.settings_dir.exists():
            return
        # picks only files directly under `<workspace>/.dlt/`
        for entry in sorted(self.settings_dir.iterdir()):
            if not entry.is_file():
                continue
            name = entry.name
            if not (name.endswith("config.toml") or name.endswith("secrets.toml")):
                continue
            if name.startswith(self._excluded_prefixes):
                continue
            yield entry, Path(name)
