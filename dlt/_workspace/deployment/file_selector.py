import os
from fnmatch import fnmatchcase
from typing import Iterable, Iterator, Optional, List, Tuple, TYPE_CHECKING
from pathlib import Path


if TYPE_CHECKING:
    from pathspec import PathSpec


from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.cli.dlthub.ai.utils import TOOLKITS_INDEX_FILE
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

SETTINGS_INCLUDES: List[str] = [TOOLKITS_INDEX_FILE]
"""Settings-dir files that ship with the code. Never anything holding secrets."""

GIT_IGNORES: List[str] = [".git/"]
"""Git's own folder, which a `.gitignore` never lists."""


class BaseFileSelector(Iterable[Tuple[Path, Path]]):
    """
    Base class for file selectors. For every file yields 2 paths: absolute path in the filesystem
    and relative path of the file in the resulting tarball
    """

    pass


class GitignoreFileSelector(BaseFileSelector):
    """Iterates files under a root folder selected by gitignore-style patterns, in stable order.

    Raises:
        ImportError: `pathspec` is not installed.
    """

    def __init__(
        self,
        root_path: str,
        include: Optional[List[str]] = None,
        additional_excludes: Optional[List[str]] = None,
        ignore_file: str = ".gitignore",
        use_ignore_file: bool = True,
        ignore_git: bool = True,
    ) -> None:
        """
        Args:
            root_path (str): Folder to select files from. Yielded relative paths start here.
            include (Optional[List[str]]): Patterns a file must match, e.g. `["**/*.py"]`. All
                files when omitted.
            additional_excludes (Optional[List[str]]): Ignore patterns added to the ignore file's.
            ignore_file (str): Name of the ignore file in `root_path`.
            use_ignore_file (bool): When False, neither the ignore file nor `DEFAULT_IGNORES`
                apply, only `additional_excludes` and `.git` stay out.
            ignore_git (bool): Leave out the `.git` folder.
        """
        self.ignore_git: bool = ignore_git
        from pathspec import PathSpec

        self.root_path: Path = Path(root_path).resolve()
        self.ignore_file: str = ignore_file
        self.ignore_file_found: bool = False
        self.include_spec: Optional["PathSpec"] = (
            PathSpec.from_lines("gitignore", include) if include else None
        )
        self.ignore_spec: "PathSpec" = self._build_pathspec(
            additional_excludes or [], use_ignore_file
        )

    def _base_excludes(self) -> List[str]:
        """Ignore patterns that come before the ignore file's."""
        return []

    def _build_pathspec(self, additional_excludes: List[str], use_ignore_file: bool) -> "PathSpec":
        """Build PathSpec from ignore file + defaults + additional excludes"""
        from pathspec import PathSpec

        patterns: List[str] = [*(GIT_IGNORES if self.ignore_git else []), *self._base_excludes()]
        if use_ignore_file:
            # load ignore file if exists, otherwise fall back to default ignores
            ignore_path = self.root_path / self.ignore_file
            if ignore_path.exists():
                with ignore_path.open("r", encoding="utf-8") as f:
                    patterns.extend(f.read().splitlines())
                self.ignore_file_found = True
            else:
                patterns.extend(DEFAULT_IGNORES)
        patterns.extend(additional_excludes)
        self._negations = [p[1:] for p in patterns if p.startswith("!")]
        return PathSpec.from_lines("gitignore", patterns)

    def _enters(self, relative_dir: str) -> bool:
        """Whether the walk descends into `relative_dir`."""
        if not self.ignore_spec.match_file(f"{relative_dir}/"):
            return True
        # an excluded folder is still entered when a negated pattern may bring back a file in it
        return any(_may_match_inside(pattern, relative_dir) for pattern in self._negations)

    def _selects(self, relative: Path) -> bool:
        posix = relative.as_posix()
        if self.ignore_spec.match_file(posix):
            return False
        return self.include_spec is None or self.include_spec.match_file(posix)

    def __iter__(self) -> Iterator[Tuple[Path, Path]]:
        """Yield the absolute and the relative path of each selected file."""
        for dir_path, dir_names, file_names in os.walk(self.root_path):
            relative_dir = Path(dir_path).relative_to(self.root_path)
            entered: List[str] = []
            entries: List[str] = []
            for name in dir_names:
                # a symlinked folder is yielded, never entered, so a link to a parent cannot loop
                if os.path.islink(os.path.join(dir_path, name)):
                    entries.append(name)
                elif self._enters((relative_dir / name).as_posix()):
                    entered.append(name)
            dir_names[:] = sorted(entered)
            for name in sorted(entries + file_names):
                relative = relative_dir / name
                path = self.root_path / relative
                # `exists` is False for a broken symlink
                if path.exists() and self._selects(relative):
                    yield path, relative


def _may_match_inside(pattern: str, relative_dir: str) -> bool:
    """Whether a gitignore pattern may match a path inside `relative_dir`."""
    segments = pattern.strip("/").split("/")
    # a pattern without a slash matches at any depth
    if not pattern.startswith("/") and len(segments) == 1:
        return True
    for depth, name in enumerate(relative_dir.split("/")):
        # the pattern ended on a parent folder, which covers everything below it
        if depth >= len(segments) or segments[depth] == "**":
            return True
        if not fnmatchcase(name, segments[depth]):
            return False
    return True


class WorkspaceFileSelector(GitignoreFileSelector):
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
        self.settings_dir: Path = Path(context.settings_dir).resolve()
        # a deployment ships `.git` when the ignore file does not exclude it, as it always has
        super().__init__(
            context.run_dir,
            additional_excludes=additional_excludes,
            ignore_file=ignore_file,
            ignore_git=False,
        )

    def _base_excludes(self) -> List[str]:
        settings_rel = self.settings_dir.relative_to(self.root_path).as_posix()
        # the settings dir stays out, except the toolkit index: the agent launcher reads it
        # on the runner to resolve `<toolkit>:<agent>` refs
        return [f"{settings_rel}/", *(f"!{settings_rel}/{name}" for name in SETTINGS_INCLUDES)]


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
