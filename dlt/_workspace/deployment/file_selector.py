from typing import Iterable, Iterator, Optional, List, Tuple
from pathlib import Path
from pathspec import PathSpec
from pathspec.util import iter_tree_files

from dlt._workspace._workspace_context import WorkspaceRunContext


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
        self.ignore_spec: PathSpec = self._build_pathspec(additional_excludes or [])

    def _build_pathspec(self, additional_excludes: List[str]) -> PathSpec:
        """Build PathSpec from ignore file + defaults + additional excludes"""
        patterns: List[str] = [f"{self.settings_dir.relative_to(self.root_path)}/"]

        # Load ignore file if exists
        ignore_path = self.root_path / self.ignore_file
        if ignore_path.exists():
            with ignore_path.open("r", encoding="utf-8") as f:
                patterns.extend(f.read().splitlines())

        # Add caller-provided excludes
        patterns.extend(additional_excludes)

        return PathSpec.from_lines("gitwildmatch", patterns)

    def __iter__(self) -> Iterator[Tuple[Path, Path]]:
        """Yield paths of files eligible for deployment"""
        root_path = Path(self.root_path)
        for file_path in iter_tree_files(self.root_path):
            if not self.ignore_spec.match_file(file_path):
                yield root_path / file_path, Path(file_path)


class ConfigurationFileSelector(BaseFileSelector):
    """Iterates config and secrets files in workspace"""

    def __init__(
        self,
        context: WorkspaceRunContext,
    ) -> None:
        self.settings_dir: Path = Path(context.settings_dir).resolve()

    def __iter__(self) -> Iterator[Tuple[Path, Path]]:
        """Yield paths of config and secrets paths"""
        for file_path in iter_tree_files(self.settings_dir):
            if file_path.endswith("config.toml") or file_path.endswith("secrets.toml"):
                yield self.settings_dir / file_path, Path(file_path)
