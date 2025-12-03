from typing import Iterator, Optional, List
from pathlib import Path
from pathspec import PathSpec
from pathspec.util import iter_tree_files

from dlt._workspace._workspace_context import WorkspaceRunContext


class WorkspaceFileSelector:
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
        self.spec: PathSpec = self._build_pathspec(additional_excludes or [])

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

    def __iter__(self) -> Iterator[Path]:
        """Yield paths of files eligible for deployment"""
        for file_path in iter_tree_files(self.root_path):
            if not self.spec.match_file(file_path):
                yield Path(file_path)
