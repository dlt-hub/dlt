from typing import Iterator, Protocol, Optional, List
from pathlib import Path
from pathspec import PathSpec
from pathspec.util import iter_tree_files

from dlt.common.runtime.run_context import DOT_DLT


class FileSelector(Protocol):
    """Protocol for iterating over files eligible for deployment"""

    def iter_files(self) -> Iterator[Path]:
        """Yield relative paths of files eligible for deployment"""
        ...


class GitignoreFileSelector:
    """File selector that respects .gitignore and excludes workspace internals"""

    def __init__(
        self, workspace_path: str, additional_excludes: Optional[List[str]] = None
    ) -> None:
        self.root_path: Path = Path(workspace_path)
        self.spec: PathSpec = self._build_pathspec(additional_excludes or [])

    def _build_pathspec(self, additional_excludes: List[str]) -> PathSpec:
        """Build PathSpec from .gitignore + defaults + additional excludes"""
        patterns: List[str] = [f"{DOT_DLT}/"]

        # Load .gitignore if exists
        gitignore_path = self.root_path / ".gitignore"
        if gitignore_path.exists():
            with gitignore_path.open("r", encoding="utf-8") as f:
                patterns.extend(f.read().splitlines())

        # Add caller-provided excludes
        patterns.extend(additional_excludes)

        return PathSpec.from_lines("gitwildmatch", patterns)

    def iter_files(self) -> Iterator[Path]:
        """Yield paths of files eligible for deployment"""
        for file_path in iter_tree_files(self.root_path):
            if not self.spec.match_file(file_path):
                yield Path(file_path)
