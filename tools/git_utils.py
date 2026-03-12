"""Shared git helpers for tools scripts."""

import os
import subprocess
from typing import Optional


def git_show(ref: str, path: str = "pyproject.toml") -> Optional[str]:
    """Read a file at a git ref. Returns None if file doesn't exist."""
    result = subprocess.run(
        ["git", "show", f"{ref}:{path}"],
        capture_output=True,
        text=True,
    )
    return result.stdout if result.returncode == 0 else None


def git_merge_base(ref1: str, ref2: str) -> Optional[str]:
    """Find the merge-base (common ancestor) of two refs."""
    result = subprocess.run(
        ["git", "merge-base", ref1, ref2],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def detect_base_ref(explicit: Optional[str] = None) -> str:
    """Resolve the target branch for comparison.

    Priority: explicit argument > GITHUB_BASE_REF env var > "devel" fallback.
    """
    if explicit:
        return explicit
    return os.environ.get("GITHUB_BASE_REF", "") or "devel"
