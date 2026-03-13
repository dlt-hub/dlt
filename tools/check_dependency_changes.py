#!/usr/bin/env python
# ruff: noqa: T201
# flake8: noqa: T201
"""Check pyproject.toml for dependency changes a PR would introduce.

Usage:
    python tools/check_dependency_changes.py <base_ref> [<head_ref>]

Simulates merging head_ref (default: HEAD) into base_ref — the same way GitHub
computes a PR diff — and reports dependency changes the merge would introduce.

Uses three-way merge via git merge-file:
  ancestor = merge-base(base_ref, head_ref)
  ours     = base_ref (the target branch)
  theirs   = head_ref (the PR branch)

If the PR does not modify pyproject.toml at all (ancestor == head), exits early.
If pyproject.toml has merge conflicts, warns and falls back to showing the PR's
own changes (ancestor vs head) since the merged result cannot be parsed.

Reports changes in three categories:
  - Main dependencies [project.dependencies] — WARNING level
  - Optional dependencies [project.optional-dependencies] — WARNING level
  - Dev/group dependencies [dependency-groups] — informational

Exit codes:
    0 - no dependency changes
    1 - dependency changes detected
    2 - usage error or merge conflicts in pyproject.toml
"""

import os
import subprocess
import sys
import tempfile
from typing import Any, Dict, List, NamedTuple, Optional, Sequence, Tuple

import tomlkit
from packaging.requirements import Requirement

from tools.git_utils import detect_base_ref, git_merge_base, git_show

# ANSI escape codes
YELLOW = "\033[33m"
RED = "\033[31m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"


class DepChange(NamedTuple):
    name: str
    kind: str  # "added", "removed", "changed"
    old: List[str]
    new: List[str]


def normalize_name(name: str) -> str:
    """Normalize package name per PEP 503."""
    return name.lower().replace("-", "_").replace(".", "_")


def three_way_merge(ours: str, ancestor: str, theirs: str) -> Tuple[str, bool]:
    """Three-way merge using git merge-file -p.

    Returns (merged_content, has_conflicts).
    """
    paths: List[str] = []
    try:
        for content in (ours, ancestor, theirs):
            fd, path = tempfile.mkstemp(suffix=".toml")
            with os.fdopen(fd, "w") as f:
                f.write(content)
            paths.append(path)

        result = subprocess.run(
            ["git", "merge-file", "-p", paths[0], paths[1], paths[2]],
            capture_output=True,
            text=True,
        )
        return result.stdout, result.returncode > 0
    finally:
        for p in paths:
            os.unlink(p)


def parse_dep_list(deps: Sequence[Any]) -> Dict[str, List[str]]:
    """Parse dependency strings into {normalized_name: [canonical_specs]}.

    Skips non-string entries (e.g. PEP 735 include-group directives).
    """
    result: Dict[str, List[str]] = {}
    for raw in deps:
        if not isinstance(raw, str):
            continue
        s = str(raw).strip()
        if not s:
            continue
        try:
            req = Requirement(s)
        except Exception:
            continue
        key = normalize_name(req.name)
        result.setdefault(key, []).append(str(req))
    for key in result:
        result[key].sort()
    return result


def diff_deps(old: Dict[str, List[str]], new: Dict[str, List[str]]) -> List[DepChange]:
    """Return list of changes between old and new dependency dicts."""
    changes: List[DepChange] = []
    for name in sorted(set(old) | set(new)):
        old_specs = old.get(name, [])
        new_specs = new.get(name, [])
        if not old_specs:
            changes.append(DepChange(name, "added", [], new_specs))
        elif not new_specs:
            changes.append(DepChange(name, "removed", old_specs, []))
        elif old_specs != new_specs:
            changes.append(DepChange(name, "changed", old_specs, new_specs))
    return changes


def format_change_lines(change: DepChange, indent: str = "  ") -> List[str]:
    """Format a single DepChange as diff-style lines."""
    lines: List[str] = []
    if change.kind == "added":
        for s in change.new:
            lines.append(f"{indent}+ {s}")
    elif change.kind == "removed":
        for s in change.old:
            lines.append(f"{indent}- {s}")
    else:
        for s in change.old:
            lines.append(f"{indent}- {s}")
        for s in change.new:
            lines.append(f"{indent}+ {s}")
    return lines


def report_changes(compare_old: Dict[str, Any], compare_new: Dict[str, Any]) -> bool:
    """Compare two parsed pyproject.toml docs and print dependency changes.

    Returns True if any changes were found.
    """
    found = False

    # --- Main dependencies ---
    old_main = parse_dep_list(list(compare_old.get("project", {}).get("dependencies", [])))
    new_main = parse_dep_list(list(compare_new.get("project", {}).get("dependencies", [])))
    main_changes = diff_deps(old_main, new_main)

    if main_changes:
        found = True
        print(f"{YELLOW}{BOLD}⚠️  WARNING: Main dependency changes detected{RESET}")
        for c in main_changes:
            for line in format_change_lines(c):
                print(f"{YELLOW}{line}{RESET}")
        print(f"{DIM}Main dependencies affect every user of the package.{RESET}")
        print()

    # --- Optional dependencies ---
    old_opt = compare_old.get("project", {}).get("optional-dependencies", {})
    new_opt = compare_new.get("project", {}).get("optional-dependencies", {})
    all_extras = sorted(set(old_opt) | set(new_opt))
    opt_changes: Dict[str, List[DepChange]] = {}
    for extra in all_extras:
        changes = diff_deps(
            parse_dep_list(list(old_opt.get(extra, []))),
            parse_dep_list(list(new_opt.get(extra, []))),
        )
        if changes:
            opt_changes[extra] = changes

    if opt_changes:
        found = True
        print(f"{YELLOW}{BOLD}⚠️  WARNING: Optional dependency changes detected{RESET}")
        for extra, changes in opt_changes.items():
            print(f"{YELLOW}  [{extra}]:{RESET}")
            for c in changes:
                for line in format_change_lines(c, indent="    "):
                    print(f"{YELLOW}{line}{RESET}")
        print(f"{DIM}Optional dependencies affect users who install with these extras.{RESET}")
        print()

    # --- Dev/group dependencies ([dependency-groups]) ---
    old_groups = dict(compare_old.get("dependency-groups", {}))
    new_groups = dict(compare_new.get("dependency-groups", {}))
    all_groups = sorted(set(old_groups) | set(new_groups))
    grp_changes: Dict[str, List[DepChange]] = {}
    for group in all_groups:
        changes = diff_deps(
            parse_dep_list(list(old_groups.get(group, []))),
            parse_dep_list(list(new_groups.get(group, []))),
        )
        if changes:
            grp_changes[group] = changes

    if grp_changes:
        found = True
        print(f"{BOLD}Dev/group dependency changes{RESET}")
        for group, changes in grp_changes.items():
            print(f"  [{group}]:")
            for c in changes:
                for line in format_change_lines(c, indent="    "):
                    print(line)
        print()

    return found


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        return 2

    base_ref = detect_base_ref(sys.argv[1] if len(sys.argv) > 1 else None)
    head_ref = sys.argv[2] if len(sys.argv) > 2 else "HEAD"

    # Warn if pyproject.toml has uncommitted changes
    unstaged = subprocess.run(
        ["git", "diff", "--name-only", "pyproject.toml"],
        capture_output=True,
        text=True,
    ).stdout.strip()
    staged = subprocess.run(
        ["git", "diff", "--cached", "--name-only", "pyproject.toml"],
        capture_output=True,
        text=True,
    ).stdout.strip()

    if unstaged or staged:
        print(f"{YELLOW}{BOLD}⚠️  WARNING: pyproject.toml has uncommitted changes{RESET}")
        if unstaged:
            print(f"{YELLOW}  Unstaged changes detected (use 'git add' to stage){RESET}")
        if staged:
            print(f"{YELLOW}  Staged changes detected (use 'git commit' to commit){RESET}")
        print(f"{DIM}This script only analyzes committed changes (HEAD vs {base_ref}).{RESET}")
        print(f"{DIM}Commit your changes to include them in the analysis.{RESET}")
        print()

    # Find the common ancestor
    merge_base = git_merge_base(base_ref, head_ref)
    if merge_base is None:
        print(f"Error: no merge-base between {base_ref} and {head_ref}")
        return 2

    # Read pyproject.toml at all three points
    ancestor_text = git_show(merge_base) or ""
    base_text = git_show(base_ref) or ""
    head_text = git_show(head_ref) or ""

    # If the PR branch didn't touch pyproject.toml at all, nothing to report
    if ancestor_text == head_text:
        print("No dependency changes in pyproject.toml (PR does not modify this file).")
        return 0

    # Three-way merge: simulate what merging the PR into base would produce
    merged_text, has_conflicts = three_way_merge(base_text, ancestor_text, head_text)

    if has_conflicts:
        print(f"{RED}{BOLD}⚠️  CONFLICT: pyproject.toml has merge conflicts with {base_ref}{RESET}")
        print(f"{RED}Dependency analysis below shows PR intent (merge-base vs PR head),{RESET}")
        print(f"{RED}but the file must be resolved before merging.{RESET}")
        print()
        # Fall back: show what the PR changed vs common ancestor
        compare_old = tomlkit.parse(ancestor_text)
        compare_new = tomlkit.parse(head_text)
    else:
        # Clean merge: compare merged result vs base (same as GitHub's PR diff)
        compare_old = tomlkit.parse(base_text)
        compare_new = tomlkit.parse(merged_text)

    found = report_changes(compare_old, compare_new)

    if not found:
        print("No dependency changes in pyproject.toml.")

    if has_conflicts:
        return 2
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
