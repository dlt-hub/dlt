#!/usr/bin/env python
# ruff: noqa: T201
# flake8: noqa: T201
"""Check for breaking changes in dlt's public API using griffe.

Usage:
    python tools/check_api_breaking.py list
    python tools/check_api_breaking.py check [-v] [<against_ref>]

Commands:
    list    Show the public API names and their source modules
    check   Run griffe against a git ref, filtered to public API modules only

Arguments:
    against_ref: Git ref to compare against (default: GITHUB_BASE_REF or devel)

Exit codes:
    0 - no breaking changes detected (or list command)
    1 - breaking changes detected
    2 - usage error
"""

import subprocess
import sys
import types
from pathlib import Path
from typing import Dict, List, Set, Tuple

from tools.git_utils import detect_base_ref

# top-level packages whose __all__ (or public attrs) define public API
PUBLIC_API_ROOTS = ["dlt", "dlt.sources", "dlt.helpers"]


def _resolve_source_file(mod_name: str, repo_root: Path) -> str:
    """Convert a dotted module name to a relative file path."""
    parts = mod_name.split(".")
    mod_path = repo_root / Path(*parts)
    if mod_path.is_dir():
        return str((mod_path / "__init__.py").relative_to(repo_root))
    return str(mod_path.with_suffix(".py").relative_to(repo_root))


def public_api() -> Tuple[Dict[str, Dict[str, str]], Set[str]]:
    """Import public API roots and inspect exports.

    Returns (root_to_names, source_files) where root_to_names maps each root
    package to {name: source_module}, and source_files is the set of relative
    file paths containing public API symbols.
    """
    import dlt

    repo_root = Path(dlt.__file__).resolve().parent.parent
    root_to_names: Dict[str, Dict[str, str]] = {}
    source_files: Set[str] = set()

    for root in PUBLIC_API_ROOTS:
        try:
            mod = __import__(root, fromlist=["__all__"])
        except Exception:
            root_to_names[root] = {"<import error>": root}
            continue

        all_names = getattr(mod, "__all__", None)
        if all_names is None:
            all_names = [n for n in dir(mod) if not n.startswith("_")]

        name_to_module: Dict[str, str] = {}
        for name in all_names:
            try:
                obj = getattr(mod, name)
            except Exception:
                name_to_module[name] = "<import error>"
                continue

            if isinstance(obj, types.ModuleType):
                obj_mod = obj.__name__
            elif hasattr(obj, "__module__"):
                obj_mod = obj.__module__
            else:
                obj_mod = type(obj).__module__

            name_to_module[name] = obj_mod

            if obj_mod and obj_mod.startswith("dlt"):
                source_files.add(_resolve_source_file(obj_mod, repo_root))

        root_to_names[root] = name_to_module
        # always include the root's own __init__.py
        source_files.add(_resolve_source_file(root, repo_root))

    return root_to_names, source_files


def cmd_list() -> int:
    """List public API names and their source modules."""
    root_to_names, source_files = public_api()
    for root, name_to_module in root_to_names.items():
        has_all = hasattr(__import__(root, fromlist=["__all__"]), "__all__")
        marker = "__all__" if has_all else "dir()"
        print(f"{root} ({len(name_to_module)} names from {marker}):")
        for name in sorted(name_to_module):
            print(f"  {root}.{name:30s} <- {name_to_module[name]}")
        print()
    print(f"Source files ({len(source_files)}):")
    for f in sorted(source_files):
        print(f"  {f}")
    return 0


def classify_griffe_output(output: str, source_files: Set[str]) -> Tuple[List[str], List[str]]:
    """Classify griffe output lines into public API hits and pruned lines.

    Args:
        output (str): Raw griffe stdout+stderr combined.
        source_files (Set[str]): Relative file paths of public API modules.

    Returns:
        Tuple[List[str], List[str]]: (kept, pruned) lists of issue lines.
    """
    kept: List[str] = []
    pruned: List[str] = []

    for line in output.splitlines():
        line = line.strip()
        if not line or line.startswith("warning:"):
            continue
        # griffe output format: "path/to/file.py:LINE: message"
        if any(line.startswith(prefix) for prefix in source_files):
            kept.append(line)
        else:
            pruned.append(line)

    return kept, pruned


def cmd_check(against: str, verbose: bool = False) -> int:
    """Run griffe check filtered to public API modules."""
    _, source_files = public_api()

    print(f"Checking {len(source_files)} public API source files against {against}:")
    if verbose:
        for f in sorted(source_files):
            print(f"  {f}")
        print()

    result = subprocess.run(
        ["griffe", "check", "dlt", "--against", against],
        capture_output=True,
        text=True,
    )

    kept, pruned = classify_griffe_output(result.stdout + result.stderr, source_files)

    if pruned:
        print(f"Pruned {len(pruned)} issue(s) in non-public modules:")
        for line in pruned:
            print(f"  [pruned] {line}")
        print()

    if not kept:
        print("No breaking changes detected in public API.")
        return 0

    print(f"Breaking changes in public API ({len(kept)}):")
    for line in kept:
        print(f"  {line}")
    return 1


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        return 2

    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    verbose = "-v" in sys.argv or "--verbose" in sys.argv

    command = args[0] if args else ""
    if command == "list":
        return cmd_list()
    elif command == "check":
        against = detect_base_ref(args[1] if len(args) > 1 else None)
        return cmd_check(against, verbose=verbose)
    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        return 2


if __name__ == "__main__":
    sys.exit(main())
