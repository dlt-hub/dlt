"""Compute content fingerprints for pre-push scope checks."""

from __future__ import annotations

import fnmatch
import hashlib
import os
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PREK_DIR = Path(__file__).resolve().parent
SCOPES_PATH = PREK_DIR / "scopes.toml"


def _git_ls_files(pathspecs: list[str]) -> list[str]:
    if not pathspecs:
        return []
    result = subprocess.run(
        ["git", "ls-files", "--", *pathspecs],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return [line for line in result.stdout.splitlines() if line]


def _matches_globs(path: str, globs: list[str]) -> bool:
    name = os.path.basename(path)
    return any(fnmatch.fnmatch(name, pattern) for pattern in globs)


def resolve_scope_files(scope: dict[str, list[str]]) -> list[str]:
    files: set[str] = set(scope.get("files", []))
    paths = scope.get("paths", [])
    globs = scope.get("globs", [])

    for path_prefix in paths:
        candidates = _git_ls_files([path_prefix])
        if globs:
            files.update(path for path in candidates if _matches_globs(path, globs))
        else:
            files.update(candidates)

    existing = [path for path in files if (ROOT / path).is_file()]
    # LC_ALL=C byte order matches default sort for ASCII repo paths.
    return sorted(existing)


def _file_digest(path: str) -> bytes:
    digest = hashlib.sha256()
    with open(ROOT / path, "rb") as file:
        for chunk in iter(lambda: file.read(65536), b""):
            digest.update(chunk)
    return digest.digest()


def compute_fingerprint(scope_name: str) -> str:
    with open(SCOPES_PATH, "rb") as file:
        scopes = tomllib.load(file)

    try:
        scope = scopes["scopes"][scope_name]
    except KeyError as exc:
        raise SystemExit(f"Unknown scope: {scope_name}") from exc

    aggregate = hashlib.sha256()
    for path in resolve_scope_files(scope):
        aggregate.update(path.encode())
        aggregate.update(b"\0")
        aggregate.update(_file_digest(path))

    return aggregate.hexdigest()


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(f"Usage: {sys.argv[0]} <scope_name>")

    print(compute_fingerprint(sys.argv[1]))


if __name__ == "__main__":
    main()
