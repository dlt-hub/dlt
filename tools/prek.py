"""Pre-push gate: run lint/tests when scope fingerprints change."""

# ruff: noqa: T201
# flake8: noqa: T201

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import os
import subprocess
import sys
import tomli as tomllib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, NamedTuple, TypedDict, cast

import pendulum
from pendulum.datetime import DateTime

Mode = Literal["off", "auto", "confirm"]
VALID_MODES = frozenset({"off", "auto", "confirm"})
MAX_PASSED_FINGERPRINTS = 50


class PassRecord(TypedDict):
    fingerprint: str
    passed_at: str
    command: str


class CheckState(TypedDict):
    passes: list[PassRecord]


State = dict[str, CheckState]


class Check(NamedTuple):
    name: str
    make_target: str
    make_command: str


CHECKS: tuple[Check, ...] = (
    Check("lint", "fl", "make fl"),
    Check("test_common_p", "test-common-p", "make test-common-p"),
)
CHECK_NAMES = frozenset(check.name for check in CHECKS)


class ScopeDef(NamedTuple):
    files: tuple[str, ...]
    paths: tuple[str, ...]
    globs: tuple[str, ...]


class PlannedCheck(NamedTuple):
    check: Check
    mode: Mode
    fingerprint: str
    cached_fingerprint: str
    stale: bool


class GateOutcome(NamedTuple):
    exit_code: int
    new_state: State
    stderr_lines: tuple[str, ...]


class ConfigError(Exception):
    """Invalid prek local configuration."""


class UnknownScopeError(Exception):
    """Scope name is missing from [tool.prek.scopes] in pyproject.toml."""


def repo_root() -> Path:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return Path(result.stdout.strip())
    return Path.cwd()


def default_prek_dir() -> Path:
    return repo_root() / ".prek"


def parse_bool(value: Any, *, key: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ConfigError(f"Invalid boolean {value!r} for {key}")


def parse_only_when_pr_open(local_config: dict[str, Any]) -> bool:
    gate = local_config.get("gate", {})
    if not isinstance(gate, dict):
        raise ConfigError("Invalid [gate] section")
    return parse_bool(gate.get("only_when_pr_open", False), key="gate.only_when_pr_open")


def parse_mode(local_config: dict[str, Any], check_name: str) -> Mode:
    section = local_config.get(check_name, {})
    if not isinstance(section, dict):
        raise ConfigError(f"Invalid [{check_name}] section")
    mode = section.get("mode", "off")
    if mode not in VALID_MODES:
        raise ConfigError(f"Invalid mode {mode!r} for check {check_name!r}")
    return cast(Mode, mode)


def make_command_for(check_name: str) -> str:
    for check in CHECKS:
        if check.name == check_name:
            return check.make_command
    valid = ", ".join(sorted(CHECK_NAMES))
    raise ConfigError(f"Unknown check {check_name!r}; expected one of: {valid}")


def load_toml(path: Path) -> dict[str, Any]:
    with open(path, "rb") as file:
        return tomllib.load(file)


def load_local_config(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    return load_toml(path)


def load_state(path: Path) -> State:
    if not path.is_file():
        return {}
    data = load_toml(path)
    return {
        name: normalize_check_state(section)
        for name, section in data.items()
        if isinstance(section, dict)
    }


def normalize_pass_record(raw: dict[str, Any]) -> PassRecord | None:
    fingerprint = raw.get("fingerprint")
    if not isinstance(fingerprint, str) or not fingerprint:
        return None
    passed_at = raw.get("passed_at")
    command = raw.get("command")
    return PassRecord(
        fingerprint=fingerprint,
        passed_at=passed_at if isinstance(passed_at, str) else "",
        command=command if isinstance(command, str) else "",
    )


def normalize_check_state(section: dict[str, Any]) -> CheckState:
    passes: list[PassRecord] = []
    raw_passes = section.get("passes")
    if isinstance(raw_passes, list):
        for item in raw_passes:
            if isinstance(item, dict):
                record = normalize_pass_record(item)
                if record is not None:
                    passes.append(record)
    return CheckState(passes=passes[:MAX_PASSED_FINGERPRINTS])


def passed_fingerprints(check_state: CheckState | None) -> list[str]:
    if not check_state:
        return []
    return [record["fingerprint"] for record in check_state["passes"]]


def fingerprint_is_known(check_state: CheckState | None, fingerprint: str) -> bool:
    return fingerprint in passed_fingerprints(check_state)


def write_state(path: Path, state: State) -> None:
    lines: list[str] = []
    for check_name, data in state.items():
        for record in data["passes"]:
            lines.append(f"[[{check_name}.passes]]")
            lines.append(f'fingerprint = "{record["fingerprint"]}"')
            lines.append(f'passed_at = "{record["passed_at"]}"')
            lines.append(f'command = "{record["command"]}"')
            lines.append("")
    path.write_text("\n".join(lines).rstrip() + ("\n" if lines else ""), encoding="utf-8")


def scope_from_dict(scope: dict[str, list[str]]) -> ScopeDef:
    return ScopeDef(
        files=tuple(scope.get("files", [])),
        paths=tuple(scope.get("paths", [])),
        globs=tuple(scope.get("globs", [])),
    )


def matches_globs(path: str, globs: list[str]) -> bool:
    name = os.path.basename(path)
    return any(fnmatch.fnmatch(name, pattern) for pattern in globs)


def resolve_scope_files(
    scope: ScopeDef,
    *,
    list_tracked: Callable[[list[str]], list[str]],
    root: Path,
) -> list[str]:
    files: set[str] = set(scope.files)
    for path_prefix in scope.paths:
        candidates = list_tracked([path_prefix])
        if scope.globs:
            files.update(path for path in candidates if matches_globs(path, list(scope.globs)))
        else:
            files.update(candidates)
    return sorted(path for path in files if (root / path).is_file())


def fingerprint_files(paths: list[str], read_bytes: Callable[[str], bytes]) -> str:
    aggregate = hashlib.sha256()
    for path in paths:
        aggregate.update(path.encode())
        aggregate.update(b"\0")
        aggregate.update(read_bytes(path))
    return aggregate.hexdigest()


def load_scopes(config_path: Path) -> dict[str, ScopeDef]:
    raw = load_toml(config_path)
    tool = raw.get("tool", {})
    prek = tool.get("prek", {}) if isinstance(tool, dict) else {}
    scopes = prek.get("scopes", {})
    if not isinstance(scopes, dict):
        raise ValueError("Invalid [tool.prek.scopes] section in pyproject.toml")
    return {
        name: scope_from_dict(section)
        for name, section in scopes.items()
        if isinstance(section, dict)
    }


def git_ls_files(root: Path, pathspecs: list[str]) -> list[str]:
    if not pathspecs:
        return []
    result = subprocess.run(
        ["git", "ls-files", "--", *pathspecs],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return [line for line in result.stdout.splitlines() if line]


def make_fingerprint_fn(root: Path, config_path: Path) -> Callable[[str], str]:
    scopes = load_scopes(config_path)

    def fingerprint(scope_name: str) -> str:
        try:
            scope = scopes[scope_name]
        except KeyError as exc:
            raise UnknownScopeError(f"Unknown scope: {scope_name}") from exc
        paths = resolve_scope_files(
            scope,
            list_tracked=lambda pathspecs: git_ls_files(root, pathspecs),
            root=root,
        )
        return fingerprint_files(paths, lambda path: (root / path).read_bytes())

    return fingerprint


def gate_active(*, only_when_pr_open: bool, has_open_pr: bool) -> tuple[bool, str]:
    if not only_when_pr_open:
        return True, "only_when_pr_open=false"
    if has_open_pr:
        return True, "open PR on current branch"
    return False, "only_when_pr_open=true and no open PR for current branch"


def plan_checks(
    checks: Sequence[Check],
    local_config: dict[str, Any],
    state: State,
    fingerprint: Callable[[str], str],
) -> list[PlannedCheck]:
    planned: list[PlannedCheck] = []
    for check in checks:
        mode = parse_mode(local_config, check.name)
        if mode == "off":
            continue
        current = fingerprint(check.name)
        history = passed_fingerprints(state.get(check.name))
        cached = history[0] if history else ""
        planned.append(
            PlannedCheck(
                check,
                mode,
                current,
                cached,
                not fingerprint_is_known(state.get(check.name), current),
            )
        )
    return planned


def with_passed_check(
    state: State,
    check_name: str,
    *,
    fingerprint: str,
    command: str,
    passed_at: DateTime,
) -> State:
    updated: State = {name: CheckState(passes=list(data["passes"])) for name, data in state.items()}
    passes = list(updated.get(check_name, CheckState(passes=[]))["passes"])
    passes = [record for record in passes if record["fingerprint"] != fingerprint]
    passes.insert(
        0,
        PassRecord(
            fingerprint=fingerprint,
            passed_at=passed_at.replace(microsecond=0).isoformat(),
            command=command,
        ),
    )
    updated[check_name] = CheckState(passes=passes[:MAX_PASSED_FINGERPRINTS])
    return updated


def dry_run_no_config_line() -> str:
    return "prek gate (dry-run): no .prek/local.toml — hook would no-op on push"


def dry_run_lines(
    planned: list[PlannedCheck],
    *,
    local_config_path: Path,
    active: bool,
    reason: str,
    is_tty: bool,
) -> list[str]:
    lines = [
        f"prek gate (dry-run): {local_config_path}",
        f"gate active: {active} ({reason})",
    ]
    if not active:
        return lines
    if not planned:
        lines.append("no checks enabled (all off or empty config)")
        return lines

    for item in planned:
        check_name, make_command = item.check.name, item.check.make_command
        if not item.stale:
            lines.append(f"[{check_name}] mode={item.mode} up to date ({item.fingerprint[:12]}…)")
            continue
        action = f"would run {make_command}"
        if item.mode == "confirm":
            action = (
                f"would prompt, then run {make_command}"
                if is_tty
                else f"would block push ({make_command}, non-interactive)"
            )
        cached_label = item.cached_fingerprint[:12] if item.cached_fingerprint else "none"
        lines.append(
            f"[{check_name}] mode={item.mode} stale ({item.fingerprint[:12]}…, "
            f"was {cached_label}…) → {action}"
        )
    return lines


@dataclass(frozen=True)
class GateDeps:
    run_make: Callable[[str], int]
    has_open_pr: Callable[[], bool]
    confirm: Callable[[str], bool]
    fingerprint: Callable[[str], str]
    now: Callable[[], DateTime]
    is_tty: Callable[[], bool]

    @classmethod
    def from_repo(cls, root: Path) -> GateDeps:
        prek_dir = root / ".prek"
        return cls(
            run_make=lambda target: _run_make_target(root, target),
            has_open_pr=lambda: _has_open_pr(root),
            confirm=_confirm_run,
            fingerprint=make_fingerprint_fn(root, root / "pyproject.toml"),
            now=lambda: pendulum.now("UTC"),
            is_tty=sys.stdin.isatty,
        )


def _run_make_target(root: Path, target: str) -> int:
    print(f"Running make {target}...", flush=True)
    return subprocess.run(["make", target], cwd=root).returncode


def _has_open_pr(root: Path) -> bool:
    result = subprocess.run(
        ["gh", "pr", "view", "--json", "state", "-q", ".state"],
        cwd=root,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return False
    return result.stdout.strip().upper() == "OPEN"


def _confirm_run(make_command: str) -> bool:
    if not sys.stdin.isatty():
        return False
    reply = input(f"Run {make_command} before push? [Y/n] ").strip().lower()
    return not reply or reply in {"y", "yes"}


def run_gate(
    *,
    local_config: dict[str, Any],
    state: State,
    deps: GateDeps,
    checks: Sequence[Check] = CHECKS,
) -> GateOutcome:
    active, reason = gate_active(
        only_when_pr_open=parse_only_when_pr_open(local_config),
        has_open_pr=deps.has_open_pr(),
    )
    if not active:
        return GateOutcome(0, state, (f"prek gate: skipped ({reason})",))

    new_state: State = {
        name: CheckState(passes=list(data["passes"])) for name, data in state.items()
    }
    for check in checks:
        mode = parse_mode(local_config, check.name)
        if mode == "off":
            continue

        fingerprint = deps.fingerprint(check.name)
        if fingerprint_is_known(new_state.get(check.name), fingerprint):
            continue

        if mode == "confirm" and not deps.confirm(check.make_command):
            return GateOutcome(1, state, (f"Declined {check.make_command}. Push aborted.",))

        if deps.run_make(check.make_target) != 0:
            return GateOutcome(1, state, (f"{check.make_command} failed. Push aborted.",))

        new_state = with_passed_check(
            new_state,
            check.name,
            fingerprint=fingerprint,
            command=check.make_command,
            passed_at=deps.now(),
        )

    return GateOutcome(0, new_state, ())


def record_passed_check(
    *,
    check_name: str,
    state: State,
    hooks_enabled: bool,
    deps: GateDeps,
) -> tuple[State, str | None]:
    try:
        command = make_command_for(check_name)
    except ConfigError as exc:
        return state, str(exc)

    if not hooks_enabled:
        return state, None

    return (
        with_passed_check(
            state,
            check_name,
            fingerprint=deps.fingerprint(check_name),
            command=command,
            passed_at=deps.now(),
        ),
        None,
    )


def main(*, prek_dir: Path | None = None, argv: list[str] | None = None) -> int:
    prek_dir = prek_dir or default_prek_dir()
    parser = argparse.ArgumentParser(description="Pre-push gate for lint and common tests")
    parser.add_argument(
        "--dry-run", action="store_true", help="Show planned checks without running them"
    )
    parser.add_argument(
        "--record",
        metavar="CHECK",
        help="Record a successful check (lint, test_common_p)",
    )
    args = parser.parse_args(argv)

    root = prek_dir.parent
    deps = GateDeps.from_repo(root)
    local_path = prek_dir / "local.toml"
    state_path = prek_dir / ".state.toml"
    enabled_path = prek_dir / ".enabled"

    try:
        if args.record:
            return _run_record(args.record, deps, state_path, enabled_path)
        if args.dry_run:
            return _run_dry_run(deps, local_path, state_path)
        return _run_gate(deps, local_path, state_path)
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 1


def main_fingerprint(*, prek_dir: Path | None = None, argv: list[str] | None = None) -> int:
    prek_dir = prek_dir or default_prek_dir()
    if argv is None:
        argv = sys.argv[1:]
    if len(argv) != 1:
        print("Usage: python -m tools.prek fingerprint <scope_name>", file=sys.stderr)
        return 1

    root = prek_dir.parent
    try:
        print(make_fingerprint_fn(root, root / "pyproject.toml")(argv[0]))
    except UnknownScopeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


def _run_record(check_name: str, deps: GateDeps, state_path: Path, enabled_path: Path) -> int:
    state = load_state(state_path)
    new_state, error = record_passed_check(
        check_name=check_name,
        state=state,
        hooks_enabled=enabled_path.is_file(),
        deps=deps,
    )
    if error:
        print(error, file=sys.stderr)
        return 1
    if new_state != state:
        write_state(state_path, new_state)
    return 0


def _run_dry_run(deps: GateDeps, local_config_path: Path, state_path: Path) -> int:
    local_config = load_local_config(local_config_path)
    if local_config is None:
        print(dry_run_no_config_line())
        return 0

    active, reason = gate_active(
        only_when_pr_open=parse_only_when_pr_open(local_config),
        has_open_pr=deps.has_open_pr(),
    )
    planned = plan_checks(CHECKS, local_config, load_state(state_path), deps.fingerprint)
    for line in dry_run_lines(
        planned,
        local_config_path=local_config_path,
        active=active,
        reason=reason,
        is_tty=deps.is_tty(),
    ):
        print(line)
    return 0


def _run_gate(deps: GateDeps, local_config_path: Path, state_path: Path) -> int:
    local_config = load_local_config(local_config_path)
    if local_config is None:
        return 0

    state = load_state(state_path)
    outcome = run_gate(local_config=local_config, state=state, deps=deps)
    for line in outcome.stderr_lines:
        print(line, file=sys.stderr)
    if outcome.new_state != state:
        write_state(state_path, outcome.new_state)
    return outcome.exit_code


if __name__ == "__main__":
    cli_argv = sys.argv[1:]
    if cli_argv and cli_argv[0] == "fingerprint":
        raise SystemExit(main_fingerprint(argv=cli_argv[1:]))
    raise SystemExit(main(argv=cli_argv))
