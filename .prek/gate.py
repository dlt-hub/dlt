"""Pre-push gate: run lint/tests when scope fingerprints change."""

from __future__ import annotations

import argparse
import subprocess
import sys
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

PREK_DIR = Path(__file__).resolve().parent
ROOT = PREK_DIR.parent
sys.path.insert(0, str(PREK_DIR))

from fingerprint import compute_fingerprint  # noqa: E402

LOCAL_CONFIG_PATH = PREK_DIR / "local.toml"
STATE_PATH = PREK_DIR / ".state.toml"

TMode = Literal["off", "auto", "confirm"]
TCheck = tuple[str, str, str]

CHECKS: list[TCheck] = [
    ("lint", "lint", "make lint"),
    ("test_common_p", "test-common-p", "make test-common-p"),
]
VALID_MODES = {"off", "auto", "confirm"}


def _load_local_config() -> dict[str, Any]:
    if not LOCAL_CONFIG_PATH.is_file():
        return {}
    with open(LOCAL_CONFIG_PATH, "rb") as file:
        return tomllib.load(file)


def _load_state() -> dict[str, dict[str, str]]:
    if not STATE_PATH.is_file():
        return {}
    with open(STATE_PATH, "rb") as file:
        return tomllib.load(file)


def _write_state(state: dict[str, dict[str, str]]) -> None:
    lines: list[str] = []
    for check_name, data in state.items():
        lines.append(f"[{check_name}]")
        for key, value in data.items():
            lines.append(f'{key} = "{value}"')
        lines.append("")
    STATE_PATH.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _as_bool(value: Any, *, key: str) -> bool:
    if isinstance(value, bool):
        return value
    raise SystemExit(f"Invalid boolean {value!r} for {key} in {LOCAL_CONFIG_PATH}")


def _only_when_pr_open(local_config: dict[str, Any]) -> bool:
    gate = local_config.get("gate", {})
    if not isinstance(gate, dict):
        raise SystemExit(f"Invalid [gate] section in {LOCAL_CONFIG_PATH}")
    value = gate.get("only_when_pr_open", False)
    return _as_bool(value, key="gate.only_when_pr_open")


def _has_open_pr() -> bool:
    result = subprocess.run(
        ["gh", "pr", "view", "--json", "state", "-q", ".state"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return False
    return result.stdout.strip().upper() == "OPEN"


def _gate_active(local_config: dict[str, Any]) -> tuple[bool, str]:
    if not _only_when_pr_open(local_config):
        return True, "only_when_pr_open=false"
    if _has_open_pr():
        return True, "open PR on current branch"
    return False, "only_when_pr_open=true and no open PR for current branch"


def _get_mode(local_config: dict[str, Any], check_name: str) -> TMode:
    section = local_config.get(check_name, {})
    if not isinstance(section, dict):
        raise SystemExit(f"Invalid [{check_name}] section in {LOCAL_CONFIG_PATH}")
    mode = section.get("mode", "off")
    if mode not in VALID_MODES:
        raise SystemExit(f"Invalid mode {mode!r} for check {check_name!r} in {LOCAL_CONFIG_PATH}")
    return mode  # type: ignore[return-value]


def _confirm_run(make_command: str) -> bool:
    if not sys.stdin.isatty():
        return False
    reply = input(f"Run {make_command} before push? [Y/n] ").strip().lower()
    if not reply:
        return True
    return reply in {"y", "yes"}


def _run_make(target: str) -> int:
    print(f"Running make {target}...", flush=True)
    return subprocess.run(["make", target], cwd=ROOT).returncode


def _plan_checks(
    local_config: dict[str, Any], state: dict[str, dict[str, str]]
) -> list[tuple[TCheck, TMode, str, str, bool]]:
    """Return (check, mode, fingerprint, cached_fingerprint, is_stale) per configured check."""
    planned: list[tuple[TCheck, TMode, str, str, bool]] = []
    for check in CHECKS:
        check_name = check[0]
        mode = _get_mode(local_config, check_name)
        if mode == "off":
            continue
        fingerprint = compute_fingerprint(check_name)
        cached = state.get(check_name, {}).get("fingerprint", "")
        planned.append((check, mode, fingerprint, cached, fingerprint != cached))
    return planned


def _dry_run() -> int:
    if not LOCAL_CONFIG_PATH.is_file():
        print("prek gate (dry-run): no .prek/local.toml — hook would no-op on push")
        return 0

    local_config = _load_local_config()
    active, reason = _gate_active(local_config)
    print(f"prek gate (dry-run): {LOCAL_CONFIG_PATH}")
    print(f"gate active: {active} ({reason})")
    if not active:
        return 0

    planned = _plan_checks(local_config, _load_state())
    if not planned:
        print("no checks enabled (all off or empty config)")
        return 0

    for (check_name, _make_target, make_command), mode, fingerprint, cached, stale in planned:
        if not stale:
            print(f"[{check_name}] mode={mode} up to date ({fingerprint[:12]}…)")
            continue
        action = f"would run {make_command}"
        if mode == "confirm":
            if sys.stdin.isatty():
                action = f"would prompt, then run {make_command}"
            else:
                action = f"would block push ({make_command}, non-interactive)"
        cached_label = cached[:12] if cached else "none"
        print(
            f"[{check_name}] mode={mode} stale ({fingerprint[:12]}…, was {cached_label}…) → {action}"
        )
    return 0


def main() -> int:
    if not LOCAL_CONFIG_PATH.is_file():
        return 0

    local_config = _load_local_config()
    active, reason = _gate_active(local_config)
    if not active:
        print(f"prek gate: skipped ({reason})", file=sys.stderr)
        return 0

    state = _load_state()

    for check_name, make_target, make_command in CHECKS:
        mode = _get_mode(local_config, check_name)
        if mode == "off":
            continue

        fingerprint = compute_fingerprint(check_name)
        cached = state.get(check_name, {})
        if cached.get("fingerprint") == fingerprint:
            continue

        if mode == "confirm" and not _confirm_run(make_command):
            print(f"Declined {make_command}. Push aborted.", file=sys.stderr)
            return 1

        if _run_make(make_target) != 0:
            print(f"{make_command} failed. Push aborted.", file=sys.stderr)
            return 1

        state[check_name] = {
            "fingerprint": fingerprint,
            "passed_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "command": make_command,
        }
        _write_state(state)

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pre-push gate for lint and common tests")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show which checks would run without executing them or updating state",
    )
    args = parser.parse_args()
    raise SystemExit(_dry_run() if args.dry_run else main())
