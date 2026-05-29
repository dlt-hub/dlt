"""Tests for tools/prek.py."""

from collections.abc import Callable
from pathlib import Path
from typing import Optional

import pendulum
import pytest

from tools.prek import (
    CHECKS,
    ConfigError,
    CheckState,
    GateDeps,
    MAX_PASSED_FINGERPRINTS,
    PassRecord,
    FingerprintDef,
    State,
    dry_run_lines,
    fingerprint_files,
    gate_active,
    load_state,
    make_command_for,
    matches_globs,
    parse_bool,
    parse_mode,
    passed_fingerprints,
    plan_checks,
    record_passed_check,
    repo_root,
    resolve_fingerprint_files,
    run_gate,
    with_passed_check,
    write_state,
)

FIXED_NOW = pendulum.datetime(2026, 5, 29, 12, 0, 0, tz=pendulum.UTC)


def make_deps(
    *,
    run_make: Optional[Callable[[str], int]] = None,
    has_open_pr: Callable[[], bool] = lambda: True,
    confirm: Callable[[str], bool] = lambda _: True,
    fingerprint: Callable[[str], str] = lambda name: f"fp-{name}",
    is_tty: Callable[[], bool] = lambda: True,
) -> GateDeps:
    return GateDeps(
        run_make=run_make or (lambda _target: 0),
        has_open_pr=has_open_pr,
        confirm=confirm,
        fingerprint=fingerprint,
        now=lambda: FIXED_NOW,
        is_tty=is_tty,
    )


def test_repo_root_uses_git_toplevel(monkeypatch: pytest.MonkeyPatch) -> None:
    class Result:
        returncode = 0
        stdout = "/repo\n"

    monkeypatch.setattr("tools.prek.subprocess.run", lambda *args, **kwargs: Result())
    assert repo_root() == Path("/repo")


@pytest.mark.parametrize(
    ("value", "expected"),
    [(True, True), (False, False)],
    ids=["true", "false"],
)
def test_parse_bool(value: bool, expected: bool) -> None:
    assert parse_bool(value, key="test.key") is expected


def test_parse_mode_and_make_command() -> None:
    assert parse_mode({"lint": {"mode": "auto"}}, "lint") == "auto"
    assert make_command_for("lint") == "make fl"
    with pytest.raises(ConfigError, match="Unknown check"):
        make_command_for("missing")


@pytest.mark.parametrize(
    ("path", "globs", "expected"),
    [
        ("tests/common/test_utils.py", ["*.py"], True),
        ("tests/common/readme.md", ["*.py"], False),
    ],
    ids=["match", "no-match"],
)
def test_matches_globs(path: str, globs: list[str], expected: bool) -> None:
    assert matches_globs(path, globs) is expected


def test_resolve_fingerprint_files_and_fingerprint(tmp_path: Path) -> None:
    (tmp_path / "root.toml").write_text("c", encoding="utf-8")
    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / "keep.py").write_text("a", encoding="utf-8")

    fingerprint_def = FingerprintDef(files=("root.toml",), paths=("pkg",), globs=("*.py",))
    paths = resolve_fingerprint_files(
        fingerprint_def,
        list_tracked=lambda _: ["pkg/keep.py", "pkg/skip.txt"],
        root=tmp_path,
    )
    assert paths == ["pkg/keep.py", "root.toml"]

    files = {"a.py": b"one", "b.py": b"two"}
    assert fingerprint_files(["a.py", "b.py"], files.__getitem__) == fingerprint_files(
        ["a.py", "b.py"], files.__getitem__
    )


@pytest.mark.parametrize(
    ("only_when_pr_open", "has_open_pr", "active"),
    [(False, False, True), (True, False, False), (True, True, True)],
    ids=["always", "no-pr", "open-pr"],
)
def test_gate_active(only_when_pr_open: bool, has_open_pr: bool, active: bool) -> None:
    result, _reason = gate_active(only_when_pr_open=only_when_pr_open, has_open_pr=has_open_pr)
    assert result is active


def test_plan_checks_and_dry_run() -> None:
    planned = plan_checks(
        CHECKS,
        {"lint": {"mode": "confirm"}, "test_common_p": {"mode": "off"}},
        {},
        lambda _name: "fingerprint-value",
    )
    lines = dry_run_lines(
        planned,
        local_config_path=Path(".prek/local.toml"),
        active=True,
        reason="only_when_pr_open=false",
        is_tty=False,
    )
    assert any("would block push" in line for line in lines)


def test_run_gate_flow() -> None:
    calls: list[str] = []

    def record_make(target: str) -> int:
        calls.append(target)
        return 0

    deps = make_deps(run_make=record_make)

    skipped = run_gate(
        local_config={"gate": {"only_when_pr_open": True}, "lint": {"mode": "auto"}},
        state={},
        deps=make_deps(has_open_pr=lambda: False),
    )
    assert skipped.exit_code == 0
    assert "skipped" in skipped.stderr_lines[0]

    passed = run_gate(local_config={"lint": {"mode": "auto"}}, state={}, deps=deps)
    assert passed.exit_code == 0
    assert calls == ["fl"]
    assert passed.new_state["lint"]["passes"][0]["fingerprint"] == "fp-lint"
    assert passed.new_state["lint"]["passes"][0]["command"] == "make fl"

    failed = run_gate(
        local_config={"lint": {"mode": "auto"}},
        state={
            "lint": CheckState(
                passes=[
                    PassRecord(fingerprint="other", passed_at="t0", command="make fl"),
                ]
            )
        },
        deps=make_deps(run_make=lambda _target: 1),
    )
    assert failed.exit_code == 1
    assert failed.new_state["lint"]["passes"][0]["fingerprint"] == "other"

    declined = run_gate(
        local_config={"lint": {"mode": "confirm"}},
        state={},
        deps=make_deps(confirm=lambda _: False),
    )
    assert declined.exit_code == 1


def test_record_passed_check() -> None:
    deps = make_deps()
    unchanged, error = record_passed_check(
        check_name="lint", state={}, hooks_enabled=False, deps=deps
    )
    assert error is None and unchanged == {}

    updated, error = record_passed_check(check_name="lint", state={}, hooks_enabled=True, deps=deps)
    assert error is None
    assert updated["lint"]["passes"][0]["command"] == "make fl"

    _, error = record_passed_check(check_name="missing", state={}, hooks_enabled=True, deps=deps)
    assert error is not None


def test_with_passed_check_and_state_io(tmp_path: Path) -> None:
    state: State = {
        "lint": CheckState(
            passes=[
                PassRecord(fingerprint="old", passed_at="t0", command="make fl"),
            ]
        )
    }
    updated = with_passed_check(
        state, "lint", fingerprint="new", command="make fl", passed_at=FIXED_NOW
    )
    assert state["lint"]["passes"][0]["fingerprint"] == "old"
    assert updated["lint"]["passes"][0]["fingerprint"] == "new"
    assert updated["lint"]["passes"][1]["fingerprint"] == "old"

    state_path = tmp_path / ".state.toml"
    write_state(state_path, updated)
    assert load_state(state_path) == updated


def test_fingerprint_history_skips_known_tree() -> None:
    deps = make_deps(fingerprint=lambda name: {"lint": "fp-a", "test_common_p": "fp-b"}[name])
    state: State = {
        "lint": CheckState(
            passes=[
                PassRecord(fingerprint="fp-a", passed_at="t1", command="make fl"),
                PassRecord(fingerprint="fp-other", passed_at="t0", command="make fl"),
            ]
        )
    }

    outcome = run_gate(local_config={"lint": {"mode": "auto"}}, state=state, deps=deps)
    assert outcome.exit_code == 0
    assert outcome.new_state == state

    planned = plan_checks(
        CHECKS,
        {"lint": {"mode": "auto"}},
        state,
        deps.fingerprint,
    )
    assert planned[0].stale is False


def test_fingerprint_history_caps_at_max() -> None:
    history = [
        PassRecord(fingerprint=f"fp-{index}", passed_at=f"t{index}", command="make fl")
        for index in range(MAX_PASSED_FINGERPRINTS + 5)
    ]
    state: State = {"lint": CheckState(passes=history)}
    updated = with_passed_check(
        state,
        "lint",
        fingerprint="fp-new",
        command="make fl",
        passed_at=FIXED_NOW,
    )
    assert len(updated["lint"]["passes"]) == MAX_PASSED_FINGERPRINTS
    assert updated["lint"]["passes"][0]["fingerprint"] == "fp-new"
    assert updated["lint"]["passes"][-1]["fingerprint"] == f"fp-{MAX_PASSED_FINGERPRINTS - 2}"
