"""Unit tests for `dlt._workspace.deployment._run_helpers` — pure manifest transforms."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from dlt._workspace.deployment import _run_helpers as run_helpers
from dlt._workspace.deployment._run_helpers import (
    build_runtime_entry_point,
    load_manifest_with_warnings,
    narrow_candidates,
    pick_launcher,
    promote_deployment_arg,
    resolve_interval,
    resolve_profile,
    resolve_refresh,
    resolve_selector,
    select_candidates,
    select_single_job,
    warn_missing_profiles,
)
from dlt._workspace.deployment.exceptions import (
    AmbiguousJobSelector,
    DeploymentException,
    JobRefNotInCandidates,
    ManifestImportError,
    NoMatchingJobs,
)
from dlt._workspace.deployment.launchers import LAUNCHER_JOB, LAUNCHER_MODULE
from dlt._workspace.deployment.typing import (
    TEntryPoint,
    TExecuteSpec,
    TIntervalSpec,
    TJobDefinition,
    TJobRef,
    TJobsDeploymentManifest,
    TRefreshPolicy,
    TRequireSpec,
    TTrigger,
)
from dlt._workspace.profile import DEFAULT_PROFILE


NOW = datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc)


def _job(
    ref: str,
    *,
    triggers: Optional[List[str]] = None,
    default_trigger: Optional[str] = None,
    job_type: str = "batch",
    function: Optional[str] = "main",
    refresh: Optional[TRefreshPolicy] = None,
    require: Optional[TRequireSpec] = None,
    interval: Optional[TIntervalSpec] = None,
    allow_external_schedulers: bool = False,
    launcher: Optional[str] = None,
) -> TJobDefinition:
    entry: TEntryPoint = {
        "module": "my_mod",
        "function": function,
        "job_type": job_type,  # type: ignore[typeddict-item]
        "launcher": launcher or "dlt._workspace.deployment.launchers.job",
    }
    if triggers is None:
        triggers = [f"manual:{ref}"]
    jd: TJobDefinition = {
        "job_ref": TJobRef(ref),
        "entry_point": entry,
        "triggers": [TTrigger(t) for t in triggers],
        "execute": TExecuteSpec(),
    }
    if default_trigger is not None:
        jd["default_trigger"] = TTrigger(default_trigger)
    if refresh is not None:
        jd["refresh"] = refresh
    if require is not None:
        jd["require"] = require
    if interval is not None:
        jd["interval"] = interval
    if allow_external_schedulers:
        jd["allow_external_schedulers"] = True
    return jd


def _manifest(jobs: List[TJobDefinition]) -> TJobsDeploymentManifest:
    return {"engine_version": 1, "jobs": jobs}  # type: ignore[typeddict-item]


@pytest.mark.parametrize(
    "positional,deployment,expected",
    [
        ("batch.run", None, ("batch.run", None)),
        (None, "mod.py", (None, "mod.py")),
        (None, None, (None, None)),
    ],
    ids=["non-py-positional", "deployment-only", "both-none"],
)
def test_promote_deployment_arg_passthrough(
    positional: Optional[str], deployment: Optional[str], expected: Any
) -> None:
    assert promote_deployment_arg(positional, deployment) == expected


def test_promote_deployment_arg_promotes_py(tmp_path: Path) -> None:
    py = tmp_path / "jobs.py"
    py.write_text("")
    assert promote_deployment_arg(str(py), None) == (None, str(py))


def test_promote_deployment_arg_conflict_raises(tmp_path: Path) -> None:
    py = tmp_path / "a.py"
    py.write_text("")
    with pytest.raises(ValueError, match="both"):
        promote_deployment_arg(str(py), "b.py")


def test_promote_deployment_arg_missing_file_raises() -> None:
    with pytest.raises(FileNotFoundError, match="not found"):
        promote_deployment_arg("does_not_exist.py", None)


def test_resolve_selector_none_returns_default() -> None:
    manifest = _manifest([_job("jobs.a")])
    assert resolve_selector(None, manifest) == ["manual:"]
    assert resolve_selector(None, manifest, default_selector="batch") == ["batch"]


def test_resolve_selector_bare_ref_becomes_manual() -> None:
    manifest = _manifest([_job("jobs.a"), _job("jobs.section.b")])
    # full ref
    assert resolve_selector("jobs.a", manifest) == ["manual:jobs.a"]
    # qualified bare name
    assert resolve_selector("section.b", manifest) == ["manual:jobs.section.b"]


def test_resolve_selector_glob_passes_through() -> None:
    manifest = _manifest([_job("jobs.a")])
    assert resolve_selector("tag:daily", manifest) == ["tag:daily"]
    assert resolve_selector("schedule:*", manifest) == ["schedule:*"]


@pytest.mark.parametrize(
    "selectors,expected_refs",
    [
        (["manual:"], ["jobs.a", "jobs.b"]),
        (["manual:jobs.a"], ["jobs.a"]),
        (["tag:daily"], ["jobs.b"]),
    ],
    ids=["manual-glob-matches-both", "specific-manual-matches-one", "tag-matches-tagged-only"],
)
def test_select_candidates_filters_by_selector(
    selectors: List[str], expected_refs: List[str]
) -> None:
    manifest = _manifest(
        [
            _job("jobs.a"),
            _job("jobs.b", triggers=["tag:daily", "manual:jobs.b"]),
        ]
    )
    candidates = select_candidates(manifest, selectors)
    assert [jd["job_ref"] for jd, _ in candidates] == expected_refs


def test_select_candidates_substitutes_manual_with_default_trigger() -> None:
    manifest = _manifest(
        [
            _job(
                "jobs.a",
                triggers=["schedule:0 * * * *", "manual:jobs.a"],
                default_trigger="schedule:0 * * * *",
            )
        ]
    )
    candidates = select_candidates(manifest, ["manual:jobs.a"])
    # manual: hit was substituted with the job's natural schedule trigger
    assert candidates[0][1] == "schedule:0 * * * *"


def test_select_candidates_forbidden_job_type_raises_when_only_forbidden_match() -> None:
    manifest = _manifest([_job("jobs.dash", job_type="interactive")])
    with pytest.raises(DeploymentException, match="interactive"):
        select_candidates(manifest, ["manual:"], forbidden_job_type="interactive")


def test_select_candidates_forbidden_job_type_filters_when_mixed() -> None:
    manifest = _manifest(
        [
            _job("jobs.batch"),
            _job("jobs.dash", job_type="interactive"),
        ]
    )
    candidates = select_candidates(manifest, ["manual:"], forbidden_job_type="interactive")
    assert [jd["job_ref"] for jd, _ in candidates] == ["jobs.batch"]


def test_narrow_candidates_single_match_returns_it() -> None:
    cands = [(_job("jobs.a"), TTrigger("manual:jobs.a"))]
    jd, t = narrow_candidates(cands, None)
    assert jd["job_ref"] == "jobs.a"
    assert t == "manual:jobs.a"


def test_narrow_candidates_multi_match_without_job_ref_raises_ambiguous() -> None:
    cands = [
        (_job("jobs.a"), TTrigger("manual:")),
        (_job("jobs.b"), TTrigger("manual:")),
    ]
    with pytest.raises(AmbiguousJobSelector) as exc:
        narrow_candidates(cands, None)
    assert "--job-ref" in str(exc.value)
    assert exc.value.matches == cands


def test_narrow_candidates_multi_match_with_job_ref_picks_one() -> None:
    cands = [
        (_job("jobs.a"), TTrigger("manual:")),
        (_job("jobs.b"), TTrigger("manual:")),
    ]
    jd, _ = narrow_candidates(cands, "jobs.b")
    assert jd["job_ref"] == "jobs.b"


def test_narrow_candidates_job_ref_not_in_set_raises() -> None:
    cands = [
        (_job("jobs.a"), TTrigger("manual:")),
        (_job("jobs.b"), TTrigger("manual:")),
    ]
    with pytest.raises(JobRefNotInCandidates) as exc:
        narrow_candidates(cands, "jobs.c")
    assert exc.value.job_ref == "jobs.c"
    assert "jobs.a" in str(exc.value) and "jobs.b" in str(exc.value)


def test_narrow_candidates_single_match_with_mismatching_job_ref_raises() -> None:
    cands = [(_job("jobs.a"), TTrigger("manual:jobs.a"))]
    with pytest.raises(JobRefNotInCandidates):
        narrow_candidates(cands, "jobs.b")


def test_narrow_candidates_single_match_with_matching_job_ref() -> None:
    cands = [(_job("jobs.a"), TTrigger("manual:jobs.a"))]
    jd, _ = narrow_candidates(cands, "jobs.a")
    assert jd["job_ref"] == "jobs.a"


def test_select_single_job_no_match_raises_no_matching_jobs() -> None:
    """Default behavior (no `available_selectors`): every manifest job is listed."""
    manifest = _manifest([_job("jobs.a"), _job("jobs.b")])
    with pytest.raises(NoMatchingJobs, match="No jobs matched") as ei:
        select_single_job(manifest, ["tag:nope"])
    refs = {jd["job_ref"] for jd, _ in ei.value.available}
    assert refs == {"jobs.a", "jobs.b"}
    # back-compat: NoMatchingJobs is also a LookupError
    assert isinstance(ei.value, LookupError)


def test_no_matching_jobs_lists_only_batch_when_scoped() -> None:
    manifest = _manifest(
        [
            _job("jobs.b.one", job_type="batch"),
            _job("jobs.i.app", job_type="interactive"),
        ]
    )
    with pytest.raises(NoMatchingJobs) as ei:
        select_single_job(manifest, ["tag:nope"], available_selectors=["batch"])
    refs = {jd["job_ref"] for jd, _ in ei.value.available}
    assert refs == {"jobs.b.one"}
    assert "jobs.b.one" in str(ei.value)
    assert "jobs.i.app" not in str(ei.value)


def test_no_matching_jobs_lists_only_interactive_when_scoped() -> None:
    manifest = _manifest(
        [
            _job("jobs.b.one", job_type="batch"),
            _job("jobs.i.app", job_type="interactive"),
        ]
    )
    with pytest.raises(NoMatchingJobs) as ei:
        select_single_job(manifest, ["tag:nope"], available_selectors=["interactive"])
    refs = {jd["job_ref"] for jd, _ in ei.value.available}
    assert refs == {"jobs.i.app"}


def test_no_matching_jobs_pipeline_scope_uses_synthetic_trigger() -> None:
    """`pipeline_name:*` matches jobs declaring `deliver.pipeline_name`."""
    pipe = _job("jobs.p", job_type="batch")
    pipe["deliver"] = {"pipeline_name": "my_pipe"}
    plain = _job("jobs.b", job_type="batch")
    manifest = _manifest([pipe, plain])
    with pytest.raises(NoMatchingJobs) as ei:
        select_single_job(
            manifest, ["pipeline_name:other"], available_selectors=["pipeline_name:*"]
        )
    refs = {jd["job_ref"] for jd, _ in ei.value.available}
    assert refs == {"jobs.p"}


def test_no_matching_jobs_with_no_jobs_in_scope_renders_friendly_message() -> None:
    manifest = _manifest([_job("jobs.b", job_type="batch")])
    with pytest.raises(NoMatchingJobs) as ei:
        select_single_job(manifest, ["tag:nope"], available_selectors=["interactive"])
    assert ei.value.available == []
    assert "No matching jobs declared in the manifest." in str(ei.value)


@pytest.mark.parametrize(
    "policy,user_flag,expected_refresh,expect_warning",
    [
        ("auto", False, False, False),
        ("auto", True, True, False),
        ("always", False, True, False),
        ("always", True, True, False),
        ("block", False, False, False),
        ("block", True, False, True),
        (None, True, True, False),
    ],
    ids=[
        "auto-off",
        "auto-on",
        "always-off-forces-refresh",
        "always-on-forces-refresh",
        "block-off",
        "block-on-ignored",
        "default-is-auto",
    ],
)
def test_resolve_refresh(
    policy: Optional[TRefreshPolicy],
    user_flag: bool,
    expected_refresh: bool,
    expect_warning: bool,
) -> None:
    jd = _job("jobs.a", refresh=policy)
    effective, warning = resolve_refresh(user_flag, jd)
    assert effective is expected_refresh
    if expect_warning:
        assert warning and "refresh=block" in warning
    else:
        assert warning is None


@pytest.mark.parametrize(
    "user,declared,active_profile,expected_current,expect_warning",
    [
        ("access", None, "dev", "access", False),
        (None, None, "tests", "tests", False),
        (None, None, DEFAULT_PROFILE, DEFAULT_PROFILE, False),
        (None, "prod", "prod", "prod", False),
        (None, "prod", "dev", "dev", True),
        ("dev", "prod", "prod", "dev", True),
    ],
    ids=[
        "cli-override-wins",
        "active-when-no-override",
        "default-profile",
        "no-warning-when-declared-matches",
        "warns-on-declared-vs-active-mismatch",
        "cli-override-triggers-declared-mismatch",
    ],
)
def test_resolve_profile(
    user: Optional[str],
    declared: Optional[str],
    active_profile: str,
    expected_current: str,
    expect_warning: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    require: Optional[TRequireSpec] = {"profile": declared} if declared else None
    jd = _job("jobs.a", require=require)

    class _MockCtx:
        profile = active_profile

    monkeypatch.setattr(run_helpers, "active", lambda: _MockCtx())

    current, warning = resolve_profile(user, jd)
    assert current == expected_current
    if expect_warning:
        assert warning and f"'{declared}'" in warning and f"'{expected_current}'" in warning
    else:
        assert warning is None


_SCHED_HOURLY = {
    "triggers": ["schedule:0 * * * *"],
    "default_trigger": "schedule:0 * * * *",
}
_SCHED_HOURLY_BACKFILL = {
    **_SCHED_HOURLY,
    "interval": {"start": "2026-04-19T00:00:00Z", "end": "2026-04-19T06:00:00Z"},
}
_SCHED_HOURLY_WIDE_BACKFILL = {
    **_SCHED_HOURLY,
    "interval": {"start": "2026-04-19T00:00:00Z"},
}
_EVERY_5M = {
    "triggers": ["every:5m"],
    "default_trigger": "every:5m",
}
_EVERY_BACKFILL = {
    **_EVERY_5M,
    "interval": {"start": "2026-04-19T10:00:00Z"},
}


@pytest.mark.parametrize(
    "user_start,user_end,job_kwargs,refresh,expected_start,expected_end,expected_tz",
    [
        (
            "2026-04-19T00:00:00Z",
            "2026-04-19T06:00:00Z",
            _SCHED_HOURLY_BACKFILL,
            False,
            datetime(2026, 4, 19, 0, tzinfo=timezone.utc),
            datetime(2026, 4, 19, 6, tzinfo=timezone.utc),
            "UTC",
        ),
        (
            "2026-04-19T00:00:00Z",
            None,
            {},
            False,
            datetime(2026, 4, 19, 0, tzinfo=timezone.utc),
            NOW,
            "UTC",
        ),
        (
            "2026-04-19T12:00:00",
            "2026-04-19T13:00:00",
            {"require": {"timezone": "Europe/Warsaw"}},
            False,
            datetime(2026, 4, 19, 10, tzinfo=timezone.utc),
            datetime(2026, 4, 19, 11, tzinfo=timezone.utc),
            "Europe/Warsaw",
        ),
        (
            None,
            None,
            _SCHED_HOURLY_BACKFILL,
            True,
            datetime(2026, 4, 19, 0, tzinfo=timezone.utc),
            datetime(2026, 4, 19, 6, tzinfo=timezone.utc),
            "UTC",
        ),
        (
            None,
            None,
            _SCHED_HOURLY_WIDE_BACKFILL,
            True,
            datetime(2026, 4, 19, 0, tzinfo=timezone.utc),
            datetime(2026, 4, 19, 12, tzinfo=timezone.utc),
            "UTC",
        ),
        (
            None,
            None,
            _EVERY_BACKFILL,
            True,
            datetime(2026, 4, 19, 10, tzinfo=timezone.utc),
            NOW,
            "UTC",
        ),
        (
            None,
            None,
            _SCHED_HOURLY,
            True,
            datetime(2026, 4, 19, 11, tzinfo=timezone.utc),
            NOW,
            "UTC",
        ),
        (
            None,
            None,
            _SCHED_HOURLY_BACKFILL,
            False,
            datetime(2026, 4, 19, 11, tzinfo=timezone.utc),
            datetime(2026, 4, 19, 6, tzinfo=timezone.utc),
            "UTC",
        ),
        (
            None,
            None,
            _SCHED_HOURLY,
            False,
            datetime(2026, 4, 19, 11, tzinfo=timezone.utc),
            NOW,
            "UTC",
        ),
        (
            None,
            None,
            _SCHED_HOURLY_WIDE_BACKFILL,
            False,
            datetime(2026, 4, 19, 11, tzinfo=timezone.utc),
            NOW,
            "UTC",
        ),
        (None, None, {}, False, NOW, NOW, "UTC"),
    ],
    ids=[
        "user-override-wins-over-declared",
        "user-start-only-end-defaults-to-now",
        "naive-values-use-job-tz-Warsaw-CEST",
        "refresh-true-backfill-schedule-clamped-to-declared-end",
        "refresh-true-backfill-schedule-open-ended",
        "refresh-true-backfill-every",
        "refresh-true-no-declared-falls-through",
        "refresh-false-clamped-to-declared-range",
        "refresh-false-compute-run-interval-schedule",
        "refresh-false-open-ended-declared-not-passed-without-refresh",
        "refresh-false-manual-collapses-to-point",
    ],
)
def test_resolve_interval(
    user_start: Optional[str],
    user_end: Optional[str],
    job_kwargs: Dict[str, Any],
    refresh: bool,
    expected_start: datetime,
    expected_end: datetime,
    expected_tz: str,
) -> None:
    jd = _job("jobs.a", **job_kwargs)
    start, end, tz = resolve_interval(
        user_start, user_end, jd, TTrigger("manual:jobs.a"), NOW, refresh=refresh
    )
    assert start == expected_start
    assert end == expected_end
    assert tz == expected_tz


def test_build_runtime_entry_point_batch_sets_interval_and_profile() -> None:
    jd = _job("jobs.a")
    start = datetime(2026, 4, 19, 10, tzinfo=timezone.utc)
    end = datetime(2026, 4, 19, 11, tzinfo=timezone.utc)
    ep = build_runtime_entry_point(
        jd, {}, profile="prod", refresh=True, interval_start=start, interval_end=end, tz="UTC"
    )
    assert ep["interval_start"] == "2026-04-19T10:00:00+00:00"
    assert ep["interval_end"] == "2026-04-19T11:00:00+00:00"
    assert ep["interval_timezone"] == "UTC"
    assert ep["profile"] == "prod"
    assert ep["refresh"] is True
    assert ep["allow_external_schedulers"] is False
    assert "run_args" not in ep


def test_build_runtime_entry_point_interactive_sets_port() -> None:
    jd = _job("jobs.dash", job_type="interactive")
    ep = build_runtime_entry_point(jd, {}, "dev", False, NOW, NOW, "UTC")
    assert ep["run_args"] == {"port": 5000}


def test_build_runtime_entry_point_config_merges() -> None:
    jd = _job("jobs.a")
    jd["entry_point"]["config"] = {"A": "1", "B": "2"}  # type: ignore[typeddict-unknown-key]
    ep = build_runtime_entry_point(jd, {"B": "override", "C": "3"}, "dev", False, NOW, NOW, "UTC")
    assert ep["config"] == {"A": "1", "B": "override", "C": "3"}


def test_build_runtime_entry_point_propagates_allow_external_schedulers() -> None:
    jd = _job("jobs.a", allow_external_schedulers=True)
    ep = build_runtime_entry_point(jd, {}, "dev", False, NOW, NOW, "UTC")
    assert ep["allow_external_schedulers"] is True


def test_build_runtime_entry_point_does_not_mutate_job_def() -> None:
    jd = _job("jobs.a")
    original_entry = dict(jd["entry_point"])
    build_runtime_entry_point(jd, {"X": "1"}, "prod", True, NOW, NOW, "UTC")
    assert dict(jd["entry_point"]) == original_entry


@pytest.mark.parametrize(
    "launcher,function,expected",
    [
        ("custom.launcher", "main", "custom.launcher"),
        (None, "main", LAUNCHER_JOB),
        (None, None, LAUNCHER_MODULE),
    ],
    ids=["explicit-override", "function-based", "module-level"],
)
def test_pick_launcher(launcher: Optional[str], function: Optional[str], expected: str) -> None:
    ep = {"launcher": launcher, "function": function, "job_type": "batch"}
    assert pick_launcher(ep) == expected  # type: ignore[arg-type]


def test_load_manifest_plain_python_module_becomes_module_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = tmp_path / "my_pipeline.py"
    script.write_text("x = 1\n")
    monkeypatch.chdir(tmp_path)
    manifest, _, _ = load_manifest_with_warnings("my_pipeline.py", use_all=False)
    jobs = manifest["jobs"]
    assert len(jobs) == 1
    assert jobs[0]["entry_point"]["launcher"] == LAUNCHER_MODULE
    assert jobs[0]["entry_point"]["function"] is None


def test_load_manifest_missing_default_module_raises_typed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ManifestImportError, match="__deployment__") as exc:
        load_manifest_with_warnings("__deployment__", use_all=True)
    assert exc.value.kind == "default_missing"


def test_load_manifest_missing_file_raises_typed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ManifestImportError, match="Could not import") as exc:
        load_manifest_with_warnings("does_not_exist_abc123", use_all=True)
    assert exc.value.kind == "module_missing"


def test_load_manifest_import_error_inside_file_surfaces_real_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = tmp_path / "bad.py"
    script.write_text("import definitely_not_installed_xyz123\n")
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ManifestImportError, match="Failed to import") as exc:
        load_manifest_with_warnings("bad.py", use_all=False)
    assert exc.value.kind == "import_failed"


def test_warn_missing_profiles_returns_advisory_for_each_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _MockCtx:
        def available_profiles(self) -> List[str]:
            return ["dev"]

    monkeypatch.setattr(run_helpers, "active", lambda: _MockCtx())
    warnings = warn_missing_profiles()
    joined = "\n".join(warnings)
    assert "'prod'" in joined
    assert "'access'" in joined


def test_warn_missing_profiles_returns_empty_when_both_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _MockCtx:
        def available_profiles(self) -> List[str]:
            return ["prod", "access", "dev"]

    monkeypatch.setattr(run_helpers, "active", lambda: _MockCtx())
    assert warn_missing_profiles() == []
