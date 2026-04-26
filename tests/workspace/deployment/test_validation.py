"""Tests for manifest validation, hashing, versioning, and IO."""

import json as stdlib_json
from datetime import datetime, timezone  # noqa: I251
from io import BytesIO
from typing import Any, Dict, List, Optional

import pytest

from dlt._workspace.deployment import interval as interval_mod
from dlt._workspace.deployment.exceptions import InvalidJobDefinition, JobValidationResult
from dlt._workspace.deployment.manifest import (
    DASHBOARD_JOB_REF,
    InvalidManifest,
    bump_manifest_version,
    compute_default_trigger,
    generate_manifest_hash,
    hash_job_definition,
    load_manifest,
    migrate_manifest,
    save_manifest,
    validate_job_definition,
    validate_manifest,
)
from dlt._workspace.deployment.typing import (
    MANIFEST_ENGINE_VERSION,
    TJobsDeploymentManifest,
    TEntryPoint,
    TExecuteSpec,
    TJobDefinition,
    TJobRef,
    TJobType,
    TTrigger,
)


@pytest.fixture
def enable_interval_freshness(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(interval_mod, "INTERVAL_FRESHNESS_ENABLED", True)


def _make_job(
    job_ref: str,
    job_type: TJobType = "batch",
    triggers: Optional[List[str]] = None,
    **kwargs: Any,
) -> TJobDefinition:
    entry_point: TEntryPoint = {
        "module": "test_module",
        "function": job_ref.split(".")[-1],
        "job_type": job_type,
        "launcher": "dlt._workspace.deployment.launchers.job",
    }
    job: TJobDefinition = {
        "job_ref": TJobRef(job_ref),
        "entry_point": entry_point,
        "triggers": [TTrigger(t) for t in triggers] if triggers else [],
        "execute": TExecuteSpec(concurrency=1),
    }
    job.update(kwargs)  # type: ignore[typeddict-item]
    return job


def _make_manifest(jobs: List[TJobDefinition], **kwargs: Any) -> TJobsDeploymentManifest:
    manifest: TJobsDeploymentManifest = {
        "engine_version": MANIFEST_ENGINE_VERSION,
        "created_at": "2026-03-10T00:00:00Z",
        "deployment_module": "test",
        "jobs": jobs,
    }
    manifest.update(kwargs)  # type: ignore[typeddict-item]
    return manifest


def test_valid_manifest() -> None:
    manifest = _make_manifest(
        [
            _make_job("jobs.mod.batch1", triggers=["schedule:0 8 * * *"]),
            _make_job(
                "jobs.mod.api",
                job_type="interactive",
                triggers=["http:"],
                expose={"interface": "rest_api"},
            ),
        ]
    )
    result = validate_manifest(manifest)
    assert result.is_valid
    assert result.errors == []


@pytest.mark.parametrize(
    "timeout_spec",
    [
        {"timeout": 7200, "grace_period": 30},
        {"timeout": 7200.0, "grace_period": 30.0},
        {"timeout": 7200},
        {"grace_period": 10},
    ],
    ids=["int-both", "float-both", "int-timeout-only", "int-grace-only"],
)
def test_valid_manifest_timeout_accepts_int_and_float(timeout_spec: Dict[str, Any]) -> None:
    """TTimeoutSpec fields accept both int and float."""
    manifest = _make_manifest([_make_job("jobs.mod.j", execute={"timeout": timeout_spec})])
    result = validate_manifest(manifest)
    assert result.is_valid, result.errors


def test_empty_manifest() -> None:
    result = validate_manifest(_make_manifest([]))
    assert result.is_valid
    assert result.errors == []


@pytest.mark.parametrize(
    "jobs_fn,expect_valid,error_frag,warning_frag",
    [
        # duplicate job ref
        (
            lambda: [_make_job("jobs.mod.same"), _make_job("jobs.mod.same")],
            False,
            "duplicate",
            None,
        ),
        # batch + http trigger = error
        (
            lambda: [_make_job("jobs.mod.bad", job_type="batch", triggers=["http:"])],
            False,
            "batch job",
            None,
        ),
        # interactive without http = warning
        (
            lambda: [
                _make_job(
                    "jobs.mod.odd",
                    job_type="interactive",
                    triggers=["schedule:0 8 * * *"],
                    expose={"interface": "gui"},
                )
            ],
            True,
            None,
            "no http trigger",
        ),
        # invalid trigger format
        (
            lambda: [_make_job("jobs.mod.bad", triggers=["not_a_trigger"])],
            False,
            None,
            None,
        ),
        # 'py' as section is reserved
        (
            lambda: [_make_job("jobs.py.foo")],
            False,
            "section 'py' is reserved",
            None,
        ),
        # 'py' as sectioned name is reserved
        (
            lambda: [_make_job("jobs.mod.py")],
            False,
            "name 'py' is reserved",
            None,
        ),
        # 'py' as module-level name is reserved
        (
            lambda: [_make_job("jobs.py")],
            False,
            "name 'py' is reserved",
            None,
        ),
    ],
    ids=[
        "duplicate-ref",
        "batch-http",
        "interactive-no-http",
        "invalid-trigger",
        "py-section-reserved",
        "py-sectioned-name-reserved",
        "py-module-name-reserved",
    ],
)
def test_validation_errors_and_warnings(jobs_fn, expect_valid, error_frag, warning_frag) -> None:
    result = validate_manifest(_make_manifest(jobs_fn()))
    assert result.is_valid == expect_valid
    if error_frag:
        assert any(error_frag in e for e in result.errors)
    if warning_frag:
        assert any(warning_frag in w for w in result.warnings)


def test_duplicate_entry_points_warning() -> None:
    manifest = _make_manifest([_make_job("jobs.mod.job_a"), _make_job("jobs.mod.job_b")])
    manifest["jobs"][0]["entry_point"]["function"] = "job_a"
    manifest["jobs"][1]["entry_point"]["function"] = "job_a"
    result = validate_manifest(manifest)
    assert result.is_valid
    assert any("same entry point" in w for w in result.warnings)


def test_unresolved_job_triggers() -> None:
    manifest = _make_manifest(
        [
            _make_job("jobs.mod.a"),
            _make_job(
                "jobs.mod.b",
                triggers=["job.success:jobs.mod.a", "job.success:jobs.mod.missing"],
            ),
        ]
    )
    result = validate_manifest(manifest)
    assert not result.is_valid
    assert "jobs.mod.b" in result.unresolved_triggers
    assert "jobs.mod.missing" in result.unresolved_triggers["jobs.mod.b"]
    assert any("unknown jobs" in e for e in result.errors)


def test_resolved_job_triggers() -> None:
    manifest = _make_manifest(
        [
            _make_job("jobs.mod.upstream"),
            _make_job("jobs.mod.downstream", triggers=["job.success:jobs.mod.upstream"]),
        ]
    )
    result = validate_manifest(manifest)
    assert result.is_valid
    assert result.unresolved_triggers == {}


def test_interval_without_schedule_or_every_trigger() -> None:
    manifest = _make_manifest(
        [
            _make_job(
                "jobs.mod.a", triggers=["manual:jobs.mod.a"], interval={"start": "2024-01-01"}
            ),
        ]
    )
    result = validate_manifest(manifest)
    assert not result.is_valid
    assert any("no schedule or every" in e for e in result.errors)


def test_interval_with_every_trigger_valid() -> None:
    manifest = _make_manifest(
        [
            _make_job("jobs.mod.a", triggers=["every:5m"], interval={"start": "2024-01-01"}),
        ]
    )
    result = validate_manifest(manifest)
    assert result.is_valid, f"errors: {result.errors}"


def test_freshness_constraint_on_non_interval_upstream(enable_interval_freshness: None) -> None:
    manifest = _make_manifest(
        [
            _make_job("jobs.mod.up", triggers=["schedule:0 0 * * *"]),
            _make_job(
                "jobs.mod.down",
                triggers=["schedule:0 0 * * *"],
                interval={"start": "2024-01-01"},
                freshness=["job.is_matching_interval_fresh:jobs.mod.up"],
            ),
        ]
    )
    result = validate_manifest(manifest)
    assert not result.is_valid
    assert any("upstream has no interval" in e for e in result.errors)


def test_is_matching_interval_fresh_disabled_raises() -> None:
    manifest = _make_manifest(
        [
            _make_job(
                "jobs.mod.up",
                triggers=["schedule:0 0 * * *"],
                interval={"start": "2024-01-01"},
            ),
            _make_job(
                "jobs.mod.down",
                triggers=["schedule:0 0 * * *"],
                interval={"start": "2024-01-01"},
                freshness=["job.is_matching_interval_fresh:jobs.mod.up"],
            ),
        ]
    )
    with pytest.raises(NotImplementedError, match="not yet implemented"):
        validate_manifest(manifest)


def test_allow_external_schedulers_without_interval_warns() -> None:
    manifest = _make_manifest(
        [
            _make_job(
                "jobs.mod.a", triggers=["schedule:0 0 * * *"], allow_external_schedulers=True
            ),
        ]
    )
    result = validate_manifest(manifest)
    assert result.is_valid
    assert any("allow_external_schedulers" in w and "no interval" in w for w in result.warnings)


def test_misaligned_interval_start_warns() -> None:
    """Start not on a cron tick produces a warning."""
    manifest = _make_manifest(
        [
            _make_job(
                "jobs.mod.a",
                triggers=["schedule:0 0 * * *"],
                interval={"start": "2024-01-01T06:30:00Z"},
            ),
        ]
    )
    result = validate_manifest(manifest)
    assert result.is_valid
    assert any("start" in w and "snapped backward" in w for w in result.warnings)


def test_misaligned_interval_end_warns() -> None:
    """End not on a cron tick produces a warning."""
    manifest = _make_manifest(
        [
            _make_job(
                "jobs.mod.a",
                triggers=["schedule:0 0 * * *"],
                interval={"start": "2024-01-01T00:00:00Z", "end": "2024-01-05T06:30:00Z"},
            ),
        ]
    )
    result = validate_manifest(manifest)
    assert result.is_valid
    assert any("end" in w and "snapped backward" in w for w in result.warnings)


def test_aligned_interval_no_warning() -> None:
    """Aligned start and end produce no snap warnings."""
    manifest = _make_manifest(
        [
            _make_job(
                "jobs.mod.a",
                triggers=["schedule:0 0 * * *"],
                interval={"start": "2024-01-01T00:00:00Z", "end": "2024-01-05T00:00:00Z"},
            ),
        ]
    )
    result = validate_manifest(manifest)
    assert result.is_valid
    assert not any("snapped" in w for w in result.warnings)


def test_valid_interval_job_with_freshness(enable_interval_freshness: None) -> None:
    manifest = _make_manifest(
        [
            _make_job(
                "jobs.mod.up",
                triggers=["schedule:0 0 * * *"],
                interval={"start": "2024-01-01"},
            ),
            _make_job(
                "jobs.mod.down",
                triggers=["schedule:0 0 * * *"],
                interval={"start": "2024-01-01"},
                freshness=["job.is_matching_interval_fresh:jobs.mod.up"],
            ),
        ]
    )
    result = validate_manifest(manifest)
    assert result.is_valid
    assert not any("interval" in e for e in result.errors)


def test_manifest_hash_stable() -> None:
    m1 = _make_manifest([_make_job("jobs.mod.a")])
    m2 = _make_manifest([_make_job("jobs.mod.a")])
    assert generate_manifest_hash(m1) == generate_manifest_hash(m2)


def test_manifest_hash_excludes_metadata() -> None:
    m1 = _make_manifest([_make_job("jobs.mod.a")])
    m2 = _make_manifest([_make_job("jobs.mod.a")])
    m2["created_at"] = "2099-01-01T00:00:00Z"
    m2["version"] = 99
    m2["version_hash"] = "old_hash"
    m2["previous_hashes"] = ["h1", "h2"]
    assert generate_manifest_hash(m1) == generate_manifest_hash(m2)


def test_manifest_hash_changes_on_content() -> None:
    m1 = _make_manifest([_make_job("jobs.mod.a")])
    m2 = _make_manifest([_make_job("jobs.mod.b")])
    assert generate_manifest_hash(m1) != generate_manifest_hash(m2)


def test_job_hash_stable() -> None:
    j1 = _make_job("jobs.mod.a", triggers=["schedule:0 8 * * *"])
    j2 = _make_job("jobs.mod.a", triggers=["schedule:0 8 * * *"])
    assert hash_job_definition(j1) == hash_job_definition(j2)


def test_job_hash_canonical_key_order() -> None:
    """Dict key order inside nested sub-structures must not affect the hash."""
    j1 = _make_job("jobs.mod.a")
    j2 = _make_job("jobs.mod.a")
    # Rebuild entry_point with reversed key order — sort_keys must normalize it.
    ep = j2["entry_point"]
    j2["entry_point"] = {  # type: ignore[typeddict-item]
        k: ep[k] for k in reversed(list(ep.keys()))  # type: ignore[literal-required]
    }
    assert hash_job_definition(j1) == hash_job_definition(j2)


def test_job_hash_changes_on_description() -> None:
    j1 = _make_job("jobs.mod.a")
    j2 = _make_job("jobs.mod.a", description="a different description")
    assert hash_job_definition(j1) != hash_job_definition(j2)


def test_job_hash_changes_on_triggers() -> None:
    j1 = _make_job("jobs.mod.a", triggers=["schedule:0 8 * * *"])
    j2 = _make_job("jobs.mod.a", triggers=["schedule:0 9 * * *"])
    assert hash_job_definition(j1) != hash_job_definition(j2)


def test_job_hash_changes_on_entry_point() -> None:
    j1 = _make_job("jobs.mod.a")
    j2 = _make_job("jobs.mod.a")
    j2["entry_point"] = {**j2["entry_point"], "module": "other_module"}
    assert hash_job_definition(j1) != hash_job_definition(j2)


def test_job_hash_independent_per_job() -> None:
    """Changing one job must not change another job's hash — the core invariant
    that prevents the 'single-change invalidates everything' bug in reconciliation."""
    j_a = _make_job("jobs.mod.a")
    j_b_v1 = _make_job("jobs.mod.b")
    j_b_v2 = _make_job("jobs.mod.b", triggers=["schedule:0 8 * * *"])
    assert hash_job_definition(j_a) == hash_job_definition(_make_job("jobs.mod.a"))
    assert hash_job_definition(j_b_v1) != hash_job_definition(j_b_v2)


def test_bump_version_initial() -> None:
    manifest = _make_manifest([_make_job("jobs.mod.a")])
    version, new_hash, old_hash = bump_manifest_version(manifest)
    assert version == 0
    assert new_hash != ""
    assert old_hash == ""
    assert manifest["version"] == 0
    assert manifest["version_hash"] == new_hash


def test_bump_version_increments_on_change() -> None:
    manifest = _make_manifest([_make_job("jobs.mod.a")])
    bump_manifest_version(manifest)
    first_hash = manifest["version_hash"]

    manifest["jobs"].append(_make_job("jobs.mod.b"))
    version, new_hash, old_hash = bump_manifest_version(manifest)
    assert version == 1
    assert new_hash != first_hash
    assert old_hash == first_hash
    assert manifest["previous_hashes"] == [first_hash]


def test_bump_version_no_change() -> None:
    manifest = _make_manifest([_make_job("jobs.mod.a")])
    bump_manifest_version(manifest)
    first_hash = manifest["version_hash"]

    version, new_hash, _ = bump_manifest_version(manifest)
    assert version == 0
    assert new_hash == first_hash


def test_manifest_roundtrip_io() -> None:
    manifest = _make_manifest([_make_job("jobs.mod.a", triggers=["schedule:0 8 * * *"])])
    buf = BytesIO()
    save_manifest(manifest, buf)

    buf.seek(0)
    loaded = load_manifest(buf)

    assert loaded["deployment_module"] == manifest["deployment_module"]
    assert loaded["jobs"][0]["job_ref"] == "jobs.mod.a"
    assert "version_hash" in loaded


@pytest.mark.parametrize(
    "raw_start,raw_end,expected_start,expected_end",
    [
        # datetime aware → ISO with Z suffix (dlt custom encoder convention)
        (
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
            "2024-01-01T00:00:00Z",
            "2024-12-31T23:59:59Z",
        ),
        # datetime naive → ISO without offset (passes through .isoformat())
        (
            datetime(2024, 1, 1),
            datetime(2024, 12, 31, 23, 59, 59),
            "2024-01-01T00:00:00",
            "2024-12-31T23:59:59",
        ),
        # plain strings → round-trip unchanged
        (
            "2024-01-01T00:00:00Z",
            "2024-12-31T23:59:59Z",
            "2024-01-01T00:00:00Z",
            "2024-12-31T23:59:59Z",
        ),
    ],
    ids=["datetime-aware", "datetime-naive", "string"],
)
def test_manifest_interval_spec_accepts_datetime(
    raw_start: Any, raw_end: Any, expected_start: str, expected_end: str
) -> None:
    """TIntervalSpec accepts datetime at build time; round-trips as ISO string."""
    manifest = _make_manifest(
        [
            _make_job(
                "jobs.mod.a",
                triggers=["schedule:0 * * * *"],
                interval={"start": raw_start, "end": raw_end},
                allow_external_schedulers=True,
            )
        ]
    )
    buf = BytesIO()
    save_manifest(manifest, buf)
    buf.seek(0)
    loaded = load_manifest(buf)
    iv = loaded["jobs"][0]["interval"]
    assert iv["start"] == expected_start
    assert iv["end"] == expected_end
    assert isinstance(iv["start"], str)
    assert isinstance(iv["end"], str)


def test_migrate_manifest_same_version_is_noop() -> None:
    manifest: Dict[str, Any] = dict(_make_manifest([]))
    assert migrate_manifest(manifest, 1, 1) is manifest


def test_migrate_invalid_path() -> None:
    with pytest.raises(ValueError, match="no manifest migration path"):
        migrate_manifest({"engine_version": 99}, 99, 1)


def test_load_manifest_raises_invalid_manifest() -> None:
    """load_manifest raises InvalidManifest with validation field on bad manifest."""
    manifest = _make_manifest([_make_job("jobs.mod.a"), _make_job("jobs.mod.a")])  # duplicate ref
    buf = BytesIO()
    save_manifest(manifest, buf)
    buf.seek(0)
    with pytest.raises(InvalidManifest) as exc_info:
        load_manifest(buf)
    assert not exc_info.value.validation.is_valid
    assert any("duplicate" in e for e in exc_info.value.validation.errors)


def test_freshness_on_upstream_without_schedule_trigger(
    enable_interval_freshness: None,
) -> None:
    """Freshness constraint on upstream that has interval but no schedule or every trigger."""
    manifest = _make_manifest(
        [
            _make_job(
                "jobs.mod.up",
                triggers=["manual:jobs.mod.up"],
                interval={"start": "2024-01-01"},
            ),
            _make_job(
                "jobs.mod.down",
                triggers=["schedule:0 0 * * *"],
                interval={"start": "2024-01-01"},
                freshness=["job.is_matching_interval_fresh:jobs.mod.up"],
            ),
        ]
    )
    result = validate_manifest(manifest)
    assert not result.is_valid
    assert any("no schedule or every" in e for e in result.errors)


def test_multiple_interval_triggers_rejected() -> None:
    """Job with both schedule and every triggers is invalid."""
    manifest = _make_manifest(
        [
            _make_job(
                "jobs.mod.a",
                triggers=["schedule:0 0 * * *", "every:5m"],
            ),
        ]
    )
    result = validate_manifest(manifest)
    assert not result.is_valid
    assert any("multiple interval" in e for e in result.errors)


def test_two_schedule_triggers_rejected() -> None:
    """Job with two schedule triggers is invalid."""
    manifest = _make_manifest(
        [
            _make_job(
                "jobs.mod.a",
                triggers=["schedule:0 0 * * *", "schedule:0 8 * * *"],
            ),
        ]
    )
    result = validate_manifest(manifest)
    assert not result.is_valid
    assert any("multiple interval" in e for e in result.errors)


# generalized freshness validation


def test_is_fresh_on_every_upstream_valid() -> None:
    """job.is_fresh constraint on every:-triggered upstream is valid."""
    manifest = _make_manifest(
        [
            _make_job("jobs.mod.up", triggers=["every:5m"]),
            _make_job(
                "jobs.mod.down",
                triggers=["schedule:0 0 * * *"],
                freshness=["job.is_fresh:jobs.mod.up"],
            ),
        ]
    )
    result = validate_manifest(manifest)
    assert result.is_valid, f"errors: {result.errors}"


def test_is_fresh_on_schedule_no_interval_upstream_valid() -> None:
    """job.is_fresh constraint on schedule: upstream without interval is valid."""
    manifest = _make_manifest(
        [
            _make_job("jobs.mod.up", triggers=["schedule:0 0 * * *"]),
            _make_job(
                "jobs.mod.down",
                triggers=["schedule:0 0 * * *"],
                freshness=["job.is_fresh:jobs.mod.up"],
            ),
        ]
    )
    result = validate_manifest(manifest)
    assert result.is_valid, f"errors: {result.errors}"


def test_is_matching_interval_fresh_on_every_upstream_invalid(
    enable_interval_freshness: None,
) -> None:
    """job.is_matching_interval_fresh on every: upstream without interval is invalid."""
    manifest = _make_manifest(
        [
            _make_job("jobs.mod.up", triggers=["every:5m"]),
            _make_job(
                "jobs.mod.down",
                triggers=["schedule:0 0 * * *"],
                freshness=["job.is_matching_interval_fresh:jobs.mod.up"],
            ),
        ]
    )
    result = validate_manifest(manifest)
    assert not result.is_valid
    assert any("no interval" in e for e in result.errors)


def test_is_matching_interval_fresh_on_schedule_no_interval_invalid(
    enable_interval_freshness: None,
) -> None:
    """job.is_matching_interval_fresh on schedule: upstream without interval is invalid."""
    manifest = _make_manifest(
        [
            _make_job("jobs.mod.up", triggers=["schedule:0 0 * * *"]),
            _make_job(
                "jobs.mod.down",
                triggers=["schedule:0 0 * * *"],
                freshness=["job.is_matching_interval_fresh:jobs.mod.up"],
            ),
        ]
    )
    result = validate_manifest(manifest)
    assert not result.is_valid
    assert any("no interval" in e for e in result.errors)


def test_is_fresh_on_interactive_upstream_invalid() -> None:
    """job.is_fresh on interactive upstream is invalid."""
    manifest = _make_manifest(
        [
            _make_job(
                "jobs.mod.up",
                job_type="interactive",
                triggers=["http:"],
                expose={"interface": "gui"},
            ),
            _make_job(
                "jobs.mod.down",
                triggers=["schedule:0 0 * * *"],
                freshness=["job.is_fresh:jobs.mod.up"],
            ),
        ]
    )
    result = validate_manifest(manifest)
    assert not result.is_valid
    assert any("interactive" in e for e in result.errors)


def test_is_fresh_on_event_upstream_valid() -> None:
    """job.is_fresh on event-triggered upstream is valid."""
    manifest = _make_manifest(
        [
            _make_job("jobs.mod.other"),
            _make_job("jobs.mod.up", triggers=["job.success:jobs.mod.other"]),
            _make_job(
                "jobs.mod.down",
                triggers=["schedule:0 0 * * *"],
                freshness=["job.is_fresh:jobs.mod.up"],
            ),
        ]
    )
    result = validate_manifest(manifest)
    assert result.is_valid, f"errors: {result.errors}"


@pytest.mark.parametrize(
    "job_def,error_frag,warning_frag",
    [
        # valid batch job — no errors
        (
            _make_job("jobs.mod.ok", triggers=["schedule:0 8 * * *"]),
            None,
            None,
        ),
        # invalid trigger format
        (
            _make_job("jobs.mod.bad", triggers=["not_a_trigger"]),
            "Invalid trigger",
            None,
        ),
        # invalid freshness constraint format
        (
            _make_job("jobs.mod.bad", triggers=["schedule:0 0 * * *"], freshness=["bogus"]),
            "Invalid freshness",
            None,
        ),
        # batch + http trigger
        (
            _make_job("jobs.mod.bad", job_type="batch", triggers=["http:"]),
            "http trigger",
            None,
        ),
        # interactive without http
        (
            _make_job(
                "jobs.mod.odd",
                job_type="interactive",
                triggers=["schedule:0 8 * * *"],
                expose={"interface": "gui"},
            ),
            None,
            "no http trigger",
        ),
        # multiple interval-generating triggers
        (
            _make_job("jobs.mod.bad", triggers=["schedule:0 * * * *", "every:5m"]),
            "multiple interval-generating",
            None,
        ),
        # interval without schedule or every
        (
            _make_job(
                "jobs.mod.bad",
                triggers=["manual:jobs.mod.bad"],
                interval={"start": "2024-01-01"},
            ),
            "no schedule or every",
            None,
        ),
        # interval with every trigger — valid
        (
            _make_job(
                "jobs.mod.ok",
                triggers=["every:5m"],
                interval={"start": "2024-01-01"},
            ),
            None,
            None,
        ),
        # allow_external_schedulers without interval
        (
            _make_job(
                "jobs.mod.warn",
                triggers=["schedule:0 0 * * *"],
                allow_external_schedulers=True,
            ),
            None,
            "allow_external_schedulers",
        ),
        # misaligned interval start
        (
            _make_job(
                "jobs.mod.warn",
                triggers=["schedule:0 0 * * *"],
                interval={"start": "2024-01-01T06:30:00Z"},
            ),
            None,
            "snapped backward",
        ),
        # dashboard: wrong job type
        (
            {
                **_make_job(DASHBOARD_JOB_REF, job_type="batch", triggers=["http:"]),
                "expose": {"interface": "gui", "category": "dashboard"},
            },
            "must be interactive",
            None,
        ),
        # dashboard: wrong interface
        (
            {
                **_make_job(DASHBOARD_JOB_REF, job_type="interactive", triggers=["http:"]),
                "expose": {"interface": "mcp", "category": "dashboard"},
            },
            "must have gui interface",
            None,
        ),
        # dashboard: wrong category
        (
            {
                **_make_job(DASHBOARD_JOB_REF, job_type="interactive", triggers=["http:"]),
                "expose": {"interface": "gui", "category": "notebook"},
            },
            "must have dashboard category",
            None,
        ),
    ],
    ids=[
        "valid-batch",
        "invalid-trigger",
        "invalid-freshness",
        "batch-http",
        "interactive-no-http",
        "multiple-interval-triggers",
        "interval-no-schedule",
        "interval-with-every-valid",
        "external-schedulers-no-interval",
        "misaligned-interval-start",
        "dashboard-wrong-type",
        "dashboard-wrong-interface",
        "dashboard-wrong-category",
    ],
)
def test_validate_job_definition(
    job_def: TJobDefinition, error_frag: Optional[str], warning_frag: Optional[str]
) -> None:
    result = validate_job_definition(job_def)
    if error_frag:
        assert any(error_frag in e for e in result.errors), f"expected {error_frag!r} in {result}"
    else:
        assert result.errors == [], f"unexpected errors: {result.errors}"
    if warning_frag:
        assert any(
            warning_frag in w for w in result.warnings
        ), f"expected {warning_frag!r} in {result}"


def test_validate_job_definition_raises() -> None:
    """raise_on_error=True raises InvalidJobDefinition with job_ref and warnings."""
    job = _make_job("jobs.mod.bad", job_type="batch", triggers=["http:"])
    with pytest.raises(InvalidJobDefinition) as exc_info:
        validate_job_definition(job, raise_on_error=True)
    assert exc_info.value.job_ref == "jobs.mod.bad"
    assert "batch job" in str(exc_info.value)
    assert exc_info.value.validation.errors


def test_validate_job_definition_no_raise_on_valid() -> None:
    """raise_on_error=True does not raise for valid job."""
    job = _make_job("jobs.mod.ok", triggers=["schedule:0 8 * * *"])
    result = validate_job_definition(job, raise_on_error=True)
    assert result.errors == []


@pytest.mark.parametrize(
    "triggers,expected",
    [
        # schedule preferred over others
        (
            ["manual:jobs.mod.a", "schedule:0 8 * * *", "tag:daily"],
            "schedule:0 8 * * *",
        ),
        # every preferred over non-timed
        (
            ["manual:jobs.mod.a", "every:1h"],
            "every:1h",
        ),
        # schedule wins over every (first match)
        (
            ["schedule:0 0 * * *", "every:1h"],
            "schedule:0 0 * * *",
        ),
        # no schedule/every — first eligible trigger wins
        (
            ["job.success:jobs.mod.up", "manual:jobs.mod.a"],
            "job.success:jobs.mod.up",
        ),
        # manual/deployment skipped, next eligible wins
        (
            ["manual:jobs.mod.a", "deployment:prod", "tag:daily"],
            "tag:daily",
        ),
        # no triggers — None
        ([], None),
        # only manual — None (manual cannot be default)
        (["manual:jobs.mod.a"], None),
        # only deployment — None (deployment cannot be default)
        (["deployment:prod"], None),
        # only manual + deployment — None
        (["manual:jobs.mod.a", "deployment:prod"], None),
    ],
    ids=[
        "schedule-preferred",
        "every-preferred",
        "schedule-over-every",
        "first-eligible-fallback",
        "skip-manual-and-deployment",
        "empty-triggers",
        "manual-only",
        "deployment-only",
        "manual-and-deployment-only",
    ],
)
def test_compute_default_trigger(triggers: List[str], expected: Optional[str]) -> None:
    job = _make_job("jobs.mod.a", triggers=triggers)
    result = compute_default_trigger(job)
    if expected is None:
        assert result is None
    else:
        assert result == TTrigger(expected)


def test_expand_triggers_adds_pipeline_name() -> None:
    """expand_triggers adds pipeline_name: trigger from deliver spec."""
    from dlt._workspace.deployment.manifest import expand_triggers

    job = _make_job("jobs.mod.a", triggers=["schedule:0 8 * * *"])
    job["deliver"] = {"pipeline_name": "analytics"}
    expanded = expand_triggers(job)
    assert TTrigger("pipeline_name:analytics") in expanded
    assert TTrigger("manual:jobs.mod.a") in expanded  # manual also added


def test_expand_triggers_no_pipeline_name() -> None:
    """expand_triggers skips pipeline_name when deliver has no pipeline_name."""
    from dlt._workspace.deployment.manifest import expand_triggers

    job = _make_job("jobs.mod.a", triggers=["schedule:0 8 * * *"])
    expanded = expand_triggers(job)
    assert not any(str(t).startswith("pipeline_name:") for t in expanded)
