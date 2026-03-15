"""Tests for manifest validation, hashing, versioning, and IO."""

from io import BytesIO
from typing import Any, List, Optional

import pytest

from dlt._workspace.deployment.manifest import (
    bump_manifest_version,
    generate_manifest_hash,
    load_manifest,
    migrate_manifest,
    save_manifest,
    validate_manifest,
)
from dlt._workspace.deployment.typing import (
    MANIFEST_ENGINE_VERSION,
    TDeploymentManifest,
    TEntryPoint,
    TExecutionSpec,
    TJobDefinition,
    TJobRef,
    TJobType,
    TTrigger,
)


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
    }
    job: TJobDefinition = {
        "job_ref": TJobRef(job_ref),
        "entry_point": entry_point,
        "triggers": [TTrigger(t) for t in triggers] if triggers else [],
        "execution": TExecutionSpec(concurrency=1),
        "starred": False,
    }
    job.update(kwargs)  # type: ignore[typeddict-item]
    return job


def _make_manifest(jobs: List[TJobDefinition], **kwargs: Any) -> TDeploymentManifest:
    manifest: TDeploymentManifest = {
        "engine_version": MANIFEST_ENGINE_VERSION,
        "files": [],
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
    ],
    ids=["duplicate-ref", "batch-http", "interactive-no-http", "invalid-trigger"],
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
    assert result.is_valid
    assert "jobs.mod.b" in result.unresolved_triggers
    assert "jobs.mod.missing" in result.unresolved_triggers["jobs.mod.b"]
    assert any("unknown jobs" in w for w in result.warnings)


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
    "from_v,to_v,input_dict,check",
    [
        # v1 -> v2 adds defaults
        (
            1,
            2,
            {"engine_version": 1, "files": []},
            lambda r: r["engine_version"] == 2 and r["jobs"] == [] and "created_at" in r,
        ),
        # same version = noop
        (
            2,
            2,
            None,
            lambda r: True,
        ),
    ],
    ids=["v1-to-v2", "same-version"],
)
def test_migrate_manifest(from_v, to_v, input_dict, check) -> None:
    if input_dict is None:
        input_dict = _make_manifest([])
    result = migrate_manifest(input_dict, from_v, to_v)
    assert check(result)


def test_migrate_invalid_path() -> None:
    with pytest.raises(ValueError, match="no manifest migration path"):
        migrate_manifest({"engine_version": 99}, 99, 2)
