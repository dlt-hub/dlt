"""Shared builders for job definitions and manifests used across workspace tests."""

from typing import Any, List, Optional, Sequence

from dlt._workspace.deployment.launchers import LAUNCHER_JOB
from dlt._workspace.deployment.typing import (
    MANIFEST_ENGINE_VERSION,
    TEntryPoint,
    TExecuteSpec,
    TJobDefinition,
    TJobRef,
    TJobsDeploymentManifest,
    TJobType,
    TTrigger,
)


def make_job(
    job_ref: str,
    *,
    job_type: TJobType = "batch",
    triggers: Optional[Sequence[str]] = None,
    module: str = "test_module",
    function: Optional[str] = None,
    launcher: str = LAUNCHER_JOB,
    concurrency: Optional[int] = 1,
    **fields: Any,
) -> TJobDefinition:
    """Builds one job definition; `function` defaults to the last segment of `job_ref`.

    Unknown keyword arguments are written onto the definition verbatim, so migration tests
    can still build engine-1 field names that `TJobDefinition` no longer declares.
    """
    entry_point: TEntryPoint = {
        "module": module,
        "function": function or job_ref.split(".")[-1],
        "job_type": job_type,
        "launcher": launcher,
    }
    job: TJobDefinition = {
        "job_ref": TJobRef(job_ref),
        "entry_point": entry_point,
        "triggers": [TTrigger(t) for t in triggers or []],
        "execute": TExecuteSpec() if concurrency is None else TExecuteSpec(concurrency=concurrency),
    }
    job.update(fields)  # type: ignore[typeddict-item]
    return job


def make_manifest(jobs: List[TJobDefinition], **fields: Any) -> TJobsDeploymentManifest:
    manifest: TJobsDeploymentManifest = {
        "engine_version": MANIFEST_ENGINE_VERSION,
        "created_at": "2026-03-10T00:00:00Z",
        "deployment_module": "test",
        "jobs": jobs,
    }
    manifest.update(fields)  # type: ignore[typeddict-item]
    return manifest
