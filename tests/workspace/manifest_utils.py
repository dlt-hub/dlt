"""Shared builders for job definitions and manifests used across workspace tests."""

import os
from typing import Any, Dict, List, Optional, Sequence

from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.deployment.file_selector import ConfigurationFileSelector
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
from dlt._workspace.typing import TEnvironmentDefinition

MANIFEST_CASES_DIR = os.path.join(os.path.dirname(__file__), "cases", "manifests")


def manifest_case_path(name: str) -> str:
    """Path of a stored manifest under `cases/manifests`, for example `ev1/interval_jobs`."""
    return os.path.join(MANIFEST_CASES_DIR, name + ".manifest.json")


def job_profiles(manifest: TJobsDeploymentManifest) -> Dict[str, Optional[str]]:
    """The profile that each job of `manifest` runs with, by job ref."""
    return {j["job_ref"]: j.get("require", {}).get("profile") for j in manifest["jobs"]}


def shipped_settings(ctx: WorkspaceRunContext, environment: TEnvironmentDefinition) -> List[str]:
    """Names of the settings files that a deployment of `ctx` to `environment` ships."""
    return [rel.as_posix() for _, rel in ConfigurationFileSelector(ctx, environment)]


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
        "engine_version": MANIFEST_ENGINE_VERSION,
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
