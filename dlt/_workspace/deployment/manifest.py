import base64
import contextlib
import hashlib
import io
import os
import subprocess
import sys
from datetime import datetime, timezone
from contextlib import contextmanager
from copy import copy
from importlib import import_module
from typing import Any, BinaryIO, Dict, Iterator, List, NamedTuple, Optional, Set, Tuple
from types import ModuleType

from dlt.common import json
from dlt.common.exceptions import DictValidationException
from dlt.common.time import ensure_datetime_in_tz
from dlt.common.typing import DictStrAny
from dlt.common.validation import validate_dict
from dlt.common.warnings import apply_deprecations
from dlt.reflection.script_inspector import no_pipeline_execution

from dlt._workspace.deployment.decorators import AgentJobFactory, JobFactory
from dlt._workspace.deployment.detectors import (
    detect_local_module,
    detect_module_job,
    is_local_module,
)
from dlt._workspace.deployment.exceptions import (
    InvalidFreshnessConstraint,
    InvalidJobDefinition,
    InvalidJobRef,
    InvalidManifest,
    InvalidTrigger,
    ManifestEngineNoUpgradePath,
    JobValidationResult,
    ManifestValidationResult,
)
from dlt._workspace.deployment._job_ref import parse_job_ref
from dlt._workspace.profile import LOCAL_PROFILES, is_local_profile
from dlt._workspace.deployment import trigger as _triggers
from dlt._workspace.deployment._trigger_helpers import (
    is_selector,
    match_triggers_with_selectors,
    maybe_parse_schedule,
    parse_trigger,
)
from dlt._workspace.deployment.freshness import parse_freshness_constraint
from dlt._workspace.deployment.launchers import LAUNCHER_DASHBOARD
from dlt._workspace.deployment.typing import (
    DASHBOARD_JOB_REF,
    DEFAULT_DEPLOYMENT_MODULE,
    MANIFEST_ENGINE_VERSION,
    TEntryPoint,
    TExecuteSpec,
    TExposeSpec,
    TJobsDeploymentManifest,
    TFreshnessConstraint,
    TJobDefinition,
    TJobDefinitionDeprecated,
    TJobRef,
    TTrigger,
    WORKSPACE_DEPRECATED_SINCE,
)

DEPLOYMENT_ENGINE_VERSION = 1
"""Engine version of package files manifests (`TFilesManifest`), independent of job definitions."""

_JOB_EVENT_TRIGGERS = ("job.success", "job.fail")

_HASH_EXCLUDE_KEYS = ("version", "version_hash", "previous_hashes", "created_at")
_MAX_PREVIOUS_HASHES = 10


def generate_manifest_hash(manifest: TJobsDeploymentManifest) -> str:
    """SHA3-256 content hash of manifest, excluding version metadata."""
    manifest_copy = copy(manifest)
    for key in _HASH_EXCLUDE_KEYS:
        manifest_copy.pop(key, None)  # type: ignore[misc]
    content = json.dumpb(manifest_copy, sort_keys=True)
    h = hashlib.sha3_256(content)
    return base64.b64encode(h.digest()).decode("ascii")


def hash_job_definition(job_def: TJobDefinition) -> str:
    """SHA3-256 content hash of a single job definition (canonical, sorted).

    Uses the same serialization and algorithm as generate_manifest_hash so a
    caller can derive per-job hashes for change detection without recomputing
    the whole manifest hash.
    """
    content = json.dumpb(job_def, sort_keys=True)
    return base64.b64encode(hashlib.sha3_256(content).digest()).decode("ascii")


def bump_manifest_version(
    manifest: TJobsDeploymentManifest,
) -> Tuple[int, str, str]:
    """Bump version and hash if content modified.

    Returns:
        Tuple of (new_version, new_hash, old_hash).
    """
    new_hash = generate_manifest_hash(manifest)
    old_hash = manifest.get("version_hash", "")
    version = manifest.get("version", 0)

    if not old_hash:
        pass
    elif new_hash != old_hash:
        version += 1
        previous = list(manifest.get("previous_hashes", []))
        previous.insert(0, old_hash)
        manifest["previous_hashes"] = previous[:_MAX_PREVIOUS_HASHES]

    manifest["version"] = version
    manifest["version_hash"] = new_hash
    return version, new_hash, old_hash


def migrate_job_definition(
    job_dict: DictStrAny, from_engine: int, to_engine: int
) -> TJobDefinition:
    """Migrate a single job definition dict between engine versions, in place.

    WARNING: if you add new migration you MUST make sure that build_runtime_entry_point has downgrade
    path for previous dlt versions.
    """
    if from_engine == to_engine:
        return job_dict  # type: ignore[return-value]
    if from_engine == 1 and to_engine > 1:
        # engine 2: allow_external_schedulers -> incremental_mode, refresh -> refresh_propagation
        apply_deprecations(
            TJobDefinitionDeprecated,
            job_dict,
            path="jobs",
            since=WORKSPACE_DEPRECATED_SINCE,
            warn=False,
        )
        from_engine = 2

    if from_engine != to_engine:
        raise ManifestEngineNoUpgradePath("job definition", from_engine, to_engine)
    return job_dict  # type: ignore[return-value]


def migrate_manifest(
    manifest_dict: DictStrAny, from_engine: int, to_engine: int
) -> TJobsDeploymentManifest:
    """Migrate a manifest dict between engine versions.

    WARNING: if you add new migration you MUST make sure that build_runtime_entry_point has downgrade
    path for previous dlt versions.
    """
    if from_engine == to_engine:
        return manifest_dict  # type: ignore[return-value]
    # checked before the job loop so an unreachable path is reported against the manifest
    # rather than against whichever job happened to be migrated first
    if not 1 <= from_engine < to_engine:
        raise ManifestEngineNoUpgradePath("manifest", from_engine, to_engine)
    for job in manifest_dict.get("jobs", []):
        migrate_job_definition(job, from_engine, to_engine)
    # manifest-level migrations per engine version
    if from_engine == 1 and to_engine > 1:
        # engine 2: no manifest-level changes
        from_engine = 2

    if from_engine != to_engine:
        raise ManifestEngineNoUpgradePath("manifest", from_engine, to_engine)
    manifest_dict["engine_version"] = to_engine
    return manifest_dict  # type: ignore[return-value]


def save_manifest(manifest: TJobsDeploymentManifest, f: BinaryIO) -> str:
    """Bump version, serialize manifest, and write to binary IO.

    Returns:
        The new version hash.
    """
    _, new_hash, _ = bump_manifest_version(manifest)
    data = json.dumpb(manifest)
    f.write(data)
    return new_hash


def load_manifest(f: BinaryIO) -> TJobsDeploymentManifest:
    """Read, migrate, and validate a manifest from binary IO."""
    data = f.read()
    manifest_dict: DictStrAny = json.loadb(data)
    engine_version = manifest_dict.get("engine_version", 1)
    manifest = migrate_manifest(manifest_dict, engine_version, MANIFEST_ENGINE_VERSION)

    result = validate_manifest(manifest)
    if not result.is_valid:
        raise InvalidManifest(result)

    return manifest


def _newtype_validator(path: str, pk: str, pv: Any, t: Any) -> bool:
    """Custom validator for NewType fields (TTrigger, TJobRef, TFreshnessConstraint)."""
    from dlt._workspace.deployment.exceptions import (
        DeploymentException,
    )

    if t is TFreshnessConstraint:
        try:
            parse_freshness_constraint(pv)
        except (DeploymentException, TypeError) as e:
            raise DictValidationException(str(e), path, t, pk, pv)
        return True
    if t is TTrigger:
        try:
            parse_trigger(pv)
        except (DeploymentException, TypeError) as e:
            raise DictValidationException(str(e), path, t, pk, pv)
        return True
    if t is TJobRef:
        try:
            parse_job_ref(pv)
        except (DeploymentException, TypeError) as e:
            raise DictValidationException(str(e), path, t, pk, pv)
        return True
    return False


def selects_job(job_def: TJobDefinition, selector: str) -> bool:
    """Whether a selector picks this job: by its type, one of its triggers, or its ref."""
    return bool(
        match_triggers_with_selectors(
            job_def["entry_point"].get("job_type", "batch"),
            expand_triggers(job_def),
            [selector],
            job_ref=job_def["job_ref"],
        )
    )


def expand_trigger_selectors(jobs: List[TJobDefinition]) -> List[str]:
    """Replaces selector `job.success:` / `job.fail:` triggers with one trigger per matching job.

    The runtime looks a completion trigger up by exact string, so a selector has to become
    concrete refs here. A selector never expands onto the job that declares it, and an
    interactive job is never a target: it has no completion to report.

    Returns:
        List[str]: Warnings for selectors that matched no job.
    """
    warnings: List[str] = []
    candidates = [
        job_def for job_def in jobs if job_def["entry_point"].get("job_type") != "interactive"
    ]

    for job_def in jobs:
        own_ref = job_def["job_ref"]
        expanded: List[TTrigger] = []
        changed = False
        for trigger in job_def.get("triggers", []):
            trigger_type, _, expr = trigger.partition(":")
            if trigger_type not in _JOB_EVENT_TRIGGERS or not is_selector(expr):
                if trigger not in expanded:
                    expanded.append(trigger)
                continue
            changed = True
            matched = [
                candidate["job_ref"]
                for candidate in candidates
                if candidate["job_ref"] != own_ref and selects_job(candidate, expr)
            ]
            if not matched:
                warnings.append(f"job {own_ref!r}: trigger {trigger!r} matched no job")
            for ref in matched:
                concrete = TTrigger(f"{trigger_type}:{ref}")
                if concrete not in expanded:
                    expanded.append(concrete)
        if changed:
            job_def["triggers"] = expanded
    return warnings


def expand_triggers(job_def: TJobDefinition) -> List[TTrigger]:
    """Expand triggers with synthetic triggers from expose and deliver specs.

    Adds ``manual:``, ``tag:``, and ``pipeline_name:`` triggers.
    Returns a new list — does not modify job_def.
    """
    triggers = list(job_def.get("triggers", []))
    expose = job_def.get("expose", {})
    for t in expose.get("tags", []):
        tag_trigger = _triggers.tag(t)
        if tag_trigger not in triggers:
            triggers.append(tag_trigger)
    if expose.get("manual", True):
        manual_trigger = _triggers.manual(job_def["job_ref"])
        if manual_trigger not in triggers:
            triggers.append(manual_trigger)
    pipeline = job_def.get("deliver", {}).get("pipeline_name")
    if pipeline:
        pn_trigger = _triggers.pipeline_name(pipeline)
        if pn_trigger not in triggers:
            triggers.append(pn_trigger)
    return triggers


NO_DEFAULT_TRIGGER_TYPES = ("manual", "deployment", "job.success", "job.fail")
"""Trigger types that never become a job's default.

A job event names the upstream run that fired it, so standing in for a manual run would tell
the job a job failed when none did.
"""


def compute_default_trigger(job_def: TJobDefinition) -> Optional[TTrigger]:
    """Pick the default trigger for a job: prefer schedule/every, else first eligible trigger."""
    default: Optional[TTrigger] = None
    for t in job_def.get("triggers", []):
        parsed = parse_trigger(t)
        if parsed.type in ("schedule", "every"):
            return t
        if default is None and parsed.type not in NO_DEFAULT_TRIGGER_TYPES:
            default = t
    return default


def validate_job_definition(
    job_def: TJobDefinition,
    raise_on_error: bool = False,
) -> JobValidationResult:
    """Validate a single job definition (self-contained checks only).

    Parses triggers and freshness constraints for format errors, checks
    type/trigger consistency, interval alignment, and dashboard constraints.

    Args:
        job_def: The job definition to validate.
        raise_on_error: If True, raise InvalidJobDefinition when errors found.

    Raises:
        InvalidJobDefinition: When raise_on_error is True and validation fails.
    """
    errors: List[str] = []
    warnings: List[str] = []
    ref = job_def["job_ref"]

    # validate job_ref format
    try:
        section, name_part = parse_job_ref(ref)
    except InvalidJobRef as e:
        errors.append(str(e))
    else:
        # 'py' is reserved to avoid clashes with .py file extensions in
        # filesystem-mapped paths and module imports
        if section == "py":
            errors.append(f"job {ref!r}: section 'py' is reserved")
        if name_part == "py":
            errors.append(f"job {ref!r}: name 'py' is reserved")

    # parse triggers — collect format errors and trigger types
    trigger_types: Set[str] = set()
    for t in job_def.get("triggers", []):
        try:
            parsed = parse_trigger(t)
            trigger_types.add(parsed.type)
            if parsed.type in _JOB_EVENT_TRIGGERS and parsed.expr == ref:
                errors.append(
                    f"job {ref!r}: trigger {t!r} makes the job trigger itself, which never"
                    " terminates"
                )
        except InvalidTrigger as e:
            errors.append(f"job {ref!r}: {e}")

    # parse freshness constraints
    for constraint in job_def.get("freshness", []):
        try:
            parse_freshness_constraint(constraint)
        except InvalidFreshnessConstraint as e:
            errors.append(f"job {ref!r}: {e}")

    # job type vs trigger consistency
    job_type = job_def["entry_point"]["job_type"]
    if job_type == "batch" and "http" in trigger_types:
        errors.append(f"batch job {ref!r} has http trigger — use interactive job type")
    if job_type == "interactive" and "http" not in trigger_types:
        warnings.append(f"interactive job {ref!r} has no http trigger")

    # interval validation
    has_interval = "interval" in job_def
    interval_trigger_count = sum(
        1
        for t in job_def.get("triggers", [])
        if t.startswith("schedule:") or t.startswith("every:")
    )
    if interval_trigger_count > 1:
        errors.append(
            f"job {ref!r} has multiple interval-generating triggers"
            " (only one schedule: or every: allowed)"
        )
    if has_interval and "schedule" not in trigger_types and "every" not in trigger_types:
        errors.append(f"job {ref!r} has interval but no schedule or every trigger")
    if has_interval and "schedule" in trigger_types:
        # inline import for croniter
        from dlt._workspace.deployment.interval import cron_floor

        cron_expr = maybe_parse_schedule(job_def)
        if cron_expr:
            iv = job_def["interval"]
            raw_start = ensure_datetime_in_tz(iv["start"], timezone.utc)
            if cron_floor(cron_expr, raw_start) != raw_start:
                warnings.append(
                    f"job {ref!r} interval start ({iv['start']}) is not a"
                    f" cron tick for {cron_expr!r} — will be snapped backward"
                )
            end_str = iv.get("end")
            if end_str:
                raw_end = ensure_datetime_in_tz(end_str, timezone.utc)
                if cron_floor(cron_expr, raw_end) != raw_end:
                    warnings.append(
                        f"job {ref!r} interval end ({end_str}) is not a"
                        f" cron tick for {cron_expr!r} — will be snapped backward"
                    )

    if job_def.get("incremental_mode") == "interval" and not has_interval:
        warnings.append(f"job {ref!r} has incremental_mode 'interval' but no interval")

    if job_def.get("auto_refresh_pipeline_mode") and job_def.get("refresh_propagation") == "block":
        warnings.append(
            f"job {ref!r} has auto_refresh_pipeline_mode but refresh_propagation 'block'"
            " prevents refresh runs"
        )

    require = job_def.get("require") or {}
    declared_profile = require.get("profile")
    if declared_profile is not None and is_local_profile(declared_profile):
        errors.append(
            f"job {ref!r}: require.profile {declared_profile!r} is a local-only profile"
            " and cannot be assumed by deployed jobs"
            f" (local-only profiles: {', '.join(sorted(LOCAL_PROFILES))})"
        )

    instance = require.get("instance")
    if instance is not None and not isinstance(instance, dict):
        errors.append(f"job {ref!r}: require.instance must be an object")

    # dashboard job constraints
    if ref == DASHBOARD_JOB_REF:
        ep = job_def["entry_point"]
        expose = job_def.get("expose", {})
        if ep["job_type"] != "interactive":
            errors.append(f"dashboard job {DASHBOARD_JOB_REF!r} must be interactive")
        if expose.get("interface") != "gui":
            errors.append(f"dashboard job {DASHBOARD_JOB_REF!r} must have gui interface")
        if expose.get("category") != "dashboard":
            errors.append(f"dashboard job {DASHBOARD_JOB_REF!r} must have dashboard category")

    result = JobValidationResult(errors=errors, warnings=warnings)
    if raise_on_error and result.errors:
        raise InvalidJobDefinition(ref, result)
    return result


def validate_manifest(manifest: TJobsDeploymentManifest) -> ManifestValidationResult:
    """Validate a deployment manifest structurally and for consistency."""
    errors: List[str] = []
    warnings: List[str] = []
    unresolved: Dict[str, List[str]] = {}

    try:
        validate_dict(TJobsDeploymentManifest, manifest, ".", validator_f=_newtype_validator)
    except DictValidationException as e:
        errors.append(str(e))
        return ManifestValidationResult(
            is_valid=False, errors=errors, warnings=warnings, unresolved_triggers=unresolved
        )

    jobs = manifest.get("jobs", [])

    # per-job validation
    for job_def in jobs:
        result = validate_job_definition(job_def)
        errors.extend(result.errors)
        warnings.extend(result.warnings)

    # duplicate job refs
    seen_refs: Set[str] = set()
    for job_def in jobs:
        ref = job_def["job_ref"]
        if ref in seen_refs:
            errors.append(f"duplicate job_ref: {ref!r}")
        seen_refs.add(ref)

    # duplicate entry points
    seen_entry_points: Dict[str, str] = {}
    for job_def in jobs:
        ep = job_def["entry_point"]
        key = f"{ep['module']}:{ep.get('function')}"
        if key in seen_entry_points:
            warnings.append(
                f"jobs {seen_entry_points[key]!r} and {job_def['job_ref']!r}"
                f" share the same entry point {key!r}"
            )
        seen_entry_points[key] = job_def["job_ref"]

    # unresolved job event triggers — manifest must be self-contained
    for job_def in jobs:
        for trigger in job_def.get("triggers", []):
            parsed = parse_trigger(trigger)
            if parsed.type in ("job.success", "job.fail") and parsed.expr not in seen_refs:
                unresolved.setdefault(job_def["job_ref"], []).append(str(parsed.expr))

    if unresolved:
        for job_ref, refs in unresolved.items():
            errors.append(f"job {job_ref!r} has triggers referencing unknown jobs: {refs}")

    # freshness upstream resolution (requires cross-job lookup)
    jobs_by_ref = {j["job_ref"]: j for j in jobs}
    for job_def in jobs:
        ref = job_def["job_ref"]
        for constraint in job_def.get("freshness", []):
            try:
                fc = parse_freshness_constraint(constraint)
            except InvalidFreshnessConstraint:
                continue  # already reported by validate_job_definition
            us_ref = fc.expr
            us_job = jobs_by_ref.get(TJobRef(us_ref))
            if us_job is None:
                errors.append(f"job {ref!r} has freshness constraint on unknown job {us_ref!r}")
                continue

            if fc.type == "job.is_matching_interval_fresh":
                # inline import for croniter
                from dlt._workspace.deployment import interval as _interval

                if not _interval.INTERVAL_FRESHNESS_ENABLED:
                    raise NotImplementedError(
                        f"job {ref!r} declares {fc.type!r} on {us_ref!r}: not yet implemented"
                    )
                if "interval" not in us_job:
                    errors.append(
                        f"job {ref!r} has {fc.type} constraint on {us_ref!r}"
                        " but upstream has no interval"
                    )
                    continue
                if not maybe_parse_schedule(us_job):
                    errors.append(
                        f"job {ref!r} has {fc.type} constraint on {us_ref!r}"
                        " but upstream has no schedule trigger"
                    )
            elif fc.type == "job.is_fresh":
                if us_job["entry_point"]["job_type"] == "interactive":
                    errors.append(
                        f"job {ref!r} has {fc.type} constraint on {us_ref!r}"
                        " but upstream is an interactive job"
                    )

    return ManifestValidationResult(
        is_valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        unresolved_triggers=unresolved,
    )


@contextmanager
def _suppress_framework_noise() -> Iterator[None]:
    """Suppress noisy output from frameworks during module import."""

    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        yield


def import_deployment_module(module_name: str) -> ModuleType:
    """Import a deployment module with pipeline execution and framework noise suppressed."""
    with no_pipeline_execution(), _suppress_framework_noise():
        return import_module(module_name)


def default_dashboard_job() -> TJobDefinition:
    """Default workspace dashboard job definition."""
    return {
        "job_ref": DASHBOARD_JOB_REF,
        "entry_point": TEntryPoint(
            module="dlt._workspace.helpers.dashboard.dlt_dashboard",
            function=None,
            job_type="interactive",
            launcher=LAUNCHER_DASHBOARD,
        ),
        "expose": TExposeSpec(
            interface="gui",
            category="dashboard",
            manual=True,
        ),
        "triggers": [TTrigger("http:")],
        "execute": TExecuteSpec(concurrency=1),
        "require": {"dependency_groups": [DASHBOARD_JOB_REF]},
        "description": "Workspace dashboard",
    }


def default_dashboard_manifest() -> TJobsDeploymentManifest:
    """Build a manifest containing only the default workspace dashboard job."""
    module = ModuleType(DEFAULT_DEPLOYMENT_MODULE)
    module.__all__ = []  # type: ignore[attr-defined]
    manifest, _ = generate_manifest(module)
    return manifest


def generate_manifest(
    deployment_module: ModuleType,
    use_all: bool = True,
) -> Tuple[TJobsDeploymentManifest, List[str]]:
    """Generate a deployment manifest from a deployment module.

    Discovers jobs by inspecting module-level objects: `JobFactory` instances
    and framework modules (marimo, FastMCP, streamlit).

    Args:
        deployment_module (ModuleType): The imported deployment module.
        use_all (bool): If `True`, only names in `__all__` are discovered.
            If `False`, scans `__dir__()` directly (for ad-hoc deployments).

    Returns:
        Tuple of (manifest, warnings). Warnings include non-discoverable names.
    """
    warnings: List[str] = []
    jobs: List[TJobDefinition] = []

    # first: check if the deployment module itself is a framework app
    self_job = detect_module_job(deployment_module)
    if self_job is not None:
        jobs.append(self_job)
    else:
        # scan module contents for JobFactory instances and sub-modules
        has_all = hasattr(deployment_module, "__all__")
        iterate_all = has_all and use_all
        if iterate_all:
            names = list(deployment_module.__all__)
        else:
            if not has_all and use_all:
                warnings.append(
                    f"module {deployment_module.__name__!r} has no __all__, scanning full"
                    " dictionary instead. Consider adding all jobs and modules you want to deploy"
                    " to __all__ this prevents accidental deployments and avoids costly detection"
                    " for all elements of the module."
                )
            names = dir(deployment_module)

        for name in names:
            obj = getattr(deployment_module, name, None)
            if obj is None and iterate_all:
                warnings.append(f"name {name!r} listed in __all__ but not found in module")
                continue

            if isinstance(obj, JobFactory):
                # a declared agent has no function to take its module and section from
                if isinstance(obj, AgentJobFactory) and obj.is_declared:
                    obj.declare(deployment_module.__name__, name)
                jobs.append(obj.to_job_definition())
            elif isinstance(obj, ModuleType):
                # __all__: trust the user; __dir__ scan: filter to local modules
                if not iterate_all and not is_local_module(obj, deployment_module):
                    continue
                framework_job = detect_module_job(obj)
                if framework_job is not None:
                    jobs.append(framework_job)
                else:
                    local_job = detect_local_module(obj, deployment_module)
                    if local_job is not None:
                        jobs.append(local_job)
            else:
                if iterate_all and not name.startswith("_"):
                    warnings.append(f"name {name!r} is not a recognized job or framework module")

        # Ad-hoc fallback: if scanning produced no user jobs and the deployment
        # module is itself a plain local Python file, promote it as a batch job.
        if not jobs and not use_all:
            self_local = detect_local_module(deployment_module, deployment_module)
            if self_local is not None:
                jobs.append(self_local)

    # auto-include workspace dashboard only when processing the canonical
    # `__deployment__` module — ad-hoc / single-file deployments don't get one
    is_workspace_deployment = (
        deployment_module.__name__.rsplit(".", 1)[-1] == DEFAULT_DEPLOYMENT_MODULE
    )
    if is_workspace_deployment and not any(j["job_ref"] == DASHBOARD_JOB_REF for j in jobs):
        jobs.append(default_dashboard_job())

    # selectors are a source-level concept: a stored manifest is always fully expanded,
    # so this must run before default_trigger and before validation
    warnings.extend(expand_trigger_selectors(jobs))

    # set expose.manual default and compute default_trigger
    for job_def in jobs:
        expose = job_def.setdefault("expose", {})
        expose.setdefault("manual", True)

        default = compute_default_trigger(job_def)
        if default is not None:
            job_def["default_trigger"] = default

    manifest: TJobsDeploymentManifest = {
        "engine_version": MANIFEST_ENGINE_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "deployment_module": deployment_module.__name__,
        "jobs": jobs,
    }

    # use only the first non-empty line of the module docstring as the workspace description
    raw_doc = getattr(deployment_module, "__doc__", None) or ""
    first_line = raw_doc.strip().split("\n", 1)[0].strip()
    if first_line:
        manifest["description"] = first_line

    tags = getattr(deployment_module, "__tags__", None)
    if tags:
        manifest["tags"] = list(tags)

    return manifest, warnings


def _resolve_module_name(name_or_path: str) -> str:
    """Convert a file path to a module name, or return as-is if already a module name."""
    if name_or_path.endswith(".py"):
        name_or_path = name_or_path[:-3]
    if os.sep in name_or_path or "/" in name_or_path:
        name_or_path = name_or_path.replace(os.sep, ".").replace("/", ".")
    return name_or_path.lstrip(".")


def manifest_from_module(
    name_or_path: str,
    use_all: bool = True,
    isolated: bool = False,
) -> Tuple[TJobsDeploymentManifest, List[str]]:
    """Import a module, generate a manifest, and validate it.

    Args:
        name_or_path: Python module name or file path.
        use_all: Use `__all__` for job discovery.
        isolated: Run in a subprocess to avoid polluting the current interpreter.

    Returns:
        Tuple of (manifest, warnings).

    Raises:
        InvalidManifest: If the generated manifest fails validation.
    """
    if isolated:
        return _manifest_from_module_isolated(name_or_path, use_all)
    return _manifest_from_module_inprocess(name_or_path, use_all)


def _manifest_from_module_isolated(
    name_or_path: str,
    use_all: bool,
) -> Tuple[TJobsDeploymentManifest, List[str]]:
    worker = "dlt._workspace.deployment._manifest_worker"
    cmd = [sys.executable, "-m", worker, name_or_path]
    if not use_all:
        cmd.append("--no-use-all")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return _parse_worker_result(proc.stdout, proc.stderr, proc.returncode)


_WORKER_ERROR_TYPES: Dict[str, type] = {
    "ModuleNotFoundError": ModuleNotFoundError,
    "ImportError": ImportError,
    "SyntaxError": SyntaxError,
    "InvalidManifest": InvalidManifest,
    "InvalidTrigger": InvalidTrigger,
    "InvalidFreshnessConstraint": InvalidFreshnessConstraint,
    "InvalidJobDefinition": InvalidJobDefinition,
    "InvalidJobRef": InvalidJobRef,
}


def _raise_from_worker_payload(payload: Dict[str, Any]) -> None:
    msg = payload.get("error", "unknown error")
    exc_cls = _WORKER_ERROR_TYPES.get(payload.get("error_type", ""))
    if exc_cls is InvalidManifest:
        raise InvalidManifest.from_message(msg)
    if exc_cls is not None:
        raise exc_cls(msg)
    raise InvalidManifest.from_message(msg)


def _parse_worker_result(
    stdout: str, stderr: str, returncode: int
) -> Tuple[TJobsDeploymentManifest, List[str]]:
    if returncode != 0:
        try:
            _raise_from_worker_payload(json.typed_loads(stdout.strip()))
        except (ValueError, KeyError):
            pass
        raise InvalidManifest.from_message(
            stderr.strip() or stdout.strip() or f"worker exited with code {returncode}"
        )

    payload = json.typed_loads(stdout)
    if "error" in payload:
        _raise_from_worker_payload(payload)
    return payload["manifest"], payload["warnings"]


def _manifest_from_module_inprocess(
    name_or_path: str,
    use_all: bool,
) -> Tuple[TJobsDeploymentManifest, List[str]]:
    module_name = _resolve_module_name(name_or_path)

    # try importing as-is first (works for proper packages with relative imports),
    # fall back to adding cwd to sys.path for standalone modules
    try:
        mod = import_deployment_module(module_name)
    except ModuleNotFoundError:
        cwd = os.getcwd()
        if cwd not in sys.path:
            sys.path.insert(0, cwd)
        mod = import_deployment_module(module_name)
    manifest, gen_warnings = generate_manifest(mod, use_all=use_all)

    result = validate_manifest(manifest)
    all_warnings = gen_warnings + result.warnings
    if not result.is_valid:
        raise InvalidManifest(result)

    return manifest, all_warnings
