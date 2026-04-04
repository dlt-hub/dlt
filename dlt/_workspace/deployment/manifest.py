import base64
import contextlib
import hashlib
import io
import os
from contextlib import contextmanager
from copy import copy
from importlib import import_module
from typing import Any, BinaryIO, Dict, Iterator, List, NamedTuple, Optional, Set, Tuple
from types import ModuleType

from dlt.common import json
from dlt.common.exceptions import DictValidationException
from dlt.common.pendulum import pendulum
from dlt.common.typing import DictStrAny
from dlt.common.validation import validate_dict
from dlt.reflection.script_inspector import no_pipeline_execution

from dlt._workspace.deployment.decorators import JobFactory
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
    JobValidationResult,
    ManifestValidationResult,
)
from dlt._workspace.deployment._job_ref import parse_job_ref
from dlt._workspace.deployment import triggers as _triggers
from dlt._workspace.deployment._trigger_helpers import (
    maybe_parse_schedule,
    parse_trigger,
)
from dlt._workspace.deployment.freshness import parse_freshness_constraint
from dlt._workspace.deployment.launchers import LAUNCHER_DASHBOARD
from dlt._workspace.deployment.typing import (
    MANIFEST_ENGINE_VERSION,
    TEntryPoint,
    TExecuteSpec,
    TExposeSpec,
    TJobsDeploymentManifest,
    TFreshnessConstraint,
    TJobDefinition,
    TJobRef,
    TTrigger,
)

DEPLOYMENT_ENGINE_VERSION = MANIFEST_ENGINE_VERSION

DASHBOARD_JOB_REF = TJobRef("jobs.workspace.dashboard")

_HASH_EXCLUDE_KEYS = ("version", "version_hash", "previous_hashes", "created_at")
_MAX_PREVIOUS_HASHES = 10


def generate_manifest_hash(manifest: TJobsDeploymentManifest) -> str:
    """SHA3-256 content hash of manifest, excluding version metadata."""
    manifest_copy = copy(manifest)
    for key in _HASH_EXCLUDE_KEYS:
        manifest_copy.pop(key, None)  # type: ignore[misc]
    content = json.typed_dumpb(manifest_copy, sort_keys=True)
    h = hashlib.sha3_256(content)
    return base64.b64encode(h.digest()).decode("ascii")


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


def migrate_manifest(
    manifest_dict: DictStrAny, from_engine: int, to_engine: int
) -> TJobsDeploymentManifest:
    """Migrate a manifest dict from one engine version to another.

    Raises:
        ValueError: If no migration path exists.
    """
    if from_engine == to_engine:
        return manifest_dict  # type: ignore[return-value]

    if from_engine == 1 and to_engine >= 2:
        # v1 → v2: add job definitions structure
        manifest_dict.setdefault("jobs", [])
        manifest_dict.setdefault("created_at", "")
        manifest_dict.setdefault("deployment_module", "")
        manifest_dict["engine_version"] = 2
        from_engine = 2

    if from_engine != to_engine:
        raise ValueError(f"no manifest migration path from engine {from_engine} to {to_engine}")
    return manifest_dict  # type: ignore[return-value]


def save_manifest(manifest: TJobsDeploymentManifest, f: BinaryIO) -> str:
    """Bump version, serialize manifest, and write to binary IO.

    Returns:
        The new version hash.
    """
    _, new_hash, _ = bump_manifest_version(manifest)
    data = json.typed_dumpb(manifest)
    f.write(data)
    return new_hash


def load_manifest(f: BinaryIO) -> TJobsDeploymentManifest:
    """Read, migrate, and validate a manifest from binary IO."""
    data = f.read()
    manifest_dict: DictStrAny = json.typed_loadb(data)
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


def expand_triggers(job_def: TJobDefinition) -> List[TTrigger]:
    """Expand triggers with manual and tag triggers from expose spec.

    Returns a new list — does not modify job_def.
    """
    triggers = list(job_def.get("triggers", []))
    expose = job_def.get("expose", {})
    if expose.get("manual", True):
        manual_trigger = _triggers.manual(job_def["job_ref"])
        if manual_trigger not in triggers:
            triggers.append(manual_trigger)
    for t in expose.get("tags", []):
        tag_trigger = _triggers.tag(t)
        if tag_trigger not in triggers:
            triggers.append(tag_trigger)
    return triggers


def compute_default_trigger(job_def: TJobDefinition) -> Optional[TTrigger]:
    """Pick the default trigger for a job: prefer schedule/every, else first trigger."""
    default: Optional[TTrigger] = None
    for t in job_def.get("triggers", []):
        parsed = parse_trigger(t)
        if parsed.type in ("schedule", "every"):
            return t
        if default is None:
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
        parse_job_ref(ref)
    except InvalidJobRef as e:
        errors.append(str(e))

    # parse triggers — collect format errors and trigger types
    trigger_types: Set[str] = set()
    for t in job_def.get("triggers", []):
        try:
            parsed = parse_trigger(t)
            trigger_types.add(parsed.type)
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
    if has_interval and "schedule" not in trigger_types:
        errors.append(f"job {ref!r} has interval but no schedule trigger")
    if has_interval and "every" in trigger_types:
        errors.append(
            f"job {ref!r} has interval with every trigger —"
            " intervals require a schedule trigger, not every"
        )
    if has_interval and "schedule" in trigger_types:
        cron_expr = maybe_parse_schedule(job_def)
        if cron_expr:
            from dlt._workspace.deployment.interval import cron_floor
            from dlt.common.time import ensure_pendulum_datetime_utc

            iv = job_def["interval"]
            raw_start = ensure_pendulum_datetime_utc(iv["start"])
            if cron_floor(cron_expr, raw_start) != raw_start:
                warnings.append(
                    f"job {ref!r} interval start ({iv['start']}) is not a"
                    f" cron tick for {cron_expr!r} — will be snapped backward"
                )
            end_str = iv.get("end")
            if end_str:
                raw_end = ensure_pendulum_datetime_utc(end_str)
                if cron_floor(cron_expr, raw_end) != raw_end:
                    warnings.append(
                        f"job {ref!r} interval end ({end_str}) is not a"
                        f" cron tick for {cron_expr!r} — will be snapped backward"
                    )

    if job_def.get("allow_external_schedulers") and not has_interval:
        warnings.append(f"job {ref!r} has allow_external_schedulers but no interval")

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

    # -- cross-job checks below --

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


def _default_dashboard_job() -> TJobDefinition:
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
        "execute": TExecuteSpec(concurrency=1, timeout={"grace_period": 5.0}),
        "description": "Workspace dashboard",
    }


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
            If `False`, scans `__dict__` directly (for ad-hoc deployments).

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
        has_all = use_all and hasattr(deployment_module, "__all__")
        if has_all:
            names = list(deployment_module.__all__)
        else:
            if use_all:
                warnings.append(
                    f"module {deployment_module.__name__!r} has no __all__,"
                    " scanning __dict__ instead"
                )
            names = list(deployment_module.__dict__.keys())

        for name in names:
            obj = deployment_module.__dict__.get(name)
            if obj is None:
                warnings.append(f"name {name!r} listed in __all__ but not found in module")
                continue

            if isinstance(obj, JobFactory):
                jobs.append(obj.to_job_definition())
            elif isinstance(obj, ModuleType):
                # __all__: trust the user; __dict__ scan: filter to local modules
                if not has_all and not is_local_module(obj, deployment_module):
                    continue
                framework_job = detect_module_job(obj)
                if framework_job is not None:
                    jobs.append(framework_job)
                else:
                    local_job = detect_local_module(obj, deployment_module)
                    if local_job is not None:
                        jobs.append(local_job)
            else:
                if has_all and not name.startswith("_"):
                    warnings.append(f"name {name!r} is not a recognized job or framework module")

    # auto-include workspace dashboard if not user-defined
    if not any(j["job_ref"] == DASHBOARD_JOB_REF for j in jobs):
        jobs.append(_default_dashboard_job())

    # set expose.manual default and compute default_trigger
    for job_def in jobs:
        expose = job_def.setdefault("expose", {})
        expose.setdefault("manual", True)

        default = compute_default_trigger(job_def)
        if default is not None:
            job_def["default_trigger"] = default

    manifest: TJobsDeploymentManifest = {
        "engine_version": MANIFEST_ENGINE_VERSION,
        "created_at": pendulum.now("UTC").isoformat(),
        "deployment_module": deployment_module.__name__,
        "jobs": jobs,
    }

    description = getattr(deployment_module, "__doc__", None)
    if description and description.strip():
        manifest["description"] = description.strip()

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
) -> Tuple[TJobsDeploymentManifest, List[str]]:
    """Import a module, generate a manifest, and validate it.

    Resolves file paths to module names, ensures cwd is importable,
    and raises `InvalidManifest` on validation errors.

    Args:
        name_or_path: Python module name or file path.
        use_all: Use `__all__` for job discovery.

    Returns:
        Tuple of (manifest, warnings).

    Raises:
        InvalidManifest: If the generated manifest fails validation.
    """
    import sys

    module_name = _resolve_module_name(name_or_path)

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
