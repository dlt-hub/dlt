import base64
import contextlib
import hashlib
import io
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
from dlt._workspace.deployment._job_ref import parse_job_ref
from dlt._workspace.deployment import triggers as _triggers
from dlt._workspace.deployment._trigger_helpers import normalize_trigger, parse_trigger
from dlt._workspace.deployment.typing import (
    MANIFEST_ENGINE_VERSION,
    TDeliverySpec,
    TDeploymentManifest,
    TJobDefinition,
    TJobRef,
    TTrigger,
)

DEPLOYMENT_ENGINE_VERSION = MANIFEST_ENGINE_VERSION

_HASH_EXCLUDE_KEYS = ("version", "version_hash", "previous_hashes", "created_at")
_MAX_PREVIOUS_HASHES = 10


class ManifestValidation(NamedTuple):
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    unresolved_triggers: Dict[str, List[str]]
    """Maps job_ref -> list of unresolved upstream job refs from triggers."""


def generate_manifest_hash(manifest: TDeploymentManifest) -> str:
    """SHA3-256 content hash of manifest, excluding version metadata."""
    manifest_copy = copy(manifest)
    for key in _HASH_EXCLUDE_KEYS:
        manifest_copy.pop(key, None)  # type: ignore[misc]
    content = json.typed_dumpb(manifest_copy, sort_keys=True)
    h = hashlib.sha3_256(content)
    return base64.b64encode(h.digest()).decode("ascii")


def bump_manifest_version(
    manifest: TDeploymentManifest,
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
) -> TDeploymentManifest:
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


def save_manifest(manifest: TDeploymentManifest, f: BinaryIO) -> str:
    """Bump version, serialize manifest, and write to binary IO.

    Returns:
        The new version hash.
    """
    _, new_hash, _ = bump_manifest_version(manifest)
    data = json.typed_dumpb(manifest)
    f.write(data)
    return new_hash


def load_manifest(f: BinaryIO) -> TDeploymentManifest:
    """Read, migrate, and validate a manifest from binary IO."""
    data = f.read()
    manifest_dict: DictStrAny = json.typed_loadb(data)
    engine_version = manifest_dict.get("engine_version", 1)
    manifest = migrate_manifest(manifest_dict, engine_version, MANIFEST_ENGINE_VERSION)

    result = validate_manifest(manifest)
    if not result.is_valid:
        raise ValueError(f"invalid manifest: {result.errors}")

    return manifest


def _newtype_validator(path: str, pk: str, pv: Any, t: Any) -> bool:
    """Custom validator for NewType fields (TTrigger, TJobRef)."""
    if t is TTrigger:
        try:
            normalize_trigger(pv)
        except (ValueError, TypeError) as e:
            raise DictValidationException(str(e), path, t, pk, pv)
        return True
    if t is TJobRef:
        try:
            parse_job_ref(pv)
        except (ValueError, TypeError) as e:
            raise DictValidationException(str(e), path, t, pk, pv)
        return True
    return False


def validate_manifest(manifest: TDeploymentManifest) -> ManifestValidation:
    """Validate a deployment manifest structurally and for consistency."""
    errors: List[str] = []
    warnings: List[str] = []
    unresolved: Dict[str, List[str]] = {}

    try:
        validate_dict(TDeploymentManifest, manifest, ".", validator_f=_newtype_validator)
    except DictValidationException as e:
        errors.append(str(e))
        return ManifestValidation(
            is_valid=False, errors=errors, warnings=warnings, unresolved_triggers=unresolved
        )

    jobs = manifest.get("jobs", [])

    # duplicate job refs
    seen_refs: Set[str] = set()
    for job_def in jobs:
        ref = job_def["job_ref"]
        if ref in seen_refs:
            errors.append(f"duplicate job_ref: {ref!r}")
        seen_refs.add(ref)

    # job type vs trigger consistency
    for job_def in jobs:
        job_type = job_def["entry_point"]["job_type"]
        trigger_types = set()
        for t in job_def.get("triggers", []):
            try:
                parsed = parse_trigger(t)
                trigger_types.add(parsed.type)
            except ValueError:
                pass

        if job_type == "batch" and "http" in trigger_types:
            errors.append(
                f"batch job {job_def['job_ref']!r} has http trigger — use interactive job type"
            )
        if job_type == "interactive" and "http" not in trigger_types:
            warnings.append(f"interactive job {job_def['job_ref']!r} has no http trigger")

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

    # unresolved job event triggers
    for job_def in jobs:
        for trigger in job_def.get("triggers", []):
            try:
                parsed = parse_trigger(trigger)
            except ValueError:
                continue
            if parsed.type in ("job.success", "job.fail") and parsed.expr not in seen_refs:
                unresolved.setdefault(job_def["job_ref"], []).append(str(parsed.expr))

    if unresolved:
        for job_ref, refs in unresolved.items():
            warnings.append(f"job {job_ref!r} has triggers referencing unknown jobs: {refs}")

    return ManifestValidation(
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


def generate_manifest(
    deployment_module: ModuleType,
    use_all: bool = True,
) -> Tuple[TDeploymentManifest, List[str]]:
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

    if use_all and hasattr(deployment_module, "__all__"):
        names = list(deployment_module.__all__)
    else:
        if use_all and not hasattr(deployment_module, "__all__"):
            warnings.append(
                f"module {deployment_module.__name__!r} has no __all__, scanning __dict__ instead"
            )
        names = list(deployment_module.__dict__.keys())

    has_all = hasattr(deployment_module, "__all__")
    jobs: List[TJobDefinition] = []

    for name in names:
        obj = deployment_module.__dict__.get(name)
        if obj is None:
            warnings.append(f"name {name!r} listed in __all__ but not found in module")
            continue

        if isinstance(obj, JobFactory):
            jobs.append(obj.to_job_definition())
        elif isinstance(obj, ModuleType):
            if not is_local_module(obj, deployment_module):
                continue
            framework_job = detect_module_job(obj)
            if framework_job is not None:
                jobs.append(framework_job)
            else:
                local_job = detect_local_module(obj, deployment_module)
                if local_job is not None:
                    jobs.append(local_job)
        else:
            # only warn for names explicitly listed in __all__
            if has_all and not name.startswith("_"):
                warnings.append(f"name {name!r} is not a recognized job or framework module")

    if not jobs:
        framework_job = detect_module_job(deployment_module)
        if framework_job is not None:
            jobs.append(framework_job)

    # add manual trigger to every job unless opted out
    for job_def in jobs:
        if not job_def.get("manual_disabled"):
            job_def["triggers"].append(_triggers.manual(job_def["job_ref"]))

    manifest: TDeploymentManifest = {
        "engine_version": MANIFEST_ENGINE_VERSION,
        "files": [],
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

    delivery_specs = _collect_delivery_specs(jobs)
    if delivery_specs:
        manifest["delivery_specs"] = delivery_specs

    return manifest, warnings


def _collect_delivery_specs(jobs: List[TJobDefinition]) -> List[TDeliverySpec]:
    """Aggregate deliver refs from jobs into TDeliverySpec entries."""
    by_source: Dict[str, TDeliverySpec] = {}
    for job_def in jobs:
        deliver = job_def.get("deliver")
        if deliver is None:
            continue
        source_ref = deliver["source_ref"]
        if source_ref not in by_source:
            spec: TDeliverySpec = {
                "source_ref": source_ref,
                "deadline": deliver.get("deadline", ""),
                "job_refs": [],
            }
            by_source[source_ref] = spec
        by_source[source_ref]["job_refs"].append(job_def["job_ref"])
    return list(by_source.values())
