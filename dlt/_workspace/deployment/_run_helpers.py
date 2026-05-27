"""Shared `run`/`serve` orchestration helpers — pure manifest transforms, no CLI I/O."""

import copy
import os
import os.path
from datetime import datetime, timezone  # noqa: I251
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple, cast
from uuid import uuid4
from zoneinfo import ZoneInfo

from dlt.common.time import ensure_datetime_utc

from dlt._workspace._workspace_context import active
from dlt._workspace.deployment._job_ref import format_job_label, resolve_job_ref, short_name
from dlt._workspace.deployment._run_typing import TRunJobInfo
from dlt._workspace.deployment._trigger_helpers import (
    humanize_trigger,
    is_selector,
    match_triggers_with_selectors,
    maybe_parse_schedule,
    parse_trigger,
    pick_trigger,
)
from dlt._workspace.deployment.exceptions import (
    AmbiguousJobRef,
    AmbiguousJobSelector,
    DeploymentException,
    InvalidJobRef,
    InvalidTrigger,
    JobRefNotFound,
    JobRefNotInCandidates,
    ManifestImportError,
    NoMatchingJobs,
)
from dlt._workspace.deployment.interval import (
    compute_run_interval,
    resolve_interval_spec,
)
from dlt._workspace.deployment.launchers import LAUNCHER_JOB, LAUNCHER_MODULE
from dlt._workspace.deployment.manifest import (
    compute_default_trigger,
    expand_triggers,
    generate_manifest_hash,
    manifest_from_module,
)
from dlt._workspace.deployment.trigger import manual
from dlt._workspace.deployment.typing import (
    DEFAULT_DEPLOYMENT_MODULE,
    TJobDefinition,
    TJobsDeploymentManifest,
    TRuntimeEntryPoint,
    TTrigger,
)


TCandidate = Tuple[TJobDefinition, TTrigger]
TPickFn = Callable[[List[Tuple[TJobDefinition, str]]], Tuple[TJobDefinition, str]]


def promote_deployment_arg(
    selector_or_job_ref: Optional[str], deployment: Optional[str]
) -> Tuple[Optional[str], Optional[str]]:
    """Promote a positional `.py` argument to the `--deployment` slot."""
    # job refs cannot end in `.py` because names/sections must be Python
    # identifiers, so the detection is unambiguous
    if selector_or_job_ref is None or not selector_or_job_ref.lower().endswith(".py"):
        return selector_or_job_ref, deployment
    if deployment is not None:
        raise ValueError(
            f"Pass either positional {selector_or_job_ref!r} or --deployment, not both."
        )
    if not Path(selector_or_job_ref).is_file():
        raise FileNotFoundError(f"File not found: {selector_or_job_ref!r}")
    return None, selector_or_job_ref


def load_manifest_with_warnings(
    name_or_path: str, *, use_all: bool = True
) -> Tuple[TJobsDeploymentManifest, str, List[str]]:
    """Load a manifest, returning `(manifest, manifest_hash, warnings)`."""
    try:
        manifest, warnings = manifest_from_module(name_or_path, use_all=use_all)
    except ImportError as exc:
        # the catch is broad because import_module() raises both for missing
        # target modules AND for any import failing inside them
        if name_or_path.endswith(".py") or "/" in name_or_path or os.sep in name_or_path:
            file_path = Path(name_or_path).resolve()
        else:
            file_path = Path.cwd() / f"{name_or_path}.py"

        if file_path.exists():
            raise ManifestImportError(
                name_or_path, str(file_path), exc, kind="import_failed"
            ) from exc
        if name_or_path == DEFAULT_DEPLOYMENT_MODULE:
            raise ManifestImportError(
                name_or_path, str(file_path), exc, kind="default_missing"
            ) from exc
        raise ManifestImportError(name_or_path, str(file_path), exc, kind="module_missing") from exc

    return manifest, generate_manifest_hash(manifest), warnings


def resolve_selector(
    selector_or_job_ref: Optional[str],
    manifest: TJobsDeploymentManifest,
    *,
    default_selector: str = "manual:",
) -> List[str]:
    """Convert a CLI positional into a selector list. Bare refs become `manual:<ref>`."""
    if selector_or_job_ref is None:
        return [default_selector]

    try:
        resolved = resolve_job_ref(selector_or_job_ref, [j["job_ref"] for j in manifest["jobs"]])
        return [manual(resolved)]
    except (InvalidJobRef, JobRefNotFound, AmbiguousJobRef):
        # not a unique recognizable ref — treat as a selector pattern
        return [selector_or_job_ref]


def select_candidates(
    manifest: TJobsDeploymentManifest,
    selectors: List[str],
    *,
    forbidden_job_type: Optional[str] = None,
) -> List[TCandidate]:
    """Match jobs against `selectors`, substituting `manual:` hits with the job's default trigger."""
    matched: List[TCandidate] = []

    for job_def in manifest["jobs"]:
        job_type = job_def["entry_point"]["job_type"]
        expanded = expand_triggers(job_def)
        hits = match_triggers_with_selectors(job_type, expanded, selectors)
        trigger = pick_trigger(hits, job_def.get("default_trigger"))
        if trigger is None:
            continue
        if str(trigger).startswith("manual:"):
            default = compute_default_trigger(job_def)
            if default is not None:
                trigger = default
        matched.append((job_def, TTrigger(str(trigger))))

    if forbidden_job_type is None:
        return matched

    forbidden = [
        (jd, t) for jd, t in matched if jd["entry_point"]["job_type"] == forbidden_job_type
    ]
    allowed = [(jd, t) for jd, t in matched if jd["entry_point"]["job_type"] != forbidden_job_type]
    if forbidden and not allowed:
        # all hits are of the forbidden type — point users to the sibling command
        sibling = "serve" if forbidden_job_type == "interactive" else "run"
        refs = ", ".join(jd["job_ref"] for jd, _ in forbidden)
        raise DeploymentException(
            f"Matched jobs are {forbidden_job_type} (not allowed here): {refs}."
            f" Use the `{sibling}` command instead."
        )
    return allowed


def narrow_candidates(candidates: List[TCandidate], job_ref: Optional[str]) -> TCandidate:
    """Pick one candidate; `job_ref` must match a candidate's ref exactly (no manifest-wide fallback)."""
    if not candidates:
        raise LookupError("No candidates to narrow.")

    if job_ref is None:
        if len(candidates) == 1:
            return candidates[0]
        raise AmbiguousJobSelector(candidates)

    candidate_refs = [jd["job_ref"] for jd, _ in candidates]
    try:
        resolved = str(resolve_job_ref(job_ref, candidate_refs))
    except (InvalidJobRef, JobRefNotFound):
        raise JobRefNotInCandidates(job_ref, candidates)

    for cand in candidates:
        if cand[0]["job_ref"] == resolved:
            return cand
    # resolve_job_ref returned a ref outside the candidate list — should not
    # happen because we passed only candidate_refs, but treat as not-in-set
    raise JobRefNotInCandidates(job_ref, candidates)


def select_single_job(
    manifest: TJobsDeploymentManifest,
    selectors: List[str],
    *,
    forbidden_job_type: Optional[str] = None,
    job_ref: Optional[str] = None,
    available_selectors: Optional[List[str]] = None,
) -> TCandidate:
    """Resolve `selectors` (+ optional `job_ref`) to exactly one matched job."""

    candidates = select_candidates(manifest, selectors, forbidden_job_type=forbidden_job_type)
    if not candidates:
        if available_selectors is not None:
            try:
                available = select_candidates(
                    manifest, available_selectors, forbidden_job_type=forbidden_job_type
                )
            except DeploymentException:
                # forbidden_job_type combined with mismatched available_selectors
                # can raise; degrade gracefully to "no matching jobs declared".
                available = []
        else:
            available = [(j, TTrigger("")) for j in manifest["jobs"]]
        raise NoMatchingJobs(selectors=selectors, available=available)
    return narrow_candidates(candidates, job_ref)


def warn_missing_profiles() -> List[str]:
    """Advisory warnings when recommended profiles (`prod`, `access`) are missing locally."""
    warnings: List[str] = []
    try:
        available = set(active().available_profiles())
    except Exception:
        return warnings
    if "prod" not in available:
        warnings.append(
            "No 'prod' profile detected. Batch jobs will use default config/secrets only."
        )
    if "access" not in available:
        warnings.append("No 'access' profile detected. Interactive jobs will use `prod` profile.")
    return warnings


def resolve_refresh(user_refresh: bool, job_def: TJobDefinition) -> Tuple[bool, Optional[str]]:
    """Apply a job's `TRefreshPolicy` to `user_refresh`. Returns `(effective, warning_or_None)`."""
    policy = job_def.get("refresh", "auto")
    if policy == "always":
        return True, None
    if policy == "block":
        warning: Optional[str] = None
        if user_refresh:
            warning = (
                f"--refresh ignored: job {short_name(job_def['job_ref'])!r} declares refresh=block"
            )
        return False, warning
    return user_refresh, None


def resolve_profile(
    user_profile: Optional[str], job_def: TJobDefinition
) -> Tuple[str, Optional[str]]:
    """Current profile (`--profile` wins over active); warns on declared-vs-active mismatch."""
    current = user_profile if user_profile is not None else active().profile
    declared = job_def.get("require", {}).get("profile")
    warning: Optional[str] = None
    if declared is not None and declared != current:
        warning = f"Job declares profile {declared!r} but running on current profile {current!r}"
    return current, warning


def resolve_interval(
    user_start: Optional[str],
    user_end: Optional[str],
    job_def: TJobDefinition,
    picked_trigger: TTrigger,
    now_utc: datetime,
    refresh: bool,
) -> Tuple[datetime, datetime, str]:
    """Resolve `(start_utc, end_utc, tz)` for a run; user values win verbatim, otherwise clamp to declared `interval`."""
    # refresh=true + declared interval -> backfill from declared start to most
    # recent schedule tick; otherwise the most recently elapsed window
    tz = job_def.get("require", {}).get("timezone", "UTC")

    if user_start:
        target_tz = ZoneInfo(tz)
        start = ensure_datetime_utc(user_start, default_tz=target_tz)
        end = ensure_datetime_utc(user_end, default_tz=target_tz) if user_end else now_utc
        return start, end, tz

    declared = job_def.get("interval")
    trigger = job_def.get("default_trigger") or picked_trigger
    cron = maybe_parse_schedule(job_def)

    declared_start_dt: Optional[datetime] = None
    declared_end_dt: Optional[datetime] = None
    if declared:
        if cron:
            declared_start_dt, spec_end = resolve_interval_spec(declared, cron, tz=tz)
            if declared.get("end"):
                declared_end_dt = spec_end
        else:
            declared_start_dt = ensure_datetime_utc(declared["start"])
            if declared.get("end"):
                declared_end_dt = ensure_datetime_utc(declared["end"])

    natural_start, natural_end = compute_run_interval(
        trigger, now_utc, prev_interval_end=None, tz=tz
    )

    if refresh and declared_start_dt is not None:
        start, end = declared_start_dt, natural_end
    else:
        start, end = natural_start, natural_end

    if declared_start_dt is not None:
        start = max(start, declared_start_dt)
    if declared_end_dt is not None:
        end = min(end, declared_end_dt)

    return start, end, tz


def build_runtime_entry_point(
    job_def: TJobDefinition,
    cli_config: Dict[str, str],
    profile: str,
    refresh: bool,
    interval_start: datetime,
    interval_end: datetime,
    tz: str,
) -> TRuntimeEntryPoint:
    """Assemble a `TRuntimeEntryPoint` from a job def and resolved context, without mutating `job_def`."""
    entry_point: TRuntimeEntryPoint = copy.copy(job_def["entry_point"])  # type: ignore[assignment]

    if cli_config:
        merged = dict(entry_point.get("config", {}))
        merged.update(cli_config)
        entry_point["config"] = merged

    if entry_point.get("job_type") == "interactive":
        entry_point["run_args"] = {"port": 5000}

    entry_point["interval_start"] = interval_start.isoformat()
    entry_point["interval_end"] = interval_end.isoformat()
    entry_point["interval_timezone"] = tz
    entry_point["allow_external_schedulers"] = job_def.get("allow_external_schedulers", False)
    entry_point["profile"] = profile
    entry_point["refresh"] = refresh
    execute_spec = job_def.get("execute") or {}
    if "intercept_signals" in execute_spec:
        entry_point["intercept_signals"] = execute_spec["intercept_signals"]
    return entry_point


def pick_launcher(entry_point: TRuntimeEntryPoint) -> str:
    """Launcher module path: explicit override > function-based > module-level."""
    explicit = entry_point.get("launcher")
    if explicit:
        return explicit
    return LAUNCHER_JOB if entry_point.get("function") else LAUNCHER_MODULE


def fetch_run_info(
    *,
    selector: Optional[str] = None,
    selectors: Optional[List[str]] = None,
    deployment: Optional[str] = None,
    user_profile: Optional[str] = None,
    user_start: Optional[str] = None,
    user_end: Optional[str] = None,
    user_refresh: bool = False,
    cli_config: Optional[Dict[str, str]] = None,
    job_ref: Optional[str] = None,
    forbidden_job_type: Optional[str] = None,
    available_selectors: Optional[List[str]] = None,
    pick: Optional[TPickFn] = None,
    now_utc: Optional[datetime] = None,
) -> Optional[TRunJobInfo]:
    """Resolve a run/serve request to a launchable `TRunJobInfo`.

    Args:
        selector: User-supplied positional — a selector or a job ref.
        selectors: Pre-built selectors (e.g. `["pipeline_name:<name>"]`). When set,
            `selector` is ignored.
        deployment: Path or module name of the deployment to load. Defaults to the
            workspace's default deployment module.
        user_profile: Profile override; wins over the active profile.
        user_start: ISO interval start override.
        user_end: ISO interval end override.
        user_refresh: Whether the user requested `--refresh`.
        cli_config: `KEY=VALUE` config overrides to merge into the entry point.
        job_ref: Narrow the matched candidates to this exact ref.
        forbidden_job_type: Skip jobs of this `job_type` (e.g. `"interactive"`).
        available_selectors: Selectors used to scope `NoMatchingJobs.available`
            on no-match (e.g. `["batch"]`, `["interactive"]`).
        pick: Callback invoked when more than one candidate matches and no
            `job_ref` was given.
        now_utc: Clock override for tests.

    Returns:
        A `TRunJobInfo` ready to launch, or `None` when the manifest has no jobs.

    Raises:
        NoMatchingJobs: No job matched the selectors.
        AmbiguousJobSelector: Multiple jobs matched and `pick` is not provided.
        JobRefNotInCandidates: `job_ref` is not among matched candidates.
    """
    if selectors is not None:
        # caller-supplied selectors path (e.g. local pipeline run)
        explicit_deployment = deployment
    else:
        selector, explicit_deployment = promote_deployment_arg(selector, deployment)

    name_or_path = explicit_deployment if explicit_deployment else DEFAULT_DEPLOYMENT_MODULE
    use_all = explicit_deployment is None

    manifest, _, manifest_warnings = load_manifest_with_warnings(name_or_path, use_all=use_all)
    jobs: List[TJobDefinition] = manifest.get("jobs", [])
    if not jobs:
        return None

    effective_selectors = (
        selectors if selectors is not None else resolve_selector(selector, manifest)
    )

    job_def, picked_trigger = _select_with_picker(
        manifest,
        effective_selectors,
        forbidden_job_type,
        job_ref,
        pick,
        available_selectors=available_selectors,
    )

    # manual: selectors are how the user asked; the actual run uses the job's
    # default_trigger so interval math reflects the real schedule
    effective_trigger = picked_trigger
    try:
        if parse_trigger(picked_trigger).type == "manual":
            default = job_def.get("default_trigger")
            if default:
                effective_trigger = default
    except InvalidTrigger:
        pass

    effective_refresh, refresh_warning = resolve_refresh(user_refresh, job_def)
    profile, profile_warning = resolve_profile(user_profile, job_def)

    now = now_utc if now_utc is not None else datetime.now(timezone.utc)
    interval_start, interval_end, tz = resolve_interval(
        user_start, user_end, job_def, effective_trigger, now, refresh=effective_refresh
    )

    entry_point = build_runtime_entry_point(
        job_def=job_def,
        cli_config=cli_config or {},
        profile=profile,
        refresh=effective_refresh,
        interval_start=interval_start,
        interval_end=interval_end,
        tz=tz,
    )

    info: TRunJobInfo = {
        "job_ref": job_def["job_ref"],
        "display_label": format_job_label(
            job_def["job_ref"], job_def.get("expose"), job_def.get("deliver")
        ),
        "trigger": effective_trigger,
        "trigger_humanized": humanize_trigger(effective_trigger),
        "launcher": pick_launcher(entry_point),
        "run_id": str(uuid4()),
        "entry_point": dict(entry_point),
        "manifest_warnings": list(manifest_warnings),
    }
    if refresh_warning:
        info["refresh_warning"] = refresh_warning
    if profile_warning:
        info["profile_warning"] = profile_warning
    return info


def _select_with_picker(
    manifest: TJobsDeploymentManifest,
    selectors: List[str],
    forbidden_job_type: Optional[str],
    job_ref: Optional[str],
    pick: Optional[TPickFn],
    available_selectors: Optional[List[str]] = None,
) -> Tuple[TJobDefinition, TTrigger]:
    """Run `select_single_job`; on `AmbiguousJobSelector`, fall back to `pick` if provided."""
    try:
        return select_single_job(
            manifest,
            selectors,
            forbidden_job_type=forbidden_job_type,
            job_ref=job_ref,
            available_selectors=available_selectors,
        )
    except AmbiguousJobSelector as exc:
        if pick is None:
            raise
        jd, t = pick(exc.matches)
        return jd, cast(TTrigger, t)
