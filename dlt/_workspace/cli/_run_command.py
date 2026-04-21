"""Helpers for the `workspace run` CLI command."""

import copy
import os
import os.path
from datetime import datetime, timezone  # noqa: I251
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple
from uuid import uuid4
from zoneinfo import ZoneInfo

from dlt.common import json
from dlt.common.time import ensure_pendulum_datetime_non_utc, ensure_pendulum_datetime_utc

from dlt._workspace.cli import echo as fmt
from dlt._workspace.cli.exceptions import CliCommandInnerException
from dlt._workspace.deployment._job_ref import (
    format_job_label,
    resolve_job_ref,
    short_name,
)
from dlt._workspace.deployment._trigger_helpers import (
    humanize_trigger,
    is_selector,
    match_triggers_with_selectors,
    maybe_parse_schedule,
    parse_trigger,
    pick_trigger,
)
from dlt._workspace.deployment.exceptions import InvalidJobRef, InvalidTrigger
from dlt._workspace.deployment.interval import (
    compute_run_interval,
    resolve_interval_spec,
)
from dlt._workspace.deployment.launchers import LAUNCHER_JOB, LAUNCHER_MODULE
from dlt._workspace.deployment.manifest import expand_triggers, manifest_from_module
from dlt._workspace.deployment.typing import (
    DEFAULT_DEPLOYMENT_MODULE,
    TJobDefinition,
    TJobsDeploymentManifest,
    TRuntimeEntryPoint,
    TTrigger,
)
from dlt._workspace.profile import DEFAULT_PROFILE
from dlt._workspace.typing import TRunJobInfo


TPickFn = Callable[[List[Tuple[TJobDefinition, TTrigger]]], Tuple[TJobDefinition, TTrigger]]


def promote_file_arg(
    selector_or_job_ref: Optional[str], file: Optional[str]
) -> Tuple[Optional[str], Optional[str]]:
    """Promote a `.py` positional into the `--file` slot."""
    if selector_or_job_ref and selector_or_job_ref.lower().endswith(".py"):
        if file:
            raise CliCommandInnerException(
                cmd="workspace run",
                msg="Cannot specify both a .py positional and --file.",
            )
        if not os.path.isfile(selector_or_job_ref):
            raise CliCommandInnerException(
                cmd="workspace run",
                msg=f"File not found: {selector_or_job_ref!r}",
            )
        return None, selector_or_job_ref
    return selector_or_job_ref, file


def load_manifest(name_or_path: str, use_all: bool) -> Tuple[TJobsDeploymentManifest, List[str]]:
    """Load manifest, distinguishing missing module from import error inside module."""
    try:
        return manifest_from_module(name_or_path, use_all=use_all)
    except ImportError as exc:
        if name_or_path.endswith(".py") or "/" in name_or_path or os.sep in name_or_path:
            file_path = Path(name_or_path).resolve()
        else:
            file_path = Path.cwd() / f"{name_or_path}.py"

        if file_path.exists():
            raise CliCommandInnerException(
                cmd="workspace run",
                msg=f"Failed to import {file_path.name!r}: {type(exc).__name__}: {exc}",
                inner_exc=exc,
            )
        if name_or_path == DEFAULT_DEPLOYMENT_MODULE:
            raise CliCommandInnerException(
                cmd="workspace run",
                msg=(
                    f"No {DEFAULT_DEPLOYMENT_MODULE!r} module found in the workspace."
                    " Create one and import your job declarations into it, or pass a"
                    " specific file with --file."
                ),
                inner_exc=exc,
            )
        raise CliCommandInnerException(
            cmd="workspace run",
            msg=(
                f"Could not import module {name_or_path!r}. Check that the file"
                " exists and is a valid Python module."
            ),
            inner_exc=exc,
        )


def collect_candidates(
    jobs: List[TJobDefinition],
    selector: Optional[str],
    expanded_map: Dict[str, List[TTrigger]],
) -> List[Tuple[TJobDefinition, TTrigger]]:
    """`(job_def, trigger)` pairs matching `selector`. `None` → each job's default or synthetic `manual:`."""
    if selector is None:
        return [(j, j.get("default_trigger") or TTrigger(f"manual:{j['job_ref']}")) for j in jobs]

    job_refs = [j["job_ref"] for j in jobs]
    if is_selector(selector):
        resolved = selector
    else:
        try:
            resolved = f"manual:{resolve_job_ref(selector, job_refs)}"
        except (InvalidJobRef, KeyError) as exc:
            refs = ", ".join(job_refs)
            raise CliCommandInnerException(
                cmd="workspace run",
                msg=f"Could not resolve {selector!r} to a job. Available: {refs}",
                inner_exc=exc,
            )

    candidates: List[Tuple[TJobDefinition, TTrigger]] = []
    for job_def in jobs:
        matched = match_triggers_with_selectors(
            job_def["entry_point"]["job_type"],
            expanded_map[job_def["job_ref"]],
            [resolved],
        )
        picked = pick_trigger(matched, job_def.get("default_trigger"))
        if picked is not None:
            candidates.append((job_def, picked))
    return candidates


def resolve_refresh(user_refresh: bool, job_def: TJobDefinition) -> Tuple[bool, Optional[str]]:
    """Apply `TRefreshPolicy` to `--refresh`. Returns `(effective, warning_or_None)`."""
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
    user_profile: Optional[str],
    job_def: TJobDefinition,
    pinned_profile: Optional[str],
) -> Tuple[str, Optional[str]]:
    """Current profile (`--profile` > pinned > DEFAULT_PROFILE) + warning if the job declares a different profile."""
    if user_profile is not None:
        current = user_profile
    elif pinned_profile is not None:
        current = pinned_profile
    else:
        current = DEFAULT_PROFILE

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
    """Resolve `(start_utc, end_utc, tz)` for a run.

    - User `--start/--end` wins verbatim (no clamping, no cron alignment).
    - `refresh=True` + declared interval → backfill from declared start to
      the most recent schedule tick (or `now` for `every:`/others).
    - Otherwise → most recently elapsed window via `compute_run_interval`.
    - Result is clamped to the declared `interval.start`/`interval.end` when
      one is declared (`declared_start` is already cron-aligned by manifest
      generation, so this is pure min/max).
    """
    tz = job_def.get("require", {}).get("timezone", "UTC")

    if user_start:
        target_tz = ZoneInfo(tz)
        start = _to_utc(user_start, target_tz)
        end = _to_utc(user_end, target_tz) if user_end else now_utc
        return start, end, tz

    declared = job_def.get("interval")
    trigger = job_def.get("default_trigger") or picked_trigger
    cron = maybe_parse_schedule(job_def)

    # declared bounds: computed once, used for backfill start and clamping.
    # manifest generation aligns declared start to a cron tick when cron is set.
    declared_start_dt: Optional[datetime] = None
    declared_end_dt: Optional[datetime] = None
    if declared:
        if cron:
            declared_start_dt, spec_end = resolve_interval_spec(declared, cron, tz=tz)
            if declared.get("end"):
                declared_end_dt = spec_end
        else:
            declared_start_dt = ensure_pendulum_datetime_utc(declared["start"])
            if declared.get("end"):
                declared_end_dt = ensure_pendulum_datetime_utc(declared["end"])

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


def _to_utc(value: str, target_tz: ZoneInfo) -> datetime:
    pdt = ensure_pendulum_datetime_non_utc(value)
    dt = datetime(
        pdt.year,
        pdt.month,
        pdt.day,
        pdt.hour,
        pdt.minute,
        pdt.second,
        pdt.microsecond,
        tzinfo=pdt.tzinfo,
    )
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=target_tz)
    return dt.astimezone(timezone.utc)


def build_runtime_entry_point(
    job_def: TJobDefinition,
    cli_config: Dict[str, str],
    profile: str,
    refresh: bool,
    interval_start: datetime,
    interval_end: datetime,
    tz: str,
) -> TRuntimeEntryPoint:
    """Assemble a `TRuntimeEntryPoint` from the job definition and resolved context."""
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
    """Launcher module path. Explicit override > function-based > module-level."""
    explicit = entry_point.get("launcher")
    if explicit:
        return explicit
    return LAUNCHER_JOB if entry_point.get("function") else LAUNCHER_MODULE


def fetch_run_info(
    selector: Optional[str],
    file: Optional[str],
    user_profile: Optional[str],
    user_start: Optional[str],
    user_end: Optional[str],
    user_refresh: bool,
    cli_config: Dict[str, str],
    pinned_profile: Optional[str],
    pick: TPickFn,
    now_utc: Optional[datetime] = None,
) -> Optional[TRunJobInfo]:
    """Resolve a `workspace run` request into a full `TRunJobInfo` model.

    No printing. `pick` handles multi-candidate arbitration (the controller
    supplies an interactive / first-fallback picker). Returns `None` when
    the manifest has no jobs.
    """
    selector, file = promote_file_arg(selector, file)
    # ad-hoc file mode uses use_all=False so plain python modules (no @job
    # decorators) are picked up by detect_local_module
    name_or_path = file if file else DEFAULT_DEPLOYMENT_MODULE
    use_all = file is None

    manifest, manifest_warnings = load_manifest(name_or_path, use_all=use_all)
    jobs: List[TJobDefinition] = manifest.get("jobs", [])
    if not jobs:
        return None

    expanded_map: Dict[str, List[TTrigger]] = {j["job_ref"]: expand_triggers(j) for j in jobs}
    candidates = collect_candidates(jobs, selector, expanded_map)
    if not candidates:
        refs = ", ".join(j["job_ref"] for j in jobs)
        raise CliCommandInnerException(
            cmd="workspace run",
            msg=f"No job matched {selector!r}. Available: {refs}",
        )

    job_def, picked_trigger = pick(candidates)

    # manual: selectors are how the user asked for the job; for the actual run
    # we use the job's default_trigger so interval math reflects the real
    # schedule (matches runtime and the test harness)
    effective_trigger = picked_trigger
    try:
        if parse_trigger(picked_trigger).type == "manual":
            default = job_def.get("default_trigger")
            if default:
                effective_trigger = default
    except InvalidTrigger:
        pass

    effective_refresh, refresh_warning = resolve_refresh(user_refresh, job_def)
    profile, profile_warning = resolve_profile(user_profile, job_def, pinned_profile)

    now = now_utc if now_utc is not None else datetime.now(timezone.utc)
    interval_start, interval_end, tz = resolve_interval(
        user_start, user_end, job_def, effective_trigger, now, refresh=effective_refresh
    )

    entry_point = build_runtime_entry_point(
        job_def=job_def,
        cli_config=cli_config,
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


def print_run_plan(info: TRunJobInfo) -> None:
    """Render the resolved run plan (for `-v`/`--dry-run`)."""
    fmt.echo("job_ref: %s" % info["job_ref"])
    fmt.echo("trigger: %s" % info["trigger"])
    fmt.echo("launcher: %s" % info["launcher"])
    fmt.echo("run_id:  %s" % info["run_id"])
    fmt.echo("entry_point:")
    fmt.echo(json.typed_dumps(info["entry_point"], pretty=True))


def print_run_warnings(info: TRunJobInfo) -> None:
    """Emit any warnings attached to the run info (manifest/refresh/profile)."""
    for w in info["manifest_warnings"]:
        fmt.warning(w)
    if "refresh_warning" in info:
        fmt.warning(info["refresh_warning"])
    if "profile_warning" in info:
        fmt.warning(info["profile_warning"])


def print_run_starting(info: TRunJobInfo) -> None:
    """Emit the 'Starting <job> (trigger: ...)' banner before launch."""
    fmt.echo(
        "Starting %s (trigger: %s)" % (fmt.bold(info["display_label"]), info["trigger_humanized"])
    )
    if info["entry_point"].get("job_type") == "interactive":
        port = info["entry_point"]["run_args"]["port"]
        fmt.echo("Listening on http://localhost:%d" % port)
