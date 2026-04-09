"""Local workspace runner — simulates the runtime scheduler for development."""

import random
import signal
import sys
import time
import zlib
from datetime import datetime  # noqa: I251
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from uuid import uuid4

from dlt.common import json
from dlt.common.pendulum import pendulum
from dlt._workspace.deployment._job_ref import resolve_job_ref, short_name as job_short_name
from dlt._workspace.deployment.freshness import (
    get_direct_freshness_downstream,
    get_transitive_freshness_downstream,
)
from dlt._workspace.deployment.launchers import LAUNCHER_JOB, LAUNCHER_MODULE
from dlt._workspace.deployment.manifest import expand_triggers, manifest_from_module
from dlt._workspace.deployment.typing import (
    TJobDefinition,
    TJobRef,
    TRuntimeEntryPoint,
    TTrigger,
)
from dlt._workspace.deployment.interval import (
    TInterval,
    check_all_upstream_interval_fresh,
    check_all_upstream_run_fresh,
    compute_run_interval,
    next_eligible_interval,
    resolve_interval_freshness_checks,
    resolve_interval_spec,
    resolve_run_freshness_refs,
)
from dlt._workspace._runner.freshness_store import (
    DuckDBJobFreshnessStore,
    TJobFreshnessStore,
)
from dlt._workspace._runner.interval_store import DuckDBIntervalStore, TIntervalStore
from dlt._workspace._runner.process import JobProcess
from dlt._workspace._runner.run_store import (
    DuckDBJobRunsStore,
    TJobRun,
    TJobRunsStore,
    TJobRunStatus,
)
from dlt._workspace._runner.scheduler import TriggerScheduler
from dlt._workspace.deployment.exceptions import InvalidJobRef, InvalidTrigger
from dlt._workspace.deployment import trigger as _triggers
from dlt._workspace.deployment._trigger_helpers import (
    is_selector,
    match_triggers_with_selectors,
    maybe_parse_schedule,
    parse_trigger,
    pick_trigger,
)

INTERACTIVE_PORT_START = 5000

# stable color palette for job name prefixes
_JOB_COLORS = [
    "\033[36m",  # cyan
    "\033[33m",  # yellow
    "\033[35m",  # magenta
    "\033[32m",  # green
    "\033[34m",  # blue
    "\033[91m",  # bright red
    "\033[96m",  # bright cyan
    "\033[93m",  # bright yellow
    "\033[95m",  # bright magenta
    "\033[92m",  # bright green
]
_RESET = "\033[0m"
_BOLD = "\033[1m"
_use_color = sys.stderr.isatty()


def _job_color(name: str) -> str:
    """Stable color for a job name, deterministic across runs."""
    if not _use_color:
        return ""
    return _JOB_COLORS[zlib.crc32(name.encode()) % len(_JOB_COLORS)]


# module-level state for signal handler
_processes: Dict[str, JobProcess] = {}
_shutting_down = False
_interval_store: Optional[TIntervalStore] = None
_runs_store: Optional[TJobRunsStore] = None
_freshness_store: Optional[TJobFreshnessStore] = None
_running_run_ids: Dict[str, str] = {}
_running_refresh_flags: Dict[str, bool] = {}
"""Per-job-ref flag captured at dispatch — `True` if the run started with `refresh=True`.
Consumed by `_collect_completions` to decide whether the `auto` policy propagates."""
_all_jobs_map: Dict[str, TJobDefinition] = {}
_config: Dict[str, Any] = {}
_display_names: Dict[str, str] = {}
_max_name_len: int = 8


def _build_display_names(jobs: List[TJobDefinition]) -> Dict[str, str]:
    """Build display name map: pipeline_name(short) for pipeline jobs, short otherwise."""
    names: Dict[str, str] = {}
    for j in jobs:
        ref = j["job_ref"]
        short = job_short_name(ref)
        pipeline_name = j.get("deliver", {}).get("pipeline_name")
        names[ref] = f"{pipeline_name}({short})" if pipeline_name else short
    return names


def _short(job_ref: str) -> str:
    """Display name for a job_ref, using pipeline prefix when available."""
    return _display_names.get(job_ref, job_short_name(job_ref))


def _close_stores() -> None:
    """Close all per-runner stores. Safe to call repeatedly."""
    if _interval_store is not None:
        _interval_store.close()
    if _runs_store is not None:
        _runs_store.close()
    if _freshness_store is not None:
        _freshness_store.close()


def _on_non_interval_success(
    job_ref: str,
    job_def: TJobDefinition,
    run_record: Optional[TJobRun],
    this_run_was_refresh: bool,
) -> None:
    """Update `prev_completed_run` and apply the refresh policy after a successful run.

    Sets the job's `prev_completed_run` to the run's `started_at` so the watermark
    advances incrementally — using `interval_start` would freeze the watermark at
    the first cron tick because `compute_run_interval(prev_set, now)` returns
    `(prev, now)`, making `interval_start == prev` for every subsequent run.

    Then evaluates `TJobDefinition.refresh`:

    - `auto` (default) clears direct downstream `prev_completed_run` only if
      this run itself was a refresh run.
    - `always` clears direct downstream unconditionally.
    - `block` does nothing.
    """
    if run_record is None:
        return
    new_prev = run_record.get("started_at")
    if new_prev is not None:
        _freshness_store.set_prev_completed_run(job_ref, new_prev)

    policy = job_def.get("refresh", "auto")
    should_propagate = policy == "always" or (policy == "auto" and this_run_was_refresh)
    if should_propagate:
        for ds_ref in get_direct_freshness_downstream(job_ref, _all_jobs_map):
            _freshness_store.clear_prev_completed_run(ds_ref)


def _eager_refresh_cascade(
    targets: List[Tuple[TJobDefinition, TTrigger]],
    warn: Callable[[str], None],
) -> None:
    """Eager `--refresh` reset for the given dispatch targets.

    For each non-interval target whose pre-flight check would pass, clears
    `prev_completed_run` for the target and all transitively-reachable
    downstream jobs (via the freshness graph). Skips interval-store targets
    silently. Skips with a warning when constraints would block dispatch.
    """
    cleared: Set[str] = set()
    for job_def, _trigger in targets:
        if _is_interval_job(job_def):
            continue
        job_ref = job_def["job_ref"]
        ok, reasons = _can_dispatch_now(job_def)
        if not ok:
            short = _short(job_ref)
            reason_text = "; ".join(reasons) if reasons else "constraints not met"
            warn(f"{short}: --refresh skipped ({reason_text})")
            continue
        for ref in [job_ref, *get_transitive_freshness_downstream(job_ref, _all_jobs_map)]:
            if ref in cleared:
                continue
            downstream = _all_jobs_map.get(ref)
            if downstream is not None and _is_interval_job(downstream):
                continue
            _freshness_store.clear_prev_completed_run(ref)
            cleared.add(ref)


def _timestamp() -> str:
    return pendulum.now("UTC").format("HH:mm:ss.SSS")


def _log(msg: str) -> None:
    """Log a generic runner message to stderr."""
    print(f"{_timestamp()} {msg}", file=sys.stderr, flush=True)  # noqa: T201


def _log_job(short: str, stream_no: int, line: str) -> None:
    """Log a job-prefixed line.

    stream_no=1: subprocess stdout → stdout
    stream_no=2: subprocess stderr or runner status → stderr
    """
    target = sys.stdout if stream_no == 1 else sys.stderr
    ts = _timestamp() + " " if stream_no == 2 else ""
    if _use_color:
        color = _job_color(short)
        prefix = f"{ts}{color}{_BOLD}{short:<{_max_name_len}}{_RESET} | "
    else:
        prefix = f"{ts}{short:<{_max_name_len}} | "
    print(f"{prefix}{line}", file=target, flush=True)  # noqa: T201


def _handle_signal(signum: int, frame: object) -> None:
    """Handle SIGINT/SIGTERM for graceful shutdown."""
    global _shutting_down
    if _shutting_down:
        _log("force kill...")
        for proc in _processes.values():
            if proc.is_alive():
                proc.terminate(grace_period=0)
        sys.exit(1)
    _shutting_down = True
    _log("shutting down (CTRL-C again to force)...")
    for proc in _processes.values():
        if proc.is_alive():
            proc.terminate(grace_period=proc.grace_period)


def _is_interval_job(job_def: TJobDefinition) -> bool:
    return "interval" in job_def and job_def.get("allow_external_schedulers", False)


def _try_start_interval_job(
    job_def: TJobDefinition,
    trigger: TTrigger,
    port_counter: List[int],
) -> bool:
    """For interval jobs: find next eligible, check freshness, start if ready."""
    job_ref = job_def["job_ref"]
    short = _short(job_ref)

    if job_ref in _processes and _processes[job_ref].is_alive():
        return False

    cron = maybe_parse_schedule(job_def)
    if cron is None:
        _log_job(short, 2, "no schedule trigger — cannot compute intervals")
        return False

    tz = job_def.get("require", {}).get("timezone", "UTC")
    overall = resolve_interval_spec(job_def["interval"], cron, tz=tz)
    completed = _interval_store.get_completed_intervals(job_ref, overall)
    iv = next_eligible_interval(cron, overall, completed)
    if iv is None:
        return False

    freshness = job_def.get("freshness", [])
    if freshness:
        checks, reasons = resolve_interval_freshness_checks(iv, overall, freshness, _all_jobs_map)
        if not reasons:
            completions = {
                (c.upstream_ref, c.effective_interval): _interval_store.is_interval_completed(
                    c.upstream_ref, c.effective_interval
                )
                for c in checks
            }
            _, reasons = check_all_upstream_interval_fresh(checks, completions)
        if reasons:
            for reason in reasons:
                _log_job(short, 2, f"skipped: {reason}")
            return False

    _log_job(short, 2, f"interval [{iv[0]}, {iv[1]}) selected")
    _start_job(job_def, trigger, port_counter, interval=iv)
    return True


def _check_run_freshness_for(job_def: TJobDefinition) -> List[str]:
    """Evaluate the run-based freshness constraints for `job_def` (no side effects).

    Returns the list of failure reasons (empty if fresh / no constraints). Sources
    `prev_completed_run` from `_freshness_store` for non-interval upstreams and falls
    back to `_runs_store.get_last_run()` for interval upstreams.
    """
    freshness = job_def.get("freshness", [])
    if not freshness:
        return []
    refs, reasons = resolve_run_freshness_refs(freshness, _all_jobs_map)
    if reasons:
        return reasons
    prev_runs: Dict[str, Optional[datetime]] = {}
    for ref in refs:
        upstream = _all_jobs_map.get(ref)
        if upstream is not None and _is_interval_job(upstream):
            # interval upstreams have no prev_completed_run — derive from runs_store
            run = _runs_store.get_last_run(ref)
            prev_runs[ref] = run["scheduled_at"] if run and run["status"] == "completed" else None
        else:
            prev_runs[ref] = _freshness_store.get_prev_completed_run(ref)
    _, reasons = check_all_upstream_run_fresh(freshness, _all_jobs_map, prev_runs)
    return reasons


def _can_dispatch_now(job_def: TJobDefinition) -> Tuple[bool, List[str]]:
    """Pre-flight check: would `_try_start_job` actually start this job right now?

    No side effects. Returns `(ok, reasons)` — `reasons` is non-empty when blocked.
    Used by `--refresh` to gate the eager `prev_completed_run` reset.
    """
    job_ref = job_def["job_ref"]
    if job_ref in _processes and _processes[job_ref].is_alive():
        return False, ["already running"]
    reasons = _check_run_freshness_for(job_def)
    return (len(reasons) == 0, reasons)


def _try_start_job(
    job_def: TJobDefinition,
    trigger: TTrigger,
    port_counter: List[int],
    freshness_retry: bool = False,
) -> bool:
    """Check freshness, then start a non-interval job."""
    if freshness_retry:
        last = _runs_store.get_last_run(job_def["job_ref"])
        if last and last["status"] == "completed":
            return False
    reasons = _check_run_freshness_for(job_def)
    if reasons:
        short = _short(job_def["job_ref"])
        for reason in reasons:
            _log_job(short, 2, f"skipped: {reason}")
        return False
    _start_job(job_def, trigger, port_counter)
    return True


def _dispatch_job(
    job_def: TJobDefinition,
    trigger: TTrigger,
    port_counter: List[int],
) -> None:
    """Start a job — routes through interval logic if applicable."""
    # for manual triggers, use default_trigger so interval jobs get their schedule trigger
    try:
        parsed = parse_trigger(trigger)
        if parsed.type == "manual" and "default_trigger" in job_def:
            trigger = job_def["default_trigger"]
    except InvalidTrigger:
        pass

    if _is_interval_job(job_def):
        _try_start_interval_job(job_def, trigger, port_counter)
    else:
        # detect freshness-triggered re-dispatch
        freshness_retry = False
        try:
            p = parse_trigger(trigger)
            if p.type == "job.success" and job_def.get("freshness"):
                freshness_retry = True
        except InvalidTrigger:
            pass
        _try_start_job(job_def, trigger, port_counter, freshness_retry=freshness_retry)


def _start_job(
    job_def: TJobDefinition,
    trigger: TTrigger,
    port_counter: List[int],
    interval: Optional[TInterval] = None,
) -> None:
    """Start a job subprocess."""
    job_ref = job_def["job_ref"]
    if job_ref in _processes and _processes[job_ref].is_alive():
        return

    entry_point: TRuntimeEntryPoint = dict(job_def["entry_point"])  # type: ignore[assignment]

    if _config:
        entry_point["config"] = dict(_config)
    job_type = entry_point["job_type"]

    if job_type == "interactive":
        entry_point["run_args"] = {"port": port_counter[0]}
        port_counter[0] += 1

    now = pendulum.now("UTC")
    refresh_signal = False
    eff_interval: Optional[TInterval] = interval

    if interval is not None:
        # interval-store path: caller computed the interval, no refresh signal
        entry_point["interval_start"] = interval[0].isoformat()
        entry_point["interval_end"] = interval[1].isoformat()
    elif not _is_interval_job(job_def):
        # non-interval path: derive refresh signal + interval from prev_completed_run.
        # use default_trigger when present so an event-dispatched job (job.success,
        # manual, etc.) still computes its interval against its scheduling cron rather
        # than against the event trigger.
        tz = job_def.get("require", {}).get("timezone", "UTC")
        prev = _freshness_store.get_prev_completed_run(job_ref)
        refresh_signal = prev is None
        entry_point["refresh"] = refresh_signal
        interval_trigger = job_def.get("default_trigger") or trigger
        eff_interval = compute_run_interval(interval_trigger, now, prev, tz=tz)
        entry_point["interval_start"] = eff_interval[0].isoformat()
        entry_point["interval_end"] = eff_interval[1].isoformat()

    # propagate allow_external_schedulers from manifest to launcher (only meaningful
    # when an interval is provided)
    if "interval_start" in entry_point:
        entry_point["allow_external_schedulers"] = job_def.get("allow_external_schedulers", False)

    # pass profile from require spec
    require = job_def.get("require", {})
    if "profile" in require:
        entry_point["profile"] = require["profile"]

    # select launcher
    launcher = entry_point.get("launcher")
    if launcher is None:
        launcher = LAUNCHER_JOB if entry_point.get("function") else LAUNCHER_MODULE

    run_id = str(uuid4())[:8]
    cmd = [
        "uv",
        "run",
        "python",
        "-m",
        launcher,
        "--run-id",
        run_id,
        "--trigger",
        trigger,
        "--entry-point",
        json.dumps(entry_point),
    ]

    short = _short(job_ref)
    _log_job(short, 2, f"starting (trigger: {trigger})")

    if job_type == "interactive":
        port = entry_point["run_args"]["port"]
        _log_job(short, 2, f"listening on http://localhost:{port}")

    # extract grace period from job definition timeout spec
    grace_period = JobProcess.DEFAULT_GRACE_PERIOD
    timeout_spec = job_def.get("execute", {}).get("timeout")
    if isinstance(timeout_spec, dict):
        grace_period = timeout_spec.get("grace_period", grace_period)

    proc = JobProcess(job_ref, cmd, grace_period=grace_period)
    proc.start()
    _processes[job_ref] = proc

    # record run in storage
    run: TJobRun = {
        "run_id": run_id,
        "job_ref": job_ref,
        "trigger": trigger,
        "scheduled_at": now,
        "started_at": now,
        "status": "running",
    }
    if eff_interval is not None:
        run["interval_start"] = eff_interval[0]
        run["interval_end"] = eff_interval[1]
    _runs_store.create_run(run)
    _running_run_ids[job_ref] = run_id
    _running_refresh_flags[job_ref] = refresh_signal


def _drain_all_output() -> None:
    """Drain and print output from all running processes."""
    for proc in list(_processes.values()):
        for stream_no, line in proc.drain_output():
            short = _short(proc.job_ref)
            _log_job(short, stream_no, line)


def _collect_completions(
    scheduler: TriggerScheduler,
    failed_refs: List[str],
    port_counter: List[int],
) -> None:
    """Check completed processes, fire events, start triggered jobs."""
    for job_ref, proc in list(_processes.items()):
        if not proc.is_alive():
            exit_code = proc.poll()
            short = _short(job_ref)

            job_def = _all_jobs_map.get(job_ref)
            is_interval = job_def is not None and _is_interval_job(job_def)

            # retrieve run record (contains interval if any)
            run_id = _running_run_ids.pop(job_ref, None)
            this_run_was_refresh = _running_refresh_flags.pop(job_ref, False)
            iv: Optional[TInterval] = None
            run_record: Optional[TJobRun] = None
            if run_id:
                run_record = _runs_store.get_run(run_id)
                if run_record and "interval_start" in run_record:
                    iv = (run_record["interval_start"], run_record["interval_end"])

            finished_at = pendulum.now("UTC")
            run_status: TJobRunStatus = "completed" if exit_code == 0 else "failed"
            if run_id:
                _runs_store.update_run(run_id, run_status, finished_at=finished_at)

            if exit_code == 0:
                if is_interval and iv is not None:
                    _interval_store.mark_interval_completed(job_ref, iv)
                    _log_job(short, 2, f"interval [{iv[0]}, {iv[1]}) completed")
                else:
                    _log_job(short, 2, "completed successfully")
                if not is_interval and job_def is not None:
                    _on_non_interval_success(job_ref, job_def, run_record, this_run_was_refresh)
                event = f"job.success:{job_ref}"
            else:
                if is_interval and iv is not None:
                    _log_job(short, 2, f"interval [{iv[0]}, {iv[1]}) failed")
                else:
                    _log_job(short, 2, f"failed (exit code {exit_code})")
                event = f"job.fail:{job_ref}"
                failed_refs.append(job_ref)

            del _processes[job_ref]

            triggered = scheduler.fire_event(event)
            for jd, trig in triggered:
                if not _shutting_down:
                    if is_interval and iv is not None:
                        ds_short = _short(jd["job_ref"])
                        _log_job(
                            ds_short,
                            2,
                            f"triggered by {short} completing [{iv[0]}, {iv[1]})",
                        )
                    _dispatch_job(jd, trig, port_counter)

            if exit_code == 0 and is_interval and iv is not None:
                if job_def is not None and not _shutting_down:
                    cron = maybe_parse_schedule(job_def)
                    if cron:
                        _try_start_interval_job(
                            job_def,
                            TTrigger(f"schedule:{cron}"),
                            port_counter,
                        )


def _describe_trigger(trigger: TTrigger) -> str:
    """Human-readable description of a trigger."""
    try:
        parsed = parse_trigger(trigger)
    except InvalidTrigger:
        return str(trigger)
    tt = parsed.type
    expr = parsed.expr
    if tt == "http":
        return "interactive service"
    if tt == "deployment":
        return "after deployment"
    if tt == "tag":
        return f"tag:{expr}"
    if tt == "manual":
        return f"manual ({job_short_name(str(expr))})" if expr else "manual"
    if tt == "every":
        return f"every {expr}s" if isinstance(expr, float) else f"every {expr}"
    if tt == "schedule":
        return f"schedule: {expr}"
    if tt == "once":
        return f"once at {expr}"
    if tt == "job.success":
        return f"after {job_short_name(str(expr))} succeeds"
    if tt == "job.fail":
        return f"after {job_short_name(str(expr))} fails"
    if tt == "webhook":
        return f"webhook {expr}" if expr else "webhook"
    if tt == "pipeline_name":
        return f"pipeline {expr}"
    return str(trigger)


def _resolve_selectors(selectors: List[str], job_refs: List[TJobRef]) -> List[str]:
    """Resolve selectors: job refs become manual: selectors, others pass through."""
    resolved: List[str] = []
    for sel in selectors:
        if is_selector(sel):
            resolved.append(sel)
            continue
        # try resolving as job ref
        try:
            ref = resolve_job_ref(sel, job_refs)
            resolved.append(f"manual:{ref}")
        except (InvalidJobRef, KeyError):
            # might be a glob pattern for manual refs (e.g. "batch_jobs.*")
            resolved.append(f"manual:jobs.{sel}")
    return resolved


TSelectedJob = Tuple[TJobDefinition, List[TTrigger]]
"""A job paired with its matched triggers for scheduler registration."""


def select_jobs(
    all_jobs: List[TJobDefinition],
    selectors: List[str],
    collect_followup: bool = True,
) -> Tuple[List[TSelectedJob], List[TSelectedJob]]:
    """Select jobs by matching selectors against triggers.

    Selectors match triggers, not jobs. A job is included when at least one
    of its triggers matches. Only matched triggers are registered with the
    scheduler. Event-triggered downstream jobs are collected transitively.

    Args:
        all_jobs: All jobs from the manifest.
        selectors: Trigger selectors (OR-ed). Each matches triggers via fnmatch.

    Returns:
        Tuple of (selected, followup). Each entry is (job_def, matched_triggers).
        Followup jobs have their event triggers as matched triggers.
    """
    if not selectors:
        return [], []

    selected: List[TSelectedJob] = []
    for j in all_jobs:
        matched = match_triggers_with_selectors(
            j["entry_point"]["job_type"],
            j.get("triggers", []),
            selectors,
        )
        if matched:
            selected.append((j, matched))

    if not selected:
        return [], []

    if not collect_followup:
        return selected, []

    # collect event-triggered downstream jobs transitively
    active_refs: Set[str] = {j["job_ref"] for j, _ in selected}
    followup: List[TSelectedJob] = []

    changed = True
    while changed:
        changed = False
        for j in all_jobs:
            if j["job_ref"] in active_refs:
                continue
            event_triggers: List[TTrigger] = []
            for trigger in j.get("triggers", []):
                try:
                    parsed = parse_trigger(trigger)
                except InvalidTrigger:
                    continue
                if parsed.type in ("job.success", "job.fail") and str(parsed.expr) in active_refs:
                    event_triggers.append(trigger)
            if event_triggers:
                followup.append((j, event_triggers))
                active_refs.add(j["job_ref"])
                changed = True

    return selected, followup


_DEFAULT_TRIGGER_MARK = "🎯 "


def _display_manifest(
    jobs: List[TJobDefinition],
    expanded_map: Dict[str, List[TTrigger]],
) -> None:
    """Display manifest job summary to stderr.

    Uses post-expansion triggers so synthetic `tag:`, `manual:`, and
    `pipeline_name:` triggers from `expose`/`deliver` are visible. The job's
    `default_trigger` is shown first with a 🎯 marker.
    """
    for j in jobs:
        ref = j["job_ref"]
        short = _short(ref)
        expanded = expanded_map.get(ref, list(j.get("triggers", [])))
        default = j.get("default_trigger")

        # default first (marked), then everything else excluding manual:
        ordered: List[str] = []
        if default and default in expanded:
            ordered.append(f"{_DEFAULT_TRIGGER_MARK}{default}")
        for t in expanded:
            if t == default:
                continue
            if t.startswith("manual:"):
                continue
            ordered.append(t)

        if not ordered:
            ordered = ["(manual only)"]
        if _use_color:
            color = _job_color(short)
            name = f"{color}{_BOLD}{short:<{_max_name_len}}{_RESET}"
        else:
            name = f"{short:<{_max_name_len}}"
        _log(f"  {name}  {', '.join(ordered)}")


def _is_interactive_job(job_def: TJobDefinition) -> bool:
    """True if job is interactive (mcp, streamlit, marimo, etc.)."""
    return job_def["entry_point"]["job_type"] == "interactive"


def _is_idle_job(job_def: TJobDefinition, selected_refs: Set[str]) -> bool:
    """True if job won't fire: interactive or manual-only, and not selected."""
    if job_def["job_ref"] in selected_refs:
        return False
    if _is_interactive_job(job_def):
        return True
    for trigger in job_def.get("triggers", []):
        try:
            parsed = parse_trigger(trigger)
        except InvalidTrigger:
            continue
        if parsed.type != "manual":
            return False
    return True


def run(
    jobs: List[TJobDefinition],
    trigger_selectors: Optional[List[str]] = None,
    no_future: bool = False,
    once: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
    config: Optional[Dict[str, Any]] = None,
    refresh: bool = False,
) -> int:
    """Simulate the runtime scheduler for a deployment manifest.

    All manifest jobs are registered. Timed triggers (schedule, every, once)
    fire automatically. Event triggers and freshness connect all jobs.
    ``--select`` fires specific jobs immediately on top of automatic scheduling.

    Args:
        jobs: Job definitions from the manifest.
        trigger_selectors: ``--select`` patterns — fire matching jobs immediately.
        no_future: Suppress timed trigger scheduling.
        once: Fire timed triggers once (don't repeat).
        dry_run: Display plan without launching jobs.
        verbose: Show full manifest JSON before running.
        config: Config key=value pairs passed to all jobs.
        refresh: When `True`, clear `prev_completed_run` for each immediate
            non-interval target and its transitive freshness-downstream before
            dispatch. Targets whose pre-flight check fails are skipped with a
            warning. Interval-store jobs are silently excluded.

    Returns:
        Exit code (0 = all jobs succeeded, 1 = any job failed).
    """
    global _processes, _shutting_down, _interval_store, _runs_store, _freshness_store
    global _running_run_ids, _running_refresh_flags
    global _all_jobs_map, _config, _display_names, _max_name_len

    _processes = {}
    _shutting_down = False
    _config = dict(config) if config else {}
    _running_run_ids = {}
    _running_refresh_flags = {}
    _interval_store = DuckDBIntervalStore()
    _runs_store = DuckDBJobRunsStore()
    _freshness_store = DuckDBJobFreshnessStore()
    _all_jobs_map = {j["job_ref"]: j for j in jobs}
    _display_names = _build_display_names(jobs)

    # expand triggers (manual/tag from expose) without mutating job defs
    expanded_map: Dict[str, List[TTrigger]] = {}
    for j in jobs:
        expanded_map[j["job_ref"]] = expand_triggers(j)

    if verbose:
        # show post-expansion form so synthetic triggers from expose/deliver are visible
        expanded_jobs: List[Dict[str, Any]] = []
        for j in jobs:
            j_copy = dict(j)
            j_copy["triggers"] = expanded_map[j["job_ref"]]
            expanded_jobs.append(j_copy)
        _log(json.dumps(expanded_jobs, pretty=True))
        _log("")

    _max_name_len = max((len(_short(j["job_ref"])) for j in jobs), default=8)

    # always show manifest
    _log(f"manifest: {len(jobs)} job(s)")
    _display_manifest(jobs, expanded_map)
    _log("")

    # resolve --select to one trigger per matched job
    selected_refs: Set[str] = set()
    selected_immediate: List[Tuple[TJobDefinition, TTrigger]] = []
    if trigger_selectors:
        selectors = _resolve_selectors(list(trigger_selectors), [j["job_ref"] for j in jobs])
        for job_def in jobs:
            ref = job_def["job_ref"]
            matched = match_triggers_with_selectors(
                job_def["entry_point"]["job_type"],
                expanded_map[ref],
                selectors,
            )
            trigger = pick_trigger(matched, job_def.get("default_trigger"))
            if trigger is not None:
                selected_refs.add(ref)
                selected_immediate.append((job_def, trigger))

    # register ALL expanded triggers for ALL jobs
    scheduler = TriggerScheduler(
        with_future=not no_future,
        with_future_once=once,
    )
    failed_refs: List[str] = []
    port_counter = [INTERACTIVE_PORT_START]

    # register all triggers with scheduler (timed triggers scheduled, events registered)
    for job_def in jobs:
        scheduler.register_triggers(job_def, expanded_map[job_def["job_ref"]])

    # --select triggers fire immediately (interactive jobs only via --select)
    immediate: List[Tuple[TJobDefinition, TTrigger]] = []
    for jd, trigger in selected_immediate:
        if _is_interactive_job(jd) and jd["job_ref"] not in selected_refs:
            continue
        immediate.append((jd, trigger))

    # freshness listeners for ALL jobs with freshness constraints
    for job_def in jobs:
        if job_def.get("freshness"):
            scheduler.register_freshness_listeners(job_def)

    # display execution plan
    idle_count = 0
    active_count = 0
    for job_def in jobs:
        short = _short(job_def["job_ref"])
        if _is_idle_job(job_def, selected_refs):
            idle_count += 1
            _log_job(short, 2, "idle (use --select to start)")
            continue
        active_count += 1
        job_immediate = [t for jd, t in immediate if jd["job_ref"] == job_def["job_ref"]]
        if job_immediate:
            descs = ", ".join(_describe_trigger(t) for t in job_immediate)
            _log_job(short, 2, f"run now: {descs}")
        # show scheduled triggers
        for trigger in job_def.get("triggers", []):
            try:
                parsed = parse_trigger(trigger)
            except InvalidTrigger:
                continue
            tt = parsed.type
            if tt == "schedule" and not no_future:
                _log_job(short, 2, f"scheduled: {parsed.expr}")
            elif tt == "once" and not no_future:
                _log_job(short, 2, f"scheduled once: {parsed.expr}")
        # show event-triggered jobs as waiting
        event_descs: List[str] = []
        for trigger in job_def.get("triggers", []):
            try:
                parsed = parse_trigger(trigger)
            except InvalidTrigger:
                continue
            if parsed.type in ("job.success", "job.fail"):
                event_descs.append(_describe_trigger(trigger))
        if event_descs and not job_immediate:
            _log_job(short, 2, f"waiting: {', '.join(event_descs)}")
    _log("")

    if not immediate and scheduler.is_empty() and not scheduler.has_only_event_triggers():
        _log("nothing to run")
        _close_stores()
        return 0

    if dry_run:
        _log("dry run — no jobs launched")
        _close_stores()
        return 0

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    if refresh and immediate:
        _eager_refresh_cascade(immediate, warn=lambda m: _log(f"warning: {m}"))

    # start immediate jobs with random stagger
    for job_def, trigger in immediate:
        stagger = random.uniform(0, min(2.0, 0.5 * len(immediate)))
        time.sleep(stagger)
        if not _shutting_down:
            _dispatch_job(job_def, trigger, port_counter)

    # event loop
    while not _shutting_down:
        _drain_all_output()
        _collect_completions(scheduler, failed_refs, port_counter)

        # check timed triggers
        due = scheduler.pop_due_jobs()
        for job_def, trigger in due:
            if not _shutting_down:
                _dispatch_job(job_def, trigger, port_counter)

        # display scheduler warnings (e.g. one-shot exhaustion)
        for warning in scheduler.pop_warnings():
            _log(f"warning: {warning}")

        # exit condition: no running processes and no timed items
        if not _processes and scheduler.is_empty():
            if scheduler.has_only_event_triggers():
                for pending_ref in scheduler.pending_event_jobs():
                    short = _short(pending_ref)
                    _log_job(short, 2, "will not execute — upstream job not running")
            break

        next_time = scheduler.get_next_fire_time()
        sleep_time = min(next_time, 0.5) if next_time is not None else 0.5
        time.sleep(sleep_time)

    _drain_all_output()

    _close_stores()

    if failed_refs:
        _log(f"{len(failed_refs)} job(s) failed: {', '.join(failed_refs)}")
        return 1
    return 0


def run_from_module(
    module_name: str,
    trigger_selectors: Optional[List[str]] = None,
    use_all: bool = True,
    no_future: bool = False,
    once: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
    config: Optional[Dict[str, Any]] = None,
    refresh: bool = False,
    warn: Callable[[str], None] = lambda msg: print(  # noqa: T201,T202
        f"warning: {msg}", file=sys.stderr
    ),
) -> int:
    """Import a deployment module, generate manifest, and run matched jobs.

    Args:
        module_name: File path or module name for job discovery.
        trigger_selectors: ``--select`` patterns — fire matching jobs immediately.
        use_all: Use ``__all__`` for discovery.
        no_future: Suppress timed trigger scheduling.
        once: Fire timed triggers once (don't repeat).
        dry_run: Display plan without launching jobs.
        verbose: Show full manifest JSON before running.
        config: Config key=value pairs passed to all jobs.
        refresh: When `True`, clear `prev_completed_run` for each immediate
            non-interval target and its transitive freshness-downstream before
            dispatch (see `run`).
        warn: Output function for warnings.

    Returns:
        Exit code (0 = success, 1 = failure).
    """
    manifest, warnings = manifest_from_module(module_name, use_all=use_all)
    for w in warnings:
        warn(w)

    jobs = manifest.get("jobs", [])
    if not jobs:
        _log("no jobs found in manifest")
        return 0

    return run(
        jobs=jobs,
        trigger_selectors=trigger_selectors,
        no_future=no_future,
        once=once,
        dry_run=dry_run,
        verbose=verbose,
        config=config,
        refresh=refresh,
    )
