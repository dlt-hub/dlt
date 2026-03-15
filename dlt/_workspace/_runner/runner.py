"""Local workspace runner — simulates the runtime scheduler for development."""

import os
import random
import signal
import sys
import time
import zlib
from typing import Callable, Dict, List, Optional, Set, Tuple
from uuid import uuid4

from dlt.common import json
from dlt._workspace.deployment._job_ref import resolve_job_ref, short_name as job_short_name
from dlt._workspace.deployment.launchers import LAUNCHER_JOB, LAUNCHER_MODULE
from dlt._workspace.deployment.manifest import generate_manifest, import_deployment_module
from dlt._workspace.deployment.typing import (
    TJobDefinition,
    TRuntimeEntryPoint,
    TTrigger,
)
from dlt._workspace._runner.process import JobProcess
from dlt._workspace._runner.scheduler import TriggerScheduler
from dlt._workspace.deployment._trigger_helpers import filter_jobs_by_selectors, parse_trigger

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


def _log(msg: str) -> None:
    """Log a generic runner message to stderr."""
    print(msg, file=sys.stderr, flush=True)  # noqa: T201


def _log_job(short: str, max_len: int, stream_no: int, line: str) -> None:
    """Log a job-prefixed line.

    stream_no=1: subprocess stdout → stdout
    stream_no=2: subprocess stderr or runner status → stderr
    """
    target = sys.stdout if stream_no == 1 else sys.stderr
    if _use_color:
        color = _job_color(short)
        prefix = f"{color}{_BOLD}{short:<{max_len}}{_RESET} | "
    else:
        prefix = f"{short:<{max_len}} | "
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


def _start_job(
    job_def: TJobDefinition,
    trigger: TTrigger,
    port_counter: List[int],
    max_name_len: int,
) -> None:
    """Start a job subprocess."""
    job_ref = job_def["job_ref"]
    if job_ref in _processes and _processes[job_ref].is_alive():
        return

    entry_point: TRuntimeEntryPoint = dict(job_def["entry_point"])  # type: ignore[assignment]
    job_type = entry_point["job_type"]

    # provision port for interactive jobs
    if job_type == "interactive":
        entry_point["run_args"] = {"port": port_counter[0]}
        port_counter[0] += 1

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

    short = job_short_name(job_ref)
    _log_job(short, max_name_len, 2, f"starting (trigger: {trigger})")

    if job_type == "interactive":
        port = entry_point["run_args"]["port"]
        _log_job(short, max_name_len, 2, f"listening on http://localhost:{port}")

    # extract grace period from job definition timeout spec
    grace_period = JobProcess.DEFAULT_GRACE_PERIOD
    timeout_spec = job_def.get("execution", {}).get("timeout")
    if isinstance(timeout_spec, dict):
        grace_period = timeout_spec.get("grace_period", grace_period)

    proc = JobProcess(job_ref, cmd, grace_period=grace_period)
    proc.start()
    _processes[job_ref] = proc


def _drain_all_output(max_name_len: int) -> None:
    """Drain and print output from all running processes."""
    for proc in list(_processes.values()):
        for stream_no, line in proc.drain_output():
            short = job_short_name(proc.job_ref)
            _log_job(short, max_name_len, stream_no, line)


def _collect_completions(
    scheduler: TriggerScheduler,
    failed_refs: List[str],
    max_name_len: int,
    port_counter: List[int],
) -> None:
    """Check completed processes, fire events, start triggered jobs."""
    for job_ref, proc in list(_processes.items()):
        if not proc.is_alive():
            exit_code = proc.poll()
            short = job_short_name(job_ref)
            if exit_code == 0:
                _log_job(short, max_name_len, 2, "completed successfully")
                event = f"job.success:{job_ref}"
            else:
                _log_job(short, max_name_len, 2, f"failed (exit code {exit_code})")
                event = f"job.fail:{job_ref}"
                failed_refs.append(job_ref)

            triggered = scheduler.fire_event(event)
            for jd, trig in triggered:
                if not _shutting_down:
                    _start_job(jd, trig, port_counter, max_name_len)

            del _processes[job_ref]


def _describe_trigger(trigger: TTrigger) -> str:
    """Human-readable description of a trigger."""
    try:
        parsed = parse_trigger(trigger)
    except ValueError:
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
    return str(trigger)


def _resolve_selectors(selectors: List[str], jobs: List[TJobDefinition]) -> List[str]:
    """Resolve selectors: job refs become manual: selectors, others pass through."""
    resolved: List[str] = []
    for sel in selectors:
        # already a trigger selector (has : or is a known type keyword)
        if ":" in sel or sel in ("batch", "interactive", "stream", "job"):
            resolved.append(sel)
            continue
        # try resolving as job ref
        try:
            ref = resolve_job_ref(sel, jobs)
            resolved.append(f"manual:{ref}")
        except ValueError:
            # might be a glob pattern for manual refs (e.g. "batch_jobs.*")
            resolved.append(f"manual:jobs.{sel}")
    return resolved


def select_jobs(
    all_jobs: List[TJobDefinition],
    selectors: List[str],
) -> Tuple[List[TJobDefinition], List[TJobDefinition]]:
    """Select jobs by selectors and collect their transitive followup chain.

    Returns:
        Tuple of (directly_selected, followup). Directly selected jobs run with
        all their triggers. Followup jobs only run via event triggers (job.success/fail).
    """
    directly_selected = filter_jobs_by_selectors(all_jobs, selectors)
    if not directly_selected:
        return [], []

    active_refs: Set[str] = {j["job_ref"] for j in directly_selected}
    followup: List[TJobDefinition] = []

    changed = True
    while changed:
        changed = False
        for j in all_jobs:
            if j["job_ref"] in active_refs:
                continue
            for trigger in j.get("triggers", []):
                try:
                    parsed = parse_trigger(trigger)
                except ValueError:
                    continue
                if parsed.type in ("job.success", "job.fail") and parsed.expr in active_refs:
                    followup.append(j)
                    active_refs.add(j["job_ref"])
                    changed = True
                    break

    return directly_selected, followup


def _display_manifest(jobs: List[TJobDefinition], max_name_len: int) -> None:
    """Display manifest job summary to stderr."""
    for j in jobs:
        short = job_short_name(j["job_ref"])
        triggers = j.get("triggers", [])
        display: List[str] = [t for t in triggers if not t.startswith("manual:")]
        if not display:
            display = ["(manual only)"]
        if _use_color:
            color = _job_color(short)
            name = f"{color}{_BOLD}{short:<{max_name_len}}{_RESET}"
        else:
            name = f"{short:<{max_name_len}}"
        _log(f"  {name}  {', '.join(display)}")


def run(
    jobs: List[TJobDefinition],
    trigger_selectors: Optional[List[str]] = None,
    run_manual: bool = False,
    with_future: bool = False,
    with_future_once: bool = False,
    dry_run: bool = False,
) -> int:
    """Run jobs from a deployment manifest locally.

    Args:
        jobs: Job definitions from the manifest.
        trigger_selectors: Glob patterns to filter triggers.
        run_manual: If True, trigger all jobs with manual: trigger.
        with_future: Schedule future jobs (cron, every, once).
        with_future_once: Schedule future jobs but fire only once.
        dry_run: Display plan without launching jobs.

    Returns:
        Exit code (0 = all jobs succeeded, 1 = any job failed).
    """
    global _processes, _shutting_down
    _processes = {}
    _shutting_down = False

    selectors = list(trigger_selectors or [])
    if run_manual:
        selectors.append("manual:*")
    if with_future or with_future_once:
        selectors.extend(["schedule:*", "every:*", "once:*"])

    # resolve job ref selectors into manual: selectors
    selectors = _resolve_selectors(selectors, jobs)

    max_name_len = max((len(job_short_name(j["job_ref"])) for j in jobs), default=8)

    # always show manifest
    _log(f"manifest: {len(jobs)} job(s)")
    _display_manifest(jobs, max_name_len)
    _log("")

    if not selectors:
        _log("nothing to run (use --run-manual or --select to choose jobs)")
        return 0

    directly_selected, followup = select_jobs(jobs, selectors)
    if not directly_selected:
        _log("no jobs matched the provided selectors")
        return 0

    all_selected = directly_selected + followup

    scheduler = TriggerScheduler(
        with_future=with_future,
        with_future_once=with_future_once,
    )
    failed_refs: List[str] = []
    max_name_len = max(len(job_short_name(j["job_ref"])) for j in all_selected)
    port_counter = [INTERACTIVE_PORT_START]

    # register directly selected jobs with all triggers
    immediate: List[Tuple[TJobDefinition, TTrigger]] = []
    for job_def in directly_selected:
        job_immediate = scheduler.register_job(job_def)
        immediate.extend(job_immediate)

    # register followup jobs with event triggers only
    for job_def in followup:
        scheduler.register_followup(job_def)

    # display execution plan
    _log(f"selected {len(all_selected)} job(s):")
    for job_def in directly_selected:
        short = job_short_name(job_def["job_ref"])
        job_triggers = [t for jd, t in immediate if jd["job_ref"] == job_def["job_ref"]]
        if job_triggers:
            descs = ", ".join(_describe_trigger(t) for t in job_triggers)
            _log_job(short, max_name_len, 2, f"run now: {descs}")

    for job_def in followup:
        short = job_short_name(job_def["job_ref"])
        waiting = []
        for trigger in job_def.get("triggers", []):
            try:
                parsed = parse_trigger(trigger)
            except ValueError:
                continue
            if parsed.type in ("job.success", "job.fail"):
                waiting.append(_describe_trigger(trigger))
        if waiting:
            _log_job(short, max_name_len, 2, f"waiting: {', '.join(waiting)}")

    # display warnings for future triggers
    for job_def in all_selected:
        short = job_short_name(job_def["job_ref"])
        for trigger in job_def.get("triggers", []):
            try:
                parsed = parse_trigger(trigger)
            except ValueError:
                continue
            tt = parsed.type
            if tt == "schedule" and not with_future:
                _log_job(
                    short,
                    max_name_len,
                    2,
                    f"warning: {trigger} will not fire (use --with-future)",
                )
            elif tt == "every" and not with_future:
                _log_job(
                    short,
                    max_name_len,
                    2,
                    f"warning: {trigger} will not repeat (use --with-future)",
                )
            elif tt == "once" and not with_future:
                _log_job(
                    short,
                    max_name_len,
                    2,
                    f"warning: {trigger} will not fire (use --with-future)",
                )
            elif tt == "schedule" and with_future:
                _log_job(short, max_name_len, 2, f"scheduled: {parsed.expr}")
            elif tt == "once" and with_future:
                _log_job(short, max_name_len, 2, f"scheduled once: {parsed.expr}")
    _log("")

    if dry_run:
        _log("dry run — no jobs launched")
        return 0

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # start immediate jobs with random stagger
    for job_def, trigger in immediate:
        stagger = random.uniform(0, min(2.0, 0.5 * len(immediate)))
        time.sleep(stagger)
        if not _shutting_down:
            _start_job(job_def, trigger, port_counter, max_name_len)

    # event loop
    while not _shutting_down:
        _drain_all_output(max_name_len)
        _collect_completions(scheduler, failed_refs, max_name_len, port_counter)

        # check timed triggers
        due = scheduler.pop_due_jobs()
        for job_def, trigger in due:
            if not _shutting_down:
                _start_job(job_def, trigger, port_counter, max_name_len)

        # display scheduler warnings (e.g. one-shot exhaustion)
        for warning in scheduler.pop_warnings():
            _log(f"warning: {warning}")

        # exit condition
        if not _processes and scheduler.is_empty():
            break
        if not _processes and scheduler.has_only_event_triggers():
            orphaned = scheduler.pending_event_jobs()
            for ref in orphaned:
                short = job_short_name(ref)
                _log_job(short, max_name_len, 2, "will not execute — upstream job not running")
            break

        next_time = scheduler.get_next_fire_time()
        sleep_time = min(next_time, 0.5) if next_time is not None else 0.5
        time.sleep(sleep_time)

    # final drain
    _drain_all_output(max_name_len)

    if failed_refs:
        _log(f"{len(failed_refs)} job(s) failed: {', '.join(failed_refs)}")
        return 1
    return 0


def _resolve_module_name(name_or_path: str) -> str:
    """Convert a file path to a module name, or return as-is if already a module name."""
    if name_or_path.endswith(".py"):
        # strip .py, convert path separators to dots
        name_or_path = name_or_path[:-3]
    if os.sep in name_or_path or "/" in name_or_path:
        name_or_path = name_or_path.replace(os.sep, ".").replace("/", ".")
    # strip leading dots
    return name_or_path.lstrip(".")


def run_from_module(
    module_name: str,
    trigger_selectors: List[str],
    use_all: bool = True,
    run_manual: bool = False,
    with_future: bool = False,
    with_future_once: bool = False,
    dry_run: bool = False,
    warn: Callable[[str], None] = lambda msg: print(  # noqa: T201,T202
        f"warning: {msg}", file=sys.stderr
    ),
) -> int:
    """Import a deployment module, generate manifest, and run matched jobs.

    Args:
        module_name: File path or module name for job discovery.
        trigger_selectors: Trigger selector patterns.
        use_all: Use `__all__` for discovery.
        run_manual: Trigger all manual jobs.
        with_future: Schedule future jobs.
        with_future_once: Schedule future jobs, fire once.
        dry_run: Display plan without launching jobs.
        echo: Output function for messages.
        warn: Output function for warnings.

    Returns:
        Exit code (0 = success, 1 = failure).
    """
    module_name = _resolve_module_name(module_name)

    # ensure cwd is on sys.path so relative imports work
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    mod = import_deployment_module(module_name)

    manifest, warnings = generate_manifest(mod, use_all=use_all)
    for w in warnings:
        warn(w)

    jobs = manifest.get("jobs", [])
    if not jobs:
        _log("no jobs found in manifest")
        return 0

    return run(
        jobs=jobs,
        trigger_selectors=trigger_selectors,
        run_manual=run_manual,
        with_future=with_future,
        with_future_once=with_future_once,
        dry_run=dry_run,
    )
