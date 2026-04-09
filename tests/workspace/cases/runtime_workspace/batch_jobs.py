"""Batch jobs for the test workspace."""

import dlt
from dlt.hub.run import job, TJobRunContext


@job(execute={"timeout": {"timeout": 86400.0}})
def backfill():
    """Backfill historical data."""
    return "backfill_done"


@job(trigger="0 8 * * *", execute={"timeout": {"timeout": 14400.0}})
def daily_ingest():
    return "ingested"


@job(trigger=[backfill.success, daily_ingest.success])
def transform():
    """Transform ingested data."""
    return "transformed"


@job(expose={"starred": True, "tags": ["ops"]})
def maintenance(cleanup_days=dlt.config.value):
    """Run maintenance tasks."""
    pass


@job
def context_aware(run_context: TJobRunContext):
    """Job that receives run context."""
    return f"run_id={run_context['run_id']},trigger={run_context['trigger']}"


@job
def context_optional(run_context: TJobRunContext = None):
    """Job with optional run context — works with and without injection."""
    if run_context is not None:
        return f"got_context:{run_context['run_id']}"
    return "no_context"


@job
def interval_aware(run_context: TJobRunContext):
    """Job that reads interval from run_context and dlt.current.interval()."""
    import dlt

    ctx_start = run_context.get("interval_start")
    iv_from_current = dlt.current.interval()
    parts = []
    if ctx_start:
        parts.append(f"ctx_start={ctx_start.isoformat()}")
    if iv_from_current:
        parts.append(f"current_start={iv_from_current[0].isoformat()}")
    return ",".join(parts) if parts else "no_interval"


@job
def incremental_interval_job(run_context: TJobRunContext):
    """Job that creates an incremental resource and checks scheduler join."""
    from datetime import datetime  # noqa: I251
    from dlt.common.pendulum import pendulum

    @dlt.resource()
    def my_events(
        updated_at: dlt.sources.incremental[datetime] = dlt.sources.incremental("updated_at"),
    ):
        yield {"updated_at": pendulum.datetime(2024, 1, 15, 12, tz="UTC")}

    r = my_events()
    items = list(r)
    inc = r.incremental._incremental
    return f"iv={inc.initial_value},end={inc.end_value},items={len(items)}"


@job
def profile_aware(run_context: TJobRunContext):
    """Job that reads the workspace profile env var set by the launcher."""
    import os

    return f"profile={os.environ.get('WORKSPACE__PROFILE', '')}"


@job
def refresh_observer(run_context: TJobRunContext):
    """Test job: prints observed refresh signal and interval bounds to stdout."""
    print(f"REFRESH_FLAG={run_context.get('refresh', False)}")  # noqa: T201
    iv_start = run_context.get("interval_start")
    iv_end = run_context.get("interval_end")
    if iv_start is not None:
        print(f"INTERVAL_START={iv_start.isoformat()}")  # noqa: T201
    else:
        print("INTERVAL_START=<unset>")  # noqa: T201
    if iv_end is not None:
        print(f"INTERVAL_END={iv_end.isoformat()}")  # noqa: T201
    else:
        print("INTERVAL_END=<unset>")  # noqa: T201
    return "observed"
