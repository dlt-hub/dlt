"""Batch jobs for the test workspace."""

import threading
import time
from typing import Iterator

import dlt
from dlt.common.runtime import signals
from dlt.common.utils import uniq_id
from dlt.hub.run import job, TJobRunContext

EXTRACT_STARTED = threading.Event()
"""Set by `long_extract` on first yield."""


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
def timezone_aware():
    """Job that reports the context timezone, and how `dlt` stores a value."""
    from datetime import datetime

    from dlt.common.configuration.container import Container
    from dlt.common.configuration.specs.timezone_context import TimezoneContext
    from dlt.common.time import get_context_timezone, normalize_timezone

    stored = normalize_timezone(datetime(2024, 1, 15, 23, 30), True)
    # `in` does not create a default instance, unlike `get`
    has_ctx = TimezoneContext in Container()
    return f"tz={get_context_timezone()},stored={stored.isoformat()},tz_ctx={has_ctx}"


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


@job(
    incremental_mode="interval",
    interval={"start": "2024-01-15T00:00:00Z"},
    trigger="0 0 * * *",
)
def incremental_interval_job(run_context: TJobRunContext):
    """Job that creates an incremental resource and checks scheduler join."""
    from datetime import datetime, timezone
    from dlt.extract.incremental.context import get_interval_context

    ctx = get_interval_context()
    ctx_flag = ctx.allow_external_schedulers if ctx else None

    @dlt.resource()
    def my_events(
        updated_at: dlt.sources.incremental[datetime] = dlt.sources.incremental("updated_at"),
    ):
        yield {"updated_at": datetime(2024, 1, 15, 12, tzinfo=timezone.utc)}

    r = my_events()
    items = list(r)
    inc = r.incremental._incremental
    return f"iv={inc.initial_value},end={inc.end_value},items={len(items)},allow_ext={ctx_flag}"


@job(
    name="daily",
    section="clean_sec",
    interval={"start": "2024-01-15T00:00:00Z"},
    trigger="0 0 * * *",
)
def named_section_job(run_context: TJobRunContext):
    """Job configured under its own section, `[jobs.clean_sec.daily]`."""
    from dlt.extract.incremental.context import get_interval_context

    ctx = get_interval_context()
    return f"allow_ext={ctx.allow_external_schedulers if ctx else None}"


@job(
    incremental_mode="interval",
    interval={"start": "2020-01-01T00:00:00Z"},
    trigger="0 0 * * *",
)
def epoch_override_job(run_context: TJobRunContext):
    """Override interval start via `dlt.current.interval.update` before incremental bind."""
    from datetime import datetime, timezone
    from dlt.common.time import ensure_datetime_in_tz

    dlt.current.interval.update(start=ensure_datetime_in_tz("2023-06-01T00:00:00Z"))

    @dlt.resource()
    def my_events(
        updated_at: dlt.sources.incremental[datetime] = dlt.sources.incremental("updated_at"),
    ):
        yield {"updated_at": datetime(2024, 1, 15, 12, tzinfo=timezone.utc)}

    r = my_events()
    list(r)
    inc = r.incremental._incremental
    return f"iv={inc.initial_value.isoformat()},end={inc.end_value.isoformat()}"


@job
def profile_aware(run_context: TJobRunContext):
    """Job that reads the workspace profile env var set by the launcher."""
    import os

    return f"profile={os.environ.get('WORKSPACE__PROFILE', '')}"


@job
def auto_refresh_pipeline():
    """Job that creates a pipeline and reports its refresh mode."""
    p = dlt.pipeline(f"auto_refresh_{uniq_id()}")
    return f"refresh={p.refresh}"


@job
def explicit_refresh_pipeline():
    """Job that creates a pipeline with an explicit refresh argument."""
    p = dlt.pipeline(f"explicit_refresh_{uniq_id()}", refresh="drop_data")
    return f"refresh={p.refresh}"


JOB_STARTED = threading.Event()
"""Set by `wait_for_signal` on entry."""


@job
def wait_for_signal() -> None:
    """Signal-aware wait that proves launcher-level signal interception."""
    JOB_STARTED.set()
    signals.sleep(30)
    signals.raise_if_signalled()


@job
def long_extract() -> None:
    """Pipeline whose extract yields one item per second for up to 60s."""
    EXTRACT_STARTED.clear()

    @dlt.resource(name="slow_numbers")
    def _slow_numbers() -> Iterator[int]:
        for i in range(60):
            EXTRACT_STARTED.set()
            time.sleep(1)
            yield i

    pipeline = dlt.pipeline(
        pipeline_name="signal_long_extract",
        destination="duckdb",
        dataset_name="_data",
    )
    pipeline.run(_slow_numbers())


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
