---
title: Triggers and scheduling
description: Schedule jobs on the dltHub platform with cron, intervals, follow-up chains, freshness constraints, and refresh cascades
keywords: [dlthub platform, triggers, scheduling, cron, interval, backfill, follow-up, freshness, refresh, tags]
---

# Triggers and scheduling

A **trigger** declares when a job runs. Triggers are attached to a decorated job via the `trigger=` argument and are the source of truth for scheduling on the dltHub platform — there is no separate CLI for adding or removing schedules. Change the decorator, redeploy.

```py
from dlt.hub import run
from dlt.hub.run import trigger

@run.pipeline("github_pipeline", trigger=trigger.every("5m"))
def load_commits():
    ...
```

This page covers all the trigger types and the related scheduling features.

## Basic triggers

| Trigger | Meaning |
|---------|---------|
| `trigger.every("5m")` | Recurring interval (`"5m"`, `"6h"`, seconds as float) |
| `trigger.schedule("0 * * * *")` | Cron expression |
| `trigger.once("2026-12-31T23:59:59Z")` | One-shot at a timestamp |
| `"*/5 * * * *"` | Bare cron string — auto-detected |
| `upstream_job.success` | Follow-up — fires when an upstream job completes successfully |
| `upstream_job.fail` | Follow-up — fires when an upstream job fails |
| `upstream_job.completed` | Follow-up — fires on success or failure |

## Multiple triggers

A job can have any number of triggers. Pass a list and inspect `run_context["trigger"]` to discover which one fired:

```py
from dlt.hub.run import TJobRunContext

@run.job(
    trigger=[
        trigger.schedule("0 * * * *"),
        upstream_ingest.success,
    ],
)
def transform(run_context: TJobRunContext):
    if run_context["trigger"] == "schedule":
        ...
    elif run_context["trigger"] == "followup":
        ...
```

`TJobRunContext` is a dict injected by the launcher with: `run_id`, `trigger`, `refresh`, and the scheduler-supplied `interval_start` / `interval_end` (see [Scheduler-driven intervals](#scheduler-driven-intervals) below).

## Follow-up triggers

Every decorated job exposes `.success`, `.fail`, and `.completed` trigger properties. Use them to chain jobs into a dependency graph.

```py
from dlt.hub.run import TJobRunContext

@run.pipeline("transform_pipeline", trigger=ingest_job.success)
def transform(run_context: TJobRunContext):
    ...
```

Follow-up triggers fire as soon as the upstream completes — no polling, no scheduler delay.

## Scheduler-driven intervals

For incremental pipelines, declare the overall time range with `interval=` and let the dltHub platform hand each run a `[interval_start, interval_end]` window:

```py
@run.pipeline(
    my_pipeline,
    interval={"start": "2026-01-01T00:00:00Z"},
    trigger=trigger.schedule("*/3 * * * *"),
)
def daily_ingest(run_context: TJobRunContext):
    start = run_context["interval_start"]
    end = run_context["interval_end"]
    # pass start/end into your source so it is a pure function of inputs
    ...
```

Behaviour:

- Each run gets the interval that just elapsed
- Missed runs are backfilled automatically — windows extend back continuously
- On refresh, the dltHub platform resets the interval pointer to `interval.start`
- Source code stays stateless — no cursor persistence, no state lookups

### Schedule/cron and every intervals

A `schedule` trigger is defined with a cron expression and produces intervals that start and end at absolute points in time — the cron ticks. The scheduler starts a job when an interval **closes**, handing it the window that just elapsed. Take a daily schedule firing at 3am, `trigger.schedule("0 3 * * *")`: the run started on May 26th at 3:00 receives the interval from May 25th 3:00 to May 26th 3:00. The same rule applies to a newly deployed job — it does not run on deployment but starts for the first time when the current interval elapses: deploy the 3am job at noon on May 25th and the first run happens on May 26th at 3:00, covering the May 25th–26th window.

When such a job is started manually (e.g., `dlthub job trigger` or `dlthub run`), it receives the *current* interval: from the end of the last completed interval up to the most recent elapsed cron tick. In most cases these coincide — the scheduled run already covered everything up to the last tick, and the window currently in progress belongs to the next tick — so the interval is **empty** (`interval_start == interval_end`). An empty interval means there is no new window to process, and incremental jobs should treat it as a no-op. The exception is a missed or failed scheduled tick: a manual run then backfills the uncovered gap. To force already-loaded windows to be reprocessed, use a [refresh](#refresh-cascade) instead.

An `every` trigger generates relative intervals of a fixed period, starting from now rather than at absolute tick times. A newly deployed job runs for the first time once the period has elapsed: deploy `trigger.every("1h")` at 14:20 and the first run starts at 15:20 with the interval 14:20 to 15:20. When run manually, the interval spans from the previous run start to now — so unlike cron jobs, manual runs of `every` jobs always receive a non-empty interval.


## Freshness checks

`freshness=[upstream.is_fresh]` blocks a job until the upstream's most recent interval has fully completed:

```py
@run.pipeline(
    "report_pipeline",
    trigger=trigger.schedule("0 * * * *"),
    freshness=[ingest_job.is_fresh],
)
def build_report(run_context: TJobRunContext):
    ...
```

Unlike a trigger, the job still runs on its own schedule — it just skips while upstream is mid-load. Use for transforms that must not observe partial data.

:::note
Invalidation and replacement of a single interval is not yet supported. Once done you'll be able to do:

* **Parallel backfills** — splitting a historical range into many intervals processed concurrently.
* **Partial refresh** — invalidating specific intervals so only the affected windows are reloaded.
* **Interval-based freshness** — today freshness is a single watermark: a downstream job is gated only on the upstream's most recent completed interval. With interval-based freshness, every upstream interval that arrives (including late or replayed ones) would mark the matching downstream window stale, so the downstream job is re-run exactly for the affected windows instead of relying on a full refresh cascade.
:::

## Refresh cascade

A backfill job with `refresh="always"` originates a refresh signal that propagates through all downstream jobs in the dependency graph. Downstream jobs receive `run_context["refresh"] = True` and react accordingly (for example `pipeline.refresh = "drop_sources"`).

Refresh policies:

| Policy | Behaviour |
|--------|-----------|
| `"always"` | Originate a refresh signal on every run |
| `"auto"` | Pass through any refresh signal received from upstream (default) |
| `"block"` | Stop refresh propagation here |

```py
@run.job(expose={"tags": ["backfill"]}, refresh="always")
def backfill():
    """Cascade a refresh; does not load data."""
```

Then trigger it from the CLI:

```sh
dlthub job trigger "tag:backfill"
dlthub run backfill --refresh    # explicit refresh on a single job
```

Note that the refresh signal will not drop your data automatically, you should use one of the [refresh](../../general-usage/pipeline.md#refresh-pipeline-data-and-state) options available.
```py
@run.pipeline(
    "report_pipeline",
    trigger=trigger.schedule("0 * * * *"),
    freshness=[ingest_job.is_fresh],
)
def build_report(run_context: TJobRunContext):
    ...
    report_pipeline.run(
        data_source(),
        refresh="drop_data" if run_context["refresh"] else None
    )
```
Above we tell `dlt` to truncate all tables belonging to resources in `data_source()` if the refresh signal got passed in the `refresh` flag.

## Tags and bulk triggering

Tags are labels on jobs (set via `expose={"tags": [...]}`). They are used to:

1. Group related jobs in the dashboard
2. Run bulk operations from the CLI via **selectors**

```sh
# trigger every job tagged "ingest"
dlthub job trigger "tag:ingest"

# trigger every job that has a schedule
dlthub job trigger "schedule:*"

# preview without running
dlthub job trigger "tag:ingest" --dry-run
```

## Timezone

Cron expressions default to UTC. To interpret them in a specific IANA timezone, declare it on the job:

```py
@run.pipeline(
    my_pipeline,
    trigger=trigger.schedule("0 9 * * *"),    # 9am
    require={"timezone": "Europe/Berlin"},    # ...in Berlin time
)
def morning_load():
    ...
```

Intervals in `run_context` remain UTC datetimes, but they align to tick boundaries in the declared timezone.

## Next steps

- [Job configuration](job-configuration.md) — execution timeouts, dependency groups, TOML config sections
- [Deployments](deployments.md) — `dlthub deploy` and the deployment manifest
- [Monitoring and debugging](monitoring.md) — watch what triggers fire and diagnose failures
