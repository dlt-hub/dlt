---
title: Job configuration
description: Per-job options on the dltHub platform — execution timeouts, dependency groups, instance size, TOML configuration sections, and retries
keywords: [dlthub platform, job configuration, timeout, retry, retries, tags, dependency groups, instance, size, require.instance, static egress, execute, require, expose, section]
---
# Job configuration

This page documents the per-job options that aren't about *when* a job runs (those live in [Triggers and scheduling](triggers.md)) but about *how* it runs — execution limits, runner resources, the Python environment it gets, the configuration values it reads at runtime, and retries.

The options below are arguments to the `@run.pipeline`, `@run.job`, `@run.interactive`, and `@run.agent` decorators.

## Execution constraints

`execute={"timeout": "6h"}` overrides the default 120-minute job timeout. Use the dict form to also customize the grace period — the window for the job to finish in-flight work before the dltHub platform hard-kills the process:

```py notype
@run.pipeline(
    my_pipeline,
    execute={"timeout": {"timeout": 7200, "grace_period": 60}},
)
def long_load():
    ...
```

Accepted timeout formats: a duration string (`"6h"`, `"30m"`) or an integer number of seconds.

## Retries

`retry=` on `@run.job`, `@run.pipeline`, and `@run.agent` records how a retryable failure is run again. Pass an integer or a dict. `retry=3` means `max_retries=3`.

```py notype
@run.job(
    retry={
        "max_retries": 3,
        "initial_delay": 30,
        "backoff": 2.0,
        "max_delay": 3600,
        "jitter": True,
    },
)
def load():
    ...
```

| Key             | Meaning                                                             | Default |
| --------------- | ------------------------------------------------------------------- | ------- |
| `max_retries`   | Retries after the first attempt. An integer from 0 to 10. Required. | —       |
| `initial_delay` | Seconds before the first retry.                                     | `30`    |
| `backoff`       | Multiplier applied to the delay after each retry.                   | `2.0`   |
| `max_delay`     | Longest delay, in seconds.                                          | `3600`  |
| `jitter`        | Randomize each delay.                                               | `true`  |

The decorator fills the defaults and stores the policy on the job definition as `execute.retry`, next to `timeout` and `concurrency`. The policy is fixed at decoration, so configuration files and environment variables leave it unchanged. `0` runs the job once.

### Retry tags

Older dlt versions have no `retry=` argument. Configure the same policy with reserved tags. Put them in `expose={"tags": [...]}`, in the same list as any other tag. A `__retry:{N}` tag must be passed to turn retries on. Adding other `__retry_*` tags (such as `__retry_initial_delay`) without `__retry` has no effect.

```py notype
@run.pipeline("github_pipeline", expose={
    "tags": [
        "nightly",
        "__retry:2",
        "__retry_initial_delay:10s",
        "__retry_backoff_factor:2",
        "__retry_max_delay:1m",
    ],
})
def load_commits():
    ...
```

`__retry:2` means two additional attempts (up to 3 runs in total). `__retry:0` means no retries.

Because retries are plain tags, they work with any dlt version that supports job tags, including older releases. No dlt upgrade is needed, and the configuration is forward compatible.

| Tag                               | Meaning                                                                    | Default | Limits                                           |
| --------------------------------- | -------------------------------------------------------------------------- | ------- | ------------------------------------------------ |
| `__retry:<n>`                     | Extra attempts after the first run. This is the tag that turns retries on. | off     | Non-negative integer in plain digits, at most 10 |
| `__retry_initial_delay:<period>`  | Base delay for the first retry                                             | `30s`   | Greater than zero                                |
| `__retry_backoff_factor:<number>` | Multiplier applied after each failed attempt                               | `2`     | At least 1, at most 100                          |
| `__retry_max_delay:<period>`      | Cap on the delay before jitter                                             | `1h`    | Finite and greater than zero                     |

Omit a delay tag to keep its default. A period is `30s`, `1m`, `1h`, `1d`, `1.5s`, or a bare number of seconds. Zero, negative values, milliseconds, and `inf` are rejected. `__retry_max_delay` uses that same syntax.

### How delays are computed

After failed attempt `n` (counting from 1), the delay is `min(initial_delay * backoff_factor^(n - 1), max_delay)`. Jitter then picks the wait uniformly at random between 50% and 100% of that delay, so the wait never exceeds the max delay. Jitter is always on. It is not configurable.

With the tags in the example, the first failure waits between 5s and 10s. The next failure waits between 10s and 20s. Later waits grow until they are capped at `1m`.

Tag configuration always applies that jitter. On `retry=`, `jitter` defaults to on and can be set to `False`, which waits the computed delay with no random reduction. `initial_delay`, `backoff`, and `max_delay` on `retry=` are seconds. `30`, `2.0`, and `3600` match the tag defaults `30s`, `2`, and `1h`.

### What is retried

When the launcher exits with code 75 (`EX_TEMPFAIL`), dltHub will schedule retries, if configured. Any other exit code different from 0 (success) and 75 (`EX_TEMPFAIL`) is considered terminal and won't be retried.

Some common exceptions are automatically converted into retryable errors by `dlt` and will emit exit code 75 without any additional configuration.

The dltHub platform retries a failed run in following cases:

- The script exited with code `75` (`EX_TEMPFAIL`). Use that code to signal a transient error, for example `sys.exit(75)`.
- a builtin `TimeoutError` or `ConnectionError`.
- `requests` `ConnectionError`, `Timeout`, or `ChunkedEncodingError`.
- The run failed to start or to provision.
- The run failed without an exit code (e.g. infrastructure failure, process died unexpectedly, etc).

The following failures are not retried:

- Exit code `1` and any other exit code different from 0 or 75.
- An out-of-memory failure.
- A job's execution limit timeout.
- A cancellation.

```py notype
import sys

sys.exit(75)
```

### Invalid retry tags

An invalid `__retry*` tag fails the deployment. A few of the errors:

- `Unknown retry tag '__retry_foo:1'.`
- `Duplicate retry tag '__retry'.`
- `Retry tag '__retry:' needs a value.`
- `Retry count 'many' must be a non-negative integer.`
- `Retry count must be at most 10.`
- `Retry backoff '0.5' must be at least 1.`
- `Retry backoff must be at most 100.`
- `Retry backoff 'nope' must be a number.`
- `Retry delay 'later' is not a period.`
- `Retry delay '0s' must be greater than zero.`

### Attempts, cancellation, and notifications

A retry is a new attempt of the same run. The run number stays the same and the attempt number increases by one. The UI shows that as `#42 (2)`. The attempt starts from scratch, with the same deployment, job version, configuration, and [trigger](triggers.md). It runs the job from the start.

Archiving the job or the workspace, starting a newer run of the job, or resetting the job's incremental state cancels pending retries. Pausing a job stops its schedule, but pending retries still run. If a concurrency limit blocks a retry, it waits and tries again shortly.

When retries are configured for the job, no notifications are send for intermediary failures. The failure notification, and any [on-failure follow-up jobs](triggers.md#follow-up-triggers), happen only after the final attempt, if it keeps failing. See [email alerts](../notifications/email.md) and [Slack alerts](../notifications/slack.md).

:::caution Reserved tags
Tags with a double underscore prefix (`__`) are **reserved** by dlt. Don't use that prefix for your own tags. A tag with that prefix also never acts as a [trigger](triggers.md).
:::

## Dependency groups

Install extra packages only for the jobs that need them. Declare a group in `pyproject.toml`:

```toml
[dependency-groups]
ibis = ["ibis-framework[duckdb]", "duckdb<2"]
```

:::note
Ibis doesn't yet support DuckDB 2.
:::

Then opt into it in the decorator:

```py notype
@run.pipeline(my_pipeline, require={"dependency_groups": ["ibis"]})
def transform(run_context: TJobRunContext):
    ...
```

The dltHub platform composes the execution environment from the workspace's base dependencies plus the job's declared groups.

## Instance size

Pick how much CPU and memory the job’s runner gets. Pass it under `require.instance`:

```py notype
@run.pipeline(
    my_pipeline,
    require={"instance": {"size": "medium"}},
)
def heavy_sync():
    ...
```

| `size`   | vCPU | Memory | Disk   | Multiplier |
| -------- | ---- | ------ | ------ | ---------- |
| `small`  | 2    | 4 GiB  | 500 GB | 1×         |
| `medium` | 4    | 8 GiB  | 500 GB | 2×         |
| `large`  | 8    | 16 GiB | 500 GB | 4×         |
| `xlarge` | 16   | 32 GiB | 500 GB | 8×         |

If you omit `instance`, jobs default to `small`. Larger sizes use a higher `multiplier` against your organization's run time budget. For example, a one-hour `large` run consumes four hours of budget.

Pipeline-level tuning (chunking, parallelism, memory settings) often lowers the size you need, see [Optimizing dlt](../../reference/performance.md).

## Static egress IPs

Use this when you must whitelist outbound IP addresses so external systems can grant your jobs access to private resources. Opt in per job so outbound requests use your workspace's static egress IPs:

```py notype
@run.pipeline(my_pipeline, require={"static_egress_ips": True})
def sync_from_vendor():
    ...
```

Which static egress IPs your jobs use depends on your organization's region and data residency settings. See [Regions and data residency](../platform-capabilities/regions.md) for how regional data planes relate to your organization.

The static egress IPs for the **EU region** are:

- 63.181.217.92
- 18.156.57.4
- 63.183.227.2
- 63.182.151.74

The static egress IPs for the **US region** are:

- 34.205.113.62
- 44.221.24.144
- 34.193.87.36
- 98.80.106.70
- 54.81.217.233

## Inputs and outputs

The parameters of a job with a default or `dlt.config.value` are its inputs, and a TypedDict return type is its output. `dlthub deploy` writes both into the deployment manifest as JSON Schemas. Describe a parameter or a field with `run.Doc`. Mark a parameter or a field that holds the id of a workspace entity with `run.Entity`:

```py notype
from typing import Annotated, TypedDict

class Report(TypedDict):
    """Rows the report counted."""
    pipeline: Annotated[str, run.Entity("pipeline")]
    rows: int

@run.job
def rerun_report(
    run_id: Annotated[str, run.Entity("job-runs"), run.Doc("Run to report on")] = dlt.config.value,
) -> Report:
    ...
```

- Entity types are `job-runs`, `job`, `workspace`, `pipeline`, and `dataset`. Another type fails `dlthub deploy`.
- The [job result](deployments.md#job-results) of a run lists the entities from its entity-typed inputs, overwritten by output fields of the same name.
- The first entity-typed input becomes `expose.object_input` in the manifest. The Web UI reads it to link the job to entities.

See [Entity-typed inputs and outputs](../agents/agent-definitions.md#entity-typed-inputs-and-outputs) for how agent jobs use them.

## Job configuration via TOML

Jobs read configuration through dlt's standard config system. The default section is the containing module name:

```toml
# applies to every job defined in usgs_pipeline.py
[jobs.usgs_pipeline]
epoch = "2026-04-05T00:00:00+00:00"

# overrides for one specific job
[jobs.usgs_pipeline.usgs_daily]
epoch = "2026-04-10T00:00:00+00:00"
```

For inline jobs in `__deployment__.py`, pass `section="my_job"` to the decorator to give it a clean section name. Profile-aware overrides live in `dev.config.toml`, `prod.config.toml`, etc. — see [Workspace setup](workspace-setup.md#setting-up-configuration-files).

## Display metadata

`expose={...}` controls how the job appears in the dashboard and to selectors:

```py notype
@run.pipeline(
    "github_pipeline",
    expose={
        "tags": ["ingest"],
        "display_name": "GitHub commits ingest",
    },
)
def load_commits():
    ...
```

| Key            | Purpose                                                                                |
| -------------- | -------------------------------------------------------------------------------------- |
| `tags`         | List of labels for grouping in the dashboard and matching CLI selectors (`tag:ingest`) |
| `display_name` | Human-readable label shown in the dashboard                                            |
| `manual`       | Whether the runner adds the `manual:` trigger. `True` by default                       |

See [Tags and bulk triggering](triggers.md#tags-and-bulk-triggering) for how tags drive `dlthub job trigger` selectors. The same list holds [retry settings](#retry-tags). A tag that starts with `__` is reserved by dlt.

## Next steps

- [Triggers and scheduling](triggers.md) — schedule and chain jobs
- [Deployments](deployments.md) — push these decorators to the dltHub platform
