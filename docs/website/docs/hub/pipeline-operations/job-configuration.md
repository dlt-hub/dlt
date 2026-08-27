---
title: Job configuration
description: Per-job options on the dltHub platform, including execution timeouts, dependency groups, instance size, TOML configuration sections, and jobs created from module imports
keywords: [dlthub platform, job configuration, timeout, dependency groups, instance, size, require.instance, static egress, execute, require, expose, section, module imports, dunders]
---

# Job configuration

This page documents the per-job options that aren't about *when* a job runs (those live in [Triggers and scheduling](triggers.md)) but about *how* it runs: execution limits, runner resources, the Python environment it gets, and the configuration values it reads at runtime.

Most options below are arguments to the `@run.pipeline`, `@run.job`, and `@run.interactive` decorators. A module deployed without a decorator takes `expose` and `require` through the `__expose__` and `__require__` dunders instead, see [Jobs created from module imports](#jobs-created-from-module-imports).

## Execution constraints

`execute={"timeout": "6h"}` overrides the default 120-minute job timeout. Use the dict form to also customize the grace period, the window for the job to finish in-flight work before the dltHub platform hard-kills the process:

```py
@run.pipeline(
    my_pipeline,
    execute={"timeout": 7200, "grace_period": 60},
)
def long_load():
    ...
```

Accepted timeout formats: a duration string (`"6h"`, `"30m"`) or an integer number of seconds.

## Dependency groups

Install extra packages only for the jobs that need them. Declare a group in `pyproject.toml`:

```toml
[dependency-groups]
ibis = ["ibis-framework[duckdb]"]
```

Then opt into it in the decorator:

```py
@run.pipeline(my_pipeline, require={"dependency_groups": ["ibis"]})
def transform(run_context: TJobRunContext):
    ...
```

The dltHub platform composes the execution environment from the workspace's base dependencies plus the job's declared groups.

## Instance size

:::warning
This feature is in public preview
:::

Pick how much CPU and memory the job’s runner gets. Pass it under `require.instance`:

```py
@run.pipeline(
    my_pipeline,
    require={"instance": {"size": "medium"}},
)
def heavy_sync():
    ...
```

| `size` | vCPU | Memory | Disk | Multiplier |
|--------|------|--------|------|------------|
| `small` | 2 | 4 GiB | 500 GB | 1× |
| `medium` | 4 | 8 GiB | 500 GB | 2× |
| `large` | 8 | 16 GiB | 500 GB | 4× |
| `xlarge` | 16 | 32 GiB | 500 GB | 8× |

If you omit `instance`, jobs default to `small`. Larger sizes use a higher `multiplier` against your organization's run time budget. For example, a one-hour `large` run consumes four hours of budget.

A notebook, dashboard, or plain module deployed without a decorator takes the same spec through the `__require__` dunder, which accepts the same keys as `require=`:

```py
"""Rebuild the reporting tables from scratch."""

__require__ = {"instance": {"size": "large"}, "dependency_groups": ["heavy"]}

def main():
    ...

if __name__ == "__main__":
    main()
```

:::note
`require.machine` is deprecated since dlt 1.29.0. Setting it, on the decorator or as `__require__`, emits a deprecation warning. Use `require={"instance": {"size": ...}}` instead.
:::

Pipeline-level tuning (chunking, parallelism, memory settings) often lowers the size you need, see [Optimizing dlt](../../reference/performance.md).

## Static egress IPs

Use this when you must whitelist outbound IP addresses so external systems can grant your jobs access to private resources. Opt in per job so outbound requests use your workspace's static egress IPs:

```py
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

For inline jobs in `__deployment__.py`, pass `section="my_job"` to the decorator to give it a clean section name. Profile-aware overrides live in `dev.config.toml`, `prod.config.toml`, etc., see [Workspace setup](workspace-setup.md#setting-up-configuration-files).

## Display metadata

`expose={...}` controls how the job appears in the dashboard and to selectors:

```py
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

| Key | Purpose |
|-----|---------|
| `tags` | List of labels for grouping in the dashboard and matching CLI selectors (`tag:ingest`) |
| `display_name` | Human-readable label shown in the dashboard |

See [Tags and bulk triggering](triggers.md#tags-and-bulk-triggering) for how tags drive `dlthub job trigger` selectors.

## Jobs created from module imports

A job does not have to be a decorated function. When `__deployment__.py` imports a **module** and lists it in `__all__` (the [module imports](deployments.md#the-deployment-module) rule), the manifest generator inspects the module itself and turns it into one job. No `@run.pipeline`, `@run.job`, or `@run.interactive` appears anywhere in the file. This is how notebooks, MCP servers, dashboards, and plain scripts are deployed.

```py
"""Analytics workspace."""

import sales_report       # marimo notebook -> interactive job
import warehouse_tools    # FastMCP server  -> interactive job
import ops_dashboard      # Streamlit app   -> interactive job
import backfill_script    # plain module    -> batch job

__all__ = ["sales_report", "warehouse_tools", "ops_dashboard", "backfill_script"]
```

Such a job is named after its module and has no function part. `sales_report.py` becomes the job `jobs.sales_report`, and it reads configuration from the `[jobs.sales_report]` section, see [Job configuration via TOML](#job-configuration-via-toml).

There are two detection paths: framework-detected modules and plain Python modules.

### Framework-detected modules

The generator probes an imported module for a known framework object: a [marimo](../../general-usage/dataset-access/marimo.md) `marimo.App` instance (a variable named `app` is preferred, any other public name also matches), a `fastmcp.FastMCP` instance (`mcp`, `server`, and `app` are the preferred names), or the [Streamlit](../cookbook/build-streamlit-dashboard.md) module present in the module namespace under any alias. The detectors run in the order marimo, FastMCP, Streamlit, and the first match wins. A module that creates a `FastMCP` instance and also imports `streamlit` is deployed as an MCP server.

All three produce **interactive** jobs: job type `interactive`, a single `http:` trigger, and `execute.concurrency` of `1`. Interactive jobs run under the `access` profile. See [Batch vs interactive](overview.md#batch-vs-interactive).

The description comes from the module docstring, prefixed with the framework's own title when it has one. A marimo notebook declared as `marimo.App(app_title="Revenue report")` with the docstring `Revenue by region.` gets the description `Revenue report: Revenue by region.` A FastMCP server uses its server name the same way.

### Plain Python modules

A local `.py` module that no framework detector claims becomes a **batch** job. The launcher starts it with `python -m <module>`, so the work has to sit under `if __name__ == "__main__":`. The manifest generator imports the module, so anything running at import time would run on every deploy.

```py
"""Rebuild the reporting tables from scratch."""

def main():
    ...

if __name__ == "__main__":
    main()
```

A module is only detected this way when it is local to the workspace: its file must live below the directory holding `__deployment__.py`, and installed packages are rejected (any path containing `site-packages`, `.venv`, `venv`, `.tox`, or `.nox`).

Unlike the framework detectors, this one adds **no triggers**. Without `__trigger__` the job is deployed but never scheduled, so you start it by hand with `dlthub run backfill_script` or from the dashboard.

### Module-level dunders

Four module-level names override what the detector produced. Set them at the top of the module file. They work for both detection paths.

| Dunder | Decorator equivalent | Effect |
|--------|----------------------|--------|
| `__doc__` (the module docstring) | the job function's docstring | Becomes the job description |
| `__trigger__` | `trigger=` | **Appended** to the triggers the detector set |
| `__expose__` | `expose=` | **Replaces** the detected `expose` dict |
| `__require__` | `require=` | Sets the job's `require` spec |

`__trigger__` takes a single trigger or a list, in the same forms accepted by `trigger=`: a `trigger.*` constructor, a bare cron string, or a raw `"type:expr"` string. See [Triggers and scheduling](triggers.md).

## Next steps

- [Triggers and scheduling](triggers.md): schedule and chain jobs
- [Deployments](deployments.md): push these decorators to the dltHub platform
