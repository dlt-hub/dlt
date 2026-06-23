---
title: Job configuration
description: Per-job options on the dltHub platform — execution timeouts, dependency groups, and TOML configuration sections
keywords: [dlthub platform, job configuration, timeout, dependency groups, static egress, execute, require, expose, section]
---

# Job configuration

This page documents the per-job options that aren't about *when* a job runs (those live in [Triggers and scheduling](triggers.md)) but about *how* it runs — execution limits, the Python environment it gets, and the configuration values it reads at runtime.

All options below are arguments to the `@run.pipeline`, `@run.job`, and `@run.interactive` decorators.

## Execution constraints

`execute={"timeout": "6h"}` overrides the default 120-minute job timeout. Use the dict form to also customize the grace period — the window for the job to finish in-flight work before the dltHub platform hard-kills the process:

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
- 18.197.112.47

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

For inline jobs in `__deployment__.py`, pass `section="my_job"` to the decorator to give it a clean section name. Profile-aware overrides live in `dev.config.toml`, `prod.config.toml`, etc. — see [Workspace setup](workspace-setup.md#setting-up-configuration-files).

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

## Next steps

- [Triggers and scheduling](triggers.md) — schedule and chain jobs
- [Deployments](deployments.md) — push these decorators to the dltHub platform
