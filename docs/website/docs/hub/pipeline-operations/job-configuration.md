---
title: Job configuration
description: Per-job options on the dltHub platform, including execution timeouts, dependency groups, instance size, profile, provider, region, and TOML configuration sections
keywords: [dlthub platform, job configuration, timeout, dependency groups, instance, size, require.instance, require.profile, require.provider, require.region, machine, static egress, execute, require, expose, section]
---

# Job configuration

This page documents the per-job options that aren't about *when* a job runs (those live in [Triggers and scheduling](triggers.md)) but about *how* it runs — execution limits, runner resources, the Python environment it gets, and the configuration values it reads at runtime.

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

## Runner requirements

`require={...}` declares what a job needs from its runner: the Python environment, the profile it runs under, the hardware it lands on, and how it reaches the outside world. Every key is optional, and the same set is accepted by all three decorators.

| Key | Type | Default | Documented in |
|-----|------|---------|---------------|
| `dependency_groups` | list of strings | the workspace's base dependencies only | [Dependency groups](#dependency-groups) |
| `instance` | dict, `size` only | `{"size": "small"}` | [Instance size](#instance-size) |
| `machine` | string | not read | [Deprecated: machine](#deprecated-machine) |
| `profile` | string | `prod` for batch jobs, `access` for interactive ones | [Profile](#profile) |
| `provider` | string | `modal` | [Provider](#provider) |
| `region` | string | your organization's data plane region | [Region](#region) |
| `static_egress_ips` | boolean | `False` | [Static egress IPs](#static-egress-ips) |
| `timezone` | IANA timezone name | `UTC` | [Timezone](triggers.md#timezone) |

A key that is not in this table is rejected when you deploy, so a typo never reaches the platform unnoticed:

```text
Path `./jobs[0]/require`: received unexpected fields `{'machin'}`
```

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

`size` is the only key the platform reads out of the `instance` dict, and it must be one of the four names above. Anything else fails the deploy, and so does asking for a size on a [provider](#provider) that does not support sizing.

Pipeline-level tuning (chunking, parallelism, memory settings) often lowers the size you need, see [Optimizing dlt](../../reference/performance.md).

### Deprecated: machine

`require={"machine": "2xlarge"}` is the older way of asking for hardware. It is deprecated in favour of `require.instance` and it does nothing: the platform sizes runners from `instance` and never reads `machine`.

Importing a job that declares it prints a deprecation warning once:

```text
DltDeprecationWarning: `require.machine` is deprecated, use `require.instance` instead
(e.g. `{'instance': {'size': 'medium'}}`). Deprecated in dlt 1.29.0 to be removed in 2.0.0.
```

Replace it with a size from the table above. The old machine identifiers have no one-to-one mapping onto instance sizes, so pick the size your job actually needs:

```py
# deprecated, and ignored by the platform
@run.pipeline(my_pipeline, require={"machine": "2xlarge"})
def legacy_sync():
    ...

# current
@run.pipeline(my_pipeline, require={"instance": {"size": "xlarge"}})
def heavy_sync():
    ...
```

## Profile

`require={"profile": "analytics"}` runs the job under that workspace [profile](profiles.md). The launcher activates it before your function runs, so the pipeline and every config lookup resolve from `analytics.config.toml` and `analytics.secrets.toml`:

```py
@run.pipeline(my_pipeline, require={"profile": "analytics"})
def load_analytics():
    ...
```

Without the key, batch jobs (`@run.pipeline`, `@run.job`) run under `prod` and interactive jobs (`@run.interactive`) under `access`. A workspace that has never synced an `access` configuration falls back to `prod` for interactive jobs.

Things to know before you set it:

- **`dev` and `tests` are rejected.** They are local-only profiles and are never uploaded, so a deployed job cannot assume them. Declaring one fails the deploy with `require.profile 'dev' is a local-only profile and cannot be assumed by deployed jobs`.
- **Custom profiles need their own TOML files.** Every `<profile>.config.toml` and `<profile>.secrets.toml` in `.dlt/` that is not a local-only profile is uploaded on deploy. See [Setting up configuration files](workspace-setup.md#setting-up-configuration-files).
- **Local runs don't switch profiles.** `dlthub local run` uses the pinned or active profile and warns when the job declares a different one (`Job declares profile 'analytics' but running on current profile 'dev'`). Pass `--profile NAME` to override both.

## Provider

`require={"provider": "modal"}` names the infrastructure backend that executes the job. The platform accepts exactly three names (`modal`, `tower`, and `local`) and rejects any other value at deploy time, including a valid name in the wrong case. `modal` is what a job gets when you leave the key unset.

Of the three, only `modal` supports [instance sizing](#instance-size). Declaring `instance` together with `tower` or `local` fails the deploy with a 400: the platform validates the pair when it stamps the job version, rather than dropping the requested size and running the job on unsized hardware.

Leave the key unset unless dltHub tells you which provider to name.

## Region

`require={"region": ...}` does not choose where a job runs, and the platform does not read it. Placement follows your organization's data plane: each plane pins the region its runners execute in, and an organization is bound to exactly one plane, chosen at creation and permanent afterwards.

So setting `require.region` changes neither placement nor data residency. To run workloads in another region, create an organization there and redeploy. See [Regions and data residency](../platform-capabilities/regions.md).

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
