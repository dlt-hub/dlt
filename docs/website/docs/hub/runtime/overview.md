---
title: Overview
description: Deploy and run dlt pipelines, transformations and notebooks in the cloud with dltHub
keywords: [dlthub, deployment, cloud, scheduling, notebooks, dashboard, jobs, triggers, manifest]
---

# dltHub Platform

dltHub is a managed cloud platform for running your `dlt` pipelines, transformations, and notebooks. It provides:

- Cloud execution of batch pipelines and interactive applications (notebooks, dashboards, MCP servers)
- Scheduling with cron expressions, intervals, and event-driven followup triggers
- A web dashboard for monitoring runs, viewing logs, and managing jobs
- Secure secrets management with multiple [profiles](../core-concepts/profiles-dlthub.md)

dltHub mirrors your local workspace into the cloud (called a **workspace deployment**). Your familiar dlt pipelines, datasets, notebooks, and dashboards run remotely with the same code that runs on your machine.

:::caution
A single GitHub repository can be connected to only one remote workspace at a time. You connect with `dlthub workspace connect`. If you point the same repo at a different remote workspace, jobs deployed under the previous binding are deactivated — run history is preserved but their triggers no longer fire.

Connecting multiple local repositories to the same remote workspace is not yet supported.
:::

## Workspace setup

A dltHub-ready workspace is a regular Python project with a few additions. You can convert any existing OSS dlt project into one in a couple of minutes — all CLI examples below use the `dlthub` host, which becomes the single entry point once the workspace is initialized.

### 1. Create a Python project

```sh
uv venv
```

### 2. Install dltHub

```sh
uv pip install "dlt[hub]"
```

The `[hub]` extra pulls in everything needed to run the `dlthub` CLI and deploy to dltHub.

### 3. Initialize the local workspace

```sh
dlthub init
```

This scaffolds:

```text
.dlt/
├── .workspace          # marker that enables the extended `dlthub` command surface
├── config.toml         # workspace-wide config
└── secrets.toml        # workspace-wide secrets (gitignored)
pyproject.toml          # or requirements.txt if `uv` isn't on PATH
.gitignore
```

Pass `--name <workspace>` to override the default (the current directory's basename), or `--dry-run` to preview the file plan without writing.

### 4. Add pipelines

```sh
dlthub pipeline init <source> <destination>
```

This reuses the same machinery as `dlt init`, so verified sources and templates work as you'd expect.

:::note
The first time you run `dlthub deploy`, `dlthub run`, or `dlthub serve`, the CLI walks you through GitHub OAuth and then prompts you to pick (or create) a remote workspace — no separate setup step required.

You can also run these by hand up front:

- `dlthub login` opens the OAuth device flow and authenticates the current user.
- `dlthub workspace connect [<name_or_id>] [--org-id <id>]` binds this repo to a remote workspace. With no argument, an interactive picker is shown, grouped by organization. The chosen `workspace_id` (and `organization_id`, on the first connect) is persisted to `.dlt/config.toml`.

`organization_id` is write-once. To switch organizations later, remove the line from `.dlt/config.toml` by hand and run `dlthub workspace connect` again.
:::

## Credentials and configs

### Workspace profiles

A profile's primary role is to define **how dlt accesses your data** — destination type, credentials, dataset names, source-specific tokens. On top of that it carries other dltHub-related settings such as telemetry, log levels, and runtime tuning.

**Some profiles stay local; others are synchronized with the backend.** Local-only profiles live in your repo and are never uploaded. Synced profiles are pushed to dltHub on every deploy so the cloud runtime can use the same configuration when it executes your jobs.

The built-in profiles are:

| Profile | Scope | Purpose | Credentials |
|---------|-------|---------|-------------|
| `dev` | Local only | Local development (default when running on your machine) | Local DuckDB / test credentials |
| `tests` | Local only | Automated tests | Test credentials |
| `prod` | Synced with backend | Production batch jobs running on dltHub | Read/write access to your destination |
| `access` | Synced with backend | Interactive notebooks and dashboards on dltHub | Read-only access (for safe data exploration) |

Any custom profile you reference in a job decorator (e.g. `require={"profile": "analytics"}`) is also synced to the cloud configuration.

When you run a script locally, dlt uses `dev`. When dltHub executes a **batch job**, it uses `prod`. When dltHub serves an **interactive job** (notebook, dashboard, MCP), it uses `access`. If `access` is not configured, interactive jobs fall back to `prod`.

See [profiles in dltHub](../core-concepts/profiles-dlthub.md) for the full reference.

### Setting up configuration files

Configuration files live in the `.dlt/` directory:

```text
.dlt/
├── .workspace              # marker file (created by `dlthub init`)
├── config.toml             # workspace-wide config (all profiles)
├── secrets.toml            # workspace-wide secrets (gitignored)
├── dev.config.toml         # dev profile config
├── prod.config.toml        # production profile config
├── prod.secrets.toml       # production secrets (gitignored)
├── access.config.toml      # access profile config
└── access.secrets.toml     # access secrets (gitignored)
```

Settings in profile-scoped files override workspace-scoped files. Below is an example using **named destinations** so the same `destination="warehouse"` resolves to DuckDB locally and MotherDuck in production. You can swap MotherDuck for any cloud destination — see for example [BigQuery](../../dlt-ecosystem/destinations/bigquery.md), [Snowflake](../../dlt-ecosystem/destinations/snowflake.md), or [filesystem/S3](../../dlt-ecosystem/destinations/filesystem.md).

**`config.toml`** (defaults shared by all profiles):

```toml
[runtime]
log_level = "WARNING"
dlthub_telemetry = true

# Set automatically by `dlthub workspace connect`
workspace_id = "your-workspace-id"
organization_id = "your-organization-id"
```

`api_base_url` defaults to `https://api.dlthub.com` and only needs to be set when targeting a self-hosted control plane. For non-interactive auth (CI, scripts), set `api_key` in `secrets.toml` under `[runtime]` instead of relying on the OAuth flow.

**`dev.config.toml`** (local DuckDB):

```toml
[destination.warehouse]
destination_type = "duckdb"
```

**`prod.config.toml`** (production destination):

```toml
[destination.warehouse]
destination_type = "motherduck"
```

**`prod.secrets.toml`** (read/write credentials for batch jobs):

```toml
[destination.warehouse.credentials]
database = "your_database"
password = "your-motherduck-service-token"
```

**`access.config.toml`** + **`access.secrets.toml`** (read-only credentials for interactive jobs):

```toml
[destination.warehouse]
destination_type = "motherduck"
```

```toml
[destination.warehouse.credentials]
database = "your_database"
password = "your-motherduck-read-only-token"
```

:::warning Security
Files matching `*.secrets.toml` and `secrets.toml` are gitignored by default. Never commit secrets to version control. dltHub stores your secrets securely when you sync your configuration.
:::

## Command line fundamentals

The `dlthub` CLI is split into two scopes:

- **local** — `dlthub local ...` operates on the **local workspace** (files in `.dlt/`, your machine's pipeline working dirs, local profiles).
- **remote** — `dlthub ...` (unqualified) operates on the **connected dltHub workspace** (the cloud deployment, configurations, jobs, runs).

Most actions and entities exist in both scopes: running a job, serving an interactive app, inspecting workspace state, listing pipelines. Use the local scope to validate jobs before pushing them to the cloud.

Verbs have consistent meaning regardless of the scope or the entity they operate on:

| Verb | Meaning |
|------|---------|
| `info` | Print structured information about the entity (workspace, job, run, deployment, configuration) |
| `list` | Enumerate entities |
| `run` | Execute a batch job or pipeline |
| `serve` | Start an interactive job (notebook, dashboard, MCP server, REST app) |
| `show` | Open the GUI / human-readable interface (web dashboard remotely, marimo view locally) for the entity |
| `clean` | Remove local artefacts (e.g. wipe pipeline working dirs and locally loaded data) |
| `sync` | Push local changes to the cloud counterpart |
| `cancel` | Cancel an in-flight job or run |
| `connect` | Bind a local entity to a remote one |
| `deploy` | Push the deployment manifest to the cloud |

The local/remote split makes most pages of this guide read in pairs:

| Local | Remote |
|-------|--------|
| `dlthub local run [<selector_or_job>]` | `dlthub run [<selector_or_job>]` |
| `dlthub local serve [<selector_or_job>]` | `dlthub serve [<selector_or_job>]` |
| `dlthub local pipeline run <pipeline_name>` | `dlthub pipeline run <pipeline_name>` |
| `dlthub local info` | `dlthub workspace info` |
| `dlthub local show` | `dlthub show` |

Run the local form first to catch missing dependencies, misconfigured destinations, or broken decorators without burning a remote slot.

## Quick deploy: ad-hoc launch

The fastest way to run an existing script on dltHub is to point `run` or `serve` at a Python file:

```sh
# test locally first
dlthub local run fruitshop_pipeline.py

# deploy and run a batch script in the cloud (uses `prod` profile)
dlthub run fruitshop_pipeline.py

# stream logs until the run completes
dlthub run fruitshop_pipeline.py -f

# deploy and serve an interactive app (uses `access` profile)
dlthub local serve fruitshop_notebook.py    # local
dlthub serve fruitshop_notebook.py          # remote
```

Under the hood, `dlthub run` / `dlthub serve` generate a single-job deployment manifest from the file and sync it to dltHub. This **ad-hoc deploy** is great for getting started but does not support:

- Scheduled triggers (cron, intervals)
- Followup jobs (run B after A succeeds)
- Freshness constraints
- Multi-job workspaces deployed as a unit

For all of these you need **job decorators** and a **deployment module**, described next.

## Jobs and deployments

A dltHub workspace can contain many jobs scheduled on different cadences, chained together by triggers and freshness constraints. The three building blocks are:

- **Job decorators** that attach scheduling and metadata to Python functions
- **`__deployment__.py`** that declares which jobs exist in the workspace
- **`dlthub deploy`** that syncs the entire job graph to dltHub in one step

### Job decorators

The `dlt.hub.run` module provides three decorators:

| Decorator | Used for |
|-----------|----------|
| `@run.pipeline` | A batch job bound to a named `dlt.pipeline` (gets pipeline-aware retries and dataset linking) |
| `@run.job` | A general-purpose batch job (any Python function — DQ checks, reports, custom scripts) |
| `@run.interactive` | A long-running HTTP service (notebook, MCP server, Streamlit app, REST API) |

Example: an ingestion pipeline that runs every 5 minutes and is tagged for bulk operations.

```py
import dlt
from dlt.hub import run
from dlt.hub.run import trigger

@run.pipeline(
    "github_pipeline",
    trigger=trigger.every("5m"),
    expose={"tags": ["ingest"], "display_name": "GitHub commits ingest"},
)
def load_commits():
    """Load commits and contributors from the GitHub REST API."""
    pipeline = dlt.pipeline(
        pipeline_name="github_pipeline",
        destination="warehouse",
        dataset_name="github_data",
    )
    pipeline.run(github_rest_api_source())
```

:::tip
The first argument to `@run.pipeline` (here `"github_pipeline"`) becomes a
pipeline-name selector that both the local and remote CLI honor:

```sh
dlthub local pipeline run github_pipeline   # locally
dlthub pipeline run github_pipeline         # in the cloud
```

This works only for `@run.pipeline` jobs — `@run.job` and `@run.interactive`
jobs aren't addressable by pipeline name. Run those with `dlthub run <job_name>`
or `dlthub job trigger <selector>` instead.
:::

A general-purpose job, scheduled hourly:

```py
@run.job(
    trigger=trigger.schedule("0 * * * *"),
    expose={"display_name": "GitHub data quality"},
)
def run_dq_checks():
    """Validate ingested data; the job fails if any check fails."""
    if not all_passed:
        raise RuntimeError("Data quality checks failed")
```

### Triggers

A trigger tells dltHub **when** to run a job. You can pass a single trigger or a list.

| Trigger | Meaning |
|---------|---------|
| `trigger.every("5m")` | Recurring interval (`"5m"`, `"6h"`, seconds as float) |
| `trigger.schedule("0 * * * *")` | Cron expression |
| `trigger.once("2026-12-31T23:59:59Z")` | One-shot at a timestamp |
| `"*/5 * * * *"` | Bare cron string — auto-detected |
| `upstream_job.success` | Followup — fires when upstream completes successfully |
| `upstream_job.fail` | Fires when upstream fails |

Triggers declared in code are the **source of truth**. There is no separate CLI for adding or removing schedules — change the decorator, redeploy.

### Tags

Tags are labels on jobs (set via `expose={"tags": [...]}`). They are used to:

1. Group related jobs in the dashboard
2. Run bulk operations from the CLI via **selectors**

```sh
# trigger every job tagged "ingest"
dlthub job trigger "tag:ingest"

# trigger every job that has a schedule
dlthub job trigger "schedule:*"
```

### The deployment module

`__deployment__.py` is a Python module that declares everything deployable in the workspace. dltHub discovers jobs by inspecting it.

```py
"""GitHub ingest workspace -- loads and monitors GitHub API data"""

from github_pipeline import load_commits
from github_dq_pipeline import run_dq_checks

import github_transformations_notebook
import github_dq_notebook
import github_report_notebook

__all__ = [
    "load_commits",
    "run_dq_checks",
    "github_transformations_notebook",
    "github_dq_notebook",
    "github_report_notebook",
]
```

Rules:

- **Function imports** (`from github_pipeline import load_commits`) produce one job per function. The function must be decorated with `@run.pipeline`, `@run.job`, or `@run.interactive`.
- **Module imports** (`import github_report_notebook`) produce one job per module. The framework is auto-detected — marimo notebooks become interactive notebook jobs, FastMCP modules become MCP servers, Streamlit modules become dashboards.
- **`__all__`** lists exactly the names to deploy. Without it, the manifest generator scans `__dict__` and warns.
- **`__doc__`** (the module docstring) becomes the workspace description in the dltHub dashboard.

You can also define decorated jobs **inline** in `__deployment__.py` — useful for small MCP servers or one-off batch jobs.

### Deploying with `dlthub deploy`

This is the central command for manifest-based deployment. It reads `__deployment__.py`, generates a manifest, and syncs it to dltHub:

```sh
dlthub deploy
```

The deploy command:

1. Imports `__deployment__.py` and collects every job
2. Generates a deployment manifest (a JSON document describing every job's triggers, entry point, and metadata)
3. Syncs your code and configuration to dltHub
4. Sends the manifest for **reconciliation**

#### Reconciliation

dltHub compares the new manifest against the currently deployed jobs:

| Status | Meaning |
|--------|---------|
| **added** | New job — will be created |
| **updated** | Job definition changed — will be updated |
| **unchanged** | No changes — left as-is |
| **archived** | Job was in the previous manifest but not in this one — triggers disabled, history preserved |

Removing a job from `__deployment__.py` does not delete it — it archives it, preserving run history and logs.

#### Preview before deploying

```sh
# see what would change without applying
dlthub deploy --dry-run

# dump the full expanded manifest as YAML
dlthub deploy --show-manifest
```

### Running and monitoring deployed jobs

Once deployed, scheduled jobs run automatically. You can also run them by hand — and run the local counterpart first whenever you want to debug locally:

```sh
# run a specific job by name (ad-hoc, syncs code first)
dlthub local run load_commits         # locally
dlthub run load_commits -f            # in the cloud

# trigger jobs without re-syncing code (uses currently deployed code)
dlthub job trigger "tag:ingest"
dlthub job trigger "schedule:*"
dlthub job trigger "tag:ingest" --dry-run    # preview only

# trigger by pipeline name
dlthub local pipeline run github_pipeline    # locally
dlthub pipeline run github_pipeline          # in the cloud

# serve an interactive job
dlthub local serve github_report_notebook    # locally
dlthub serve github_report_notebook          # in the cloud
```

:::note
`dlthub pipeline run` (and its local sibling) can only trigger jobs decorated with `@run.pipeline` — they are matched by `deliver.pipeline_name`. Jobs declared with `@run.job` or `@run.interactive` are not addressable this way; use `dlthub run <job_name>` or `dlthub job trigger <selector>` instead.
:::

## Advanced patterns

The decorators support more powerful patterns for production workspaces with multiple connected pipelines. They are summarized here — see the [dltHub starter pack](https://github.com/dlt-hub/runtime-starter-pack) for full working examples.

### Followup triggers and `TJobRunContext`

Chain a transform to run automatically after ingestion succeeds:

```py
from dlt.hub.run import TJobRunContext

@run.pipeline("transform_pipeline", trigger=ingest_job.success)
def transform(run_context: TJobRunContext):
    ...
```

Every decorated job exposes `.success`, `.fail`, and `.completed` trigger properties. A job can have **multiple triggers** (pass a list) — `run_context["trigger"]` tells you which one fired.

`TJobRunContext` is a dict injected by the launcher with: `run_id`, `trigger`, `refresh`, and the scheduler-supplied `interval_start` / `interval_end`.

### Scheduler-driven intervals

For incremental pipelines, declare the overall time range with `interval=` and let dltHub hand each run a `[interval_start, interval_end]` window:

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

- Each run gets the cron tick that just elapsed
- Missed ticks are backfilled automatically — windows extend back continuously
- On refresh, dltHub resets the interval pointer to `interval.start`
- Source code stays stateless — no cursor persistence, no state lookups

### Freshness checks

`freshness=[upstream.is_fresh]` blocks a job until the upstream's most recent interval has fully completed. Unlike a trigger, the job still runs on its own schedule — it just skips while upstream is mid-load. Use for transforms that must not observe partial data.

### Refresh cascade

A backfill job with `refresh="always"` originates a refresh signal that propagates through all downstream jobs in the dependency graph. Downstream jobs receive `run_context["refresh"] = True` and react accordingly (e.g. `pipeline.refresh = "drop_sources"`). Refresh policies: `"always"` (originate), `"auto"` (pass through, default), `"block"` (stop propagation).

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

### Execution constraints

`execute={"timeout": "6h"}` overrides the default 120-minute job timeout. Use the dict form (`{"timeout": 7200, "grace_period": 60}`) to set a custom grace period — the window for the job to finish in-flight work before dltHub hard-kills the process.

### Dependency groups

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

dltHub composes the execution environment from the workspace's base dependencies plus the job's declared groups.

### Timezone

`require={"timezone": "Europe/Berlin"}` interprets cron expressions in that IANA timezone. Intervals in `run_context` remain UTC datetimes, but they align to tick boundaries in the declared timezone.

### Job configuration

Jobs read configuration through dlt's standard config system. The default section is the containing module name:

```toml
# applies to every job defined in usgs_pipeline.py
[jobs.usgs_pipeline]
epoch = "2026-04-05T00:00:00+00:00"

# overrides for one specific job
[jobs.usgs_pipeline.usgs_daily]
epoch = "2026-04-10T00:00:00+00:00"
```

For inline jobs in `__deployment__.py`, pass `section="my_job"` to the decorator to give it a clean section name. Profile-aware overrides live in `dev.config.toml`, `prod.config.toml`, etc.

## Web UI

Visit [dlthub.app](https://dlthub.app) to access the web dashboard, which provides:

- **Overview** — workspace overview with all jobs and recent runs (auto-refreshes every 10 seconds)
- **Jobs** — view and manage all jobs; change or cancel schedules; create **public links** for interactive jobs (notebooks/dashboards)
- **Runs** — monitor run status (pending, running, completed, failed, cancelled), start time, duration, and trigger type
- **Run details** — full execution logs, run metadata, pipeline information
- **Deployment & config** — current deployment version, configuration profiles, file listing
- **Dashboard** — visualize pipeline schemas, load info, data lineage
- **Settings** — workspace settings and metadata

Open it directly from the CLI with:

```sh
dlthub show
```

#### Public links for interactive jobs

Notebooks and dashboards can be shared via public links. Open a job's context menu (or its detail page), click **Manage Public Link**, and toggle to enable or revoke the link. Anyone with an active link can view the running notebook or dashboard — useful for sharing dashboards with stakeholders without dltHub access.

You can also generate / revoke a public link from the CLI:

```sh
dlthub job publish path/to/notebook.py
dlthub job unpublish path/to/notebook.py
```

## CLI reference

For detailed CLI documentation, see [CLI](../command-line-interface.md).

### Common commands

| Command | Description |
|---------|-------------|
| `dlthub init [--name <name>]` | Initialize a new dlthub workspace in the current directory |
| `dlthub pipeline init <source> <destination>` | Add a pipeline (verified source or template) |
| `dlthub login [--resume DEVICE_CODE]` | Authenticate with GitHub OAuth |
| `dlthub logout` | Clear local credentials |
| `dlthub workspace connect [<name_or_id>] [--org-id <id>]` | Bind this repo to a remote workspace |
| `dlthub local info` | Show local workspace info |
| `dlthub show` | Open the dltHub dashboard |
| `dlthub local run <script_or_job>` | Run a batch job on the local machine (recommended before deploying) |
| `dlthub local serve <script_or_job>` | Serve an interactive app on the local machine |
| `dlthub run <script_or_job> [-f]` | Deploy and run a batch script or named job in the cloud |
| `dlthub serve <script_or_job>` | Deploy and serve an interactive application in the cloud |
| `dlthub deploy [--dry-run] [--show-manifest]` | Deploy jobs from `__deployment__.py` |
| `dlthub job trigger <selector> [--refresh] [--dry-run]` | Trigger jobs matching a selector |
| `dlthub pipeline run <pipeline_name>` | Trigger a `@run.pipeline` job by pipeline name |
| `dlthub job logs <name> [run#] [-f]` | View or stream logs for a run |

### Workspace commands

```sh
# bind this repo to a remote workspace (interactive picker if no argument)
dlthub workspace connect [<name_or_id>] [--org-id <id>]

# list workspaces you have access to
dlthub workspace list

# show overview of the connected workspace
dlthub workspace info

# open the dashboard for the connected workspace
dlthub workspace show

# deploy from this workspace (same as the top-level `dlthub deploy`)
dlthub workspace deploy [--dry-run] [--show-manifest]

# manage deployment versions
dlthub workspace deployment list
dlthub workspace deployment info [version_number]
dlthub workspace deployment sync [--dry-run] [-v]

# manage configuration versions
dlthub workspace configuration list
dlthub workspace configuration info [version_number]
dlthub workspace configuration sync [--dry-run] [-v]
```

### Job commands

Commands accept job names, script paths, or **selectors** (`batch`, `tag:ingest`, `schedule:*`):

```sh
dlthub job list                    # all jobs
dlthub job list "tag:ingest"       # jobs matching selector
dlthub job list batch              # only batch jobs
dlthub job info <name>             # details for one job
dlthub job show <name>             # open the job page in the dashboard
dlthub job trigger "tag:ingest" [--refresh] [--profile <name>] [--dry-run]
dlthub job publish path/to/notebook.py
dlthub job unpublish path/to/notebook.py
dlthub job logs <name> [run#] [-f]
dlthub job cancel <name_or_selector> [--dry-run]
```

### Job run commands

```sh
# list runs (optionally filter by job name or selector)
dlthub job runs list [name_or_selector] [--running]

# run details
dlthub job runs info <name> [run#]
dlthub job runs show <name> [run#]

# view or stream logs
dlthub job runs logs <name> [run#] [-f]
dlthub job logs <name> [run#] [-f]    # shorthand for `runs logs`

# cancel a run
dlthub job runs cancel <name> [run#]
```

### Local commands

The `dlthub local` scope runs the same job graph as the cloud commands but entirely on your machine:

```sh
dlthub local info                          # workspace info
dlthub local show                          # open the local pipeline dashboard
dlthub local run [<selector_or_job>]       # run a batch job locally
dlthub local serve [<selector_or_job>]     # serve an interactive job locally
dlthub local pipeline run <pipeline_name>  # run a `@run.pipeline` job by pipeline name
dlthub local pipeline list                 # list local pipelines
dlthub local pipeline info <name>          # dlt OSS pipeline verbs (info, drop, sync, ...)
dlthub local profile use <profile_name>    # pin a profile for subsequent local runs
dlthub local clean [--skip-local-data-dir] # wipe locally loaded data for the active profile
dlthub local schema ...                    # pipeline schema management
```

### Profile commands

```sh
dlthub profile info     # show the active profile and configuration locations
dlthub profile list     # list all available profiles
```

## Key concepts

### Jobs vs runs

- A **Job** is a script registered in your workspace. It defines what code to run and optionally a schedule.
- A **Run** is a single execution of a job. Each run has its own logs, status, and metadata.

### Batch vs interactive

- **Batch jobs** run with the `prod` profile and are meant for scheduled data loading.
- **Interactive jobs** run with the `access` profile and are meant for notebooks, dashboards, MCP servers, and Streamlit apps.

### Interactive application types

| Type | Description |
|------|-------------|
| Notebooks | Marimo notebooks for the pipeline dashboard, exploration, and analysis |
| Streamlit apps | Interactive Streamlit dashboards |
| MCP servers | FastMCP HTTP servers (mounted at `/mcp`) |
| REST APIs | Starlette / FastAPI / similar applications |

Each interactive application is exposed via a unique public URL tied to its run. MCP modules must expose an `mcp` object created with `FastMCP`, or use `@run.interactive(interface="mcp")` and return a `FastMCP` from the function.

### Profiles

Profiles let you keep different configurations for different environments:

- Local development can use DuckDB with no credentials needed
- Production runs use MotherDuck (or any cloud destination) with full read/write access
- Interactive sessions use read-only credentials for safety

See [profiles in dltHub](../core-concepts/profiles-dlthub.md) for details.

### Deployments and configurations

- **Deployment** — your code files (`.py` scripts, notebooks)
- **Configuration** — your `.dlt/*.toml` files (settings and secrets)

Both are versioned separately, so you can update code without changing secrets and vice versa.

## Current limitations

- **Execution limits**: jobs default to 120 minutes maximum execution time (override with `execute={"timeout": "6h"}` in the decorator)
- **Interactive timeout**: notebooks are killed after about 5 minutes of inactivity (no open browser tab)
- **UI operations**: creating jobs must currently be done via CLI (schedules can be changed in the WebUI)
- **Pagination**: list views show the top 100 items
- **Log latency**: logs may lag 20–30 seconds during execution; they are guaranteed complete after the run finishes (completed or failed state)
- **One workspace per repo**: a single GitHub repository can only be connected to one remote workspace at a time; reconnecting deactivates jobs deployed under the previous binding

## Troubleshooting

### No 'access' profile detected

Your interactive notebooks will use the `prod` (or default) configuration. Create `access.config.toml` and `access.secrets.toml` with read-only credentials.

### No 'prod' profile detected

Batch jobs will use the default configuration. Create `prod.config.toml` and `prod.secrets.toml` with read/write credentials.

### Job not using latest code

The CLI does not yet detect whether local code differs from remote. Run `dlthub workspace deployment sync` (or any `run` / `serve` / `deploy`) to ensure your latest code is deployed.

### Job failed

1. `dlthub job runs info <name> [run#]` — check exit status and timing
2. `dlthub job logs <name> [run#]` — read the error output

Common causes:

- **Missing dependencies** in `pyproject.toml` — all packages must be declared, not just locally installed (run `dlthub local run <job>` first to catch this)
- **Secrets not configured for `prod` profile** — dltHub uses `prod` for batch jobs; check `.dlt/prod.secrets.toml`
- **Script missing `if __name__ == "__main__":`** — the job does nothing without it
- **`dev_mode=True` left in** — drops and recreates the dataset on every run, destroying production data
- **Wrong destination credentials** — the `prod` profile may point to a different destination than `dev`
- **Job timeout** — default is 120 minutes; override with `execute={"timeout": "6h"}`

### Logs not appearing

Logs may lag 20–30 seconds during execution. Wait for the run to complete, or stream them in real time:

```sh
dlthub job logs my_pipeline.py --follow
```
