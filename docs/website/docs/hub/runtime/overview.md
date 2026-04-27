---
title: Overview
description: Deploy and run dlt pipelines, transformations and notebooks in the cloud with dltHub Runtime
keywords: [runtime, deployment, cloud, scheduling, notebooks, dashboard, jobs, triggers, manifest]
---

# dltHub Runtime

dltHub Runtime is a managed cloud platform for running your `dlt` pipelines, transformations, and notebooks. It provides:

- Cloud execution of batch pipelines and interactive applications (notebooks, dashboards, MCP servers)
- Scheduling with cron expressions, intervals, and event-driven followup triggers
- A web dashboard for monitoring runs, viewing logs, and managing jobs
- Secure secrets management with multiple [profiles](../core-concepts/profiles-dlthub.md)

dltHub Runtime mirrors your local workspace into the cloud (called a **workspace deployment**). Your familiar dlt pipelines, datasets, notebooks, and dashboards run remotely with the same code that runs on your machine.

:::caution
Each GitHub account can have only one remote workspace. When you run `dlt runtime login`, it connects your current local workspace to that remote workspace. If you later connect a different local repository and deploy, it will replace your existing [**deployment** and **configuration**](#deployments-and-configurations), making any previously scheduled jobs defunct.

Support for multiple remote workspaces (mirroring multiple local repositories) is planned.
:::

## Workspace setup

A Runtime-ready workspace is a regular Python project with a few additions. You can convert any existing dlt project into one in a couple of minutes.

### 1. Initialize a Python project

If your project doesn't have a `pyproject.toml` yet, create one:

```sh
uv init
```

Runtime uses `pyproject.toml` to install dependencies remotely.

### 2. Enable workspace and runtime features

Install `dlt` with the `workspace` and `hub` extras and create the workspace marker file:

```sh
uv add "dlt[workspace,hub]"
touch .dlt/.workspace
```

The `.dlt/.workspace` file activates [profile support](../core-concepts/profiles-dlthub.md) and enables the `dlt runtime` and `dlt profile` CLI commands.

### 3. Log in to Runtime

```sh
dlt runtime login
```

This opens a GitHub OAuth flow. After authentication, the CLI prompts you to select or create a remote workspace. The workspace ID is stored in `.dlt/config.toml` under `[runtime] workspace_id`.

To skip the interactive prompt, pass `--workspace <name_or_id>`. To switch workspaces later without logging out, use `dlt runtime workspace switch <name_or_id>`.

## Credentials and configs

### Understanding workspace profiles

dlt Runtime uses **profiles** to manage different configurations for different environments. The relevant profiles are:

| Profile | Purpose | Credentials |
|---------|---------|-------------|
| `dev` | Local development (default when running on your machine) | Local DuckDB / test credentials |
| `prod` | Production batch jobs running on Runtime | Read/write access to your destination |
| `access` | Interactive notebooks and dashboards on Runtime | Read-only access (for safe data exploration) |

When you run a script locally, dlt uses `dev`. When Runtime executes a **batch job**, it uses `prod`. When Runtime serves an **interactive job** (notebook, dashboard, MCP), it uses `access`. If `access` is not configured, interactive jobs fall back to `prod`.

See [profiles in dltHub](../core-concepts/profiles-dlthub.md) for the full reference.

### Setting up configuration files

Configuration files live in the `.dlt/` directory:

```text
.dlt/
├── .workspace              # Marker file enabling profiles + runtime CLI
├── config.toml             # Workspace-wide config (all profiles)
├── secrets.toml            # Workspace-wide secrets (gitignored)
├── dev.config.toml         # Dev profile config
├── prod.config.toml        # Production profile config
├── prod.secrets.toml       # Production secrets (gitignored)
├── access.config.toml      # Access profile config
└── access.secrets.toml     # Access secrets (gitignored)
```

Settings in profile-scoped files override workspace-scoped files. Below is an example using **named destinations** so the same `destination="warehouse"` resolves to DuckDB locally and MotherDuck in production. You can swap MotherDuck for any cloud destination — see for example [BigQuery](../../dlt-ecosystem/destinations/bigquery.md), [Snowflake](../../dlt-ecosystem/destinations/snowflake.md), or [filesystem/S3](../../dlt-ecosystem/destinations/filesystem.md).

**`config.toml`** (defaults shared by all profiles):

```toml
[runtime]
log_level = "WARNING"
dlthub_telemetry = true

# Runtime connection settings (set automatically by `dlt runtime login`)
auth_base_url = "https://dlthub.app/api/auth"
api_base_url = "https://dlthub.app/api/api"
workspace_id = "your-workspace-id"
```

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
Files matching `*.secrets.toml` and `secrets.toml` are gitignored by default. Never commit secrets to version control. Runtime stores your secrets securely when you sync your configuration.
:::

## Quick deploy: ad-hoc launch

The fastest way to run an existing script on Runtime is to point `launch` or `serve` at a Python file:

```sh
# Deploy and run a batch script (uses `prod` profile)
dlt runtime launch fruitshop_pipeline.py

# Stream logs in your terminal until the run completes
dlt runtime launch fruitshop_pipeline.py -f

# Deploy and serve an interactive app (notebook, dashboard, MCP — uses `access` profile)
dlt runtime serve fruitshop_notebook.py
```

Under the hood, the CLI generates a single-job deployment manifest from that file and syncs it to Runtime. This **ad-hoc deploy** is great for getting started but does not support:

- Scheduled triggers (cron, intervals)
- Followup jobs (run B after A succeeds)
- Freshness constraints
- Multi-job workspaces deployed as a unit

For all of these you need **job decorators** and a **deployment module**, described next.

## Jobs and deployments

A Runtime workspace can contain many jobs scheduled on different cadences, chained together by triggers and freshness constraints. The three building blocks are:

- **Job decorators** that attach scheduling and metadata to Python functions
- **`__deployment__.py`** that declares which jobs exist in the workspace
- **`dlt runtime deploy`** that syncs the entire job graph to Runtime in one step

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

A trigger tells Runtime **when** to run a job. You can pass a single trigger or a list.

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
dlt runtime trigger "tag:ingest"

# trigger every job that has a schedule
dlt runtime trigger "schedule:*"
```

### The deployment module

`__deployment__.py` is a Python module that declares everything deployable in the workspace. Runtime discovers jobs by inspecting it.

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
- **`__doc__`** (the module docstring) becomes the workspace description in the Runtime dashboard.

You can also define decorated jobs **inline** in `__deployment__.py` — useful for small MCP servers or one-off batch jobs.

### Deploying with `dlt runtime deploy`

This is the central command for manifest-based deployment. It reads `__deployment__.py`, generates a manifest, and syncs it to Runtime:

```sh
dlt runtime deploy
```

The deploy command:

1. Imports `__deployment__.py` and collects every job
2. Generates a deployment manifest (a JSON document describing every job's triggers, entry point, and metadata)
3. Syncs your code and configuration to Runtime
4. Sends the manifest for **reconciliation**

#### Reconciliation

Runtime compares the new manifest against the currently deployed jobs:

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
dlt runtime deploy --dry-run

# dump the full expanded manifest as YAML
dlt runtime deploy --show-manifest
```

### Running and monitoring deployed jobs

Once deployed, scheduled jobs run automatically. You can also run them by hand:

```sh
# launch a specific job by name (ad-hoc run, syncs code first)
dlt runtime launch load_commits -f

# trigger jobs without re-syncing code (uses currently deployed code)
dlt runtime trigger "tag:ingest"
dlt runtime trigger "schedule:*"
dlt runtime trigger "tag:ingest" --dry-run    # preview only

# trigger by pipeline name
dlt runtime run-pipeline github_pipeline

# serve an interactive job
dlt runtime serve github_report_notebook
```

## Advanced patterns

The decorators support more powerful patterns for production workspaces with multiple connected pipelines. They are summarized here — see the [runtime starter pack](https://github.com/dlt-hub/runtime-starter-pack) for full working examples.

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

For incremental pipelines, declare the overall time range with `interval=` and let Runtime hand each run a `[interval_start, interval_end]` window:

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
- On refresh, Runtime resets the interval pointer to `interval.start`
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
dlt runtime trigger "tag:backfill"
dlt runtime launch backfill --refresh    # explicit refresh on a single job
```

### Execution constraints

`execute={"timeout": "6h"}` overrides the default 120-minute job timeout. Use the dict form (`{"timeout": 7200, "grace_period": 60}`) to set a custom grace period — the window for the job to finish in-flight work before Runtime hard-kills the process.

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

Runtime composes the execution environment from the workspace's base dependencies plus the job's declared groups.

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

#### Public links for interactive jobs

Notebooks and dashboards can be shared via public links. Open a job's context menu (or its detail page), click **Manage Public Link**, and toggle to enable or revoke the link. Anyone with an active link can view the running notebook or dashboard — useful for sharing dashboards with stakeholders without Runtime access.

## CLI reference

For detailed CLI documentation, see [CLI](../command-line-interface.md).

### Common commands

| Command | Description |
|---------|-------------|
| `dlt runtime login [--workspace <name>]` | Authenticate with GitHub OAuth and select a workspace |
| `dlt runtime logout` | Clear local credentials |
| `dlt runtime workspace switch <name_or_id>` | Switch workspaces without re-login |
| `dlt runtime info` | Show workspace deployment overview |
| `dlt runtime dashboard` | Open the web dashboard |
| `dlt runtime launch <script_or_job> [-f]` | Deploy and run a batch script or named job |
| `dlt runtime serve <script_or_job>` | Deploy and run an interactive application |
| `dlt runtime deploy [--dry-run] [--show-manifest]` | Deploy jobs from `__deployment__.py` |
| `dlt runtime trigger <selector> [--refresh] [--dry-run]` | Trigger jobs matching a selector (e.g. `tag:backfill`, `schedule:*`) |
| `dlt runtime run-pipeline <pipeline_name>` | Trigger job by pipeline name |
| `dlt runtime cancel <name_or_selector>` | Cancel active runs for matching jobs |
| `dlt runtime logs <name> [run#] [-f]` | View or stream logs for a run |

### Deployment commands

For workspaces with a `__deployment__.py` manifest:

```sh
# Sync code + config and deploy the manifest
dlt runtime deploy

# Preview reconciliation without applying
dlt runtime deploy --dry-run

# Dump the expanded manifest as YAML (useful for debugging)
dlt -v runtime deploy --dry-run --show-manifest

# Sync code and configuration without reconciling the manifest
dlt runtime sync

# Sync only code
dlt runtime deployment sync

# Sync only configuration
dlt runtime configuration sync

# List deployment versions
dlt runtime deployment list

# Inspect a specific deployment version
dlt runtime deployment info [version_number]
```

### Job commands

Commands accept job names, script paths, or **selectors** (`batch`, `tag:ingest`, `schedule:*`):

```sh
dlt runtime job list                  # all jobs
dlt runtime job "tag:ingest" list     # jobs matching selector
dlt runtime job batch list            # only batch jobs
dlt runtime job <name> info           # details for one job
```

### Job run commands

```sh
# List runs (optionally filter by job name or selector)
dlt runtime job-run list [name_or_selector]

# Run details
dlt runtime job-run <name> [run#] info

# View or stream logs
dlt runtime job-run <name> [run#] logs [-f]
dlt runtime logs <name> [-f]

# Cancel a run
dlt runtime job-run <name> [run#] cancel
```

### Configuration commands

```sh
dlt runtime configuration list              # list configuration versions
dlt runtime configuration info [version]    # inspect a version
dlt runtime configuration sync              # sync local configuration
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

- **Runtime limits**: jobs default to 120 minutes maximum execution time (override with `execute={"timeout": "6h"}` in the decorator)
- **Interactive timeout**: notebooks are killed after about 5 minutes of inactivity (no open browser tab)
- **UI operations**: creating jobs must currently be done via CLI (schedules can be changed in the WebUI)
- **Pagination**: list views show the top 100 items
- **Log latency**: logs may lag 20–30 seconds during execution; they are guaranteed complete after the run finishes (completed or failed state)
- **One workspace per GitHub account**: connecting a new local repo and deploying replaces the existing remote workspace

## Troubleshooting

### No 'access' profile detected

Your interactive notebooks will use the `prod` (or default) configuration. Create `access.config.toml` and `access.secrets.toml` with read-only credentials.

### No 'prod' profile detected

Batch jobs will use the default configuration. Create `prod.config.toml` and `prod.secrets.toml` with read/write credentials.

### Job not using latest code

The CLI does not yet detect whether local code differs from remote. Run `dlt runtime deployment sync` (or any `launch` / `serve` / `deploy`) to ensure your latest code is deployed.

### Job failed

1. `dlt runtime job-run <name> [run#] info` — check exit status and timing
2. `dlt runtime logs <name> [run#]` — read the error output

Common causes:

- **Missing dependencies** in `pyproject.toml` — all packages must be declared, not just locally installed
- **Secrets not configured for `prod` profile** — Runtime uses `prod` for batch jobs; check `.dlt/prod.secrets.toml`
- **Script missing `if __name__ == "__main__":`** — the job does nothing without it
- **`dev_mode=True` left in** — drops and recreates the dataset on every run, destroying production data
- **Wrong destination credentials** — the `prod` profile may point to a different destination than `dev`
- **Job timeout** — default is 120 minutes; override with `execute={"timeout": "6h"}`

### Logs not appearing

Logs may lag 20–30 seconds during execution. Wait for the run to complete, or stream them in real time:

```sh
dlt runtime logs my_pipeline.py --follow
```
