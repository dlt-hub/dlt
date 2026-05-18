---
title: Overview
description: Deploy and run dlt pipelines, transformations and notebooks in the cloud with the dltHub platform
keywords: [dlthub platform, deployment, cloud, scheduling, notebooks, dashboard, jobs, triggers, manifest]
---

# dltHub platform

The dltHub platform is a managed cloud platform for running your [`dlt` pipelines](../../general-usage/pipeline.md), [transformations](../features/transformations/index.md), and [notebooks](../../general-usage/dataset-access/marimo.md). It provides:

- Cloud execution of batch pipelines and interactive applications ([marimo notebooks](../../general-usage/dataset-access/marimo.md), [Streamlit dashboards](../../general-usage/dashboard.md), [MCP servers](../features/mcp-server.md))
- Flexible [scheduling & triggers](triggers.md) — cron, intervals, followup chains (`.success`/`.fail`/`.completed`), scheduler-driven intervals with automatic backfill, freshness checks, refresh cascades, and timezone-aware crons
- Built-in [monitoring & debugging](monitor-and-debug.md) — real-time streaming logs, run-state lifecycle, pipeline metrics dashboards (success rate, rows, duration), and a deployment & config inspector
- Tag-based job selectors for bulk operations (`dlthub job trigger tag:ingest`)
- Secure secrets management with multiple [profiles](../core-concepts/profiles-dlthub.md)

The dltHub platform mirrors your local [dltHub Workspace](../workspace/overview.md) into the cloud (called a **workspace deployment**). Your familiar dlt pipelines, [datasets](../core-concepts/datasets.md), notebooks, and dashboards run remotely with the same code that runs on your machine.

## Where to start

| If you want to... | Go to |
|-------------------|-------|
| Convert a Python project into a dltHub workspace and set up credentials | [Workspace setup](workspace-setup.md) |
| Push code to the cloud — ad-hoc runs or full manifest deploys | [Deploying jobs](deploying.md) |
| Schedule with cron/intervals, chain followups, backfill with scheduler-driven intervals, gate on freshness, cascade refreshes, tag jobs for bulk operations | [Triggers & scheduling](triggers.md) |
| Configure timeouts, dependencies, timezone, and per-job TOML sections | [Job configuration](job-configuration.md) |
| Stream logs in real time, inspect run states, view metric dashboards, diagnose failures, cancel runs | [Monitor & debug](monitor-and-debug.md) |
| Pick a deployment region | [Regions](regions.md) |

If you prefer a guided walkthrough, follow the [dltHub platform tutorial](../getting-started/runtime-tutorial.md).

## Key concepts

### Jobs vs runs

- A **Job** is a script registered in your workspace. It defines what code to run and optionally a schedule.
- A **Run** is a single execution of a job. Each run has its own logs, status, and metadata. See [run states](monitor-and-debug.md#understand-run-states).

### Batch vs interactive

- **Batch jobs** run with the [`prod` profile](../core-concepts/profiles-dlthub.md) and are meant for scheduled [data loading](../../general-usage/pipeline.md).
- **Interactive jobs** run with the [`access` profile](../core-concepts/profiles-dlthub.md) and are meant for [notebooks](../../general-usage/dataset-access/marimo.md), [dashboards](../../general-usage/dashboard.md), [MCP servers](../features/mcp-server.md), and Streamlit apps.

### Interactive application types

| Type | Description |
|------|-------------|
| Notebooks | [Marimo notebooks](../../general-usage/dataset-access/marimo.md) for the pipeline dashboard, exploration, and analysis |
| Streamlit apps | Interactive [Streamlit dashboards](../../general-usage/dashboard.md) |
| MCP servers | [FastMCP](../features/mcp-server.md) HTTP servers (mounted at `/mcp`) |
| REST APIs | Starlette / FastAPI / similar applications |

Each interactive application is exposed via a unique public URL tied to its run. MCP modules must expose an `mcp` object created with `FastMCP`, or use `@run.interactive(interface="mcp")` and return a `FastMCP` from the function.

### Profiles

[Profiles](../core-concepts/profiles-dlthub.md) let you keep different configurations for different environments:

- Local development can use [DuckDB](../../dlt-ecosystem/destinations/duckdb.md) with no credentials needed
- Production runs use [MotherDuck](../../dlt-ecosystem/destinations/motherduck.md) (or [any cloud destination](../../dlt-ecosystem/destinations/index.md)) with full read/write access
- Interactive sessions use read-only credentials for safety

See [profiles in dltHub](../core-concepts/profiles-dlthub.md) for details, and [Workspace setup](workspace-setup.md#understanding-workspace-profiles) for the relevant profile table.

### Deployments and configurations

- **Deployment** — your code files (`.py` scripts, notebooks)
- **Configuration** — your `.dlt/*.toml` files ([settings and secrets](../../general-usage/credentials/index.md))

Both are versioned separately, so you can update code without changing secrets and vice versa.

## Web UI

Visit [app.dlthub.com](https://app.dlthub.com) to access the web dashboard. It provides workspace overview, jobs and runs management, run details with execution logs, deployment & config inspection, pipeline dashboards, and workspace settings.

For monitoring runs, streaming logs, and diagnosing failures, see [Monitor and debug](monitor-and-debug.md).

#### Public links for interactive jobs

Notebooks and dashboards can be shared via public links. Open a job's context menu (or its detail page), click **Manage Public Link**, and toggle to enable or revoke the link. Anyone with an active link can view the running notebook or dashboard — useful for sharing dashboards with stakeholders without dltHub platform access.

## CLI reference

For detailed CLI documentation, see [CLI](../command-line-interface.md).

### Common commands

| Command | Description |
|---------|-------------|
| `dlthub login [--workspace <name>]` | Authenticate with GitHub OAuth and select a workspace |
| `dlthub logout` | Clear local credentials |
| `dlthub workspace connect <name_or_id>` | Switch workspaces without re-login |
| `dlthub info` | Show workspace deployment overview |
| `dlthub show` | Open the web dashboard |
| `dlthub run <script_or_job> [-f]` | Deploy and run a batch script or named job |
| `dlthub serve <script_or_job>` | Deploy and run an interactive application |
| `dlthub deploy [--dry-run] [--show-manifest]` | Deploy jobs from `__deployment__.py` |
| `dlthub job trigger <selector> [--refresh] [--dry-run]` | Trigger jobs matching a selector (e.g. `tag:backfill`, `schedule:*`) |
| `dlthub pipeline run <pipeline_name>` | Trigger job by pipeline name |
| `dlthub job runs cancel <name_or_selector>` | Cancel active runs for matching jobs |
| `dlthub job logs <name> [run#] [-f]` | View or stream logs for a run |

## Current limitations

- **Platform limits**: jobs default to 120 minutes maximum execution time (override with `execute={"timeout": "6h"}` in the decorator — see [Job configuration](job-configuration.md#execution-constraints))
- **Interactive timeout**: notebooks are killed after about 5 minutes of inactivity (no open browser tab)
- **UI operations**: creating jobs must currently be done via CLI (schedules can be changed in the WebUI)
- **Pagination**: list views show the top 100 items
- **Log latency**: logs may lag 20–30 seconds during execution; they are guaranteed complete after the run finishes (completed or failed state)
- **One workspace per GitHub account**: connecting a new local repo and deploying replaces the existing remote workspace
