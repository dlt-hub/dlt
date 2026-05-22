---
title: Overview
description: Deploy and run dlt pipelines, transformations and notebooks in the cloud with the dltHub platform
keywords: [dlthub platform, deployment, cloud, scheduling, notebooks, dashboard, jobs, triggers, manifest]
---

# dltHub platform

The dltHub platform is a managed cloud platform for running your [`dlt` pipelines](../../general-usage/pipeline.md), [transformations](../features/transformations/index.md), and [notebooks](../../general-usage/dataset-access/marimo.md). It mirrors your local [dltHub Workspace](../workspace/overview.md) into the cloud (called a **workspace deployment**), so your familiar dlt pipelines, [datasets](../core-concepts/datasets.md), notebooks, and dashboards run remotely with the same code that runs on your machine.

For a high-level summary of platform capabilities, see [Pipeline operations](../introduction.md#pipeline-operations) in the introduction.

## Where to start

| If you want to... | Go to |
|-------------------|-------|
| Convert a Python project into a dltHub workspace and set up credentials | [Workspace setup](workspace-setup.md) |
| Push code to the cloud — ad-hoc runs or full manifest deploys | [Deployments](deploying.md) |
| Schedule with cron/intervals, chain follow-ups, backfill with scheduler-driven intervals, gate on freshness, cascade refreshes, tag jobs for bulk operations | [Triggers and scheduling](triggers.md) |
| Configure timeouts, dependencies, timezone, and per-job TOML sections | [Job configuration](job-configuration.md) |
| Stream logs in real time, inspect run states, view metric dashboards, diagnose failures, cancel runs | [Monitoring and debugging](monitor-and-debug.md) |
| Pick a deployment region | [Regions](regions.md) |

If you prefer a guided walkthrough, follow the [dltHub platform tutorial](../getting-started/runtime-tutorial.md).

## Key concepts

### Jobs vs runs

- A **Job** is a script registered in your workspace. It defines what code to run and optionally a schedule.
- A **Run** is a single execution of a job. Each run has its own logs, status, and metadata. See [run states](monitor-and-debug.md#understand-run-states).

### Batch vs interactive

- **Batch jobs** run with the [`prod` profile](../core-concepts/profiles-dlthub.md) and are meant for scheduled [data loading](../../general-usage/pipeline.md).
- **Interactive jobs** run with the [`access` profile](../core-concepts/profiles-dlthub.md) and are meant for [notebooks](../../general-usage/dataset-access/marimo.md), [dashboards](../workspace/dashboard.md), and Streamlit apps.

### Interactive application types

| Type | Description |
|------|-------------|
| Notebooks | [Marimo notebooks](../../general-usage/dataset-access/marimo.md) for the pipeline dashboard, exploration, and analysis |
| Streamlit apps | Interactive [Streamlit dashboards](../workspace/dashboard.md) |
| MCP servers | Model Context Protocol servers that provide tool and data access for AI assistants and agents |

Each interactive application is exposed via a unique public URL tied to its run.

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

For monitoring runs, streaming logs, and diagnosing failures, see [Monitoring and debugging](monitor-and-debug.md).

#### Public links for interactive jobs

Notebooks and dashboards can be shared via public links. Manage them either from the dashboard — open the job's context menu (or its detail page) and click **Manage Public Link** to toggle the link — or from the CLI:

```sh
# Generate a public link
dlthub job publish fruitshop_notebook.py

# Revoke an active link
dlthub job unpublish fruitshop_notebook.py
```

Anyone with an active link can view the running notebook or dashboard — useful for sharing dashboards with stakeholders without dltHub platform access.

## CLI reference

For detailed CLI documentation, see [CLI](../command-line-interface.md).

### Common commands

| Command | Description |
|---------|-------------|
| `dlthub login` | Authenticate with GitHub OAuth (interactive workspace selection) |
| `dlthub logout` | Clear local credentials |
| `dlthub workspace list` | List all accessible workspaces |
| `dlthub workspace connect [name_or_id]` | Connect project to a workspace (interactive picker if no arg) |
| `dlthub local info` | Show local workspace info |
| `dlthub show` | Open the dltHub dashboard |
| `dlthub local run <script_or_job>` | Run a batch job on the local machine (recommended before deploying) |
| `dlthub local serve <script_or_job>` | Serve an interactive app on the local machine |
| `dlthub run [<script_or_selector>] [-f] [--refresh]` | Deploy and run a batch script or named job |
| `dlthub serve [<script_or_selector>] [-f]` | Deploy and serve an interactive application |
| `dlthub deploy [--dry-run] [--show-manifest]` | Deploy jobs from `__deployment__.py` |
| `dlthub job trigger <selectors...> [--refresh] [--dry-run] [--profile NAME]` | Trigger runs for matching jobs (for example `tag:backfill`, `schedule:*`) |
| `dlthub pipeline run <pipeline_name> [-f] [--refresh]` | Run a job by pipeline name |
| `dlthub job cancel <selector_or_name>...` | Cancel active runs for matching jobs |
| `dlthub job runs cancel <selector_or_name> [run_number]` | Cancel a specific run (defaults to latest) |
| `dlthub job logs <selector_or_name> [run_number] [-f]` | View or stream logs for a run |
| `dlthub job publish <script_path>` | Generate a public link for an interactive notebook/app |
| `dlthub job unpublish <script_path>` | Revoke a public link |

## Platform limits

- **Platform limits**: non-interactive jobs default to 2 hours maximum execution time (override with `execute={"timeout": "6h"}` in the decorator — see [Job configuration](job-configuration.md#execution-constraints))
- **Interactive timeout**: interactive jobs (notebooks, dashboards, MCP servers) are capped at 15 minutes of execution time and are not extended
- **UI operations**: new jobs must currently be created via the CLI; once a job exists, subsequent runs can be triggered from the Web UI (and schedules can be changed there too)
- **Pagination**: list views are paginated; the page size can be adjusted in the Web UI
- **Log latency**: logs typically lag a few seconds during execution and are guaranteed complete after the run finishes (completed or failed state)
