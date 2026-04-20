---
title: Overview
description: Deploy and run dlt pipelines and notebooks in the cloud
keywords: [runtime, deployment, cloud, scheduling, notebooks, dashboard]
---

# dltHub Runtime

dltHub Runtime is a managed cloud platform for running your `dlt` pipelines and notebooks. It provides:

- Cloud execution of batch pipelines and interactive applications (notebooks, dashboards, and MCP servers)
- Scheduling with cron expressions
- A web dashboard for monitoring runs, viewing logs, and managing jobs
- Secure secrets management with multiple profiles

dltHub Runtime creates a mirror of your local workspace (called a **workspace deployment**). You continue working with your familiar dlt pipelines, datasets, notebooks, and dashboards - they just run remotely instead of on your machine.

:::caution
Each GitHub account can have only one remote workspace. When you run `dlt runtime login`, it connects your current local workspace to this remote workspace. If you later connect a different local repository and deploy or launch a job, it will replace your existing [**deployment** and **configuration**](#deployments-and-configurations), making any previously scheduled jobs defunct.

Support for multiple remote workspaces (mirroring multiple local repositories) is planned for next year.
:::

## Credentials and configs

### Understanding workspace profiles

dlt Runtime uses **profiles** to manage different configurations for different environments. The two main profiles are:

| Profile | Purpose | Credentials |
|---------|---------|-------------|
| `prod` | Production/batch jobs | Read/write access to your destination |
| `access` | Interactive notebooks and dashboards | Read-only access (for safe data exploration) |

### Setting up configuration files

Configuration files live in the `.dlt/` directory:

```text
.dlt/
├── config.toml           # Default config (local development)
├── secrets.toml          # Default secrets (gitignored, local only)
├── prod.config.toml      # Production profile config
├── prod.secrets.toml     # Production secrets (gitignored)
├── access.config.toml    # Access profile config
└── access.secrets.toml   # Access secrets (gitignored)
```

Below you will find an example with the credentials set for the MotherDuck destination. You can swap it for any other cloud destination you prefer (for example
   [BigQuery](../../dlt-ecosystem/destinations/bigquery.md),
   [Snowflake](../../dlt-ecosystem/destinations/snowflake.md),
   [AWS S3](../../dlt-ecosystem/destinations/filesystem.md), …).

**Default `config.toml`** (for local development with DuckDB):

```toml
[runtime]
log_level = "WARNING"
dlthub_telemetry = true

# Runtime connection settings (set after login)
auth_base_url = "https://dlthub.app/api/auth"
api_base_url = "https://dlthub.app/api/api"
workspace_id = "your-workspace-id" # will be set by the runtime cli automatically

[destination.fruitshop_destination]
destination_type = "duckdb"
```

**`prod.config.toml`** (for batch jobs running on Runtime):

```toml
[destination.fruitshop_destination]
destination_type = "motherduck"
```

**`prod.secrets.toml`** (for batch jobs - read/write credentials):

```toml
[destination.fruitshop_destination.credentials]
database = "your_database"
password = "your-motherduck-service-token"  # Read/write token
```

**`access.config.toml`** (for interactive notebooks):

```toml
[destination.fruitshop_destination]
destination_type = "motherduck"
```

**`access.secrets.toml`** (for interactive notebooks - read-only credentials):

```toml
[destination.fruitshop_destination.credentials]
database = "your_database"
password = "your-motherduck-read-only-token"  # Read-only token
```

:::warning Security
Files matching `*.secrets.toml` and `secrets.toml` are gitignored by default. Never commit secrets to version control. The Runtime securely stores your secrets when you sync your configuration.
:::

## Web UI

Visit [dlthub.app](https://dlthub.app) to access the web dashboard. The dashboard provides:

### Overview
The workspace overview shows all your jobs and recent runs at a glance. Lists auto-refresh every 10 seconds.

### Jobs
View and manage all jobs in your workspace. A **job** represents a script that can be run on demand or on a schedule.

From the Jobs page you can:
- View job details and run history
- Change or cancel schedules for batch jobs
- Create and manage **public links** for interactive jobs (notebooks/dashboards)

#### Public links for interactive jobs

Interactive jobs like notebooks and dashboards can be shared via public links. To manage public links:
1. Open the context menu on a job in the job list, or go to the job detail page
2. Click "Manage Public Link"
3. Enable the link to generate a shareable URL, or disable it to revoke access

Anyone with an active public link can view the running notebook or dashboard. This is useful for sharing dashboards with stakeholders who don't have Runtime access.

### Runs
Monitor all job runs with:
- Run status (pending, running, completed, failed, cancelled)
- Start time and duration
- Trigger type (manual, scheduled, API)

### Run details
Click on any run to see:
- Full execution logs
- Run metadata
- Pipeline information

### Deployment & config
View the files deployed to Runtime:
- Current deployment version
- Configuration profiles
- File listing

### Dashboard
Access the dlt pipeline dashboard to visualize:
- Pipeline schemas
- Load information
- Data lineage

### Settings
Manage workspace settings and view workspace metadata.

## CLI reference

For detailed CLI documentation, see [CLI](../command-line-interface.md).

### Common commands

| Command | Description |
|---------|-------------|
| `dlt runtime login` | Authenticate with GitHub OAuth |
| `dlt runtime logout` | Clear local credentials |
| `dlt runtime launch <script_or_job>` | Deploy and run a batch script |
| `dlt runtime serve <script_or_job>` | Deploy and run an interactive application |
| `dlt runtime deploy` | Deploy jobs from `__deployment__.py` manifest |
| `dlt runtime trigger <selector> [--refresh]` | Trigger jobs matching a selector (e.g. `tag:backfill`, `schedule:*`) |
| `dlt runtime run-pipeline <pipeline_name>` | Trigger job by pipeline name |
| `dlt runtime logs <name> [-f]` | View or stream logs for a run |
| `dlt runtime cancel <name_or_selector>` | Cancel active runs for matching jobs |
| `dlt runtime dashboard` | Open the web dashboard |
| `dlt runtime info` | Show workspace deployment overview |

### Deployment commands

For workspaces with a `__deployment__.py` manifest (required for scheduling, followup jobs, and multi-job workspaces):

```sh
# Deploy jobs from __deployment__.py manifest
dlt runtime deploy

# Preview what would change without applying
dlt runtime deploy --dry-run

# Dump the expanded manifest as YAML (useful for debugging)
dlt runtime deploy --show-manifest

# Sync code + config without deploying manifest
dlt runtime sync

# Sync only code (deployment)
dlt runtime deployment sync

# Sync only configuration (secrets and config)
dlt runtime configuration sync

# List all deployments
dlt runtime deployment list

# Get deployment details
dlt runtime deployment info [version_number]
```

### Job commands

Commands accept job names, script paths, or **selectors** (`batch`, `tag:ingest`, `schedule:*`):

```sh
# List all jobs
dlt runtime job list

# List jobs matching a selector
dlt runtime job "tag:ingest" list

# Get job details
dlt runtime job <name> info
```

### Job run commands

```sh
# List all runs (or filter by job name/selector)
dlt runtime job-run list [name_or_selector]

# Get run details
dlt runtime job-run <name> [run_number] info

# View run logs
dlt runtime job-run <name> [run_number] logs [-f/--follow]

# Cancel a run
dlt runtime job-run <name> [run_number] cancel
```

### Configuration commands

```sh
# List configuration versions
dlt runtime configuration list

# Get configuration details
dlt runtime configuration info [version_number]

# Sync local configuration to Runtime
dlt runtime configuration sync
```

## Key concepts

### Jobs vs runs

- A **Job** is a script registered in your workspace. It defines what code to run and optionally a schedule.
- A **Run** is a single execution of a job. Each run has its own logs, status, and metadata.

### Batch vs interactive

- **Batch jobs** run with the `prod` profile and are meant for scheduled data loading
- **Interactive jobs** run with the `access` profile and are meant for notebooks, dashboards, mcp servers and streamlit apps.

### Interactive application types

dltHub Runtime supports multiple types of interactive applications. All interactive jobs run with the `access` profile by default.

| Type | Description |
|-----|-------------|
| Notebooks | Marimo notebooks for the pipeline dashboard, exploration and analysis |
| Streamlit apps | Interactive Streamlit dashboards |
| MCP servers* | Model Context Protocol (MCP) HTTP servers (mounted at `/mcp`) |

Each interactive application is exposed via a unique public URL tied to its run.

\* **MCP servers requirement**

MCP applications must expose an `mcp` object created with `FastMCP`.
The Runtime imports this object to start the server.

Minimal example:

```py
from fastmcp import FastMCP

mcp = FastMCP("simple-mcp")

@mcp.tool
def ping() -> str:
    return "pong"
```

### Serving Streamlit and MCP applications

Use the `serve` command to deploy and run interactive applications:

```sh
dlt runtime serve my_notebook.py          # marimo (auto-detected)
dlt runtime serve my_mcp_server.py        # FastMCP (auto-detected)
dlt runtime serve my_streamlit_app.py     # Streamlit (auto-detected)
```

The application type is auto-detected from the module contents. For workspaces with a `__deployment__.py` manifest, import the module there and deploy with `dlt runtime deploy` -- interactive jobs are detected automatically.

### Profiles

Profiles allow you to have different configurations for different environments:

- Local development can use DuckDB with no credentials needed
- Production runs use MotherDuck (or other destinations) with full read/write access
- Interactive sessions use read-only credentials for safety

### Deployments and configurations

- **Deployment**: Your code files (`.py` scripts, notebooks)
- **Configuration**: Your `.dlt/*.toml` files (settings and secrets)

Both are versioned separately, allowing you to update code without changing secrets and vice versa.

## Current limitations

- **Runtime limits**: Jobs default to 120 minutes maximum execution time (override with `execute={"timeout": "6h"}` in the decorator)
- **Interactive timeout**: Notebooks are killed after about 5 minutes of inactivity (no open browser tab)
- **UI operations**: Creating jobs must currently be done via CLI (schedules can be changed in the WebUI)
- **Pagination**: List views show the top 100 items
- **Log latency**: Logs may lag 20-30 seconds during execution; they are guaranteed complete after run finishes (completed or failed state)

## Troubleshooting

### No 'access' profile detected
If you see this warning, your interactive notebooks will use the default configuration. Create `access.config.toml` and `access.secrets.toml` files with read-only credentials.

### No 'prod' profile detected
Batch jobs will use the default configuration. Create `prod.config.toml` and `prod.secrets.toml` files with read/write credentials.

### Job not using latest code
The CLI does not yet detect whether local code differs from remote. Run `dlt runtime deployment sync` to ensure your latest code is deployed.

### Logs not appearing
Logs may lag 20-30 seconds during execution. Wait for the run to complete for guaranteed complete logs, or use `--follow` to tail logs in real-time:
```sh
dlt runtime logs my_pipeline.py --follow
```