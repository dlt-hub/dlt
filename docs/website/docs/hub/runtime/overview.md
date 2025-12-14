---
title: Overview
description: Deploy and run dlt pipelines and notebooks in the cloud
keywords: [runtime, deployment, cloud, scheduling, notebooks, dashboard]
---

# dltHub Runtime

dltHub Runtime is a managed cloud platform for running your `dlt` pipelines and notebooks. It provides:

- Cloud execution of batch pipelines and interactive notebooks
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

#### Public kinks for interactive jobs

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
| `dlt runtime launch <script>` | Deploy and run a batch script |
| `dlt runtime serve <script>` | Deploy and run an interactive notebook |
| `dlt runtime schedule <script> "<cron>"` | Schedule a script with cron expression |
| `dlt runtime schedule <script> cancel` | Cancel a scheduled script |
| `dlt runtime logs <script> [run_number]` | View logs for a run |
| `dlt runtime cancel <script> [run_number]` | Cancel a running job |
| `dlt runtime dashboard` | Open the web dashboard |
| `dlt runtime deploy` | Sync code and config without running |
| `dlt runtime info` | Show workspace overview |

### Deployment Commands

```sh
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

```sh
# List all jobs
dlt runtime job list

# Get job details
dlt runtime job info <script_path_or_job_name>

# Create a job without running it
dlt runtime job create <script_path> [--name NAME] [--schedule "CRON"] [--interactive]
```

### Job run commands

```sh
# List all runs
dlt runtime job-run list [script_path_or_job_name]

# Get run details
dlt runtime job-run info <script_path_or_job_name> [run_number]

# Create a new run
dlt runtime job-run create <script_path_or_job_name>

# View run logs
dlt runtime job-run logs <script_path_or_job_name> [run_number] [-f/--follow]

# Cancel a run
dlt runtime job-run cancel <script_path_or_job_name> [run_number]
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

## Development workflow

A typical development flow:

1. **Develop locally** with DuckDB (`dev` profile):
   ```sh
   uv run python fruitshop_pipeline.py
   ```

2. **Test your notebook locally**:
   ```sh
   uv run marimo edit fruitshop_notebook.py
   ```

3. **Run pipeline in Runtime** (`prod` profile):
   ```sh
   uv run dlt runtime launch fruitshop_pipeline.py
   ```

4. **Run notebook in Runtime** (`access` profile):
   ```sh
   uv run dlt runtime serve fruitshop_notebook.py
   ```

5. **Check run status and logs**:
   ```sh
   uv run dlt runtime logs fruitshop_pipeline.py
   ```

## Key concepts

### Jobs vs runs

- A **Job** is a script registered in your workspace. It defines what code to run and optionally a schedule.
- A **Run** is a single execution of a job. Each run has its own logs, status, and metadata.

### Batch vs interactive

- **Batch jobs** run with the `prod` profile and are meant for scheduled data loading
- **Interactive jobs** run with the `access` profile and are meant for notebooks and dashboards

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

- **Runtime limits**: Jobs are limited to 120 minutes maximum execution time
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
