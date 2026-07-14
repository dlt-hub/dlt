---
title: Command Line Interface
description: Command line interface (CLI) full reference of dlt
keywords: [command line interface, cli, dlt init]
---


# Command Line Interface Reference

<!-- this page is fully generated from the argparse object of dlt, run make update-cli-docs to update it -->

This page contains all commands available in the dlt CLI and is generated
automatically from the fully populated python argparse object of dlt.
:::note
Flags and positional commands are inherited from the parent command. Position within the command string
is important. For example if you want to enable debug mode on the pipeline command, you need to add the
debug flag to the base dlt command:

```sh
dlt --debug pipeline
```

Adding the flag after the pipeline keyword will not work.
:::

## `dlthub`

Creates, adds, inspects and deploys dlt pipelines. Further help is available at https://dlthub.com/docs/reference/command-line-interface.

**Usage**
```sh
dlthub [-h] [-v] [--non-interactive] [-y] [--debug] [--version]
    [--disable-telemetry] [--enable-telemetry] [--no-pwd]
    {dbt,workspace,show,serve,run,logout,login,job,deploy,profile,pipeline,local,init,ai}
    ...
```

<details>

<summary>Show Arguments and Options</summary>

**Options**
* `-h, --help` - Show this help message and exit
* `-v, --verbose` - Increase verbosity. repeat for more (-v, -vv, -vvv).
* `--non-interactive` - Use prompt defaults; fail if a prompt has none. implied when stdin is not a tty.
* `-y, --yes` - Auto-accept confirmations. free-form prompts still need defaults.
* `--debug` - Show full stack traces on exceptions.
* `--version` - Show program's version number and exit
* `--disable-telemetry` - Disables telemetry before command is executed
* `--enable-telemetry` - Enables telemetry before command is executed
* `--no-pwd` - Do not add current working directory to sys.path. by default $pwd is added to reproduce python behavior when running scripts.

**Available subcommands**
* [`dbt`](#dlthub-dbt) - Dlthub dbt transformation generator
* [`workspace`](#dlthub-workspace) - Workspace operations: connect, list, info, show, deploy, deployment, configuration
* [`show`](#dlthub-show) - Open the dlthub dashboard (alias for `dlthub workspace show`)
* [`serve`](#dlthub-serve) - Deploy and serve an interactive notebook/app (alias for `dlthub job serve`)
* [`run`](#dlthub-run) - Deploy code/config and run a script (alias for `dlthub job run`)
* [`logout`](#dlthub-logout) - Log out from dlthub
* [`login`](#dlthub-login) - Log in to dlthub (identity only)
* [`job`](#dlthub-job) - Job operations: list, info, run, serve, trigger, publish, unpublish, logs, cancel, runs
* [`deploy`](#dlthub-deploy) - Sync code/config and deploy jobs
* [`profile`](#dlthub-profile) - Manage workspace built-in profiles
* [`pipeline`](#dlthub-pipeline) - Interact with pipelines running in dlthub
* [`local`](#dlthub-local) - Operations on the local workspace (run, serve, info, show, clean, schema, telemetry, pipeline)
* [`init`](#dlthub-init) - Initialize a new dlthub workspace
* [`ai`](#dlthub-ai) - Use ai-powered development tools and utilities

</details>

## `dlthub dbt`

dlthub dbt transformation generator.

**Usage**
```sh
dlthub dbt [-h] {generate} ...
```

**Description**

dlthub dbt transformation generator.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub`](#dlthub).

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`generate`](#dlthub-dbt-generate) - Generate dbt project

</details>

### `dlthub dbt generate`

Generate dbt project.

**Usage**
```sh
dlthub dbt generate [-h] [--include_dlt_tables] [--fact [FACT]] [--force]
    [--mart_table_prefix [MART_TABLE_PREFIX]] pipeline_name
```

**Description**

Generate dbt project.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub dbt`](#dlthub-dbt).

**Positional arguments**
* `pipeline_name` - The pipeline to create a dbt project for

**Options**
* `-h, --help` - Show this help message and exit
* `--include_dlt_tables` - Do not render _dlt tables
* `--fact [FACT]` - Create a fact table for a given table
* `--force` - Force overwrite of existing files
* `--mart_table_prefix [MART_TABLE_PREFIX]` - Prefix for mart tables

</details>

## `dlthub workspace`

Workspace operations: connect, list, info, show, deploy, deployment, configuration.

**Usage**
```sh
dlthub workspace [-h] [--timestamps]
    {list,connect,info,show,deploy,deployment,configuration} ...
```

**Description**

Bind this project to a remote dltHub workspace and manage its deployments and configurations.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub`](#dlthub).

**Options**
* `-h, --help` - Show this help message and exit
* `--timestamps` - Show exact iso timestamps and precise durations (e.g. 1.291 s) instead of humanized relative times.

**Available subcommands**
* [`list`](#dlthub-workspace-list) - List all workspaces you have access to
* [`connect`](#dlthub-workspace-connect) - Connects local to a remote workspace by name or id
* [`info`](#dlthub-workspace-info) - Show overview of current dlthub workspace (workspace, job count, latest run, latest deployment, latest configuration)
* [`show`](#dlthub-workspace-show) - Open the dlthub dashboard (alias for `dlthub workspace show`)
* [`deploy`](#dlthub-workspace-deploy) - Sync code/config and deploy jobs
* [`deployment`](#dlthub-workspace-deployment) - Manipulate deployments in the workspace
* [`configuration`](#dlthub-workspace-configuration) - Manipulate configurations in the workspace

</details>

### `dlthub workspace list`

List all workspaces you have access to.

**Usage**
```sh
dlthub workspace list [-h]
```

**Description**

List all workspaces you have access to.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub workspace`](#dlthub-workspace).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub workspace connect`

Connects local to a remote workspace by name or ID.

**Usage**
```sh
dlthub workspace connect [-h] [--create] [--org-id ORG_ID] [workspace]
```

**Description**

Connects local and remote workspaces. Jobs, pipelines and code available locally can then be deployed, scheduled and run in remote workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub workspace`](#dlthub-workspace).

**Positional arguments**
* `workspace` - Workspace name or id to connect to. when omitted interactive picker will allow to select existing or create a new one

**Options**
* `-h, --help` - Show this help message and exit
* `--create` - 
* `--org-id ORG_ID` - Organization uuid to scope the connection to. required in non-interactive mode when you belong to multiple organizations and local workspace has no organization pinned.

</details>

### `dlthub workspace info`

Show overview of current dltHub workspace (workspace, job count, latest run, latest deployment, latest configuration).

**Usage**
```sh
dlthub workspace info [-h]
```

**Description**

Show workspace ID and summary of deployments, configurations and jobs.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub workspace`](#dlthub-workspace).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub workspace show`

Open the dltHub dashboard (alias for `dlthub workspace show`).

**Usage**
```sh
dlthub workspace show [-h]
```

**Description**

Open link to the dltHub dashboard for current remote workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub workspace`](#dlthub-workspace).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub workspace deploy`

Sync code/config and deploy jobs.

**Usage**
```sh
dlthub workspace deploy [-h] [--deployment DEPLOYMENT] [--dry-run]
    [--show-manifest]
```

**Description**

Sync workspace files, generate job manifest from \_\_deployment__.py, and reconcile jobs with the runtime. Use --dry-run to preview changes.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub workspace`](#dlthub-workspace).

**Options**
* `-h, --help` - Show this help message and exit
* `--deployment DEPLOYMENT` - Python file to use as manifest source (instead of \_\_deployment__)
* `--dry-run` - Preview changes without applying them
* `--show-manifest` - Dump the expanded deployment manifest as yaml and exit

</details>

### `dlthub workspace deployment`

Manipulate deployments in the workspace.

**Usage**
```sh
dlthub workspace deployment [-h] [deployment_version_no] {list,info,sync} ...
```

**Description**

Manipulate deployments in the workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub workspace`](#dlthub-workspace).

**Positional arguments**
* `deployment_version_no` - Deployment version number. only used in the `info` subcommand

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`list`](#dlthub-workspace-deployment-list) - List all deployments in workspace
* [`info`](#dlthub-workspace-deployment-info) - Get detailed information about a deployment
* [`sync`](#dlthub-workspace-deployment-sync) - Create new deployment if local workspace content changed

</details>

### `dlthub workspace deployment list`

List all deployments in workspace.

**Usage**
```sh
dlthub workspace deployment [deployment_version_no] list [-h]
```

**Description**

List all deployments in workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub workspace deployment`](#dlthub-workspace-deployment).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub workspace deployment info`

Get detailed information about a deployment.

**Usage**
```sh
dlthub workspace deployment [deployment_version_no] info [-h]
```

**Description**

Get detailed information about a deployment.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub workspace deployment`](#dlthub-workspace-deployment).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub workspace deployment sync`

Create new deployment if local workspace content changed.

**Usage**
```sh
dlthub workspace deployment [deployment_version_no] sync [-h] [--dry-run] [-v]
```

**Description**

Create new deployment if local workspace content changed.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub workspace deployment`](#dlthub-workspace-deployment).

**Options**
* `-h, --help` - Show this help message and exit
* `--dry-run` - Compare local files to latest deployment without uploading
* `-v, --verbose` - Print per-file added/updated/deleted tree alongside the summary

</details>

### `dlthub workspace configuration`

Manipulate configurations in the workspace.

**Usage**
```sh
dlthub workspace configuration [-h] [configuration_version_no] {list,info,sync}
    ...
```

**Description**

Manipulate configurations in the workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub workspace`](#dlthub-workspace).

**Positional arguments**
* `configuration_version_no` - Configuration version number. only used in the `info` subcommand

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`list`](#dlthub-workspace-configuration-list) - List all configuration versions
* [`info`](#dlthub-workspace-configuration-info) - Get detailed information about a configuration
* [`sync`](#dlthub-workspace-configuration-sync) - Create new configuration if local config content changed

</details>

### `dlthub workspace configuration list`

List all configuration versions.

**Usage**
```sh
dlthub workspace configuration [configuration_version_no] list [-h]
```

**Description**

List all configuration versions.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub workspace configuration`](#dlthub-workspace-configuration).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub workspace configuration info`

Get detailed information about a configuration.

**Usage**
```sh
dlthub workspace configuration [configuration_version_no] info [-h]
```

**Description**

Get detailed information about a configuration.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub workspace configuration`](#dlthub-workspace-configuration).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub workspace configuration sync`

Create new configuration if local config content changed.

**Usage**
```sh
dlthub workspace configuration [configuration_version_no] sync [-h] [--dry-run]
    [-v]
```

**Description**

Create new configuration if local config content changed.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub workspace configuration`](#dlthub-workspace-configuration).

**Options**
* `-h, --help` - Show this help message and exit
* `--dry-run` - Compare local config to latest configuration without uploading
* `-v, --verbose` - Print per-file added/updated/deleted tree alongside the summary

</details>

## `dlthub show`

Open the dltHub dashboard (alias for `dlthub workspace show`).

**Usage**
```sh
dlthub show [-h]
```

**Description**

Open link to the dltHub dashboard for current remote workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub`](#dlthub).

**Options**
* `-h, --help` - Show this help message and exit

</details>

## `dlthub serve`

Deploy and serve an interactive notebook/app (alias for `dlthub job serve`).

**Usage**
```sh
dlthub serve [-h] [--deployment DEPLOYMENT] [--timestamps] [-f] [--job-ref REF]
    [selector_or_job_ref]
```

**Description**

Deploy current workspace and run a notebook as a read-only web app. A plain `.py` script (marimo notebook, Streamlit app, FastMCP server, etc.) may also be passed and will be deployed and served remotely as a regular script. Alias for `dlthub job serve`.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub`](#dlthub).

**Positional arguments**
* `selector_or_job_ref` - Selector or job ref to pick an interactive app from the manifest, or a .py file path to deploy and serve as a regular script

**Options**
* `-h, --help` - Show this help message and exit
* `--deployment DEPLOYMENT` - Python file to use as manifest source (instead of \_\_deployment__)
* `--timestamps` - Show exact iso timestamps and precise durations (e.g. 1.291 s) instead of humanized relative times.
* `-f, --follow` - Stream logs until the app stops
* `--job-ref REF` - Pick this job from the matched candidate set when the selector matches multiple jobs. errors if ref is not in the matched set.

</details>

## `dlthub run`

Deploy code/config and run a script (alias for `dlthub job run`).

**Usage**
```sh
dlthub run [-h] [--deployment DEPLOYMENT] [--timestamps] [-f] [--refresh]
    [--job-ref REF] [selector_or_job_ref]
```

**Description**

Deploy current workspace and run a batch script remotely. Use -f/--follow to tail logs until completion. A plain `.py` script may also be passed: if it exposes no jobs it is deployed and executed remotely as a regular Python script. Alias for `dlthub job run`.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub`](#dlthub).

**Positional arguments**
* `selector_or_job_ref` - Selector or job ref to pick a job from the manifest, or a .py file path to deploy and run as a regular script

**Options**
* `-h, --help` - Show this help message and exit
* `--deployment DEPLOYMENT` - Python file to use as manifest source (instead of \_\_deployment__)
* `--timestamps` - Show exact iso timestamps and precise durations (e.g. 1.291 s) instead of humanized relative times.
* `-f, --follow` - Follow status changes and stream logs until the run completes
* `--refresh` - Re-run from scratch (full reload). cascades to freshness-graph downstream jobs.
* `--job-ref REF` - Pick this job from the matched candidate set when the selector matches multiple jobs. errors if ref is not in the matched set.

</details>

## `dlthub logout`

Log out from dltHub.

**Usage**
```sh
dlthub logout [-h]
```

**Description**

Log out from dltHub.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub`](#dlthub).

**Options**
* `-h, --help` - Show this help message and exit

</details>

## `dlthub login`

Log in to dltHub (identity only).

**Usage**
```sh
dlthub login [-h] [--resume DEVICE_CODE]
```

**Description**

Log in to dltHub. Authenticates the current user; does not connect a workspace. Run `dlthub workspace connect` to bind this project to a remote workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub`](#dlthub).

**Options**
* `-h, --help` - Show this help message and exit
* `--resume DEVICE_CODE` - Resume a previously started device flow login. the device_code is printed by `dlthub login` when no tty is attached.

</details>

## `dlthub job`

Job operations: list, info, run, serve, trigger, publish, unpublish, logs, cancel, runs.

**Usage**
```sh
dlthub job [-h] [--timestamps]
    {list,info,show,trigger,publish,unpublish,logs,cancel,runs,serve,run} ...
```

**Description**

List and operate on jobs registered in the connected workspace, plus their runs.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub`](#dlthub).

**Options**
* `-h, --help` - Show this help message and exit
* `--timestamps` - Show exact iso timestamps and precise durations (e.g. 1.291 s) instead of humanized relative times.

**Available subcommands**
* [`list`](#dlthub-job-list) - List jobs (filter with selectors: batch, schedule:*, tag:ops, ...)
* [`info`](#dlthub-job-info) - Show job info
* [`show`](#dlthub-job-show) - Open the job page in the web gui
* [`trigger`](#dlthub-job-trigger) - Trigger jobs matching selectors (does not sync or deploy)
* [`publish`](#dlthub-job-publish) - Generate or revoke a public link for an interactive notebook/app
* [`unpublish`](#dlthub-job-unpublish) - Revoke the public link for an interactive notebook/app
* [`logs`](#dlthub-job-logs) - Show logs for latest or selected job run
* [`cancel`](#dlthub-job-cancel) - Cancel active runs for matching jobs
* [`runs`](#dlthub-job-runs) - Manage job runs: list, info, logs, cancel
* [`serve`](#dlthub-job-serve) - Deploy and serve an interactive notebook/app
* [`run`](#dlthub-job-run) - Deploy code/config and run a batch job

</details>

### `dlthub job list`

List jobs (filter with selectors: batch, schedule:*, tag:ops, ...).

**Usage**
```sh
dlthub job list [-h] [--archived] [selector_or_job_name ...]
```

**Description**

List jobs registered in the workspace. Pass selectors to filter: batch, interactive, schedule:*, tag:&lt;name&gt;, manual:*, etc.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub job`](#dlthub-job).

**Positional arguments**
* `selector_or_job_name` - Selector(s) or job name(s) used to filter the listing

**Options**
* `-h, --help` - Show this help message and exit
* `--archived` - Include archived jobs in the listing (hidden by default)

</details>

### `dlthub job info`

Show job info.

**Usage**
```sh
dlthub job info [-h] [selector_or_job_name]
```

**Description**

Display detailed information about the job.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub job`](#dlthub-job).

**Positional arguments**
* `selector_or_job_name` - Job name, script path, or selector identifying the job

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub job show`

Open the job page in the web GUI.

**Usage**
```sh
dlthub job show [-h] [selector_or_job_name]
```

**Description**

Print the URL of the job page in the dltHub dashboard and open it in a browser when interactive.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub job`](#dlthub-job).

**Positional arguments**
* `selector_or_job_name` - Job name, script path, or selector identifying the job

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub job trigger`

Trigger jobs matching selectors (does not sync or deploy).

**Usage**
```sh
dlthub job trigger [-h] [--dry-run] [--profile PROFILE] [--refresh] selectors
    [selectors ...]
```

**Description**

Trigger runs for jobs matching the given selectors. Can select only jobs already deployed. Does not sync code or deploy jobs. Examples: 'tag:backfill', 'manual:jobs.etl.*', 'schedule:*'.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub job`](#dlthub-job).

**Positional arguments**
* `selectors` - Trigger selectors (fnmatch patterns), e.g. 'tag:backfill', 'manual:jobs.etl.*'

**Options**
* `-h, --help` - Show this help message and exit
* `--dry-run` - Preview matched jobs without creating runs
* `--profile PROFILE` - Profile override for all triggered runs
* `--refresh` - Force a refresh on every triggered job (jobs skipped by freshness are not refreshed).

</details>

### `dlthub job publish`

Generate or revoke a public link for an interactive notebook/app.

**Usage**
```sh
dlthub job publish [-h] [--cancel] script_path
```

**Description**

Generate a public link for a notebook/app, or revoke it with --cancel.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub job`](#dlthub-job).

**Positional arguments**
* `script_path` - Local path to the notebook/app

**Options**
* `-h, --help` - Show this help message and exit
* `--cancel` - Revoke the public link for the notebook/app

</details>

### `dlthub job unpublish`

Revoke the public link for an interactive notebook/app.

**Usage**
```sh
dlthub job unpublish [-h] script_path
```

**Description**

Revoke the public link for an interactive notebook/app.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub job`](#dlthub-job).

**Positional arguments**
* `script_path` - Local path to the notebook/app

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub job logs`

Show logs for latest or selected job run.

**Usage**
```sh
dlthub job logs [-h] [-f] selector_or_job_name [run_number]
```

**Description**

Show logs for the latest run of a job or a specific run number. Use -f/--follow to stream logs in real-time.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub job`](#dlthub-job).

**Positional arguments**
* `selector_or_job_name` - Job name, script path, or selector (e.g. batch, schedule:*).
* `run_number` - Run number (optional)

**Options**
* `-h, --help` - Show this help message and exit
* `-f, --follow` - Follow logs in real-time until the run completes

</details>

### `dlthub job cancel`

Cancel active runs for matching jobs.

**Usage**
```sh
dlthub job cancel [-h] [--dry-run] selector_or_job_name [selector_or_job_name
    ...]
```

**Description**

Cancel active (non-terminal) runs for jobs matching selectors or names. Multiple values cancel active runs for all matching jobs. Use --dry-run to preview.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub job`](#dlthub-job).

**Positional arguments**
* `selector_or_job_name` - Job name, script path, or selector (e.g. batch, schedule:*).

**Options**
* `-h, --help` - Show this help message and exit
* `--dry-run` - Show what would be cancelled without actually cancelling

</details>

### `dlthub job runs`

Manage job runs: list, info, logs, cancel.

**Usage**
```sh
dlthub job runs [-h] {list,info,logs,show,cancel} ...
```

**Description**

Operate on runs of a job: list runs, show info, stream logs, cancel.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub job`](#dlthub-job).

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`list`](#dlthub-job-runs-list) - List job runs (filter with a selector: batch, schedule:*, ...)
* [`info`](#dlthub-job-runs-info) - Show job run info
* [`logs`](#dlthub-job-runs-logs) - Show logs for the latest or selected job run
* [`show`](#dlthub-job-runs-show) - Open the job run page in the web gui
* [`cancel`](#dlthub-job-runs-cancel) - Cancel the latest or selected job run

</details>

### `dlthub job runs list`

List job runs (filter with a selector: batch, schedule:*, ...).

**Usage**
```sh
dlthub job runs list [-h] [--running] [selector_or_job_name]
```

**Description**

List job runs registered in the workspace. Pass a selector to filter by matching jobs.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub job runs`](#dlthub-job-runs).

**Positional arguments**
* `selector_or_job_name` - Selector or job name to filter runs by

**Options**
* `-h, --help` - Show this help message and exit
* `--running` - Show only runs that are not in a terminal state

</details>

### `dlthub job runs info`

Show job run info.

**Usage**
```sh
dlthub job runs info [-h] selector_or_job_name [run_number]
```

**Description**

Display detailed information about the job run.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub job runs`](#dlthub-job-runs).

**Positional arguments**
* `selector_or_job_name` - Job name, script path, or selector
* `run_number` - Run number (defaults to latest run of the given job)

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub job runs logs`

Show logs for the latest or selected job run.

**Usage**
```sh
dlthub job runs logs [-h] [-f] selector_or_job_name [run_number]
```

**Description**

Show logs for the latest or selected job run. Use -f/--follow to stream logs in real-time until completion.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub job runs`](#dlthub-job-runs).

**Positional arguments**
* `selector_or_job_name` - Job name, script path, or selector
* `run_number` - Run number (defaults to latest run)

**Options**
* `-h, --help` - Show this help message and exit
* `-f, --follow` - Follow logs in real-time until the run completes

</details>

### `dlthub job runs show`

Open the job run page in the web GUI.

**Usage**
```sh
dlthub job runs show [-h] selector_or_job_name [run_number]
```

**Description**

Print the URL of the job run page in the dltHub dashboard and open it in a browser when interactive.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub job runs`](#dlthub-job-runs).

**Positional arguments**
* `selector_or_job_name` - Job name, script path, or selector
* `run_number` - Run number (defaults to latest run of the given job)

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub job runs cancel`

Cancel the latest or selected job run.

**Usage**
```sh
dlthub job runs cancel [-h] selector_or_job_name [run_number]
```

**Description**

Cancel the latest or selected job run.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub job runs`](#dlthub-job-runs).

**Positional arguments**
* `selector_or_job_name` - Job name, script path, or selector
* `run_number` - Run number (defaults to latest run)

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub job serve`

Deploy and serve an interactive notebook/app.

**Usage**
```sh
dlthub job serve [-h] [--deployment DEPLOYMENT] [--timestamps] [-f] [--job-ref
    REF] [selector_or_job_ref]
```

**Description**

Deploy current workspace and run a notebook as a read-only web app. A plain `.py` script (marimo notebook, Streamlit app, FastMCP server, etc.) may also be passed and will be deployed and served remotely as a regular script.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub job`](#dlthub-job).

**Positional arguments**
* `selector_or_job_ref` - Selector or job ref to pick an interactive app from the manifest, or a .py file path to deploy and serve as a regular script

**Options**
* `-h, --help` - Show this help message and exit
* `--deployment DEPLOYMENT` - Python file to use as manifest source (instead of \_\_deployment__)
* `--timestamps` - Show exact iso timestamps and precise durations (e.g. 1.291 s) instead of humanized relative times.
* `-f, --follow` - Stream logs until the app stops
* `--job-ref REF` - Pick this job from the matched candidate set when the selector matches multiple jobs. errors if ref is not in the matched set.

</details>

### `dlthub job run`

Deploy code/config and run a batch job.

**Usage**
```sh
dlthub job run [-h] [--deployment DEPLOYMENT] [--timestamps] [-f] [--refresh]
    [--job-ref REF] [selector_or_job_ref]
```

**Description**

Deploy current workspace and run a batch script remotely. Use -f/--follow to tail logs until completion. A plain `.py` script may also be passed: if it exposes no jobs it is deployed and executed remotely as a regular Python script.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub job`](#dlthub-job).

**Positional arguments**
* `selector_or_job_ref` - Selector or job ref to pick a job from the manifest, or a .py file path to deploy and run as a regular script

**Options**
* `-h, --help` - Show this help message and exit
* `--deployment DEPLOYMENT` - Python file to use as manifest source (instead of \_\_deployment__)
* `--timestamps` - Show exact iso timestamps and precise durations (e.g. 1.291 s) instead of humanized relative times.
* `-f, --follow` - Follow status changes and stream logs until the run completes
* `--refresh` - Re-run from scratch (full reload). cascades to freshness-graph downstream jobs.
* `--job-ref REF` - Pick this job from the matched candidate set when the selector matches multiple jobs. errors if ref is not in the matched set.

</details>

## `dlthub deploy`

Sync code/config and deploy jobs.

**Usage**
```sh
dlthub deploy [-h] [--timestamps] [--deployment DEPLOYMENT] [--dry-run]
    [--show-manifest]
```

**Description**

Sync workspace files, generate job manifest from \_\_deployment__.py, and reconcile jobs with the runtime. Use --dry-run to preview changes.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub`](#dlthub).

**Options**
* `-h, --help` - Show this help message and exit
* `--timestamps` - Show exact iso timestamps and precise durations (e.g. 1.291 s) instead of humanized relative times.
* `--deployment DEPLOYMENT` - Python file to use as manifest source (instead of \_\_deployment__)
* `--dry-run` - Preview changes without applying them
* `--show-manifest` - Dump the expanded deployment manifest as yaml and exit

</details>

## `dlthub profile`

Manage Workspace built-in profiles.

**Usage**
```sh
dlthub profile [-h] {info,list} ...
```

**Description**

Show and list workspace profiles.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub`](#dlthub).

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`info`](#dlthub-profile-info) - Display the active profile (paths, providers, pinned status)
* [`list`](#dlthub-profile-list) - List all available profiles

</details>

### `dlthub profile info`

Display the active profile (paths, providers, pinned status).

**Usage**
```sh
dlthub profile info [-h]
```

**Description**

Display the active profile (paths, providers, pinned status).

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub profile`](#dlthub-profile).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub profile list`

List all available profiles.

**Usage**
```sh
dlthub profile list [-h]
```

**Description**

List all available profiles.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub profile`](#dlthub-profile).

**Options**
* `-h, --help` - Show this help message and exit

</details>

## `dlthub pipeline`

Interact with pipelines running in dlthub.

**Usage**
```sh
dlthub pipeline [-h] {init,show,run} ...
```

**Description**

Create, run, inspect and monitor pipelines at dltHub.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub`](#dlthub).

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`init`](#dlthub-pipeline-init) - Creates a pipeline in the current folder by adding existing verified source or creating a new one from template.
* [`show`](#dlthub-pipeline-show) - Open the pipeline observability view in the dlthub dashboard
* [`run`](#dlthub-pipeline-run) - Run a job by pipeline name

</details>

### `dlthub pipeline init`

Creates a pipeline in the current folder by adding existing verified source or creating a new one from template.

**Usage**
```sh
dlthub pipeline init [-h] [--list-sources] [--list-destinations] [--location
    LOCATION] [--branch BRANCH] [--eject] [source] [destination]
```

**Description**

This command creates a new dlt pipeline script that loads data from `source` to `destination`. When you run the command, several things happen:

1. Creates a basic project structure if the current folder is empty by adding `.dlt/config.toml`, `.dlt/secrets.toml`, and `.gitignore` files.
2. Checks if the `source` argument matches one of our verified sources and, if so, adds it to your project.
3. If the `source` is unknown, uses a generic template to get you started.
4. Rewrites the pipeline scripts to use your `destination`.
5. Creates sample config and credentials in `secrets.toml` and `config.toml` for the specified source and destination.
6. Creates `requirements.txt` with dependencies required by the source and destination. If one exists, prints instructions on what to add to it.

This command can be used several times in the same folder to add more sources, destinations, and pipelines. It will also update the verified source code to the newest
version if run again with an existing `source` name. You will be warned if files will be overwritten or if the `dlt` version needs an upgrade to run a particular pipeline.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub pipeline`](#dlthub-pipeline).

**Positional arguments**
* `source` - Name of data source for which to create a pipeline. adds existing verified source or creates a new pipeline template if verified source for your data source is not yet implemented.
* `destination` - Name of a destination i.e. bigquery or redshift

**Options**
* `-h, --help` - Show this help message and exit
* `--list-sources, -l` - Shows all available verified sources and their short descriptions. for each source, it checks if your local `dlt` version requires an update and prints the relevant warning.
* `--list-destinations` - Shows the name of all core dlt destinations.
* `--location LOCATION` - Advanced. uses a specific url or local path to verified sources repository.
* `--branch BRANCH` - Advanced. uses specific branch of the verified sources repository to fetch the template.
* `--eject` - Ejects the source code of the core source like sql_database or rest_api so they will be editable by you.

</details>

### `dlthub pipeline show`

Open the pipeline observability view in the dltHub dashboard.

**Usage**
```sh
dlthub pipeline show [-h] pipeline_name
```

**Description**

Show the URL of the pipeline observability view in the dltHub dashboard and open it in a browser when interactive. Replaces the core dlt local-marimo `pipeline show`.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub pipeline`](#dlthub-pipeline).

**Positional arguments**
* `pipeline_name` - Name of the pipeline to show

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub pipeline run`

Run a job by pipeline name.

**Usage**
```sh
dlthub pipeline run [-h] [--timestamps] [-f] [--refresh] [--job-ref REF]
    pipeline_name
```

**Description**

Run a job decorated with @run.pipeline, using pipeline_name: selector.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub pipeline`](#dlthub-pipeline).

**Positional arguments**
* `pipeline_name` - Name of the pipeline to run

**Options**
* `-h, --help` - Show this help message and exit
* `--timestamps` - Show exact iso timestamps and precise durations (e.g. 1.291 s) instead of humanized relative times.
* `-f, --follow` - Follow status changes and stream logs until the run completes
* `--refresh` - Re-run from scratch (full reload). cascades to freshness-graph downstream jobs.
* `--job-ref REF` - Pick this job from the matched candidate set when the selector matches multiple jobs. errors if ref is not in the matched set.

</details>

## `dlthub local`

Operations on the local Workspace (run, serve, info, show, clean, schema, telemetry, pipeline).

**Usage**
```sh
dlthub local [-h] {info,show,run,serve,clean,profile,schema,telemetry,pipeline}
    ...
```

**Description**

Local-only operations on the current workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub`](#dlthub).

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`info`](#dlthub-local-info) - Display detailed local workspace info
* [`show`](#dlthub-local-show) - Show workspace dashboard
* [`run`](#dlthub-local-run) - Run a single batch workspace job locally
* [`serve`](#dlthub-local-serve) - Serve an interactive workspace job locally (notebook, dashboard, app)
* [`clean`](#dlthub-local-clean) - Clean local data for the current profile. locally loaded data and pipelines working dirs are deleted by default. remote destinations are not affected.
* [`profile`](#dlthub-local-profile) - Profile operations that affect only the local workspace
* [`schema`](#dlthub-local-schema) - Shows, converts and upgrades schemas
* [`telemetry`](#dlthub-local-telemetry) - Shows telemetry status
* [`pipeline`](#dlthub-local-pipeline) - Local pipeline operations (info, drop, sync, load-package, etc.)

</details>

### `dlthub local info`

Display detailed local workspace info.

**Usage**
```sh
dlthub local info [-h]
```

**Description**

Display detailed local workspace info.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local`](#dlthub-local).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub local show`

Show workspace dashboard.

**Usage**
```sh
dlthub local show [-h] [--edit]
```

**Description**

Show workspace dashboard.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local`](#dlthub-local).

**Options**
* `-h, --help` - Show this help message and exit
* `--edit` - Eject dashboard and start editable version

</details>

### `dlthub local run`

Run a single batch workspace job locally.

**Usage**
```sh
dlthub local run [-h] [--deployment FILE] [--job-ref REF] [--profile NAME]
    [--dry-run] [-c KEY=VALUE] [--start ISO] [--end ISO] [--refresh]
    [selector_or_job_ref]
```

**Description**

Run one batch job by selector or job ref. A plain `.py` path is run as a regular script.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local`](#dlthub-local).

**Positional arguments**
* `selector_or_job_ref` - Job ref, trigger selector (tag:..., schedule:*), or a .py file to run as a script.

**Options**
* `-h, --help` - Show this help message and exit
* `--deployment FILE` - Path to a .py deployment module. defaults to \_\_deployment__.py.
* `--job-ref REF` - Pick this job when the selector matches multiple jobs.
* `--profile NAME` - Override require.profile and the workspace pinned profile.
* `--dry-run` - Resolve the job and print the entry point without launching
* `-c KEY=VALUE, --config KEY=VALUE` - Config key=value pairs passed to the job (repeatable)
* `--start ISO` - Override interval start (iso 8601). naive values use the job's timezone.
* `--end ISO` - Override interval end (iso 8601). defaults to now if --start is set.
* `--refresh` - Request a refresh run. honored unless the job declares refresh=block.

</details>

### `dlthub local serve`

Serve an interactive workspace job locally (notebook, dashboard, app).

**Usage**
```sh
dlthub local serve [-h] [--deployment FILE] [--job-ref REF] [--profile NAME]
    [--dry-run] [-c KEY=VALUE] [selector_or_job_ref]
```

**Description**

Serve one interactive job (marimo, Streamlit, FastMCP, ...). Same selector / `--job-ref` semantics as `dlthub local run`.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local`](#dlthub-local).

**Positional arguments**
* `selector_or_job_ref` - Job ref, trigger selector (tag:..., schedule:*), or a .py file to run as a script.

**Options**
* `-h, --help` - Show this help message and exit
* `--deployment FILE` - Path to a .py deployment module. defaults to \_\_deployment__.py.
* `--job-ref REF` - Pick this job when the selector matches multiple jobs.
* `--profile NAME` - Override require.profile and the workspace pinned profile.
* `--dry-run` - Resolve the job and print the entry point without launching
* `-c KEY=VALUE, --config KEY=VALUE` - Config key=value pairs passed to the job (repeatable)

</details>

### `dlthub local clean`

Clean local data for the current profile. Locally loaded data and pipelines working dirs are deleted by default. Remote destinations are not affected.

**Usage**
```sh
dlthub local clean [-h] [--skip-local-data-dir]
```

**Description**

Clean local data for the current profile. Locally loaded data and pipelines working dirs are deleted by default. Remote destinations are not affected.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local`](#dlthub-local).

**Options**
* `-h, --help` - Show this help message and exit
* `--skip-local-data-dir` - Does not delete locally loaded data but removes pipeline working dirs.

</details>

### `dlthub local profile`

Profile operations that affect only the local workspace.

**Usage**
```sh
dlthub local profile [-h] {use} ...
```

**Description**

Profile operations scoped to the local workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local`](#dlthub-local).

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`use`](#dlthub-local-profile-use) - Pin a profile in the local workspace so subsequent local commands use it by default

</details>

### `dlthub local profile use`

Pin a profile in the local workspace so subsequent local commands use it by default.

**Usage**
```sh
dlthub local profile use [-h] profile_name
```

**Description**

Pin a profile in the local workspace so subsequent local commands use it by default.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local profile`](#dlthub-local-profile).

**Positional arguments**
* `profile_name` - Profile name to pin

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub local schema`

Shows, converts and upgrades schemas.

**Usage**
```sh
dlthub local schema [-h] [--format {json,yaml,dbml,dot,mermaid}]
    [--remove-defaults] file
```

**Description**

Loads, validates and prints out a dlt schema from a yaml or json file.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local`](#dlthub-local).

**Positional arguments**
* `file` - Schema file name, in yaml or json format, will autodetect based on extension

**Options**
* `-h, --help` - Show this help message and exit
* `--format {json,yaml,dbml,dot,mermaid}` - Display schema in this format
* `--remove-defaults` - Does not show default hint values

</details>

### `dlthub local telemetry`

Shows telemetry status.

**Usage**
```sh
dlthub local telemetry [-h]
```

**Description**

Shows the current status of dlt telemetry. Learn more about telemetry and what we send in our telemetry docs.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local`](#dlthub-local).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub local pipeline`

Local pipeline operations (info, drop, sync, load-package, etc.).

**Usage**
```sh
dlthub local pipeline [-h] [--pipelines-dir PIPELINES_DIR]
    {list,run,info,show,failed-jobs,drop-pending-packages,sync,trace,schema,drop,load-package}
    ...
```

**Description**

Local pipeline operations (info, drop, sync, load-package, etc.).

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local`](#dlthub-local).

**Options**
* `-h, --help` - Show this help message and exit
* `--pipelines-dir PIPELINES_DIR` - Pipelines working directory

**Available subcommands**
* [`list`](#dlthub-local-pipeline-list) - List local pipelines
* [`run`](#dlthub-local-pipeline-run) - Run a job by pipeline name
* [`info`](#dlthub-local-pipeline-info) - Displays state of the pipeline, use -v or -vv for more info
* [`show`](#dlthub-local-pipeline-show) - Generates and launches workspace dashboard with the loading status and dataset explorer
* [`failed-jobs`](#dlthub-local-pipeline-failed-jobs) - Displays information on all the failed loads in all completed packages, failed jobs and associated error messages
* [`drop-pending-packages`](#dlthub-local-pipeline-drop-pending-packages) - Deletes all extracted and normalized packages including those that are partially loaded.
* [`sync`](#dlthub-local-pipeline-sync) - Drops the local state of the pipeline and resets all the schemas and restores it from destination. the destination state, data and schemas are left intact.
* [`trace`](#dlthub-local-pipeline-trace) - Displays last run trace, use -v or -vv for more info
* [`schema`](#dlthub-local-pipeline-schema) - Displays default schema
* [`drop`](#dlthub-local-pipeline-drop) - Selectively drop tables and reset state
* [`load-package`](#dlthub-local-pipeline-load-package) - Displays information on load package, use -v or -vv for more info

</details>

### `dlthub local pipeline list`

List local pipelines.

**Usage**
```sh
dlthub local pipeline list [-h]
```

**Description**

List pipelines in the working directory.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local pipeline`](#dlthub-local-pipeline).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub local pipeline run`

Run a job by pipeline name.

**Usage**
```sh
dlthub local pipeline run [-h] [--job-ref REF] [--profile NAME] [--refresh]
    [--dry-run] pipeline_name
```

**Description**

Run the job whose `deliver.pipeline_name` matches. Use --job-ref when multiple jobs target the same pipeline.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local pipeline`](#dlthub-local-pipeline).

**Positional arguments**
* `pipeline_name` - Pipeline name to match against `deliver.pipeline_name`

**Options**
* `-h, --help` - Show this help message and exit
* `--job-ref REF` - Narrow to this job when multiple jobs deliver to the same pipeline
* `--profile NAME` - Override require.profile and the workspace pinned profile.
* `--refresh` - Request a refresh run. honored unless the job declares refresh=block.
* `--dry-run` - Resolve the job and print the entry point without launching

</details>

### `dlthub local pipeline info`

Displays state of the pipeline, use -v or -vv for more info.

**Usage**
```sh
dlthub local pipeline info [-h] [pipeline_name]
```

**Description**

Displays the content of the working directory of the pipeline: dataset name, destination, list of
schemas, resources in schemas, list of completed and normalized load packages, and optionally a
pipeline state set by the resources during the extraction process.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local pipeline`](#dlthub-local-pipeline).

**Positional arguments**
* `pipeline_name` - Pipeline name

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub local pipeline show`

Generates and launches workspace dashboard with the loading status and dataset explorer.

**Usage**
```sh
dlthub local pipeline show [-h] [--edit] [pipeline_name]
```

**Description**

Launches the workspace dashboard with a comprehensive interface to inspect the pipeline state, schemas, and data in the destination.

This dashboard should be executed from the same folder from which you ran the pipeline script to be able access destination credentials.

If the --edit flag is used, will launch the editable version of the dashboard if it exists in the current directory, or create this version and launch it in edit mode.

Requires `marimo` to be installed in the current environment: `pip install marimo`.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local pipeline`](#dlthub-local-pipeline).

**Positional arguments**
* `pipeline_name` - Pipeline name

**Options**
* `-h, --help` - Show this help message and exit
* `--edit` - Creates editable version of workspace dashboard in current directory if it does not exist there yet and launches it in edit mode.

</details>

### `dlthub local pipeline failed-jobs`

Displays information on all the failed loads in all completed packages, failed jobs and associated error messages.

**Usage**
```sh
dlthub local pipeline failed-jobs [-h] [pipeline_name]
```

**Description**

This command scans all the load packages looking for failed jobs and then displays information on
files that got loaded and the failure message from the destination.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local pipeline`](#dlthub-local-pipeline).

**Positional arguments**
* `pipeline_name` - Pipeline name

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub local pipeline drop-pending-packages`

Deletes all extracted and normalized packages including those that are partially loaded.

**Usage**
```sh
dlthub local pipeline drop-pending-packages [-h] [pipeline_name]
```

**Description**

Removes all extracted and normalized packages in the pipeline's working dir.
`dlt` keeps extracted and normalized load packages in the pipeline working directory. When the `run` method is called, it will attempt to normalize and load
pending packages first. This command removes such packages. Note that **pipeline state** is not reverted to the state at which the deleted packages
were created. Using the `sync` sub-command is recommended if your destination supports state sync.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local pipeline`](#dlthub-local-pipeline).

**Positional arguments**
* `pipeline_name` - Pipeline name

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub local pipeline sync`

Drops the local state of the pipeline and resets all the schemas and restores it from destination. The destination state, data and schemas are left intact.

**Usage**
```sh
dlthub local pipeline sync [-h] [--destination DESTINATION] [--dataset-name
    DATASET_NAME] [pipeline_name]
```

**Description**

This command will remove the pipeline working directory with all pending packages, not synchronized
state changes, and schemas and retrieve the last synchronized data from the destination. If you drop
the dataset the pipeline is loading to, this command results in a complete reset of the pipeline state.

In case of a pipeline without a working directory, this command may be used to create one from the
destination. In order to do that, you need to pass the dataset name and destination name to the CLI
and provide the credentials to connect to the destination (i.e., in `.dlt/secrets.toml`) placed in the
folder where you run it.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local pipeline`](#dlthub-local-pipeline).

**Positional arguments**
* `pipeline_name` - Pipeline name

**Options**
* `-h, --help` - Show this help message and exit
* `--destination DESTINATION` - Sync from this destination when local pipeline state is missing.
* `--dataset-name DATASET_NAME` - Dataset name to sync from when local pipeline state is missing.

</details>

### `dlthub local pipeline trace`

Displays last run trace, use -v or -vv for more info.

**Usage**
```sh
dlthub local pipeline trace [-h] [pipeline_name]
```

**Description**

Displays the trace of the last pipeline run containing the start date of the run, elapsed time, and the
same information for all the steps (`extract`, `normalize`, and `load`). If any of the steps failed,
you'll see the message of the exceptions that caused that problem. Successful `load` and `run` steps
will display the load info instead.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local pipeline`](#dlthub-local-pipeline).

**Positional arguments**
* `pipeline_name` - Pipeline name

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub local pipeline schema`

Displays default schema.

**Usage**
```sh
dlthub local pipeline schema [-h] [--format {json,yaml,dbml,dot,mermaid}]
    [--remove-defaults] [pipeline_name]
```

**Description**

Displays the default schema for the selected pipeline.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local pipeline`](#dlthub-local-pipeline).

**Positional arguments**
* `pipeline_name` - Pipeline name

**Options**
* `-h, --help` - Show this help message and exit
* `--format {json,yaml,dbml,dot,mermaid}` - Display schema in this format
* `--remove-defaults` - Does not show default hint values

</details>

### `dlthub local pipeline drop`

Selectively drop tables and reset state.

**Usage**
```sh
dlthub local pipeline drop [-h] [--destination DESTINATION] [--dataset-name
    DATASET_NAME] [--drop-all] [--state-paths [STATE_PATHS ...]] [--schema
    SCHEMA_NAME] [--state-only] [pipeline_name] [resources ...]
```

**Description**

Selectively drop tables and reset state.

```sh
dlt pipeline <pipeline name> drop [resource_1] [resource_2]
```

Drops tables generated by selected resources and resets the state associated with them. Mainly used
to force a full refresh on selected tables. In the example below, we drop all tables generated by
the `repo_events` resource in the GitHub pipeline:

```sh
dlt pipeline github_events drop repo_events
```

`dlt` will inform you of the names of dropped tables and the resource state slots that will be
reset:

```text
About to drop the following data in dataset airflow_events_1 in destination dlt.destinations.duckdb:
Selected schema:: github_repo_events
Selected resource(s):: ['repo_events']
Table(s) to drop:: ['issues_event', 'fork_event', 'pull_request_event', 'pull_request_review_event', 'pull_request_review_comment_event', 'watch_event', 'issue_comment_event', 'push_event__payload__commits', 'push_event']
Resource(s) state to reset:: ['repo_events']
Source state path(s) to reset:: []
Do you want to apply these changes? [y/N]
```

As a result of the command above the following will happen:

1. All the indicated tables will be dropped in the destination. Note that `dlt` drops the nested
   tables as well.
2. All the indicated tables will be removed from the indicated schema.
3. The state for the resource `repo_events` was found and will be reset.
4. New schema and state will be stored in the destination.

The `drop` command accepts several advanced settings:

1. You can use regexes to select resources. Prepend the `re:` string to indicate a regex pattern. The example
   below will select all resources starting with `repo`:

```sh
dlt pipeline github_events drop "re:^repo"
```

2. You can drop all tables in the indicated schema:

```sh
dlt pipeline chess drop --drop-all
```

3. You can indicate additional state slots to reset by passing JsonPath to the source state. In the example
   below, we reset the `archives` slot in the source state:

```sh
dlt pipeline chess_pipeline drop --state-paths archives
```

This will select the `archives` key in the `chess` source.

```json
{
  "sources":{
    "chess": {
      "archives": [
        "https://api.chess.com/pub/player/magnuscarlsen/games/2022/05"
      ]
    }
  }
}
```

**This command is still experimental** and the interface will most probably change.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local pipeline`](#dlthub-local-pipeline).

**Positional arguments**
* `pipeline_name` - Pipeline name
* `resources` - One or more resources to drop. can be exact resource name(s) or regex pattern(s). regex patterns must start with re:

**Options**
* `-h, --help` - Show this help message and exit
* `--destination DESTINATION` - Sync from this destination when local pipeline state is missing.
* `--dataset-name DATASET_NAME` - Dataset name to sync from when local pipeline state is missing.
* `--drop-all` - Drop all resources found in schema. supersedes [resources] argument.
* `--state-paths [STATE_PATHS ...]` - State keys or json paths to drop
* `--schema SCHEMA_NAME` - Schema name to drop from (if other than default schema).
* `--state-only` - Only wipe state for matching resources without dropping tables.

</details>

### `dlthub local pipeline load-package`

Displays information on load package, use -v or -vv for more info.

**Usage**
```sh
dlthub local pipeline load-package [-h] [pipeline_name] [load-id]
```

**Description**

Shows information on a load package with a given `load_id`. The `load_id` parameter defaults to the
most recent package. Package information includes its state (`COMPLETED/PROCESSED`) and list of all
jobs in a package with their statuses, file sizes, types, and in case of failed jobs—the error
messages from the destination. With the verbose flag set (`-v`), you can also see the
list of all tables and columns created at the destination during the loading of that package.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub local pipeline`](#dlthub-local-pipeline).

**Positional arguments**
* `pipeline_name` - Pipeline name
* `load-id` - Load id of completed or normalized package. defaults to the most recent package.

**Options**
* `-h, --help` - Show this help message and exit

</details>

## `dlthub init`

Initialize a new dlthub workspace.

**Usage**
```sh
dlthub init [-h] [--name NAME] [--force] [--dependencies
    {auto,pyproject,requirements}] [--dry-run]
```

**Description**

Creates local workspace files: config, secrets, gitignore and Python pyproject/requirements.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub`](#dlthub).

**Options**
* `-h, --help` - Show this help message and exit
* `--name NAME` - Workspace name (defaults to current directory basename).
* `--force` - Overwrite existing pyproject.toml/requirements.txt/.gitignore/config.toml.
* `--dependencies {auto,pyproject,requirements}` - Dependency file to scaffold. `auto` (default) uses pyproject.toml when uv is on path and requirements.txt otherwise. `pyproject` / `requirements` force the choice.
* `--dry-run` - Print the file plan without writing anything.

</details>

## `dlthub ai`

Use AI-powered development tools and utilities.

**Usage**
```sh
dlthub ai [-h] {status,init,secrets,toolkit,mcp} ...
```

**Description**

Configure your LLM-enabled IDE and MCP server.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub`](#dlthub).

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`status`](#dlthub-ai-status) - Show ai setup status: dlt version, agent, toolkits, readiness checks
* [`init`](#dlthub-ai-init) - Install initial ai rules and skills for your ai coding agent
* [`secrets`](#dlthub-ai-secrets) - Manage secrets files used by dlt
* [`toolkit`](#dlthub-ai-toolkit) - Manage ai toolkit plugins (list, info, install)
* [`mcp`](#dlthub-ai-mcp) - Run or install the dlt mcp server

</details>

### `dlthub ai status`

Show AI setup status: dlt version, agent, toolkits, readiness checks.

**Usage**
```sh
dlthub ai status [-h]
```

**Description**

Show AI setup status: dlt version, agent, toolkits, readiness checks.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub ai`](#dlthub-ai).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub ai init`

Install initial AI rules and skills for your AI coding agent.

**Usage**
```sh
dlthub ai init [-h] [--agent {claude,cursor,codex}] [--location LOCATION]
    [--branch BRANCH] [--overwrite]
```

**Description**

Install initial AI rules and skills for your AI coding agent.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub ai`](#dlthub-ai).

**Options**
* `-h, --help` - Show this help message and exit
* `--agent {claude,cursor,codex}` - Ai coding agent to install for. auto-detected if omitted.
* `--location LOCATION` - Advanced. git url or local path to ai workbench repository.
* `--branch BRANCH` - Advanced. git branch to fetch from.
* `--overwrite` - Overwrite existing files instead of skipping them.

</details>

### `dlthub ai secrets`

Manage secrets files used by dlt.

**Usage**
```sh
dlthub ai secrets [-h] {list,view-redacted,update-fragment} ...
```

**Description**

List, view (redacted), or update secret files used by dlt providers.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub ai`](#dlthub-ai).

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`list`](#dlthub-ai-secrets-list) - List secret file locations from providers
* [`view-redacted`](#dlthub-ai-secrets-view-redacted) - Print secrets toml with all values replaced by '***'
* [`update-fragment`](#dlthub-ai-secrets-update-fragment) - Merge a toml fragment into the secrets file

</details>

### `dlthub ai secrets list`

List secret file locations from providers.

**Usage**
```sh
dlthub ai secrets list [-h]
```

**Description**

List secret file locations from providers.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub ai secrets`](#dlthub-ai-secrets).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub ai secrets view-redacted`

Print secrets TOML with all values replaced by '***'.

**Usage**
```sh
dlthub ai secrets view-redacted [-h] [--path PATH]
```

**Description**

Without --path, shows the unified view merged from all project secret files. With --path, shows that exact file.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub ai secrets`](#dlthub-ai-secrets).

**Options**
* `-h, --help` - Show this help message and exit
* `--path PATH` - Show this exact file instead of the unified provider view

</details>

### `dlthub ai secrets update-fragment`

Merge a TOML fragment into the secrets file.

**Usage**
```sh
dlthub ai secrets update-fragment [-h] --path PATH [fragment]
```

**Description**

Merge a TOML fragment into the secrets file.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub ai secrets`](#dlthub-ai-secrets).

**Positional arguments**
* `fragment` - Toml fragment string to merge; reads from stdin if omitted

**Options**
* `-h, --help` - Show this help message and exit
* `--path PATH` - Path to the secrets toml file to write to

</details>

### `dlthub ai toolkit`

Manage AI toolkit plugins (list, info, install).

**Usage**
```sh
dlthub ai toolkit [-h] {list,info,install} ...
```

**Description**

Manage AI toolkit plugins (list, info, install).

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub ai`](#dlthub-ai).

**Positional arguments**
* `list` - List available toolkits
* `info` - Show toolkit contents and components
* `install` - Install toolkit components into project

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlthub ai toolkit list`

List available toolkits.

**Usage**
```sh
dlthub ai toolkit list [-h] [--location LOCATION] [--branch BRANCH]
```

**Description**

List available toolkits.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub ai toolkit`](#dlthub-ai-toolkit).

**Options**
* `-h, --help` - Show this help message and exit
* `--location LOCATION` - Advanced. git url or local path to toolkit repository.
* `--branch BRANCH` - Advanced. git branch to fetch toolkit from.

</details>

### `dlthub ai toolkit info`

Show toolkit contents and components.

**Usage**
```sh
dlthub ai toolkit info [-h] [--location LOCATION] [--branch BRANCH] name
```

**Description**

Show toolkit contents and components.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub ai toolkit`](#dlthub-ai-toolkit).

**Positional arguments**
* `name` - Toolkit name

**Options**
* `-h, --help` - Show this help message and exit
* `--location LOCATION` - Advanced. git url or local path to toolkit repository.
* `--branch BRANCH` - Advanced. git branch to fetch toolkit from.

</details>

### `dlthub ai toolkit install`

Install toolkit components into project.

**Usage**
```sh
dlthub ai toolkit install [-h] [--location LOCATION] [--branch BRANCH] [--agent
    {claude,cursor,codex}] [--overwrite] [--strict] name
```

**Description**

Install toolkit components into project.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub ai toolkit`](#dlthub-ai-toolkit).

**Positional arguments**
* `name` - Toolkit name

**Options**
* `-h, --help` - Show this help message and exit
* `--location LOCATION` - Advanced. git url or local path to toolkit repository.
* `--branch BRANCH` - Advanced. git branch to fetch toolkit from.
* `--agent {claude,cursor,codex}` - Ai coding agent to install for. auto-detected if omitted.
* `--overwrite` - Overwrite existing files instead of skipping them.
* `--strict` - Fail on validation warnings (invalid frontmatter, etc.).

</details>

### `dlthub ai mcp`

Run or install the dlt MCP server.

**Usage**
```sh
dlthub ai mcp [-h] [--stdio] [--sse] [--port PORT] [--features [FEATURES ...]]
    {run,install} ...
```

**Description**

Run or install the dlt MCP server.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub ai`](#dlthub-ai).

**Positional arguments**
* `run` - Start the mcp server (default)
* `install` - Install mcp server config into the current project

**Options**
* `-h, --help` - Show this help message and exit
* `--stdio` - Use stdio transport mode
* `--sse` - Use legacy sse transport instead of streamable-http
* `--port PORT` - Port for the mcp server (default: 8000)
* `--features [FEATURES ...]` - Mcp features to enable/disable. default: context, pipeline, secrets, toolkit, workspace. use +name to add, -name to remove (e.g. --features=-secrets,+context)

</details>

### `dlthub ai mcp run`

Start the MCP server (default).

**Usage**
```sh
dlthub ai mcp run [-h] [--stdio] [--sse] [--port PORT] [--features [FEATURES
    ...]]
```

**Description**

Start the MCP server (default).

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub ai mcp`](#dlthub-ai-mcp).

**Options**
* `-h, --help` - Show this help message and exit
* `--stdio` - Use stdio transport mode
* `--sse` - Use legacy sse transport instead of streamable-http
* `--port PORT` - Port for the mcp server (default: 8000)
* `--features [FEATURES ...]` - Mcp features to enable/disable. default: context, pipeline, secrets, toolkit, workspace. use +name to add, -name to remove (e.g. --features=-secrets,+context)

</details>

### `dlthub ai mcp install`

Install MCP server config into the current project.

**Usage**
```sh
dlthub ai mcp install [-h] [--agent {claude,cursor,codex}] [--features [FEATURES
    ...]] [--name NAME] [--overwrite]
```

**Description**

Install MCP server config into the current project.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlthub ai mcp`](#dlthub-ai-mcp).

**Options**
* `-h, --help` - Show this help message and exit
* `--agent {claude,cursor,codex}` - Ai coding agent to install for. auto-detected if omitted.
* `--features [FEATURES ...]` - Mcp feature sets to include in the server config
* `--name NAME` - Server name in the mcp config (default: dlt-workspace)
* `--overwrite` - Overwrite existing server config instead of skipping.

</details>

