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

## `dlt`

Creates, adds, inspects and deploys dlt pipelines. Further help is available at https://dlthub.com/docs/reference/command-line-interface.

**Usage**
```sh
dlt [-h] [--version] [--disable-telemetry] [--enable-telemetry]
    [--non-interactive] [--debug] [--no-pwd]
    {workspace,telemetry,schema,profile,pipeline,init,render-docs,deploy,dashboard,ai,license,runtime}
    ...
```

<details>

<summary>Show Arguments and Options</summary>

**Options**
* `-h, --help` - Show this help message and exit
* `--version` - Show program's version number and exit
* `--disable-telemetry` - Disables telemetry before command is executed
* `--enable-telemetry` - Enables telemetry before command is executed
* `--non-interactive` - Non interactive mode. default choices are automatically made for confirmations and prompts.
* `--debug` - Displays full stack traces on exceptions. useful for debugging if the output is not clear enough.
* `--no-pwd` - Do not add current working directory to sys.path. by default $pwd is added to reproduce python behavior when running scripts.

**Available subcommands**
* [`workspace`](#dlt-workspace) - Manage current workspace
* [`profile`](#dlt-profile) - Manage workspace built-in profiles
* [`license`](#dlt-license) - View dlthub license status
* [`runtime`](#dlt-runtime) - Connect to dlthub runtime and run your code remotely

</details>

## `dlt workspace`

Manage current Workspace.

**Usage**
```sh
dlt workspace [-h] [--verbose] {clean,info,mcp,show} ...
```

**Description**

Commands to get info, cleanup local files and launch Workspace MCP. Run without command get
workspace info.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Options**
* `-h, --help` - Show this help message and exit
* `--verbose, -v` - Provides more information for certain commands.

**Available subcommands**
* [`clean`](#dlt-workspace-clean) - Cleans local data for the selected profile. locally loaded data will be deleted. pipelines working directories are also deleted by default. data in remote destinations is not affected.
* [`info`](#dlt-workspace-info) - Displays workspace info.
* [`mcp`](#dlt-workspace-mcp) - Launch dlt mcp server in current python environment and workspace in sse transport mode by default.
* [`show`](#dlt-workspace-show) - Shows workspace dashboard for the pipelines and data in this workspace.

</details>

### `dlt workspace clean`

Cleans local data for the selected profile. Locally loaded data will be deleted. Pipelines working directories are also deleted by default. Data in remote destinations is not affected.

**Usage**
```sh
dlt workspace clean [-h] [--skip-data-dir]
```

**Description**

Cleans local data for the selected profile. Locally loaded data will be deleted. Pipelines working directories are also deleted by default. Data in remote destinations is not affected.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt workspace`](#dlt-workspace).

**Options**
* `-h, --help` - Show this help message and exit
* `--skip-data-dir` - Do not delete pipelines working dir.

</details>

### `dlt workspace info`

Displays workspace info.

**Usage**
```sh
dlt workspace info [-h]
```

**Description**

Displays workspace info.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt workspace`](#dlt-workspace).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt workspace mcp`

Launch dlt MCP server in current Python environment and Workspace in SSE transport mode by default.

**Usage**
```sh
dlt workspace mcp [-h] [--stdio] [--port PORT]
```

**Description**

This MCP allows to attach to any pipeline that was previously ran in this workspace and then facilitates schema and data exploration in the pipeline's dataset.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt workspace`](#dlt-workspace).

**Options**
* `-h, --help` - Show this help message and exit
* `--stdio` - Use stdio transport mode
* `--port PORT` - Sse port to use (default: 43654)

</details>

### `dlt workspace show`

Shows Workspace Dashboard for the pipelines and data in this workspace.

**Usage**
```sh
dlt workspace show [-h] [--edit]
```

**Description**

Shows Workspace Dashboard for the pipelines and data in this workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt workspace`](#dlt-workspace).

**Options**
* `-h, --help` - Show this help message and exit
* `--edit` - Eject dashboard and start editable version

</details>

## `dlt profile`

Manage Workspace built-in profiles.

**Usage**
```sh
dlt profile [-h] [profile_name] {info,list,pin} ...
```

**Description**

Commands to list and pin profiles
Run without arguments to list all profiles, the default profile and the
pinned profile in current project.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Positional arguments**
* `profile_name` - Name of the profile

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`info`](#dlt-profile-info) - Show information about the current profile.
* [`list`](#dlt-profile-list) - Show list of built-in profiles.
* [`pin`](#dlt-profile-pin) - Pin a profile to the workspace.

</details>

### `dlt profile info`

Show information about the current profile.

**Usage**
```sh
dlt profile [profile_name] info [-h]
```

**Description**

Show information about the current profile.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt profile`](#dlt-profile).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt profile list`

Show list of built-in profiles.

**Usage**
```sh
dlt profile [profile_name] list [-h]
```

**Description**

Show list of built-in profiles.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt profile`](#dlt-profile).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt profile pin`

Pin a profile to the Workspace.

**Usage**
```sh
dlt profile [profile_name] pin [-h]
```

**Description**

Pin a profile to the Workspace, this will be the new default profile while it is pinned.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt profile`](#dlt-profile).

**Options**
* `-h, --help` - Show this help message and exit

</details>

## `dlt license`

View dlthub license status.

**Usage**
```sh
dlt license [-h] {info,scopes,issue} ...
```

**Description**

View dlthub license status.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`info`](#dlt-license-info) - Show the installed license
* [`scopes`](#dlt-license-scopes) - Show available scopes
* [`issue`](#dlt-license-issue) - Issues a self-signed trial license that may be used for development, testing and for ci ops.

</details>

### `dlt license info`

Show the installed license.

**Usage**
```sh
dlt license info [-h]
```

**Description**

Show the installed license.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt license`](#dlt-license).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt license scopes`

Show available scopes.

**Usage**
```sh
dlt license scopes [-h]
```

**Description**

Show available scopes.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt license`](#dlt-license).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt license issue`

Issues a self-signed trial license that may be used for development, testing and for ci ops.

**Usage**
```sh
dlt license issue [-h] scope
```

**Description**

Issue a new self-signed trial license.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt license`](#dlt-license).

**Positional arguments**
* `scope` - Scope of the license, a comma separated list of the scopes: ['dlthub.dbt_generator', 'dlthub.sources.mssql', 'dlthub.project', 'dlthub.transformation', 'dlthub.data_quality', 'dlthub.destinations.iceberg', 'dlthub.destinations.snowflake_plus', 'dlthub.runner']

**Options**
* `-h, --help` - Show this help message and exit

</details>

## `dlt runtime`

Connect to dltHub Runtime and run your code remotely.

**Usage**
```sh
dlt runtime [-h]
    {login,logout,launch,serve,publish,schedule,logs,cancel,dashboard,deploy,info,deployment,job,jobs,job-run,job-runs,configuration}
    ...
```

**Description**

Allows to connect to the dltHub Runtime, deploy and run local workspaces there. Requires dltHub license.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`login`](#dlt-runtime-login) - Login to dlthub runtime using github oauth and connect current workspace to the remote one
* [`logout`](#dlt-runtime-logout) - Logout from dlthub runtime
* [`launch`](#dlt-runtime-launch) - Deploy code/config and run a script (follow status and logs by default)
* [`serve`](#dlt-runtime-serve) - Deploy and serve an interactive notebook/app (read-only) and follow until ready
* [`publish`](#dlt-runtime-publish) - Generate or revoke a public link for an interactive notebook/app
* [`schedule`](#dlt-runtime-schedule) - Deploy and schedule a script with a cron timetable, or cancel the scheduled script from future runs
* [`logs`](#dlt-runtime-logs) - Show logs for latest or selected job run
* [`cancel`](#dlt-runtime-cancel) - Cancel latest or selected job run
* [`dashboard`](#dlt-runtime-dashboard) - Open the runtime dashboard for this workspace
* [`deploy`](#dlt-runtime-deploy) - Sync code and configuration to runtime without running anything
* [`info`](#dlt-runtime-info) - Show overview of current runtime workspace
* [`deployment`](#dlt-runtime-deployment) - Manipulate deployments in workspace
* [`job`](#dlt-runtime-job) - List, create and inspect jobs
* [`jobs`](#dlt-runtime-jobs) - List, create and inspect jobs
* [`job-run`](#dlt-runtime-job-run) - List, create and inspect job runs
* [`job-runs`](#dlt-runtime-job-runs) - List, create and inspect job runs
* [`configuration`](#dlt-runtime-configuration) - Manipulate configurations in workspace

</details>

### `dlt runtime login`

Login to dltHub Runtime using Github OAuth and connect current workspace to the remote one.

**Usage**
```sh
dlt runtime login [-h]
```

**Description**

Login to dltHub Runtime using Github OAuth.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime logout`

Logout from dltHub Runtime.

**Usage**
```sh
dlt runtime logout [-h]
```

**Description**

Logout from dltHub Runtime.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime launch`

Deploy code/config and run a script (follow status and logs by default).

**Usage**
```sh
dlt runtime launch [-h] [-d] script_path
```

**Description**

Deploy current workspace and run a batch script remotely.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `script_path` - Local path to the script

**Options**
* `-h, --help` - Show this help message and exit
* `-d, --detach` - Do not follow status changes and logs after starting

</details>

### `dlt runtime serve`

Deploy and serve an interactive notebook/app (read-only) and follow until ready.

**Usage**
```sh
dlt runtime serve [-h] script_path
```

**Description**

Deploy current workspace and run a notebook as a read-only web app.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `script_path` - Local path to the notebook/app

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime publish`

Generate or revoke a public link for an interactive notebook/app.

**Usage**
```sh
dlt runtime publish [-h] [--cancel] script_path
```

**Description**

Generate a public link for a notebook/app, or revoke it with --cancel.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `script_path` - Local path to the notebook/app

**Options**
* `-h, --help` - Show this help message and exit
* `--cancel` - Revoke the public link for the notebook/app

</details>

### `dlt runtime schedule`

Deploy and schedule a script with a cron timetable, or cancel the scheduled script from future runs.

**Usage**
```sh
dlt runtime schedule [-h] [--current] script_path cron_expr_or_cancel
```

**Description**

Schedule a batch script to run on a cron timetable, or cancel the scheduled script from future runs.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `script_path` - Local path to the script
* `cron_expr_or_cancel` - Either a cron schedule string if you want to schedule the script, or the literal 'cancel' command if you want to cancel it

**Options**
* `-h, --help` - Show this help message and exit
* `--current` - When cancelling the schedule, also cancel the currently running instance if any

</details>

### `dlt runtime logs`

Show logs for latest or selected job run.

**Usage**
```sh
dlt runtime logs [-h] [-f] script_path_or_job_name [run_number]
```

**Description**

Show logs for the latest run of a job or a specific run number.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `script_path_or_job_name` - Local path or job name
* `run_number` - Run number (optional)

**Options**
* `-h, --help` - Show this help message and exit
* `-f, --follow` - Follow the logs of the run in tailing mode

</details>

### `dlt runtime cancel`

Cancel latest or selected job run.

**Usage**
```sh
dlt runtime cancel [-h] script_path_or_job_name [run_number]
```

**Description**

Cancel the latest run of a job or a specific run number.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `script_path_or_job_name` - Local path or job name
* `run_number` - Run number (optional)

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime dashboard`

Open the Runtime dashboard for this workspace.

**Usage**
```sh
dlt runtime dashboard [-h]
```

**Description**

Open link to the Runtime dashboard for current remote workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime deploy`

Sync code and configuration to Runtime without running anything.

**Usage**
```sh
dlt runtime deploy [-h]
```

**Description**

Upload deployment and configuration if changed.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime info`

Show overview of current Runtime workspace.

**Usage**
```sh
dlt runtime info [-h]
```

**Description**

Show workspace id and summary of deployments, configurations and jobs.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime deployment`

Manipulate deployments in workspace.

**Usage**
```sh
dlt runtime deployment [-h] [deployment_version_no] {list,info,sync} ...
```

**Description**

Manipulate deployments in workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `deployment_version_no` - Deployment version number. only used in the `info` subcommand

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`list`](#dlt-runtime-deployment-list) - List all deployments in workspace
* [`info`](#dlt-runtime-deployment-info) - Get detailed information about a deployment
* [`sync`](#dlt-runtime-deployment-sync) - Create new deployment if local workspace content changed

</details>

### `dlt runtime deployment list`

List all deployments in workspace.

**Usage**
```sh
dlt runtime deployment [deployment_version_no] list [-h]
```

**Description**

List all deployments in workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime deployment`](#dlt-runtime-deployment).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime deployment info`

Get detailed information about a deployment.

**Usage**
```sh
dlt runtime deployment [deployment_version_no] info [-h]
```

**Description**

Get detailed information about a deployment.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime deployment`](#dlt-runtime-deployment).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime deployment sync`

Create new deployment if local workspace content changed.

**Usage**
```sh
dlt runtime deployment [deployment_version_no] sync [-h]
```

**Description**

Create new deployment if local workspace content changed.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime deployment`](#dlt-runtime-deployment).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime job`

List, create and inspect jobs.

**Usage**
```sh
dlt runtime job [-h] [script_path_or_job_name] {list,info,create} ...
```

**Description**

List and manipulate jobs registered in the workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `script_path_or_job_name` - Local script path or job name. required for all commands except `list`

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`list`](#dlt-runtime-job-list) - List the jobs registered in the workspace
* [`info`](#dlt-runtime-job-info) - Show job info
* [`create`](#dlt-runtime-job-create) - Create a job without running it

</details>

### `dlt runtime job list`

List the jobs registered in the workspace.

**Usage**
```sh
dlt runtime job [script_path_or_job_name] list [-h]
```

**Description**

List the jobs registered in the workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime job`](#dlt-runtime-job).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime job info`

Show job info.

**Usage**
```sh
dlt runtime job [script_path_or_job_name] info [-h]
```

**Description**

Display detailed information about the job.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime job`](#dlt-runtime-job).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime job create`

Create a job without running it.

**Usage**
```sh
dlt runtime job [script_path_or_job_name] create [-h] [--name [NAME]]
    [--schedule [SCHEDULE]] [--interactive] [--description [DESCRIPTION]]
```

**Description**

Manually create the job.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime job`](#dlt-runtime-job).

**Options**
* `-h, --help` - Show this help message and exit
* `--name [NAME]` - Job name to create
* `--schedule [SCHEDULE]` - Cron schedule for the job if it's a scheduled one
* `--interactive` - Run the job interactively, e.g. for a notebook
* `--description [DESCRIPTION]` - Job description

</details>

### `dlt runtime jobs`

List, create and inspect jobs.

**Usage**
```sh
dlt runtime jobs [-h] [script_path_or_job_name] {list,info,create} ...
```

**Description**

List and manipulate jobs registered in the workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `script_path_or_job_name` - Local script path or job name. required for all commands except `list`

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`list`](#dlt-runtime-jobs-list) - List the jobs registered in the workspace
* [`info`](#dlt-runtime-jobs-info) - Show job info
* [`create`](#dlt-runtime-jobs-create) - Create a job without running it

</details>

### `dlt runtime jobs list`

List the jobs registered in the workspace.

**Usage**
```sh
dlt runtime jobs [script_path_or_job_name] list [-h]
```

**Description**

List the jobs registered in the workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime jobs`](#dlt-runtime-jobs).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime jobs info`

Show job info.

**Usage**
```sh
dlt runtime jobs [script_path_or_job_name] info [-h]
```

**Description**

Display detailed information about the job.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime jobs`](#dlt-runtime-jobs).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime jobs create`

Create a job without running it.

**Usage**
```sh
dlt runtime jobs [script_path_or_job_name] create [-h] [--name [NAME]]
    [--schedule [SCHEDULE]] [--interactive] [--description [DESCRIPTION]]
```

**Description**

Manually create the job.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime jobs`](#dlt-runtime-jobs).

**Options**
* `-h, --help` - Show this help message and exit
* `--name [NAME]` - Job name to create
* `--schedule [SCHEDULE]` - Cron schedule for the job if it's a scheduled one
* `--interactive` - Run the job interactively, e.g. for a notebook
* `--description [DESCRIPTION]` - Job description

</details>

### `dlt runtime job-run`

List, create and inspect job runs.

**Usage**
```sh
dlt runtime job-run [-h] [script_path_or_job_name] [run_number]
    {list,info,create,logs,cancel} ...
```

**Description**

List and manipulate job runs registered in the workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `script_path_or_job_name` - Local script path or job name. required for all commands except `list`
* `run_number` - Run number. used in all commands except `list` and `create` as optional argument. if not specified, the latest run of given script be used.

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`list`](#dlt-runtime-job-run-list) - List the job runs registered in the workspace
* [`info`](#dlt-runtime-job-run-info) - Show job run info
* [`create`](#dlt-runtime-job-run-create) - Create a job run without running it
* [`logs`](#dlt-runtime-job-run-logs) - Show logs for the latest or selected job run
* [`cancel`](#dlt-runtime-job-run-cancel) - Cancel the latest or selected job run

</details>

### `dlt runtime job-run list`

List the job runs registered in the workspace.

**Usage**
```sh
dlt runtime job-run [script_path_or_job_name] [run_number] list [-h]
```

**Description**

List the job runs registered in the workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime job-run`](#dlt-runtime-job-run).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime job-run info`

Show job run info.

**Usage**
```sh
dlt runtime job-run [script_path_or_job_name] [run_number] info [-h]
```

**Description**

Display detailed information about the job run.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime job-run`](#dlt-runtime-job-run).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime job-run create`

Create a job run without running it.

**Usage**
```sh
dlt runtime job-run [script_path_or_job_name] [run_number] create [-h]
```

**Description**

Manually create the job run.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime job-run`](#dlt-runtime-job-run).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime job-run logs`

Show logs for the latest or selected job run.

**Usage**
```sh
dlt runtime job-run [script_path_or_job_name] [run_number] logs [-h] [-f]
```

**Description**

Show logs for the latest or selected job run. Use --follow to follow the logs in tailing mode.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime job-run`](#dlt-runtime-job-run).

**Options**
* `-h, --help` - Show this help message and exit
* `-f, --follow` - Follow the logs of the run in tailing mode

</details>

### `dlt runtime job-run cancel`

Cancel the latest or selected job run.

**Usage**
```sh
dlt runtime job-run [script_path_or_job_name] [run_number] cancel [-h]
```

**Description**

Cancel the latest or selected job run.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime job-run`](#dlt-runtime-job-run).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime job-runs`

List, create and inspect job runs.

**Usage**
```sh
dlt runtime job-runs [-h] [script_path_or_job_name] [run_number]
    {list,info,create,logs,cancel} ...
```

**Description**

List and manipulate job runs registered in the workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `script_path_or_job_name` - Local script path or job name. required for all commands except `list`
* `run_number` - Run number. used in all commands except `list` and `create` as optional argument. if not specified, the latest run of given script be used.

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`list`](#dlt-runtime-job-runs-list) - List the job runs registered in the workspace
* [`info`](#dlt-runtime-job-runs-info) - Show job run info
* [`create`](#dlt-runtime-job-runs-create) - Create a job run without running it
* [`logs`](#dlt-runtime-job-runs-logs) - Show logs for the latest or selected job run
* [`cancel`](#dlt-runtime-job-runs-cancel) - Cancel the latest or selected job run

</details>

### `dlt runtime job-runs list`

List the job runs registered in the workspace.

**Usage**
```sh
dlt runtime job-runs [script_path_or_job_name] [run_number] list [-h]
```

**Description**

List the job runs registered in the workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime job-runs`](#dlt-runtime-job-runs).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime job-runs info`

Show job run info.

**Usage**
```sh
dlt runtime job-runs [script_path_or_job_name] [run_number] info [-h]
```

**Description**

Display detailed information about the job run.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime job-runs`](#dlt-runtime-job-runs).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime job-runs create`

Create a job run without running it.

**Usage**
```sh
dlt runtime job-runs [script_path_or_job_name] [run_number] create [-h]
```

**Description**

Manually create the job run.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime job-runs`](#dlt-runtime-job-runs).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime job-runs logs`

Show logs for the latest or selected job run.

**Usage**
```sh
dlt runtime job-runs [script_path_or_job_name] [run_number] logs [-h] [-f]
```

**Description**

Show logs for the latest or selected job run. Use --follow to follow the logs in tailing mode.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime job-runs`](#dlt-runtime-job-runs).

**Options**
* `-h, --help` - Show this help message and exit
* `-f, --follow` - Follow the logs of the run in tailing mode

</details>

### `dlt runtime job-runs cancel`

Cancel the latest or selected job run.

**Usage**
```sh
dlt runtime job-runs [script_path_or_job_name] [run_number] cancel [-h]
```

**Description**

Cancel the latest or selected job run.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime job-runs`](#dlt-runtime-job-runs).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime configuration`

Manipulate configurations in workspace.

**Usage**
```sh
dlt runtime configuration [-h] [configuration_version_no] {list,info,sync} ...
```

**Description**

Manipulate configurations in workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `configuration_version_no` - Configuration version number. only used in the `info` subcommand

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`list`](#dlt-runtime-configuration-list) - List all configuration versions
* [`info`](#dlt-runtime-configuration-info) - Get detailed information about a configuration
* [`sync`](#dlt-runtime-configuration-sync) - Create new configuration if local config content changed

</details>

### `dlt runtime configuration list`

List all configuration versions.

**Usage**
```sh
dlt runtime configuration [configuration_version_no] list [-h]
```

**Description**

List all configuration versions.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime configuration`](#dlt-runtime-configuration).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime configuration info`

Get detailed information about a configuration.

**Usage**
```sh
dlt runtime configuration [configuration_version_no] info [-h]
```

**Description**

Get detailed information about a configuration.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime configuration`](#dlt-runtime-configuration).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime configuration sync`

Create new configuration if local config content changed.

**Usage**
```sh
dlt runtime configuration [configuration_version_no] sync [-h]
```

**Description**

Create new configuration if local config content changed.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime configuration`](#dlt-runtime-configuration).

**Options**
* `-h, --help` - Show this help message and exit

</details>

