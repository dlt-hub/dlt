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
    {workspace,telemetry,schema,profile,pipeline,init,deploy,dashboard,ai,runtime,license}
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
* [`runtime`](#dlt-runtime) - Connect to dlthub runtime and run your code remotely
* [`license`](#dlt-license) - View dlthub license status

</details>

## `dlt workspace`

Manage current Workspace.

**Usage**
```sh
dlt workspace [-h] [--verbose] {clean,info,show,run} ...
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
* [`show`](#dlt-workspace-show) - Shows workspace dashboard for the pipelines and data in this workspace.
* [`run`](#dlt-workspace-run) - Run a single workspace job locally

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

### `dlt workspace run`

Run a single workspace job locally.

**Usage**
```sh
dlt workspace run [-h] [--file FILE] [--profile NAME] [--start ISO] [--end ISO]
    [--dry-run] [-v] [-c KEY=VALUE] [--refresh] [selector_or_job_ref]
```

**Description**

Run a single job from a deployment module locally. Loads the manifest, matches exactly one job by selector or job reference, builds a runtime entry point, and spawns the launcher subprocess. Freshness checks are skipped — use runtime for scheduled execution.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt workspace`](#dlt-workspace).

**Positional arguments**
* `selector_or_job_ref` - Job reference (backfill, batch.backfill), trigger selector (tag:backfill, schedule:*), or a .py file path (auto-promoted to --file). if omitted, the job's default trigger is used.

**Options**
* `-h, --help` - Show this help message and exit
* `--file FILE, -f FILE` - Path to a .py deployment module. if omitted, loads the default '__deployment__' module from the workspace.
* `--profile NAME` - Override require.profile and the workspace pinned profile.
* `--start ISO` - Override interval start (iso 8601). naive values use the job's timezone.
* `--end ISO` - Override interval end (iso 8601). defaults to now if --start is set.
* `--dry-run` - Resolve the job and print the entry point without launching
* `-v, --verbose` - Print the resolved entry point before running
* `-c KEY=VALUE, --config KEY=VALUE` - Config key=value pairs passed to the job (repeatable)
* `--refresh` - Request a refresh run. respects tjobdefinition.refresh: `always` forces refresh regardless, `block` ignores the flag with a warning (run proceeds), `auto` honors it.

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

## `dlt runtime`

Connect to dltHub Runtime and run your code remotely.

**Usage**
```sh
dlt runtime [-h] [--help-all] [--timestamps]
    {login,logout,launch,serve,publish,unpublish,trigger,run-pipeline,logs,cancel,dashboard,deploy,info,deployment,job,jobs,job-run,job-runs,configuration,workspace}
    ...
```

**Description**

Allows you to connect to the dltHub Runtime, deploy and run local workspaces there. Requires dltHub license.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Options**
* `-h, --help` - Show this help message and exit
* `--help-all` - Show all commands including subcommands
* `--timestamps` - Show exact iso timestamps and precise durations (e.g. 1.291 s) instead of humanized relative times.

**Available subcommands**
* [`login`](#dlt-runtime-login) - Log in to dlthub runtime and connect the current workspace to the remote one
* [`logout`](#dlt-runtime-logout) - Log out from dlthub runtime
* [`launch`](#dlt-runtime-launch) - Deploy code/config and run a script
* [`serve`](#dlt-runtime-serve) - Deploy and serve an interactive notebook/app (read-only) and follow until ready
* [`publish`](#dlt-runtime-publish) - Generate or revoke a public link for an interactive notebook/app
* [`unpublish`](#dlt-runtime-unpublish) - Revoke the public link for an interactive notebook/app
* [`trigger`](#dlt-runtime-trigger) - Trigger jobs matching selectors (does not sync or deploy)
* [`run-pipeline`](#dlt-runtime-run-pipeline) - Run a job by pipeline name
* [`logs`](#dlt-runtime-logs) - Show logs for latest or selected job run (shortcut for 'job-run logs')
* [`cancel`](#dlt-runtime-cancel) - Cancel active runs for matching jobs
* [`dashboard`](#dlt-runtime-dashboard) - Open the runtime dashboard for this workspace
* [`deploy`](#dlt-runtime-deploy) - Sync code/config and deploy jobs from __deployment__ manifest
* [`info`](#dlt-runtime-info) - Show overview of current runtime workspace (shows workspace, job count, latest run, latest deployment, and latest configuration)
* [`deployment`](#dlt-runtime-deployment) - Manipulate deployments in the workspace
* [`job`](#dlt-runtime-job) - List, create and inspect jobs
* [`job-run`](#dlt-runtime-job-run) - List, create and inspect job runs
* [`configuration`](#dlt-runtime-configuration) - Manipulate configurations in the workspace
* [`workspace`](#dlt-runtime-workspace) - List and manage workspaces

</details>

### `dlt runtime login`

Log in to dltHub Runtime and connect the current workspace to the remote one.

**Usage**
```sh
dlt runtime login [-h] [--workspace WORKSPACE] [--resume DEVICE_CODE]
```

**Description**

Log in to dltHub Runtime.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Options**
* `-h, --help` - Show this help message and exit
* `--workspace WORKSPACE, -w WORKSPACE` - Select workspace by name or id (skip interactive prompt)
* `--resume DEVICE_CODE` - Resume a previously started device flow login. the device_code is printed by `dlt runtime login` when no tty is attached.

</details>

### `dlt runtime logout`

Log out from dltHub Runtime.

**Usage**
```sh
dlt runtime logout [-h]
```

**Description**

Log out from dltHub Runtime.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime launch`

Deploy code/config and run a script.

**Usage**
```sh
dlt runtime launch [-h] [--file FILE] [-f] [--refresh] [selector_or_job_ref]
```

**Description**

Deploy current workspace and run a batch script remotely. Use -f/--follow to tail logs until completion.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `selector_or_job_ref` - Selector or job ref to pick a job from the manifest

**Options**
* `-h, --help` - Show this help message and exit
* `--file FILE` - Python file to use as manifest source (instead of __deployment__)
* `-f, --follow` - Follow status changes and stream logs until the run completes
* `--refresh` - Re-run from scratch (full reload). cascades to freshness-graph downstream jobs.

</details>

### `dlt runtime serve`

Deploy and serve an interactive notebook/app (read-only) and follow until ready.

**Usage**
```sh
dlt runtime serve [-h] [--file FILE] [-f] [selector_or_job_ref]
```

**Description**

Deploy current workspace and run a notebook as a read-only web app.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `selector_or_job_ref` - Selector or job ref to pick an interactive app from the manifest

**Options**
* `-h, --help` - Show this help message and exit
* `--file FILE` - Python file to use as manifest source (instead of __deployment__)
* `-f, --follow` - Stream logs until the app stops

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

### `dlt runtime unpublish`

Revoke the public link for an interactive notebook/app.

**Usage**
```sh
dlt runtime unpublish [-h] script_path
```

**Description**

Revoke the public link for an interactive notebook/app.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `script_path` - Local path to the notebook/app

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime trigger`

Trigger jobs matching selectors (does not sync or deploy).

**Usage**
```sh
dlt runtime trigger [-h] [--dry-run] [--profile PROFILE] [--refresh] selectors
    [selectors ...]
```

**Description**

Trigger runs for jobs matching the given selectors. Does not sync code or deploy jobs. Examples: 'tag:backfill', 'manual:jobs.etl.*', 'schedule:*'.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `selectors` - Trigger selectors (fnmatch patterns), e.g. 'tag:backfill', 'manual:jobs.etl.*'

**Options**
* `-h, --help` - Show this help message and exit
* `--dry-run` - Preview matched jobs without creating runs
* `--profile PROFILE` - Profile override for all triggered runs
* `--refresh` - Force a refresh on every triggered job (jobs skipped by freshness are not refreshed).

</details>

### `dlt runtime run-pipeline`

Run a job by pipeline name.

**Usage**
```sh
dlt runtime run-pipeline [-h] [--job-ref JOB_REF] [-f] [--refresh] pipeline_name
```

**Description**

Run a job that uses the given pipeline. Uses 'pipeline_name:' trigger selector.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `pipeline_name` - Name of the pipeline to run

**Options**
* `-h, --help` - Show this help message and exit
* `--job-ref JOB_REF` - Specific job ref if multiple jobs use the same pipeline
* `-f, --follow` - Follow status changes and stream logs until the run completes
* `--refresh` - Re-run from scratch (full reload). cascades to freshness-graph downstream jobs.

</details>

### `dlt runtime logs`

Show logs for latest or selected job run (shortcut for 'job-run logs').

**Usage**
```sh
dlt runtime logs [-h] [-f] selector_or_job_name [run_number]
```

**Description**

Show logs for the latest run of a job or a specific run number. Use -f/--follow to stream logs in real-time.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `selector_or_job_name` - Job name, script path, or selector (e.g. batch, schedule:*).
* `run_number` - Run number (optional)

**Options**
* `-h, --help` - Show this help message and exit
* `-f, --follow` - Follow logs in real-time until the run completes

</details>

### `dlt runtime cancel`

Cancel active runs for matching jobs.

**Usage**
```sh
dlt runtime cancel [-h] [--dry-run] selector_or_job_name [selector_or_job_name
    ...]
```

**Description**

Cancel active (non-terminal) runs for jobs matching selectors or names. Accepts one or more selectors (batch, schedule:*, tag:ops, etc.) or job names. Use --dry-run to preview.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `selector_or_job_name` - Job name, script path, or selector (e.g. batch, schedule:*). multiple values cancel active runs for all matching jobs.

**Options**
* `-h, --help` - Show this help message and exit
* `--dry-run` - Show what would be cancelled without actually cancelling

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

Sync code/config and deploy jobs from __deployment__ manifest.

**Usage**
```sh
dlt runtime deploy [-h] [--file FILE] [--dry-run] [--show-manifest]
```

**Description**

Sync workspace files, generate job manifest from __deployment__.py, and reconcile jobs with the runtime. Use --dry-run to preview changes.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Options**
* `-h, --help` - Show this help message and exit
* `--file FILE` - Python file to use as manifest source (instead of __deployment__)
* `--dry-run` - Preview changes without applying them
* `--show-manifest` - Dump the expanded deployment manifest as yaml and exit

</details>

### `dlt runtime info`

Show overview of current Runtime workspace (shows workspace, job count, latest run, latest deployment, and latest configuration).

**Usage**
```sh
dlt runtime info [-h]
```

**Description**

Show workspace ID and summary of deployments, configurations and jobs.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime deployment`

Manipulate deployments in the workspace.

**Usage**
```sh
dlt runtime deployment [-h] [deployment_version_no] {list,info,sync} ...
```

**Description**

Manipulate deployments in the workspace.

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
dlt runtime deployment [deployment_version_no] sync [-h] [--dry-run] [-v]
```

**Description**

Create new deployment if local workspace content changed.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime deployment`](#dlt-runtime-deployment).

**Options**
* `-h, --help` - Show this help message and exit
* `--dry-run` - Compare local files to latest deployment without uploading
* `-v, --verbose` - Print per-file added/updated/deleted tree alongside the summary

</details>

### `dlt runtime job`

List, create and inspect jobs.

**Usage**
```sh
dlt runtime job [-h] [selector_or_job_name ...] {list,info} ...
```

**Description**

List and manipulate jobs registered in the workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `selector_or_job_name` - Job name, script path, or selector (e.g. batch, schedule:*). multiple selectors narrow the listing. required for `info`.

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`list`](#dlt-runtime-job-list) - List jobs (filter with selectors: batch, schedule:*, tag:ops, ...)
* [`info`](#dlt-runtime-job-info) - Show job info

</details>

### `dlt runtime job list`

List jobs (filter with selectors: batch, schedule:*, tag:ops, ...).

**Usage**
```sh
dlt runtime job [selector_or_job_name ...] list [-h]
```

**Description**

List jobs registered in the workspace. Pass selectors before `list` to filter: batch, interactive, schedule:*, tag:&lt;name&gt;, manual:*, etc.

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
dlt runtime job [selector_or_job_name ...] info [-h]
```

**Description**

Display detailed information about the job.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime job`](#dlt-runtime-job).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime job-run`

List, create and inspect job runs.

**Usage**
```sh
dlt runtime job-run [-h] [selector_or_job_name] [run_number]
    {list,info,logs,cancel} ...
```

**Description**

List and manipulate job runs registered in the workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `selector_or_job_name` - Job name, script path, or selector (e.g. batch, schedule:*). for `list`: filters runs by matching jobs. required for `info`, `logs`, `cancel`.
* `run_number` - Run number. used in all commands except `list` and `create` as optional argument. if not specified, the latest run of the given script will be used.

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`list`](#dlt-runtime-job-run-list) - List job runs (filter with a selector: batch, schedule:*, ...)
* [`info`](#dlt-runtime-job-run-info) - Show job run info
* [`logs`](#dlt-runtime-job-run-logs) - Show logs for the latest or selected job run
* [`cancel`](#dlt-runtime-job-run-cancel) - Cancel the latest or selected job run

</details>

### `dlt runtime job-run list`

List job runs (filter with a selector: batch, schedule:*, ...).

**Usage**
```sh
dlt runtime job-run [selector_or_job_name] [run_number] list [-h]
```

**Description**

List job runs registered in the workspace. Pass a selector before `list` to filter by matching jobs.

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
dlt runtime job-run [selector_or_job_name] [run_number] info [-h]
```

**Description**

Display detailed information about the job run.

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
dlt runtime job-run [selector_or_job_name] [run_number] logs [-h] [-f]
```

**Description**

Show logs for the latest or selected job run. Use -f/--follow to stream logs in real-time until completion.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime job-run`](#dlt-runtime-job-run).

**Options**
* `-h, --help` - Show this help message and exit
* `-f, --follow` - Follow logs in real-time until the run completes

</details>

### `dlt runtime job-run cancel`

Cancel the latest or selected job run.

**Usage**
```sh
dlt runtime job-run [selector_or_job_name] [run_number] cancel [-h]
```

**Description**

Cancel the latest or selected job run.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime job-run`](#dlt-runtime-job-run).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime configuration`

Manipulate configurations in the workspace.

**Usage**
```sh
dlt runtime configuration [-h] [configuration_version_no] {list,info,sync} ...
```

**Description**

Manipulate configurations in the workspace.

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
dlt runtime configuration [configuration_version_no] sync [-h] [--dry-run] [-v]
```

**Description**

Create new configuration if local config content changed.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime configuration`](#dlt-runtime-configuration).

**Options**
* `-h, --help` - Show this help message and exit
* `--dry-run` - Compare local config to latest configuration without uploading
* `-v, --verbose` - Print per-file added/updated/deleted tree alongside the summary

</details>

### `dlt runtime workspace`

List and manage workspaces.

**Usage**
```sh
dlt runtime workspace [-h] {list,switch} ...
```

**Description**

List and manage workspaces in your organization.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`list`](#dlt-runtime-workspace-list) - List all workspaces you have access to
* [`switch`](#dlt-runtime-workspace-switch) - Switch to a different workspace by name or id

</details>

### `dlt runtime workspace list`

List all workspaces you have access to.

**Usage**
```sh
dlt runtime workspace list [-h]
```

**Description**

List all workspaces you have access to.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime workspace`](#dlt-runtime-workspace).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime workspace switch`

Switch to a different workspace by name or ID.

**Usage**
```sh
dlt runtime workspace switch [-h] [workspace]
```

**Description**

Switch the locally connected workspace without re-running login.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime workspace`](#dlt-runtime-workspace).

**Positional arguments**
* `workspace` - Workspace name or id to switch to. if omitted, an interactive picker is shown with an option to create a new workspace.

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

