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
    {workspace,telemetry,schema,runtime,profile,pipeline,init,render-docs,deploy,dashboard,ai}
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
* [`runtime`](#dlt-runtime) - Connect to dlthub runtime and run your code remotely
* [`profile`](#dlt-profile) - Manage workspace built-in profiles

</details>

## `dlt workspace`

Manage current Workspace.

**Usage**
```sh
dlt workspace [-h] {clean,info,mcp,show} ...
```

**Description**

Commands to get info, cleanup local files and launch Workspace MCP. Run without command get
workspace info.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Options**
* `-h, --help` - Show this help message and exit

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

## `dlt runtime`

Connect to dltHub Runtime and run your code remotely.

**Usage**
```sh
dlt runtime [-h] {login,logout,deploy,run,runs,deployment,script,configuration}
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
* [`deploy`](#dlt-runtime-deploy) - Create, run and inspect scripts in runtime
* [`run`](#dlt-runtime-run) - Run a script in the runtime
* [`runs`](#dlt-runtime-runs) - Manipulate runs in workspace
* [`deployment`](#dlt-runtime-deployment) - Manipulate deployments in workspace
* [`script`](#dlt-runtime-script) - Create, list and inspect scripts in runtime
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

### `dlt runtime deploy`

Create, run and inspect scripts in runtime.

**Usage**
```sh
dlt runtime deploy [-h] [--profile [PROFILE]] [-i] script_name
```

**Description**

Manipulate scripts in workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `script_name` - Local path to the script

**Options**
* `-h, --help` - Show this help message and exit
* `--profile [PROFILE], -p [PROFILE]` - Profile to use for the run
* `-i, --interactive` - Whether the script should be deployed as interactive (e.g. a notebook). false by default

</details>

### `dlt runtime run`

Run a script in the Runtime.

**Usage**
```sh
dlt runtime run [-h] [--profile [PROFILE]] [-i] script_name_or_id
```

**Description**

Create or update a script and trigger a run.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `script_name_or_id` - Local path to the script or id/name of deployed script

**Options**
* `-h, --help` - Show this help message and exit
* `--profile [PROFILE], -p [PROFILE]` - Profile to use for the run
* `-i, --interactive` - Whether the script should be deployed as interactive (e.g. a notebook). false by default

</details>

### `dlt runtime runs`

Manipulate runs in workspace.

**Usage**
```sh
dlt runtime runs [-h] [--list | --no-list | -l] [script_name_or_run_id]
    {list,info,logs,cancel} ...
```

**Description**

Manipulate runs in workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `script_name_or_run_id` - The name of the script we're working with or the id of the run of this script

**Options**
* `-h, --help` - Show this help message and exit
* `--list, --no-list, -l` - List all runs in workspace

**Available subcommands**
* [`list`](#dlt-runtime-runs-list) - List all runs of the script, only works if script name is provided
* [`info`](#dlt-runtime-runs-info) - Get detailed information about a run
* [`logs`](#dlt-runtime-runs-logs) - Get the logs of a run
* [`cancel`](#dlt-runtime-runs-cancel) - Cancel a run in the runtime

</details>

### `dlt runtime runs list`

List all runs of the script, only works if script name is provided.

**Usage**
```sh
dlt runtime runs [script_name_or_run_id] list [-h]
```

**Description**

List all runs of the script, only works if script name is provided.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime runs`](#dlt-runtime-runs).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime runs info`

Get detailed information about a run.

**Usage**
```sh
dlt runtime runs [script_name_or_run_id] info [-h]
```

**Description**

Get detailed information about a run.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime runs`](#dlt-runtime-runs).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime runs logs`

Get the logs of a run.

**Usage**
```sh
dlt runtime runs [script_name_or_run_id] logs [-h]
```

**Description**

Get the logs of a run.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime runs`](#dlt-runtime-runs).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime runs cancel`

Cancel a run in the Runtime.

**Usage**
```sh
dlt runtime runs [script_name_or_run_id] cancel [-h]
```

**Description**

Cancel a run in the Runtime.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime runs`](#dlt-runtime-runs).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime deployment`

Manipulate deployments in workspace.

**Usage**
```sh
dlt runtime deployment [-h] [--list | --no-list | -l] [deployment_id]
    {info,sync} ...
```

**Description**

Manipulate deployments in workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `deployment_id` - Deployment id (uuid)

**Options**
* `-h, --help` - Show this help message and exit
* `--list, --no-list, -l` - List all deployments in workspace

**Available subcommands**
* [`info`](#dlt-runtime-deployment-info) - Get detailed information about a deployment
* [`sync`](#dlt-runtime-deployment-sync) - Create new deployment if local workspace content changed

</details>

### `dlt runtime deployment info`

Get detailed information about a deployment.

**Usage**
```sh
dlt runtime deployment [deployment_id] info [-h]
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
dlt runtime deployment [deployment_id] sync [-h]
```

**Description**

Create new deployment if local workspace content changed.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime deployment`](#dlt-runtime-deployment).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime script`

Create, list and inspect scripts in runtime.

**Usage**
```sh
dlt runtime script [-h] [--list | --no-list | -l] [script_name_or_id]
    {info,sync} ...
```

**Description**

Manipulate scripts in workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `script_name_or_id` - Local path to the script or id/name of deployed script

**Options**
* `-h, --help` - Show this help message and exit
* `--list, --no-list, -l` - List all scripts in workspace

**Available subcommands**
* [`info`](#dlt-runtime-script-info) - Get detailed information about a script
* [`sync`](#dlt-runtime-script-sync) - Create or update the script

</details>

### `dlt runtime script info`

Get detailed information about a script.

**Usage**
```sh
dlt runtime script [script_name_or_id] info [-h]
```

**Description**

Get detailed information about a script.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime script`](#dlt-runtime-script).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime script sync`

Create or update the script.

**Usage**
```sh
dlt runtime script [script_name_or_id] sync [-h]
```

**Description**

Create or update the script.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime script`](#dlt-runtime-script).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt runtime configuration`

Manipulate configurations in workspace.

**Usage**
```sh
dlt runtime configuration [-h] [--list | --no-list | -l] [configuration_id]
    {info,sync} ...
```

**Description**

Manipulate configurations in workspace.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime`](#dlt-runtime).

**Positional arguments**
* `configuration_id` - Configuration id (uuid)

**Options**
* `-h, --help` - Show this help message and exit
* `--list, --no-list, -l` - List all configurations in workspace

**Available subcommands**
* [`info`](#dlt-runtime-configuration-info) - Get detailed information about a configuration
* [`sync`](#dlt-runtime-configuration-sync) - Create new configuration if local config content changed

</details>

### `dlt runtime configuration info`

Get detailed information about a configuration.

**Usage**
```sh
dlt runtime configuration [configuration_id] info [-h]
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
dlt runtime configuration [configuration_id] sync [-h]
```

**Description**

Create new configuration if local config content changed.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt runtime configuration`](#dlt-runtime-configuration).

**Options**
* `-h, --help` - Show this help message and exit

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

