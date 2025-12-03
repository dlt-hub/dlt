---
title: Command Line Interface
description: Command line interface (CLI) full reference of dlt
keywords: [command line interface, cli, dlt init]
---


# Command line interface reference

<!-- this page is fully generated from the argparse object of dlt, run make update-cli-docs to update it -->

This page contains all commands available in the dltHub CLI and is generated
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
    {pipeline,workspace,telemetry,schema,profile,init,render-docs,deploy,dashboard,ai,transformation,source,project,license,destination,dbt,dataset,cache}
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
* `scope` - Scope of the license, a comma separated list of the scopes: ['dlthub.dbt_generator', 'dlthub.sources.mssql', 'dlthub.project', 'dlthub.transformation', 'dlthub.destinations.iceberg', 'dlthub.destinations.snowflake_plus', 'dlthub.runner']

**Options**
* `-h, --help` - Show this help message and exit

</details>

