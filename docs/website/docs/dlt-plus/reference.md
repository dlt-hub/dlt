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
    [--non-interactive] [--debug]
    {transformation,source,project,profile,pipeline,license,destination,dbt,dataset,cache,telemetry,schema,init,render-docs,deploy}
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

**Available subcommands**
* [`transformation`](#dlt-transformation) - Run transformations dlt+ project. experimental.
* [`source`](#dlt-source) - Manage dlt+ project sources
* [`project`](#dlt-project) - Manage dlt+ projects
* [`profile`](#dlt-profile) - Manage dlt+ project profiles
* [`pipeline`](#dlt-pipeline) - Operations on pipelines that were ran locally
* [`license`](#dlt-license) - View dlt+ license status
* [`destination`](#dlt-destination) - Manage project destinations
* [`dbt`](#dlt-dbt) - Dlt+ dbt transformation generator
* [`dataset`](#dlt-dataset) - Manage dlt+ project datasets
* [`cache`](#dlt-cache) - Manage dlt+ project local data cache. experimental.
* [`telemetry`](#dlt-telemetry) - Shows telemetry status
* [`schema`](#dlt-schema) - Shows, converts and upgrades schemas
* [`init`](#dlt-init) - Creates a pipeline project in the current folder by adding existing verified source or creating a new one from template.
* [`render-docs`](#dlt-render-docs) - Renders markdown version of cli docs
* [`deploy`](#dlt-deploy) - Creates a deployment package for a selected pipeline script

</details>

## `dlt transformation`

Run transformations dlt+ project. Experimental.

**Usage**
```sh
dlt transformation [-h] [--project PROJECT] [--profile PROFILE] pond_name
    {list,info,run,verify-inputs,verify-outputs,populate,flush,transform,populate-state,flush-state,render-t-layer}
    ...
```

**Description**

Commands to run transformations on local cache in dlt+ projects

**This is an experimental feature and will change substantially in the future.**

**Do not use in production.**.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Positional arguments**
* `pond_name` - Name of the transformation, use '.' for the first one found in project.

**Options**
* `-h, --help` - Show this help message and exit
* `--project PROJECT` - Name or path to the dlt package with dlt.yml
* `--profile PROFILE` - Profile to use from the project configuration file

**Available subcommands**
* [`list`](#dlt-transformation-list) - List all transformations discovered in this directory
* [`info`](#dlt-transformation-info) - Transformation info: locations, cache status etc.
* [`run`](#dlt-transformation-run) - Sync cache, run transformation and commit the outputs
* [`verify-inputs`](#dlt-transformation-verify-inputs) - Verify that cache can connect to all defined inputs and that tables declared are available
* [`verify-outputs`](#dlt-transformation-verify-outputs) - Verify that the output cache dataset contains all tables declared
* [`populate`](#dlt-transformation-populate) - Sync data from inputs to input cache dataset
* [`flush`](#dlt-transformation-flush) - Flush data from output cache dataset to outputs
* [`transform`](#dlt-transformation-transform) - Run transformations on input cache dataset and write to output cache dataset
* [`populate-state`](#dlt-transformation-populate-state) - Populate transformation state from defined output
* [`flush-state`](#dlt-transformation-flush-state) - Flush transformation state to defined output
* [`render-t-layer`](#dlt-transformation-render-t-layer) - Render a starting point for the t-layer

</details>

### `dlt transformation list`

List all transformations discovered in this directory.

**Usage**
```sh
dlt transformation pond_name list [-h]
```

**Description**

List all transformations discovered in this directory.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt transformation`](#dlt-transformation).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt transformation info`

Transformation info: locations, cache status etc.

**Usage**
```sh
dlt transformation pond_name info [-h]
```

**Description**

Transformation info: locations, cache status etc.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt transformation`](#dlt-transformation).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt transformation run`

Sync cache, run transformation and commit the outputs.

**Usage**
```sh
dlt transformation pond_name run [-h]
```

**Description**

Sync cache, run transformation and commit the outputs.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt transformation`](#dlt-transformation).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt transformation verify-inputs`

Verify that cache can connect to all defined inputs and that tables declared are available.

**Usage**
```sh
dlt transformation pond_name verify-inputs [-h]
```

**Description**

Verify that cache can connect to all defined inputs and that tables declared are available.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt transformation`](#dlt-transformation).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt transformation verify-outputs`

Verify that the output cache dataset contains all tables declared.

**Usage**
```sh
dlt transformation pond_name verify-outputs [-h]
```

**Description**

Verify that the output cache dataset contains all tables declared.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt transformation`](#dlt-transformation).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt transformation populate`

Sync data from inputs to input cache dataset.

**Usage**
```sh
dlt transformation pond_name populate [-h]
```

**Description**

Sync data from inputs to input cache dataset.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt transformation`](#dlt-transformation).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt transformation flush`

Flush data from output cache dataset to outputs.

**Usage**
```sh
dlt transformation pond_name flush [-h]
```

**Description**

Flush data from output cache dataset to outputs.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt transformation`](#dlt-transformation).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt transformation transform`

Run transformations on input cache dataset and write to output cache dataset.

**Usage**
```sh
dlt transformation pond_name transform [-h]
```

**Description**

Run transformations on input cache dataset and write to output cache dataset.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt transformation`](#dlt-transformation).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt transformation populate-state`

Populate transformation state from defined output.

**Usage**
```sh
dlt transformation pond_name populate-state [-h]
```

**Description**

Populate transformation state from defined output.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt transformation`](#dlt-transformation).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt transformation flush-state`

Flush transformation state to defined output.

**Usage**
```sh
dlt transformation pond_name flush-state [-h]
```

**Description**

Flush transformation state to defined output.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt transformation`](#dlt-transformation).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt transformation render-t-layer`

Render a starting point for the t-layer.

**Usage**
```sh
dlt transformation pond_name render-t-layer [-h]
```

**Description**

Render a starting point for the t-layer.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt transformation`](#dlt-transformation).

**Options**
* `-h, --help` - Show this help message and exit

</details>

## `dlt source`

Manage dlt+ project sources.

**Usage**
```sh
dlt source [-h] [--project PROJECT] [--profile PROFILE] [source_name]
    {check,list,add} ...
```

**Description**

Commands to manage sources for project.
Run without arguments to list all sources in current project.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Positional arguments**
* `source_name` - Name of the source to add.

**Options**
* `-h, --help` - Show this help message and exit
* `--project PROJECT` - Name or path to the dlt package with dlt.yml
* `--profile PROFILE` - Profile to use from the project configuration file

**Available subcommands**
* [`check`](#dlt-source-check) - (temporary feature) checks if the source is importable, only works for sources within the sources folder.
* [`list`](#dlt-source-list) - List all sources in the project.
* [`add`](#dlt-source-add) - Add a new source to the project.

</details>

### `dlt source check`

(temporary feature) Checks if the source is importable, only works for sources within the sources folder.

**Usage**
```sh
dlt source [source_name] check [-h]
```

**Description**

(temporary feature) Checks if the source is importable, only works for sources within the sources folder.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt source`](#dlt-source).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt source list`

List all sources in the project.

**Usage**
```sh
dlt source [source_name] list [-h]
```

**Description**

List all sources in the project context.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt source`](#dlt-source).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt source add`

Add a new source to the project.

**Usage**
```sh
dlt source [source_name] add [-h] [source_type]
```

**Description**

Add a new source to the project context.

* If source type is not specified, the source type will be the same as the source name.
* If a give source_type is not found, a default source template will be used.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt source`](#dlt-source).

**Positional arguments**
* `source_type` - Type of the source to add. if not specified, the source type will be the same as the source name.

**Options**
* `-h, --help` - Show this help message and exit

</details>

## `dlt project`

Manage dlt+ projects.

**Usage**
```sh
dlt project [-h] [--project PROJECT] [--profile PROFILE]
    {config,clean,init,list,info,audit} ...
```

**Description**

Commands to manage dlt+ projects. Run without arguments to list all projects in scope.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Options**
* `-h, --help` - Show this help message and exit
* `--project PROJECT` - Name or path to the dlt package with dlt.yml
* `--profile PROFILE` - Profile to use from the project configuration file

**Available subcommands**
* [`config`](#dlt-project-config) - Configuration management commands
* [`clean`](#dlt-project-clean) - Cleans local data for the selected profile. if tmp_dir is defined in project file, it gets deleted. pipelines and transformations working dir are also deleted by default. data in remote destinations is not affected
* [`init`](#dlt-project-init) - Initialize a new dlt+ project
* [`list`](#dlt-project-list) - List all projects that could be found in installed dlt packages
* [`info`](#dlt-project-info) - List basic project info of current project.
* [`audit`](#dlt-project-audit) - Creates and locks resource and secrets audit for a current profile.

</details>

### `dlt project config`

Configuration management commands.

**Usage**
```sh
dlt project config [-h] {validate,show} ...
```

**Description**

Configuration management commands.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt project`](#dlt-project).

**Positional arguments**
* `validate` - Validate configuration file
* `show` - Show configuration

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt project config validate`

Validate configuration file.

**Usage**
```sh
dlt project config validate [-h]
```

**Description**

Validate configuration file.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt project config`](#dlt-project-config).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt project config show`

Show configuration.

**Usage**
```sh
dlt project config show [-h] [--format {json,yaml}] [--section SECTION]
```

**Description**

Show configuration.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt project config`](#dlt-project-config).

**Options**
* `-h, --help` - Show this help message and exit
* `--format {json,yaml}` - Output format
* `--section SECTION` - Show specific configuration section (e.g., sources, pipelines)

</details>

### `dlt project clean`

Cleans local data for the selected profile. If tmp_dir is defined in project file, it gets deleted. Pipelines and transformations working dir are also deleted by default. Data in remote destinations is not affected.

**Usage**
```sh
dlt project clean [-h] [--skip-data-dir]
```

**Description**

Cleans local data for the selected profile. If tmp_dir is defined in project file, it gets deleted. Pipelines and transformations working dir are also deleted by default. Data in remote destinations is not affected.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt project`](#dlt-project).

**Options**
* `-h, --help` - Show this help message and exit
* `--skip-data-dir` - Do not delete pipelines and transformations working dir.

</details>

### `dlt project init`

Initialize a new dlt+ project.

**Usage**
```sh
dlt project init [-h] [--project-name PROJECT_NAME] [--package] [--force]
    [source] [destination]
```

**Description**

Initialize a new dlt+ project.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt project`](#dlt-project).

**Positional arguments**
* `source` - Name of a source for your dlt project
* `destination` - Name of a destination for your dlt project

**Options**
* `-h, --help` - Show this help message and exit
* `--project-name PROJECT_NAME, -n PROJECT_NAME` - Optinal name of your dlt project
* `--package` - Create a pip package instead of a flat project
* `--force` - Overwrite project even if it already exists

</details>

### `dlt project list`

List all projects that could be found in installed dlt packages.

**Usage**
```sh
dlt project list [-h]
```

**Description**

List all projects that could be found in installed dlt packages.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt project`](#dlt-project).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt project info`

List basic project info of current project.

**Usage**
```sh
dlt project info [-h]
```

**Description**

List basic project info of current project.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt project`](#dlt-project).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt project audit`

Creates and locks resource and secrets audit for a current profile.

**Usage**
```sh
dlt project audit [-h]
```

**Description**

Creates and locks resource and secrets audit for a current profile.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt project`](#dlt-project).

**Options**
* `-h, --help` - Show this help message and exit

</details>

## `dlt profile`

Manage dlt+ project profiles.

**Usage**
```sh
dlt profile [-h] [--project PROJECT] [--profile PROFILE] [profile_name]
    {info,list,add,pin} ...
```

**Description**

Commands to manage profiles for project.
Run without arguments to list all profiles, the default profile and the
pinned profile in current project.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Positional arguments**
* `profile_name` - Name of the profile to add

**Options**
* `-h, --help` - Show this help message and exit
* `--project PROJECT` - Name or path to the dlt package with dlt.yml
* `--profile PROFILE` - Profile to use from the project configuration file

**Available subcommands**
* [`info`](#dlt-profile-info) - Show information about profile settings.
* [`list`](#dlt-profile-list) - Show list of all profiles in the project.
* [`add`](#dlt-profile-add) - Add a new profile to the project.
* [`pin`](#dlt-profile-pin) - Pin a profile to the project.

</details>

### `dlt profile info`

Show information about profile settings.

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

Show list of all profiles in the project.

**Usage**
```sh
dlt profile [profile_name] list [-h]
```

**Description**

Show list of all profiles in the project.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt profile`](#dlt-profile).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt profile add`

Add a new profile to the project.

**Usage**
```sh
dlt profile [profile_name] add [-h]
```

**Description**

Add a new profile to the project.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt profile`](#dlt-profile).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt profile pin`

Pin a profile to the project.

**Usage**
```sh
dlt profile [profile_name] pin [-h]
```

**Description**

Pin a profile to the project, this will be the new default profile while it is pinned.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt profile`](#dlt-profile).

**Options**
* `-h, --help` - Show this help message and exit

</details>

## `dlt pipeline`

Operations on pipelines that were ran locally.

**Usage**
```sh
dlt pipeline [-h] [--project PROJECT] [--profile PROFILE] [--list-pipelines]
    [--hot-reload] [--pipelines-dir PIPELINES_DIR] [--verbose] [pipeline_name]
    {info,show,failed-jobs,drop-pending-packages,sync,trace,schema,drop,load-package,list,add,run}
    ...
```

**Description**

The `dlt pipeline` command provides a set of commands to inspect the pipeline working directory,
tables, and data in the destination and check for problems encountered during data loading.

Run without arguments to list all pipelines in the current project.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Positional arguments**
* `pipeline_name` - Pipeline name

**Options**
* `-h, --help` - Show this help message and exit
* `--project PROJECT` - Name or path to the dlt package with dlt.yml
* `--profile PROFILE` - Profile to use from the project configuration file
* `--list-pipelines, -l` - List local pipelines
* `--hot-reload` - Reload streamlit app (for core development)
* `--pipelines-dir PIPELINES_DIR` - Pipelines working directory
* `--verbose, -v` - Provides more information for certain commands.

**Available subcommands**
* [`info`](#dlt-pipeline-info) - Displays state of the pipeline, use -v or -vv for more info
* [`show`](#dlt-pipeline-show) - Generates and launches streamlit app with the loading status and dataset explorer
* [`failed-jobs`](#dlt-pipeline-failed-jobs) - Displays information on all the failed loads in all completed packages, failed jobs and associated error messages
* [`drop-pending-packages`](#dlt-pipeline-drop-pending-packages) - Deletes all extracted and normalized packages including those that are partially loaded.
* [`sync`](#dlt-pipeline-sync) - Drops the local state of the pipeline and resets all the schemas and restores it from destination. the destination state, data and schemas are left intact.
* [`trace`](#dlt-pipeline-trace) - Displays last run trace, use -v or -vv for more info
* [`schema`](#dlt-pipeline-schema) - Displays default schema
* [`drop`](#dlt-pipeline-drop) - Selectively drop tables and reset state
* [`load-package`](#dlt-pipeline-load-package) - Displays information on load package, use -v or -vv for more info
* [`list`](#dlt-pipeline-list) - List all pipelines in the project.
* [`add`](#dlt-pipeline-add) - Add a new pipeline to the current project
* [`run`](#dlt-pipeline-run) - Run a pipeline

</details>

### `dlt pipeline info`

Displays state of the pipeline, use -v or -vv for more info.

**Usage**
```sh
dlt pipeline [pipeline_name] info [-h]
```

**Description**

Displays the content of the working directory of the pipeline: dataset name, destination, list of
schemas, resources in schemas, list of completed and normalized load packages, and optionally a
pipeline state set by the resources during the extraction process.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt pipeline`](#dlt-pipeline).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt pipeline show`

Generates and launches Streamlit app with the loading status and dataset explorer.

**Usage**
```sh
dlt pipeline [pipeline_name] show [-h]
```

**Description**

Generates and launches Streamlit (https://streamlit.io/) app with the loading status and dataset explorer.

This is a simple app that you can use to inspect the schemas and data in the destination as well as your pipeline state and loading status/stats. It should be executed from the same folder from which you ran the pipeline script to access destination credentials.

Requires `streamlit` to be installed in the current environment: `pip install streamlit`.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt pipeline`](#dlt-pipeline).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt pipeline failed-jobs`

Displays information on all the failed loads in all completed packages, failed jobs and associated error messages.

**Usage**
```sh
dlt pipeline [pipeline_name] failed-jobs [-h]
```

**Description**

This command scans all the load packages looking for failed jobs and then displays information on
files that got loaded and the failure message from the destination.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt pipeline`](#dlt-pipeline).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt pipeline drop-pending-packages`

Deletes all extracted and normalized packages including those that are partially loaded.

**Usage**
```sh
dlt pipeline [pipeline_name] drop-pending-packages [-h]
```

**Description**

Removes all extracted and normalized packages in the pipeline's working dir.
`dlt` keeps extracted and normalized load packages in the pipeline working directory. When the `run` method is called, it will attempt to normalize and load
pending packages first. The command above removes such packages. Note that **pipeline state** is not reverted to the state at which the deleted packages
were created. Using `dlt pipeline ... sync` is recommended if your destination supports state sync.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt pipeline`](#dlt-pipeline).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt pipeline sync`

Drops the local state of the pipeline and resets all the schemas and restores it from destination. The destination state, data and schemas are left intact.

**Usage**
```sh
dlt pipeline [pipeline_name] sync [-h] [--destination DESTINATION]
    [--dataset-name DATASET_NAME]
```

**Description**

This command will remove the pipeline working directory with all pending packages, not synchronized
state changes, and schemas and retrieve the last synchronized data from the destination. If you drop
the dataset the pipeline is loading to, this command results in a complete reset of the pipeline state.

In case of a pipeline without a working directory, the command may be used to create one from the
destination. In order to do that, you need to pass the dataset name and destination name to the CLI
and provide the credentials to connect to the destination (i.e., in `.dlt/secrets.toml`) placed in the
folder where you execute the `pipeline sync` command.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt pipeline`](#dlt-pipeline).

**Options**
* `-h, --help` - Show this help message and exit
* `--destination DESTINATION` - Sync from this destination when local pipeline state is missing.
* `--dataset-name DATASET_NAME` - Dataset name to sync from when local pipeline state is missing.

</details>

### `dlt pipeline trace`

Displays last run trace, use -v or -vv for more info.

**Usage**
```sh
dlt pipeline [pipeline_name] trace [-h]
```

**Description**

Displays the trace of the last pipeline run containing the start date of the run, elapsed time, and the
same information for all the steps (`extract`, `normalize`, and `load`). If any of the steps failed,
you'll see the message of the exceptions that caused that problem. Successful `load` and `run` steps
will display the load info instead.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt pipeline`](#dlt-pipeline).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt pipeline schema`

Displays default schema.

**Usage**
```sh
dlt pipeline [pipeline_name] schema [-h] [--format {json,yaml}]
    [--remove-defaults]
```

**Description**

Displays the default schema for the selected pipeline.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt pipeline`](#dlt-pipeline).

**Options**
* `-h, --help` - Show this help message and exit
* `--format {json,yaml}` - Display schema in this format
* `--remove-defaults` - Does not show default hint values

</details>

### `dlt pipeline drop`

Selectively drop tables and reset state.

**Usage**
```sh
dlt pipeline [pipeline_name] drop [-h] [--destination DESTINATION]
    [--dataset-name DATASET_NAME] [--drop-all] [--state-paths [STATE_PATHS ...]]
    [--schema SCHEMA_NAME] [--state-only] [resources ...]
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

Inherits arguments from [`dlt pipeline`](#dlt-pipeline).

**Positional arguments**
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

### `dlt pipeline load-package`

Displays information on load package, use -v or -vv for more info.

**Usage**
```sh
dlt pipeline [pipeline_name] load-package [-h] [load-id]
```

**Description**

Shows information on a load package with a given `load_id`. The `load_id` parameter defaults to the
most recent package. Package information includes its state (`COMPLETED/PROCESSED`) and list of all
jobs in a package with their statuses, file sizes, types, and in case of failed jobs—the error
messages from the destination. With the verbose flag set `dlt pipeline -v ...`, you can also see the
list of all tables and columns created at the destination during the loading of that package.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt pipeline`](#dlt-pipeline).

**Positional arguments**
* `load-id` - Load id of completed or normalized package. defaults to the most recent package.

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt pipeline list`

List all pipelines in the project.

**Usage**
```sh
dlt pipeline [pipeline_name] list [-h]
```

**Description**

List all pipelines in the project.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt pipeline`](#dlt-pipeline).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt pipeline add`

Add a new pipeline to the current project.

**Usage**
```sh
dlt pipeline [pipeline_name] add [-h] [--dataset-name DATASET_NAME] source_name
    destination_name
```

**Description**

Adds a new pipeline to the current project. Will not create any sources
or destinations, you can reference other entities by name.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt pipeline`](#dlt-pipeline).

**Positional arguments**
* `source_name` - Name of the source to add
* `destination_name` - Name of the destination to add

**Options**
* `-h, --help` - Show this help message and exit
* `--dataset-name DATASET_NAME` - Name of the dataset to add

</details>

### `dlt pipeline run`

Run a pipeline.

**Usage**
```sh
dlt pipeline [pipeline_name] run [-h] [--limit LIMIT] [--resources RESOURCES]
```

**Description**

Run a pipeline.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt pipeline`](#dlt-pipeline).

**Options**
* `-h, --help` - Show this help message and exit
* `--limit LIMIT` - Limits the number of extracted pages for all resources. see source.add_limit.
* `--resources RESOURCES` - Comma-separated list of resource names.

</details>

## `dlt license`

View dlt+ license status.

**Usage**
```sh
dlt license [-h] {show,scopes} ...
```

**Description**

View dlt+ license status.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`show`](#dlt-license-show) - Show the installed license
* [`scopes`](#dlt-license-scopes) - Show available scopes

</details>

### `dlt license show`

Show the installed license.

**Usage**
```sh
dlt license show [-h]
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

## `dlt destination`

Manage project destinations.

**Usage**
```sh
dlt destination [-h] [--project PROJECT] [--profile PROFILE] [destination_name]
    {list,list-available,add} ...
```

**Description**

Commands to manage destinations for project.
Run without arguments to list all destinations in current project.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Positional arguments**
* `destination_name` - Name of the destination

**Options**
* `-h, --help` - Show this help message and exit
* `--project PROJECT` - Name or path to the dlt package with dlt.yml
* `--profile PROFILE` - Profile to use from the project configuration file

**Available subcommands**
* [`list`](#dlt-destination-list) - List all destinations in the project.
* [`list-available`](#dlt-destination-list-available) - List all destination types that can be added to the project.
* [`add`](#dlt-destination-add) - Add a new destination to the project

</details>

### `dlt destination list`

List all destinations in the project.

**Usage**
```sh
dlt destination [destination_name] list [-h]
```

**Description**

List all destinations in the project.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt destination`](#dlt-destination).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt destination list-available`

List all destination types that can be added to the project.

**Usage**
```sh
dlt destination [destination_name] list-available [-h]
```

**Description**

List all destination types that can be added to the project.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt destination`](#dlt-destination).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt destination add`

Add a new destination to the project.

**Usage**
```sh
dlt destination [destination_name] add [-h] [--dataset-name DATASET_NAME]
    [destination_type]
```

**Description**

Add a new destination to the project.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt destination`](#dlt-destination).

**Positional arguments**
* `destination_type` - Will default to the destination name if not specified.

**Options**
* `-h, --help` - Show this help message and exit
* `--dataset-name DATASET_NAME` - Name of the dataset to add in the datasets section. will add no dataset if not specified.

</details>

## `dlt dbt`

dlt+ dbt transformation generator.

**Usage**
```sh
dlt dbt [-h] {generate} ...
```

**Description**

dlt+ dbt transformation generator.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`generate`](#dlt-dbt-generate) - Generate dbt project

</details>

### `dlt dbt generate`

Generate dbt project.

**Usage**
```sh
dlt dbt generate [-h] [--include_dlt_tables] [--fact [FACT]] [--force]
    [--mart_table_prefix [MART_TABLE_PREFIX]] pipeline_name
```

**Description**

Generate dbt project.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt dbt`](#dlt-dbt).

**Positional arguments**
* `pipeline_name` - The pipeline to create a dbt project for

**Options**
* `-h, --help` - Show this help message and exit
* `--include_dlt_tables` - Do not render _dlt tables
* `--fact [FACT]` - Create a fact table for a given table
* `--force` - Force overwrite of existing files
* `--mart_table_prefix [MART_TABLE_PREFIX]` - Prefix for mart tables

</details>

## `dlt dataset`

Manage dlt+ project datasets.

**Usage**
```sh
dlt dataset [-h] [--project PROJECT] [--profile PROFILE] [--destination
    DESTINATION] [--schema SCHEMA] [dataset-name]
    {list,info,drop,show,row-counts,head} ...
```

**Description**

Commands to manage datasets for project.
Run without arguments to list all datasets in current project.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Positional arguments**
* `dataset-name` - Dataset name

**Options**
* `-h, --help` - Show this help message and exit
* `--project PROJECT` - Name or path to the dlt package with dlt.yml
* `--profile PROFILE` - Profile to use from the project configuration file
* `--destination DESTINATION` - Destination name, if many allowed
* `--schema SCHEMA` - Limits to schema with name for multi-schema datasets

**Available subcommands**
* [`list`](#dlt-dataset-list) - List datasets
* [`info`](#dlt-dataset-info) - Dataset info
* [`drop`](#dlt-dataset-drop) - Drops the dataset and all data in it
* [`show`](#dlt-dataset-show) - Shows the content of dataset in streamlit
* [`row-counts`](#dlt-dataset-row-counts) - Display the row counts of all tables in the dataset
* [`head`](#dlt-dataset-head) - Display the first x rows of a table, defaults to 5

</details>

### `dlt dataset list`

List Datasets.

**Usage**
```sh
dlt dataset [dataset-name] list [-h]
```

**Description**

List Datasets.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt dataset`](#dlt-dataset).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt dataset info`

Dataset info.

**Usage**
```sh
dlt dataset [dataset-name] info [-h]
```

**Description**

Dataset info.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt dataset`](#dlt-dataset).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt dataset drop`

Drops the dataset and all data in it.

**Usage**
```sh
dlt dataset [dataset-name] drop [-h]
```

**Description**

Drops the dataset and all data in it.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt dataset`](#dlt-dataset).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt dataset show`

Shows the content of dataset in Streamlit.

**Usage**
```sh
dlt dataset [dataset-name] show [-h]
```

**Description**

Shows the content of dataset in Streamlit.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt dataset`](#dlt-dataset).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt dataset row-counts`

Display the row counts of all tables in the dataset.

**Usage**
```sh
dlt dataset [dataset-name] row-counts [-h]
```

**Description**

Display the row counts of all tables in the dataset.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt dataset`](#dlt-dataset).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt dataset head`

Display the first x rows of a table, defaults to 5.

**Usage**
```sh
dlt dataset [dataset-name] head [-h] [--limit LIMIT] table_name
```

**Description**

Display the first x rows of a table, defaults to 5.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt dataset`](#dlt-dataset).

**Positional arguments**
* `table_name` - Table name

**Options**
* `-h, --help` - Show this help message and exit
* `--limit LIMIT` - Number of rows to display

</details>

## `dlt cache`

Manage dlt+ project local data cache. Experimental.

**Usage**
```sh
dlt cache [-h] [--project PROJECT] [--profile PROFILE]
    {info,show,drop,populate,flush,create-persistent-secrets,clear-persistent-secrets}
    ...
```

**Description**

Commands to manage local data cache for dlt+ project.

**This is an experimental feature and will change substantially in the future.**

**Do not use in production.**.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Options**
* `-h, --help` - Show this help message and exit
* `--project PROJECT` - Name or path to the dlt package with dlt.yml
* `--profile PROFILE` - Profile to use from the project configuration file

**Available subcommands**
* [`info`](#dlt-cache-info) - Shows cache info
* [`show`](#dlt-cache-show) - Connects to cache engine
* [`drop`](#dlt-cache-drop) - Drop the cache
* [`populate`](#dlt-cache-populate) - Populate the cache from the defined inputs
* [`flush`](#dlt-cache-flush) - Flush the cache to the defined outputs
* [`create-persistent-secrets`](#dlt-cache-create-persistent-secrets) - Create persistent secrets on cache for remote access.
* [`clear-persistent-secrets`](#dlt-cache-clear-persistent-secrets) - Clear persistent secrets from cache for remote access.

</details>

### `dlt cache info`

Shows cache info.

**Usage**
```sh
dlt cache info [-h]
```

**Description**

Shows cache info.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt cache`](#dlt-cache).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt cache show`

Connects to cache engine.

**Usage**
```sh
dlt cache show [-h]
```

**Description**

Connects to cache engine.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt cache`](#dlt-cache).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt cache drop`

Drop the cache.

**Usage**
```sh
dlt cache drop [-h]
```

**Description**

Drop the cache.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt cache`](#dlt-cache).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt cache populate`

Populate the cache from the defined inputs.

**Usage**
```sh
dlt cache populate [-h]
```

**Description**

Populate the cache from the defined inputs.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt cache`](#dlt-cache).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt cache flush`

Flush the cache to the defined outputs.

**Usage**
```sh
dlt cache flush [-h]
```

**Description**

Flush the cache to the defined outputs.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt cache`](#dlt-cache).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt cache create-persistent-secrets`

Create persistent secrets on cache for remote access.

**Usage**
```sh
dlt cache create-persistent-secrets [-h]
```

**Description**

Create persistent secrets on cache for remote access.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt cache`](#dlt-cache).

**Options**
* `-h, --help` - Show this help message and exit

</details>

### `dlt cache clear-persistent-secrets`

Clear persistent secrets from cache for remote access.

**Usage**
```sh
dlt cache clear-persistent-secrets [-h]
```

**Description**

Clear persistent secrets from cache for remote access.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt cache`](#dlt-cache).

**Options**
* `-h, --help` - Show this help message and exit

</details>

## `dlt telemetry`

Shows telemetry status.

**Usage**
```sh
dlt telemetry [-h]
```

**Description**

The `dlt telemetry` command shows the current status of dlt telemetry. Lern more about telemetry and what we send in our telemetry docs.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Options**
* `-h, --help` - Show this help message and exit

</details>

## `dlt schema`

Shows, converts and upgrades schemas.

**Usage**
```sh
dlt schema [-h] [--format {json,yaml}] [--remove-defaults] file
```

**Description**

The `dlt schema` command will load, validate and print out a dlt schema: `dlt schema path/to/my_schema_file.yaml`.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Positional arguments**
* `file` - Schema file name, in yaml or json format, will autodetect based on extension

**Options**
* `-h, --help` - Show this help message and exit
* `--format {json,yaml}` - Display schema in this format
* `--remove-defaults` - Does not show default hint values

</details>

## `dlt init`

Creates a pipeline project in the current folder by adding existing verified source or creating a new one from template.

**Usage**
```sh
dlt init [-h] [--list-sources] [--location LOCATION] [--branch BRANCH] [--eject]
    [source] [destination]
```

**Description**

The `dlt init` command creates a new dlt pipeline script that loads data from `source` to `destination`. When you run the command, several things happen:

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

Inherits arguments from [`dlt`](#dlt).

**Positional arguments**
* `source` - Name of data source for which to create a pipeline. adds existing verified source or creates a new pipeline template if verified source for your data source is not yet implemented.
* `destination` - Name of a destination ie. bigquery or redshift

**Options**
* `-h, --help` - Show this help message and exit
* `--list-sources, -l` - Shows all available verified sources and their short descriptions. for each source, it checks if your local `dlt` version requires an update and prints the relevant warning.
* `--location LOCATION` - Advanced. uses a specific url or local path to verified sources repository.
* `--branch BRANCH` - Advanced. uses specific branch of the verified sources repository to fetch the template.
* `--eject` - Ejects the source code of the core source like sql_database or rest_api so they will be editable by you.

</details>

## `dlt render-docs`

Renders markdown version of cli docs.

**Usage**
```sh
dlt render-docs [-h] [--compare] file_name
```

**Description**

The `dlt render-docs` command renders markdown version of cli docs by parsing the argparse help output and generating a markdown file.
If you are reading this on the docs website, you are looking at the rendered version of the cli docs generated by this command.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Positional arguments**
* `file_name` - Output file name

**Options**
* `-h, --help` - Show this help message and exit
* `--compare` - Compare the changes and raise if output would be updated

</details>

## `dlt deploy`

Creates a deployment package for a selected pipeline script.

**Usage**
```sh
dlt deploy [-h] pipeline-script-path
```

**Description**

The `dlt deploy` command prepares your pipeline for deployment and gives you step-by-step instructions on how to accomplish it. To enable this functionality, please first execute `pip install "dlt[cli]"` which will add additional packages to the current environment.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Positional arguments**
* `pipeline-script-path` - Path to a pipeline script

**Options**
* `-h, --help` - Show this help message and exit

</details>

