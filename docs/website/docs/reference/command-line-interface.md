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
    {telemetry,schema,pipeline,init,render-docs,deploy,dashboard,ai} ...
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
* [`telemetry`](#dlt-telemetry) - Shows telemetry status
* [`schema`](#dlt-schema) - Shows, converts and upgrades schemas
* [`pipeline`](#dlt-pipeline) - Operations on pipelines that were ran locally
* [`init`](#dlt-init) - Creates a pipeline project in the current folder by adding existing verified source or creating a new one from template.
* [`render-docs`](#dlt-render-docs) - Renders markdown version of cli docs
* [`deploy`](#dlt-deploy) - Creates a deployment package for a selected pipeline script
* [`dashboard`](#dlt-dashboard) - Starts the dlt workspace dashboard
* [`ai`](#dlt-ai) - Use ai-powered development tools and utilities

</details>

## `dlt telemetry`

Shows telemetry status.

**Usage**
```sh
dlt telemetry [-h]
```

**Description**

The `dlt telemetry` command shows the current status of dlt telemetry. Learn more about telemetry and what we send in our telemetry docs.

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
dlt schema [-h] [--format {json,yaml,dbml,dot}] [--remove-defaults] file
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
* `--format {json,yaml,dbml,dot}` - Display schema in this format
* `--remove-defaults` - Does not show default hint values

</details>

## `dlt pipeline`

Operations on pipelines that were ran locally.

**Usage**
```sh
dlt pipeline [-h] [--list-pipelines] [--pipelines-dir PIPELINES_DIR] [--verbose]
    [pipeline_name]
    {info,show,failed-jobs,drop-pending-packages,sync,trace,schema,drop,load-package,mcp}
    ...
```

**Description**

The `dlt pipeline` command provides a set of commands to inspect the pipeline working directory, tables, and data in the destination and check for problems encountered during data loading.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Positional arguments**
* `pipeline_name` - Pipeline name

**Options**
* `-h, --help` - Show this help message and exit
* `--list-pipelines, -l` - List local pipelines
* `--pipelines-dir PIPELINES_DIR` - Pipelines working directory
* `--verbose, -v` - Provides more information for certain commands.

**Available subcommands**
* [`info`](#dlt-pipeline-info) - Displays state of the pipeline, use -v or -vv for more info
* [`show`](#dlt-pipeline-show) - Generates and launches workspace dashboard with the loading status and dataset explorer
* [`failed-jobs`](#dlt-pipeline-failed-jobs) - Displays information on all the failed loads in all completed packages, failed jobs and associated error messages
* [`drop-pending-packages`](#dlt-pipeline-drop-pending-packages) - Deletes all extracted and normalized packages including those that are partially loaded.
* [`sync`](#dlt-pipeline-sync) - Drops the local state of the pipeline and resets all the schemas and restores it from destination. the destination state, data and schemas are left intact.
* [`trace`](#dlt-pipeline-trace) - Displays last run trace, use -v or -vv for more info
* [`schema`](#dlt-pipeline-schema) - Displays default schema
* [`drop`](#dlt-pipeline-drop) - Selectively drop tables and reset state
* [`load-package`](#dlt-pipeline-load-package) - Displays information on load package, use -v or -vv for more info
* [`mcp`](#dlt-pipeline-mcp) - Launch mcp server attached to this pipeline in sse transport mode

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

Generates and launches workspace dashboard with the loading status and dataset explorer.

**Usage**
```sh
dlt pipeline [pipeline_name] show [-h] [--streamlit] [--edit]
```

**Description**

Launches the workspace dashboard with a comprehensive interface to inspect the pipeline state, schemas, and data in the destination.

This dashboard should be executed from the same folder from which you ran the pipeline script to be able access destination credentials.

If the --edit flag is used, will launch the editable version of the dashboard if it exists in the current directory, or create this version and launch it in edit mode.

Requires `marimo` to be installed in the current environment: `pip install marimo`. Use the --streamlit flag to launch the legacy streamlit app.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt pipeline`](#dlt-pipeline).

**Options**
* `-h, --help` - Show this help message and exit
* `--streamlit` - Launch the legacy streamlit dashboard instead of the new workspace dashboard.
* `--edit` - Creates editable version of workspace dashboard in current directory if it does not exist there yet and launches it in edit mode. will have no effect when using the streamlit flag.

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
dlt pipeline [pipeline_name] schema [-h] [--format {json,yaml,dbml,dot}]
    [--remove-defaults]
```

**Description**

Displays the default schema for the selected pipeline.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt pipeline`](#dlt-pipeline).

**Options**
* `-h, --help` - Show this help message and exit
* `--format {json,yaml,dbml,dot}` - Display schema in this format
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

### `dlt pipeline mcp`

Launch MCP server attached to this pipeline in SSE transport mode.

**Usage**
```sh
dlt pipeline [pipeline_name] mcp [-h] [--stdio] [--port PORT]
```

**Description**

This MCP facilitates schema and data exploration for the dataset created with this pipeline.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt pipeline`](#dlt-pipeline).

**Options**
* `-h, --help` - Show this help message and exit
* `--stdio` - Use stdio transport mode
* `--port PORT` - Sse port to use (default: 43656)

</details>

## `dlt init`

Creates a pipeline project in the current folder by adding existing verified source or creating a new one from template.

**Usage**
```sh
dlt init [-h] [--list-sources] [--list-destinations] [--location LOCATION]
    [--branch BRANCH] [--eject] [source] [destination]
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
* `--list-destinations` - Shows the name of all core dlt destinations.
* `--location LOCATION` - Advanced. uses a specific url or local path to verified sources repository.
* `--branch BRANCH` - Advanced. uses specific branch of the verified sources repository to fetch the template.
* `--eject` - Ejects the source code of the core source like sql_database or rest_api so they will be editable by you.

</details>

## `dlt render-docs`

Renders markdown version of cli docs.

**Usage**
```sh
dlt render-docs [-h] [--commands [COMMANDS ...]] [--compare] file_name
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
* `--commands [COMMANDS ...]` - List of command names to render (optional)
* `--compare` - Compare the changes and raise if output would be updated

</details>

## `dlt deploy`

Creates a deployment package for a selected pipeline script.

**Usage**
```sh
dlt deploy [-h] pipeline-script-path {github-action,airflow-composer} ...
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

**Available subcommands**
* [`github-action`](#dlt-deploy-github-action) - Deploys the pipeline to github actions
* [`airflow-composer`](#dlt-deploy-airflow-composer) - Deploys the pipeline to airflow

</details>

### `dlt deploy github-action`

Deploys the pipeline to Github Actions.

**Usage**
```sh
dlt deploy pipeline-script-path github-action [-h] [--location LOCATION]
    [--branch BRANCH] --schedule SCHEDULE [--run-manually] [--run-on-push]
```

**Description**

Deploys the pipeline to GitHub Actions.

GitHub Actions (https://github.com/features/actions) is a CI/CD runner with a large free tier which you can use to run your pipelines.

You must specify when the GitHub Action should run using a cron schedule expression. The command also takes additional flags:
`--run-on-push` (default is False) and `--run-manually` (default is True). Remember to put the cron
schedule expression in quotation marks.

For the chess.com API example from our docs, you can deploy it with `dlt deploy chess.py github-action --schedule "*/30 * * * *"`.

Follow the guide on how to deploy a pipeline with GitHub Actions in our documentation for more information.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt deploy`](#dlt-deploy).

**Options**
* `-h, --help` - Show this help message and exit
* `--location LOCATION` - Advanced. uses a specific url or local path to pipelines repository.
* `--branch BRANCH` - Advanced. uses specific branch of the deploy repository to fetch the template.
* `--schedule SCHEDULE` - A schedule with which to run the pipeline, in cron format. example: '*/30 * * * *' will run the pipeline every 30 minutes. remember to enclose the scheduler expression in quotation marks!
* `--run-manually` - Allows the pipeline to be run manually form github actions ui.
* `--run-on-push` - Runs the pipeline with every push to the repository.

</details>

### `dlt deploy airflow-composer`

Deploys the pipeline to Airflow.

**Usage**
```sh
dlt deploy pipeline-script-path airflow-composer [-h] [--location LOCATION]
    [--branch BRANCH] [--secrets-format {env,toml}]
```

**Description**

Google Composer (https://cloud.google.com/composer?hl=en) is a managed Airflow environment provided by Google. Follow the guide in our docs on how to deploy a pipeline with Airflow to learn more. This command will:


* create an Airflow DAG for your pipeline script that you can customize. The DAG uses
the `dlt` Airflow wrapper (https://github.com/dlt-hub/dlt/blob/devel/dlt/helpers/airflow_helper.py#L37) to make this process trivial.

* provide you with the environment variables and secrets that you must add to Airflow.

* provide you with a cloudbuild file to sync your GitHub repository with the `dag` folder of your Airflow Composer instance.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt deploy`](#dlt-deploy).

**Options**
* `-h, --help` - Show this help message and exit
* `--location LOCATION` - Advanced. uses a specific url or local path to pipelines repository.
* `--branch BRANCH` - Advanced. uses specific branch of the deploy repository to fetch the template.
* `--secrets-format {env,toml}` - Format of the secrets

</details>

## `dlt dashboard`

Starts the dlt workspace dashboard.

**Usage**
```sh
dlt dashboard [-h] [--pipelines-dir PIPELINES_DIR] [--edit]
```

**Description**

The `dlt dashboard` command starts the dlt workspace dashboard. You can use the dashboard:

* to list and inspect local pipelines
* browse the full pipeline schema and all hints
* browse the data in the destination
* inspect the pipeline state.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Options**
* `-h, --help` - Show this help message and exit
* `--pipelines-dir PIPELINES_DIR` - Pipelines working directory
* `--edit` - Eject dashboard and start editable version

</details>

## `dlt ai`

Use AI-powered development tools and utilities.

**Usage**
```sh
dlt ai [-h] {setup} ...
```

**Description**

The `dlt ai` command provides commands to configure your LLM-enabled IDE and MCP server.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt`](#dlt).

**Options**
* `-h, --help` - Show this help message and exit

**Available subcommands**
* [`setup`](#dlt-ai-setup) - Generate ide-specific configuration and rules files

</details>

### `dlt ai setup`

Generate IDE-specific configuration and rules files.

**Usage**
```sh
dlt ai setup [-h] [--location LOCATION] [--branch BRANCH]
    {amp,codex,claude,cody,cline,cursor,continue,windsurf,copilot}
```

**Description**

Get AI rules files and configuration into your local project for the selected IDE.
Files are fetched from https://github.com/dlt-hub/verified-sources by default.

<details>

<summary>Show Arguments and Options</summary>

Inherits arguments from [`dlt ai`](#dlt-ai).

**Positional arguments**

**Options**
* `-h, --help` - Show this help message and exit
* `--location LOCATION` - Advanced. specify git url or local path to rules files and config.
* `--branch BRANCH` - Advanced. specify git branch to fetch rules files and config.

</details>

