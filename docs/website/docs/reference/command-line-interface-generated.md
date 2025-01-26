---
title: Full CLI Reference
description: Command line interface (CLI) of dlt
keywords: [command line interface, cli, dlt init]
---
# Full CLI Reference

This page contains all commands available in the dlt CLI if dlt+ is installed and is generated
automatically from the fully populated python argparse object of dlt.

:::note
Flags and positional commands are inherited from the parent command. Position within the command string
is important. For example if you want to enable debug mode on the pipeline command, you need to add the
debug flag to the base dlt command:

```bash
dlt --debug pipeline
```

Adding the flag after the pipeline keyword will not work.
:::

## `dlt`

Creates, adds, inspects and deploys dlt pipelines.

**Usage**
```bash
dlt [-h] [--version] [--disable-telemetry] [--enable-telemetry]
    [--non-interactive] [--debug]
    {telemetry,schema,pipeline,init,render-docs,deploy} ...
```

**Options**
```sh
-h, --help            show this help message and exit
--version             show program's version number and exit
--disable-telemetry   Disables telemetry before command is executed
--enable-telemetry    Enables telemetry before command is executed
--non-interactive     Non interactive mode. Default choices are automatically
                    made for confirmations and prompts.
--debug               Displays full stack traces on exceptions.
```

**Available subcommands**
```sh
{telemetry,schema,pipeline,init,render-docs,deploy}
  telemetry           Shows telemetry status
  schema              Shows, converts and upgrades schemas
  pipeline            Operations on pipelines that were ran locally
  init                Creates a pipeline project in the current folder by adding
                    existing verified source or creating a new one from
                    template.
  render-docs         Renders markdown version of cli docs
  deploy              Creates a deployment package for a selected pipeline
                    script
```

## `dlt telemetry`

Shows telemetry status

**Usage**
```bash
dlt telemetry [-h]
```

**Options**
```sh
-h, --help  show this help message and exit
```

## `dlt schema`

Shows, converts and upgrades schemas

**Usage**
```bash
dlt schema [-h] [--format {json,yaml}] [--remove-defaults] file
```

**Positional arguments**
```sh
file                  Schema file name, in yaml or json format, will autodetect
                    based on extension
```

**Options**
```sh
-h, --help            show this help message and exit
--format {json,yaml}  Display schema in this format
--remove-defaults     Does not show default hint values
```

## `dlt pipeline`

Operations on pipelines that were ran locally

**Usage**
```bash
dlt pipeline [-h] [--list-pipelines] [--hot-reload] [--pipelines-dir
    PIPELINES_DIR] [--verbose] [pipeline_name]
    {info,show,failed-jobs,drop-pending-packages,sync,trace,schema,drop,load-package}
    ...
```

**Positional arguments**
```sh
pipeline_name         Pipeline name
```

**Options**
```sh
-h, --help            show this help message and exit
--list-pipelines, -l  List local pipelines
--hot-reload          Reload streamlit app (for core development)
--pipelines-dir PIPELINES_DIR
                      Pipelines working directory
--verbose, -v         Provides more information for certain commands.
```

**Available subcommands**
```sh
{info,show,failed-jobs,drop-pending-packages,sync,trace,schema,drop,load-package}
  info                Displays state of the pipeline, use -v or -vv for more
                    info
  show                Generates and launches Streamlit app with the loading
                    status and dataset explorer
  failed-jobs         Displays information on all the failed loads in all
                    completed packages, failed jobs and associated error
                    messages
  drop-pending-packages
                      Deletes all extracted and normalized packages including
                    those that are partially loaded.
  sync                Drops the local state of the pipeline and resets all the
                    schemas and restores it from destination. The destination
                    state, data and schemas are left intact.
  trace               Displays last run trace, use -v or -vv for more info
  schema              Displays default schema
  drop                Selectively drop tables and reset state
  load-package        Displays information on load package, use -v or -vv for
                    more info
```

### `dlt pipeline info`

Displays state of the pipeline, use -v or -vv for more info

**Usage**
```bash
dlt pipeline [pipeline_name] info [-h]
```

**Options**
```sh
-h, --help  show this help message and exit
```

### `dlt pipeline show`

Generates and launches Streamlit app with the loading status and dataset explorer

**Usage**
```bash
dlt pipeline [pipeline_name] show [-h]
```

**Options**
```sh
-h, --help  show this help message and exit
```

### `dlt pipeline failed-jobs`

Displays information on all the failed loads in all completed packages, failed jobs and associated error messages

**Usage**
```bash
dlt pipeline [pipeline_name] failed-jobs [-h]
```

**Options**
```sh
-h, --help  show this help message and exit
```

### `dlt pipeline drop-pending-packages`

Deletes all extracted and normalized packages including those that are partially loaded.

**Usage**
```bash
dlt pipeline [pipeline_name] drop-pending-packages [-h]
```

**Options**
```sh
-h, --help  show this help message and exit
```

### `dlt pipeline sync`

Drops the local state of the pipeline and resets all the schemas and restores it from destination. The destination state, data and schemas are left intact.

**Usage**
```bash
dlt pipeline [pipeline_name] sync [-h] [--destination DESTINATION]
    [--dataset-name DATASET_NAME]
```

**Options**
```sh
-h, --help            show this help message and exit
--destination DESTINATION
                      Sync from this destination when local pipeline state is
                    missing.
--dataset-name DATASET_NAME
                      Dataset name to sync from when local pipeline state is
                    missing.
```

### `dlt pipeline trace`

Displays last run trace, use -v or -vv for more info

**Usage**
```bash
dlt pipeline [pipeline_name] trace [-h]
```

**Options**
```sh
-h, --help  show this help message and exit
```

### `dlt pipeline schema`

Displays default schema

**Usage**
```bash
dlt pipeline [pipeline_name] schema [-h] [--format {json,yaml}]
    [--remove-defaults]
```

**Options**
```sh
-h, --help            show this help message and exit
--format {json,yaml}  Display schema in this format
--remove-defaults     Does not show default hint values
```

### `dlt pipeline drop`

Selectively drop tables and reset state

**Usage**
```bash
dlt pipeline [pipeline_name] drop [-h] [--destination DESTINATION]
    [--dataset-name DATASET_NAME] [--drop-all] [--state-paths [STATE_PATHS ...]]
    [--schema SCHEMA_NAME] [--state-only] [resources ...]
```

**Positional arguments**
```sh
resources             One or more resources to drop. Can be exact resource
                    name(s) or regex pattern(s). Regex patterns must start with
                    re:
```

**Options**
```sh
-h, --help            show this help message and exit
--destination DESTINATION
                      Sync from this destination when local pipeline state is
                    missing.
--dataset-name DATASET_NAME
                      Dataset name to sync from when local pipeline state is
                    missing.
--drop-all            Drop all resources found in schema. Supersedes [resources]
                    argument.
--state-paths [STATE_PATHS ...]
                      State keys or json paths to drop
--schema SCHEMA_NAME  Schema name to drop from (if other than default schema).
--state-only          Only wipe state for matching resources without dropping
                    tables.
```

### `dlt pipeline load-package`

Displays information on load package, use -v or -vv for more info

**Usage**
```bash
dlt pipeline [pipeline_name] load-package [-h] [load-id]
```

**Positional arguments**
```sh
load-id     Load id of completed or normalized package. Defaults to the most
                    recent package.
```

**Options**
```sh
-h, --help  show this help message and exit
```

## `dlt init`

Creates a pipeline project in the current folder by adding existing verified source or creating a new one from template.

**Usage**
```bash
dlt init [-h] [--list-sources] [--location LOCATION] [--branch BRANCH] [--eject]
    [source] [destination]
```

**Positional arguments**
```sh
source               Name of data source for which to create a pipeline. Adds
                    existing verified source or creates a new pipeline template
                    if verified source for your data source is not yet
                    implemented.
destination          Name of a destination ie. bigquery or redshift
```

**Options**
```sh
-h, --help           show this help message and exit
--list-sources, -l   List available sources
--location LOCATION  Advanced. Uses a specific url or local path to verified
                    sources repository.
--branch BRANCH      Advanced. Uses specific branch of the init repository to
                    fetch the template.
--eject              Ejects the source code of the core source like sql_database
```

## `dlt render-docs`

Renders markdown version of cli docs

**Usage**
```bash
dlt render-docs [-h] [--compare] file_name
```

**Positional arguments**
```sh
file_name   Output file name
```

**Options**
```sh
-h, --help  show this help message and exit
--compare   Compare the changes and raise if output would be updated
```

## `dlt deploy`

Creates a deployment package for a selected pipeline script

**Usage**
```bash
dlt deploy [-h] pipeline-script-path {github-action,airflow-composer} ...
```

**Positional arguments**
```sh
pipeline-script-path  Path to a pipeline script
```

**Options**
```sh
-h, --help            show this help message and exit
```

**Available subcommands**
```sh
{github-action,airflow-composer}
  github-action       Deploys the pipeline to Github Actions
  airflow-composer    Deploys the pipeline to Airflow
```

### `dlt deploy github-action`

Deploys the pipeline to Github Actions

**Usage**
```bash
dlt deploy pipeline-script-path github-action [-h] [--location LOCATION]
    [--branch BRANCH] --schedule SCHEDULE [--run-manually] [--run-on-push]
```

**Options**
```sh
-h, --help           show this help message and exit
--location LOCATION  Advanced. Uses a specific url or local path to pipelines
                    repository.
--branch BRANCH      Advanced. Uses specific branch of the deploy repository to
                    fetch the template.
--schedule SCHEDULE  A schedule with which to run the pipeline, in cron format.
                    Example: '*/30 * * * *' will run the pipeline every 30
                    minutes. Remember to enclose the scheduler expression in
                    quotation marks!
--run-manually       Allows the pipeline to be run manually form Github Actions
                    UI.
--run-on-push        Runs the pipeline with every push to the repository.
```

### `dlt deploy airflow-composer`

Deploys the pipeline to Airflow

**Usage**
```bash
dlt deploy pipeline-script-path airflow-composer [-h] [--location LOCATION]
    [--branch BRANCH] [--secrets-format {env,toml}]
```

**Options**
```sh
-h, --help            show this help message and exit
--location LOCATION   Advanced. Uses a specific url or local path to pipelines
                    repository.
--branch BRANCH       Advanced. Uses specific branch of the deploy repository to
                    fetch the template.
--secrets-format {env,toml}
                      Format of the secrets
```

