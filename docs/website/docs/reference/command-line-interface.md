---
title: Command Line Interface
description: Command Line Interface (CLI) of dlt
keywords: [command line interface, cli, dlt init]
---

# Command Line Interface

## `dlt init`

```shell
dlt init <source> <destination>
```
This command creates new dlt pipeline script that loads data from `source` to `destination` to it. When you run the command:
1. It creates basic project structure if the current folder is empty. Adds `.dlt/config.toml` and `.dlt/secrets.toml` and `.gitignore` files.
2. It checks if `source` argument is matching one of our [verified sources](../dlt-ecosystem/verified-sources/) and if it is so, [it adds it to the project](../walkthroughs/add-a-verified-source.md).
3. If the `source` is unknown it will use a [generic template](https://github.com/dlt-hub/python-dlt-init-template) to [get you started](../walkthroughs/create-a-pipeline.md).
4. It will rewrite the pipeline scripts to use your `destination`.
5. It will create sample config and credentials in `secrets.toml` and `config.toml` for the specified source and destination.
6. It will create `requirements.txt` with dependencies required by source and destination. If one exists, it will print instructions what to add to it.

This command can be used several times in the same folders to add more sources, destinations and pipelines. It will also update the verified source code to the newest
version if run again with existing `source` name. You are warned if files will be overwritten or if `dlt` version needs upgrade to run particular pipeline.

### Specify your own "verified sources" repository.
You can use `--location <repo_url or local folder>` option to specify your own repository with sources. Typically you would [fork ours](https://github.com/dlt-hub/verified-sources) and start customizing and adding sources ie. to use them for your team or organization. You can also specify a branch with `--branch <name>` ie. to test a version being developed.

### List all verified sources
```shell
dlt init --list-verified-sources
```
Shows all available verified sources and their short descriptions. For each source, checks if your local `dlt` version requires update
and prints the relevant warning.

## `dlt deploy`
This command prepares your pipeline for deployment and gives you step by step instruction how to accomplish it. To enabled this functionality please first execute
```sh
pip install "dlt[cli]"
```
that will add additional packages to current environment.

> 💡 We ask you to install those dependencies separately to keep our core library small and make it work everywhere.

### github-action

```shell
dlt deploy <script>.py github-action --schedule "*/30 * * * *"
```

[GitHub Actions](https://github.com/features/actions) is a CI/CD runner that you can use basically
for free.

You need to specify when the GitHub Action should run using a
[cron schedule expression](https://crontab.guru/). The command also takes additional flags:
`--run-on-push` (default is False) and `--run-manually` (default is True). Remember to put the cron
schedule into quotation marks as in the example above.

For the chess.com API example above, you could deploy it with
`dlt deploy chess.py github-action --schedule "*/30 * * * *"`.

Follow the guide on [how to deploy a pipeline with Github Actions](../walkthroughs/deploy-a-pipeline/deploy-with-github-actions) to learn more.

### airflow-composer

```shell
dlt deploy <script>.py airflow-composer
```

[Google Composer](https://cloud.google.com/composer?hl=en) is a managed Airflow environment provided by Google.

Follow the guide on [how to deploy a pipeline with Airflow](../walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer) to learn more.

It will create an Airflow DAG for your pipeline script that you should customize. The DAG is using
`dlt` [Airflow wrapper](https://github.com/dlt-hub/dlt/blob/devel/dlt/helpers/airflow_helper.py#L37) to make this process trivial.

It displays the environment variables with secrets you must add to the Airflow.

You'll also get a cloudbuild file to sync the github repository with the `dag` folder of your
Airflow Composer instance.

> 💡 The command target Composer users but generated DAG and instructions will work with any Airflow
> instance.

## `dlt pipeline`

Use this command to inspect the pipeline working directory, tables and data in the destination and
check for problems with the data loading.

### Show tables and data in the destination

```shell
dlt pipeline <pipeline name> show
```

Generates and launches a simple [Streamlit](https://streamlit.io/) app that you can use to inspect
the schemas and data in the destination as well as your pipeline state and loading status / stats.
Should be executed from the same folder, from which you ran the pipeline script to access
destination credentials. Requires `streamlit` to be installed.

### Get the pipeline information

```shell
dlt pipeline <pipeline name> info
```

Displays content of the working directory of the pipeline: dataset name, destination, list of
schemas, resources in schemas, list of completed and normalized load packages, and optionally a
pipeline state set by the resources during extraction process.

### Get the load package information

```shell
dlt pipeline <pipeline name> load-package <load id>
```

Shows information on a load package with given `load_id`. The `load_id` parameter defaults to the
most recent package. Package information includes its state (`COMPLETED/PROCESSED`) and list of all
jobs in a package with their statuses, file sizes, types and in case of failed jobs—the error
messages from the destination. With verbose flag set `dlt pipeline -v ...`, you can also see the
list of all tables and columns created at the destination during loading of that package.

### List all failed jobs

```shell
dlt pipeline <pipeline name> failed-jobs
```

This commands scans all the load packages looking for failed jobs and then displays information on
files that got loaded and the failure message from the destination.

### Get the last run trace

```shell
dlt pipeline <pipeline name> trace
```

Displays the trace of last pipeline run containing the start data of the run, elapsed time and the
same information for all the steps (`extract`, `normalize` and `load`). If any of the steps failed,
you'll see message of the exceptions that caused that problem. Successful `load` and `run` steps
will display the [load info](walkthroughs/run-a-pipeline.md) instead.

### Sync pipeline with the destination

```shell
dlt pipeline <pipeline name> sync
```

This command will remove pipeline working directory with all pending packages, not synchronized
state changes and schemas and retrieve the last synchronized data from the destination. If you drop
the dataset the pipeline is loading to, this command results in a complete reset of pipeline state.

In case of a pipeline without working directory, the command may be used to create one from the
destination. In order to do that you need to pass the dataset name and destination name to the CLI
and provide the credentials to connect to destination (ie. in `.dlt/secrets.toml`) placed in the
folder where you execute the `pipeline sync` command.

### Selectively drop tables and reset state

```sh
dlt pipeline <pipeline name> drop [resource_1] [resource_2]
```

Drops tables generated by selected resources and resets the state associated with them. Mainly used
to force a full refresh on selected tables. In example below we drop all tables generated by
`repo_events` resource in github pipeline:

```sh
dlt pipeline github_events drop repo_events
```

`dlt` will inform you on the names of dropped tables and the resource state slots that will be
reset:

```
About to drop the following data in dataset airflow_events_1 in destination dlt.destinations.duckdb:
Selected schema:: github_repo_events
Selected resource(s):: ['repo_events']
Table(s) to drop:: ['issues_event', 'fork_event', 'pull_request_event', 'pull_request_review_event', 'pull_request_review_comment_event', 'watch_event', 'issue_comment_event', 'push_event__payload__commits', 'push_event']
Resource(s) state to reset:: ['repo_events']
Source state path(s) to reset:: []
Do you want to apply these changes? [y/N]
```

As a result of the command above:

1. All the indicated tables will be dropped in the destination. Note that `dlt` drops the child
   tables as well.
1. All the indicated tables will be removed from the indicated schema.
1. The state for the resource `repo_events` was found and will be reset.
1. New schema and state will be stored in the destination.

The `drop` command accepts several advanced settings:

1. You can use regexes to select resources. Prepend `re:` string to indicate regex pattern. Example
   below will select all resources starting with `repo`:

```sh
dlt pipeline github_events drop "re:^repo"
```

2. You can drop all tables in indicated schema:

```sh
dlt pipeline chess drop --drop-all
```

3. You can indicate additional state slots to reset by passing JsonPath to source state. In example
   below we reset the `archives` slot in source state:

```sh
dlt pipeline chess_pipeline drop --state-paths archives
```

This will select the `archives` key in `chess` source:

```json
sources:{
  "chess": {
    "archives": [
      "https://api.chess.com/pub/player/magnuscarlsen/games/2022/05",
    ]
  }
}
```

> ❗ This command is still **experimental** and the interface will most probably change. Resetting
> the resource state assumes that the `dlt` state layout is followed.

### List all pipelines on the local machine

```shell
dlt pipeline --list-pipelines
```

This command lists all the pipelines executed on the local machine with their working data in the
default pipelines folder.

### Drop pending and partially loaded packages
```sh
dlt pipeline <pipeline name> drop-pending-packages
```
Removes all extracted and normalized packages in the pipeline's working dir.
`dlt` keeps extracted and normalized load packages in pipeline working directory. When `run` method is called, it will attempt to normalize and load
pending packages first. The command above removes such packages. Note that **pipeline state** is not reverted to the state at which the deleted package
were created. Use `dlt pipeline ... sync` is recommended if your destination supports state sync.


## Show stack traces
If the command fails and you want to see the full stack trace add `--debug` just after `dlt` executable.
```sh
dlt --debug pipeline github info
```
