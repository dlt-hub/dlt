---
title: Command Line Interface
description: Command Line Interface (CLI) of dlt
keywords: [command line interface, cli, dlt init]
---

# Command Line Interface

## `dlt init`

```
dlt init <source> <destination>
```

If you want to start from a [generic template](https://github.com/dlt-hub/python-dlt-init-template),
then run `dlt init` with a [source](../general-usage/glossary.md#source) name of your choice and one of the three
[destination](../general-usage/glossary.md#destination) options. The optional `--generic` flag will provide a more complex
example, which can be used to speed up implementation if you have built `dlt` pipelines before.

If you don't want to start with a generic template, then try out the chess.com API to Google BigQuery
complete example by running `dlt init chess bigquery`.

Follow the [Create a pipeline](../walkthroughs/create-a-pipeline.md) walkthrough to learn more.

## `dlt deploy`
### github-action

```
dlt deploy <script>.py github-action --schedule "*/30 * * * *"
```

[GitHub Actions](https://github.com/features/actions) is a CI/CD runner that you can use basically for free.

You need to specify when the GitHub Action should run using a [cron schedule expression](https://crontab.guru/). The command also takes additional flags: `--run-on-push` (default is False) and `--run-manually` (default is True). Remember to put the cron schedule into quotation marks as in the example above.

For the chess.com API example above, you could deploy it with `dlt deploy chess.py github-action --schedule "*/30 * * * *"`.

Follow the [Deploy a pipeline](../walkthroughs/deploy-a-pipeline/deploy-with-github-actions.md) walkthrough to learn more.

### airflow-composer

```
dlt deploy <script>.py airflow-composer --schedule "*/30 * * * *"
```

[Google Composer](../running-in-production/orchestrators/airflow-gcp-cloud-composer.md) is a managed Airflow deployment provided by Google.

This deployment type follows the same process as [github-action deployment](../walkthroughs/deploy-a-pipeline/deploy-with-github-actions.md).

It will create an Airflow DAG for your pipeline script that you should customize. The DAG is using `dlt` Airflow wrapper to make this process trivial.

It displays the environment variables with secrets you must add to the Airflow.

You'll also get a cloudbuild script to sync the github repository with the `dag` folder of your Airflow Composer instance.

> 💡 The command target Composer users but generated DAG and instructions will work with any Airflow instance.


## `dlt pipeline`

Use this command to inspect the pipeline working directory, tables and data in the destination and check for problems with the data loading.

### Show tables and data in the destination

```
dlt pipeline <pipeline name> show
```

Generates and launches a simple [Streamlit](https://streamlit.io/) app that you can use to inspect the schemas and data in the destination as well as your pipeline state and loading status / stats. Should be executed from the same folder, from which you ran the pipeline script to access destination credentials. Requires `streamlit` to be installed.

### Get the pipeline information

```
dlt pipeline <pipeline name> info
```

Displays content of the working directory of the pipeline: dataset name, destination, list of schemas, resources in schemas, list of completed and normalized load packages, and optionally a pipeline state set by the resources during extraction process.

### Get the load package information

```
dlt pipeline <pipeline name> load-package <load id>
```

Shows information on a load package with given `load_id`. The `load_id` parameter defaults to the most recent package. Package information includes its state (`COMPLETED/PROCESSED`) and list of all jobs in a package with their statuses, file sizes, types and in case of failed jobs—the error messages from the destination. With verbose flag set `dlt pipeline -v ...`, you can also see the list of all tables and columns created at the destination during loading of that package.

### List all failed jobs

```
dlt pipeline <pipeline name> failed-jobs
```

This commands scans all the load packages looking for failed jobs and then displays information on files that got loaded and the failure message from the destination.

### Get the last run trace

```
dlt pipeline <pipeline name> trace
```

Displays the trace of last pipeline run containing the start data of the run, elapsed time and the same information for all the steps (`extract`, `normalize` and `load`). If any of the steps failed, you'll see message of the exceptions that caused that problem. Successful `load` and `run` steps will display the [load info](walkthroughs/run-a-pipeline.md) instead.

### Sync pipeline with the destination

```
dlt pipeline <pipeline name> sync
```

This command will remove pipeline working directory with all pending packages, not synchronized state changes and schemas and retrieve the last synchronized data from the destination. If you drop the dataset the pipeline is loading to, this command results in a complete reset of pipeline state.

In case of a pipeline without working directory, the command may be used to create one from the destination. In order to do that you need to pass the dataset name and destination name to the CLI and provide the credentials to connect to destination (ie. in `.dlt/secrets.toml`) placed in the folder where you execute the `pipeline sync` command

### Selectively drop tables and reset state

```sh
dlt pipeline <pipeline name> drop [resource_1] [resource_2]
```
Drops tables generated by selected resources and resets the state associated with them. Mainly used to force a full refresh on selected tables. In example below we drop all tables generated by `repo_events` resource in github pipeline:
```sh
dlt pipeline github_events drop repo_events
```
`dlt` will inform you on the names of dropped tables and the resource state slots that will be reset.
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
1. all the indicated tables will be dropped in the destination. Note that `dlt` drops the child tables as well.
2. all the indicated tables will be removed from the indicated schema
3. The state for the resource `repo_events` was found and will be reset.
4. New schema and state will be stored in the destination.

The `drop` command accepts several advanced settings:
1. You can use regexes to select resources. Prepend `re:` string to indicate regex patten. Example below will select all resources starting with `repo`:
```sh
dlt pipeline github_events drop "re:^repo"
```
2. You can drop all tables in indicated schema
```sh
dlt pipeline chess drop --drop-all
```
3. You can indicate additional state slots to reset by passing JsonPath to source state. In example below we reset the `archives` slot in source state:
```sh
dlt pipeline chess_pipeline drop --state-paths archives
```
This will select the `archives` key in `chess` source:
```json
sources:
{
  "chess": {
    "archives": [
      "https://api.chess.com/pub/player/magnuscarlsen/games/2022/05",
    ]
  }
}
```
> ❗ This command is still **experimental** and the interface will most probably change. Resetting the resource state assumes that the `dlt` state layout is followed.


### List all pipelines on the local machine
```
dlt pipeline --list-pipelines
```
This command lists all the pipelines executed on the local machine with their working data in the default pipelines folder.