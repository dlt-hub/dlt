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
then run `dlt init` with a [source](./glossary.md#source) name of your choice and one of the three
[destination](./destinations.md) options. The optional `--generic` flag will provide a more complex
example, which can be used to speed up implementation if you have built `dlt` pipelines before.

If you don't want to start with a generic template, then try out the chess.com API to Google BigQuery
complete example by running `dlt init chess bigquery`.

Follow the [Create a pipeline](./walkthroughs/create-a-pipeline.md) walkthrough to learn more.

## `dlt deploy`

```
dlt deploy <script>.py github-action --schedule "*/30 * * * *"
```

[GitHub Actions](https://github.com/features/actions) are the only supported deployment method at the moment.
You need to specify when the GitHub Action should run using a [cron schedule expression](https://crontab.guru/). The command also takes additional flags: `--run-on-push` (default is False) and `--run-manually` (default is True). Remember to put the cron schedule into quotation marks as in the example above.

For the chess.com API example above, you could deploy it with `dlt deploy chess.py github-action --schedule "*/30 * * * *"`.

Follow the [Deploy a pipeline](./walkthroughs/deploy-a-pipeline.md) walkthrough to learn more.

## `dlt pipeline`

Use this command to inspect the local pipeline state, tables and data in the destination and check for problems with the data loading.

### Show tables and data in the destination

```
dlt pipeline <pipeline name> show
```

Generates and launches a simple [Streamlit](https://streamlit.io/) app that you can use to inspect the schemas and data in the destination as well as your pipeline state and loading status / stats. Should be executed from the same folder, from which you ran the pipeline script to access destination credentials. Requires `streamlit` to be installed.

### Get the pipeline information

```
dlt pipeline <pipeline name> info
```

Displays current state of the pipeline: dataset name, destination, list of schemas, list of completed and normalized load packages, and optionally a state set by the resources during extraction process.

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

### Sync pipeline with the destintion

```
dlt pipeline <pipeline name> trace
```

This command will remove pipeline working folder with all pending packages, not synchronized state changes and schemas and retrieve the last synchronized data from the destination. If you drop the dataset the pipeline is loading to, this command results in a complete reset of pipeline state.

### List all pipelines on the local machine
```
dlt pipeline --list-pipelines
```
This command lists all the pipelines executed on the local machine with their working data in the default pipelines folder.