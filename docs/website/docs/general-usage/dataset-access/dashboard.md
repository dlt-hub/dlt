---
title: Inspect your pipeline with the pipeline dashboard
description: Open a comprehensive dashboard with information about your pipeline
keywords: [pipeline, schema, data, inspect]
---

# Inspect your pipeline with the pipeline dashboard

Once you have run a pipeline locally, you can launch a web app that displays detailed information about your pipeline. This app is built with the Marimo Python notebook framework. For this to work, you will need to have the `marimo` package installed.

:::tip
The pipeline dashboard app works with all destinations that are supported by our dataset. Vector databases are generally unsupported at this point; however, you can still inspect metadata such as run traces, schemas, and pipeline state.
:::

## Features

You can use the dashboard app to:

* Get an overview of the pipeline state, including whether the local state differs from the remote state on the destination
* Inspect all schemas of your pipeline, including tables, child tables, and columns, along with all column hints
* Inspect the incremental state of each resource
* Query the data from the attached destination
* Get information about exceptions encountered during the last run of the selected pipeline
* Inspect the full run trace, including which configs were found and where; the results of the extract, normalize, and load steps (with timing and row counts); and information about the execution context (dlt version, platform, etc.)
* See a history of load packages and associated table counts

You can even eject the code for the dashboard app into your current working directory and start editing it either in your code editor or in Marimo edit mode to create your own custom dashboard app!

## Prerequisites

To install Marimo, run the following command:
```sh
pip install marimo
```

## 

You can start the dashboard with an overview of all locally found pipelines with:

```sh
dlt dashboard
```

You can use the `show` [CLI command](../../reference/command-line-interface.md#dlt-pipeline-show)
with your pipeline name to directly jump to the dashboard page of this pipeline:

```sh
dlt pipeline {pipeline_name} show
```

Use the pipeline name you defined in your Python code with the `pipeline_name` argument. If you are unsure, you can use the `dlt pipeline --list` command to list all pipelines.

## Credentials

`dlt` will resolve your destination credentials from:
* `secrets.toml` and `config.toml` in the `.dlt` folder of the current working directory (CWD), which is the directory you started the dashboard from 
* `secrets.toml` and `config.toml` in the the global `dlt` folder at `~/.dlt`. 
* Environment variables

It is best to run the dashboard from the same folder where you ran your pipeline, or to keep your credentials in the global folder.

`dlt` will NOT be able to pick up any credentials that you have configured in your code, since the dlt dashboard app runs independent of any pipeline scripts you have.

## Using the dashboard

The dashboard app should mostly be self-explanatory. Go to the section that corresponds to your task and click the toggle to open and use it. The dashboard app also refreshes all data when a new local pipeline run is detected for your selected pipeline. You can switch between pipelines on your machine using the pipeline dropdown in the top-right.

## Creating your own pipeline dashboard

You can eject the code for the pipeline dashboard into your current working directory and start editing it to create a custom version that fits your needs. To do this, run the `show` command with the `--edit` flag:

```sh
dlt pipeline {pipeline_name} show --edit
# or for the overview
dlt dashboard --edit
```

This will copy the dashboard code to the local folder and start Marimo in edit mode. If a local copy already exists, it will not overwrite it but will start it in edit mode. Once you have the local version, you can also use the regular Marimo commands to run or edit this notebook. This way, you can maintain multiple versions of your dashboard or other Marimo apps in your project:

```sh
# this will run a local dashboard
marimo run dlt_dashboard.py

# this will run the marimo edit mode
marimo edit dlt_dashboard.py
```

## Further reading

If you are running `dlt` in Python interactively or in a notebook, read the [Accessing loaded data in Python](./dataset.md) guide.
