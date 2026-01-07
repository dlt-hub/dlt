---
title: Inspect your pipeline with the workspace dashboard
description: Open a comprehensive dashboard with information about your pipeline
keywords: [pipeline, schema, data, inspect]
---

# Inspect your pipeline with the workspace dashboard

Once you have run a pipeline locally, you can launch a web app that displays detailed information about your pipeline. This app is built with the Marimo Python notebook framework. For this to work, you will need to have the `dlt[workspace]` package installed.

:::tip
The workspace dashboard app works with all destinations that are supported by our dataset. Vector databases are generally unsupported at this point; however, you can still inspect metadata such as run traces, schemas, and pipeline state.
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

![Dashboard overview](https://storage.googleapis.com/dlt-blog-images/dashboard-overview.png)


## Prerequisites

You need to install dlt workspace using the following command:
```sh
pip install "dlt[workspace]"
```

## 

You can start the dashboard with an overview of all locally found pipelines with:

```sh
dlt dashboard
```

You can use the `show` [CLI command](../reference/command-line-interface.md#dlt-pipeline-show)
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

The following sections are available:


### Pipeline Info

The overview section gives you a general sense of the state of your pipeline and will also display exception information if the last run of your pipeline failed. Review last executed to ensure the pipeline is running on the expected schedule or has recently completed a load.

![Pipeline Info](https://storage.googleapis.com/dlt-blog-images/dashboard-pipeline-overview.png)

The field **last executed** tells you the timestamp of the last successful pipeline run which is beneficial for debugging, **credentials** contains the full path or connection string used to connect to the destination and **schemas** lists the schemas associated with the pipeline.

#### Remote state

This section queries and displays the state information that has been successfully committed to the **destination**, confirming what is stored outside the local working directory.

* The `state_version` shown here should generally match the `state_version` in the Pipeline Overview if the last run was successful.
* Click `Load and show remote state` to query the destination and retrieve this information.

### Schema explorer

The schema browser allows you to dive deep into your pipeline's schemas and see all tables, columns, and type hints. This view is based on the dlt schema, the internal blueprint used to create and update tables in your destination.

When you're loading your data for the first time, we suggest checking the following:

1. Click `Show child tables` to ensure that nested data is unnested successfully and at the required level.
2. Inspect the **type hints** specifically, making sure that timestamps and large numbers are captured in the right way.
3. Raw Schema as YAML can be exported and stored in your git repository as a reference for the dataset schema.

![Schema explorer](https://storage.googleapis.com/dlt-blog-images/dashboard-schema-explorer.png)

### Dataset Browser

The dataset browser allows you to query data from your destination and inspect the state of each of your resources, which contain information about their incremental status and other custom state values. Click the `Load row counts` button to refresh the record counts for all tables based on the current destination data.

![Dataset Browser](https://storage.googleapis.com/dlt-blog-images/dashboard-dataset-browser.png)

### Querying your Dataset

Here you can query your data to analyze it. This is what a query on the fruitshop dataset looks like:

![Querying Data](https://storage.googleapis.com/dlt-blog-images/dashboard-query.png)

All query results are cached. The **Query History** shows previous runs that benefit from this cache. Click `Clear cache` to force a fresh execution against the destination. Queries are executed directly on your destination (e.g., DuckDB).

### Pipeline state

This section displays a raw, JSON view of the currently stored pipeline state. This state is critical for tracking progress, especially for incremental loading logic.

![Pipeline state](https://storage.googleapis.com/dlt-blog-images/dashboard-state.png)

### Last run trace

This section provides a detailed overview of the most recent run for the selected pipeline. It is crucial for performance tuning and debugging execution issues.

#### Trace Overview
This is a summary of the entire run. It shows the full duration and start and end times you can verify. 

#### Execution Context
This section details the environment in which the pipeline was executed, including Python and dlt versions and details like operating system (os), CPU count (cpu), and whether it was a CI run (ci_run).

#### Steps Overview

This table breaks down the total duration into the three phases of a dlt pipeline run: extract, normalize, and load.
- **Bottleneck Check:** Use the duration column to identify performance bottlenecks.
    - A long extract time suggests a slow source.
    - Long normalize or load times often point to destination performance or data complexity issues.

**Deep Dive:** You can click on each step to see more specific details like table names, item counts, file sizes, and timestamps for that specific phase.

#### Resolved Config Values
This section lists the configuration values that were resolved and used during this pipeline run. Use this to ensure your intended config values were correctly picked up, or to find errors where configuration you set up was not used. The values of the resolved configs are not displayed for security reasons.

#### Raw Trace
Clicking Show displays the complete trace information (including all sections above) as a raw JSON payload, which can be viewed and downloaded for advanced analysis or error reporting.

![Last run trace](https://storage.googleapis.com/dlt-blog-images/dashboard-trace.png)

### Pipeline loads
This section displays a history of all load packages found in the _dlt_loads table, that have been successfully executed and committed to the destination dataset of the selected pipeline. 

Select a load from the list to view its specific details. Additional data is fetched from the destination. Selecting a load package allows you to inspect the number of rows associated with that load package for each affected table and the full schema that resulted from this load. You can also view and download the raw schema of this specific load as a YAML file here.


![Pipeline loads](https://storage.googleapis.com/dlt-blog-images/dashboard-loads.png)

## Creating your own workspace dashboard

You can eject the code for the workspace dashboard into your current working directory and start editing it to create a custom version that fits your needs. To do this, run the `show` command with the `--edit` flag:

```sh
dlt pipeline {pipeline_name} show --edit
# or for the overview
dlt dashboard --edit
```

This will copy the dashboard code to the local folder and start Marimo in edit mode. If a local copy already exists, it will not overwrite it but will start it in edit mode. 

Below is an example of a custom cell created to verify unique number of rows vs total rows to ensure there are no duplicates in each table after a new run. You can also use the Generate with AI feature to easily add custom cells for your own use cases.  


![Adding custom cell in dashboard](https://storage.googleapis.com/dlt-blog-images/dashboards-custom-cell.png)

Once you have the local version, you can also use the regular Marimo commands to run or edit this notebook. This way, you can maintain multiple versions of your dashboard or other Marimo apps in your project:

```sh
# this will run a local dashboard
marimo run dlt_dashboard.py

# this will run the marimo in edit mode
marimo edit dlt_dashboard.py
```

## Further reading

If you are running `dlt` in Python interactively or in a notebook, read the [Accessing loaded data in Python](./dataset-access/dataset.md) guide.
