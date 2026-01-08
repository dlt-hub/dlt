---
title: Inspect your pipeline with the pipeline dashboard
description: Open a comprehensive dashboard with information about your pipeline
keywords: [pipeline, schema, data, inspect]
---

# Dashboard: inspect the pipeline

Once you have run a [pipeline](pipeline.md) locally, you can launch a web app that displays detailed information about your pipeline. This app is built with the [Marimo](https://marimo.io/) Python notebook framework. For this to work, you will need to have the `dlt[hub]` package installed. Check installation instructions ![here](https://dlthub.com/docs/hub/getting-started/installation).

:::tip
The dashboard works with all [destinations](destination.md) that are supported by [dataset interface](dataset-access/dataset.md). Vector databases are not supported at this moment. However, you can still inspect metadata such as run traces, schemas, and pipeline state.
:::

## Overview

The Dashboard is a locally hosted application built on [Marimo](https://marimo.io/). It comes preconfigured with the key pipeline metadata and operational context you need to review pipeline health and validate data quality. 

With the Dashboard, you can:

1. Inspect pipeline metadata
2. Query data in the destination
3. Review traces and exceptions
4. Check the history of pipeline runs
5. Verify incremental loading behavior

You can also customize the Dashboard and create a personalized version tailored to your workflow.

![Dashboard overview](https://storage.googleapis.com/dlt-blog-images/dashboard-overview.png)


## Quick start

You need to install dlt workspace using the following command:
```sh
pip install "dlt[hub]"
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

`dlt` will resolve your destination [credentials](https://dlthub.com/docs/general-usage/credentials) from:
* `secrets.toml` and `config.toml` in the `.dlt` folder of the current working directory (CWD), which is the directory you started the dashboard from 
* `secrets.toml` and `config.toml` in the the global `dlt` folder at `~/.dlt`. 
* Environment variables

It is best to run the dashboard from the same folder where you ran your pipeline, or to keep your credentials in the global folder.

`dlt` will NOT be able to pick up any credentials that you have configured in your code, since the dlt dashboard app runs independent of any pipeline scripts you have.

## Using the dashboard

The dashboard provides a visual interface to your [pipeline](pipeline.md)'s local working directory and its [destination](destination.md). It is designed to help you validate data quality and troubleshoot execution without writing custom SQL or inspection scripts.

The app automatically refreshes whenever a new local pipeline run is detected. You can switch between different pipelines on your machine using the dropdown menu in the top-right corner.

### Pipeline overview

The Overview section serves as a health check for your pipeline. If a run fails, this is where dlt surfaces the exception details and stack traces. Review last executed to ensure the pipeline is running on the expected schedule or has recently completed a load.

![Pipeline overview](https://storage.googleapis.com/dlt-blog-images/dashboard-pipeline-overview.png)

* **Last executed:** The timestamp of the last successful run. This is the primary indicator that your pipeline is running on schedule.

* **Credentials:** Displays the connection string or file path currently being used to reach the destination.

* **Schemas:** Lists all schemas associated with the pipeline name.

#### Remote state

This section queries and displays the state information that has been successfully committed to the **destination**, confirming what is stored outside the local working directory.

* The `state_version` shown here should generally match the `state_version` in the Pipeline Overview if the last run was successful.
* Click `Load and show remote state` to query the destination and retrieve this information.

### Schema explorer

The schema browser allows you to dive deep into your pipeline's (schemas)[https://dlthub.com/docs/general-usage/schema] and see all tables, columns, and type hints. This view is based on the dlt schema, the internal blueprint used to create and update tables in your destination.

When loading data for the first time, use this section to:

1. Click `Show child tables` to ensure that nested data is unnested successfully and at the required level.
2. Inspect the **type hints** specifically, making sure that timestamps and large numbers are captured in the right way.
3. Raw Schema as YAML can be exported and stored in your git repository as a reference for the dataset schema.

![Schema explorer](https://storage.googleapis.com/dlt-blog-images/dashboard-schema-explorer.png)

### Dataset Browser

The dataset browser allows you to query data directly from your destination. You can inspect the state of each of your resources. Click the `Load row counts` button to refresh the record counts for all tables based on the current destination data.

![Dataset Browser](https://storage.googleapis.com/dlt-blog-images/dashboard-dataset-browser.png)

### Querying your Dataset

You can use SQL to query your dataset here. Querying the dataset works with all remote and local destinations including Iceberg tables, Delta tables and Filesystem destination. This is what a query on the fruitshop dataset looks like:

![Querying Data](https://storage.googleapis.com/dlt-blog-images/dashboard-query.png)

All query results are cached. The **Query History** shows previous runs that benefit from this cache. Click `Clear cache` to force a fresh execution against the destination. Queries are executed directly on your destination (e.g., DuckDB).

### Pipeline state

This section displays a raw, JSON view of the currently stored pipeline state. This state is critical for tracking progress, especially for (incremental loading)[https://dlthub.com/docs/general-usage/incremental-loading] logic.

![Pipeline state](https://storage.googleapis.com/dlt-blog-images/dashboard-state.png)

### Last run trace

This section provides a detailed overview of the most recent run for the selected pipeline. You can use this for performance tuning and debugging execution issues. It contains the following:

1. **Trace Overview**
    This is a summary of the entire run. It shows the full duration and start and end times you can verify. 

2. **Execution Context**
    This section details the environment in which the pipeline was executed, including Python and dlt versions and details like operating system (os), CPU count (cpu), and whether it was a CI run (ci_run).

3. **Steps Overview**

    This table breaks down the total duration into the three phases of a dlt pipeline run: extract, normalize, and load.
    - **Bottleneck Check:** Use the duration column to identify performance bottlenecks.
        - A long extract time suggests a slow source.
        - Long normalize or load times often point to destination performance or data complexity issues.

    **Deep Dive:** You can click on each step to see more specific details like table names, item counts, file sizes, and timestamps for that specific phase.

4. **Resolved Config Values**
    This section lists the configuration values that were resolved and used during this pipeline run. Use this to ensure your intended config values were correctly picked up, or to find errors where configuration you set up was not used. The values of the resolved configs are not displayed for security reasons.

5. **Raw Trace**
    Clicking Show displays the complete trace information (including all sections above) as a raw JSON payload, which can be viewed and downloaded for advanced analysis or error reporting.

    ![Last run trace](https://storage.googleapis.com/dlt-blog-images/dashboard-trace.png)

### Pipeline loads
This section displays a history of all load packages found in the _dlt_loads table. It tracks every load package committed to the destination.

By selecting a specific load, you can:

* See exactly how many rows were added to each specific table in that run.
* View the full schema that resulted from this load.
* Download the raw schema as a YAML file for historical debugging.


![Pipeline loads](https://storage.googleapis.com/dlt-blog-images/dashboard-loads.png)

## Creating your own pipeline dashboard

You can eject the code for the pipeline dashboard into your current working directory and start editing it to create a custom version that fits your needs. To do this, run the `show` command with the `--edit` flag:

```sh
dlt pipeline {pipeline_name} show --edit
# or for the overview
dlt dashboard --edit
```

This will copy the dashboard code to the local folder and start Marimo in edit mode. If a local copy already exists, it will not overwrite it but will start it in edit mode. 

Below is an example of a custom cell created to verify unique number of rows vs total rows to ensure there are no duplicates in each table after a new run. You can also use the Generate with AI feature to easily add custom cells for your own use cases.  

![Adding custom cell in dashboard](https://storage.googleapis.com/dlt-blog-images/dashboards-custom-cell.png)

Here is the [Marimo](https://docs.marimo.io/api/) code used to generate the cell above:
```sh
@app.cell
def _(dlt_pipeline: dlt.Pipeline, dlt_selected_schema_name):
    # Create a list to hold the data for the table
    data = []

    if dlt_pipeline:
        _table_names = dlt_pipeline.schemas[dlt_selected_schema_name].tables.keys()
        for table_name in _table_names:
            _schema_table = dlt_pipeline.schemas[dlt_selected_schema_name].tables[table_name]
        
            # Fetch column names assuming _schema_table is a dictionary
            column_names = _schema_table.get('columns', [])
        
            if not column_names:
                data.append({"Table Name": table_name, "Total Rows": 0, "Unique Rows": "N/A"})
                continue
        
            # Construct the SQL query to count total rows and unique rows across all columns
            columns_quoted = ', '.join(f'"{col}"' for col in column_names)  # Quote column names
            count_query = f'SELECT COUNT(*) as total_rows, COUNT(DISTINCT ({columns_quoted})) as unique_rows FROM "{table_name}"'

            # Execute the query and handle any errors
            _query_result, _error_message, _traceback_string = utils.get_query_result(dlt_pipeline, count_query)

            if _error_message:
                data.append({"Table Name": table_name, "Total Rows": "Error", "Unique Rows": "Error"})
            else:
                row_count_info = _query_result.iloc[0]
                data.append({
                    "Table Name": table_name,
                    "Total Rows": row_count_info['total_rows'],
                    "Unique Rows": row_count_info['unique_rows']  # Count of distinct rows
                })

    # Prepare the result in a tabular format to display
    if data:
        basic_data_checks_table = mo.ui.table(data, selection=None, style_cell=utils.style_cell)

    # Display the table with a title
    mo.vstack([
        mo.md("### Basic Data Checks"),
        basic_data_checks_table
    ]) if data else None
    return
```

Once you have the local version, you can also use the regular Marimo commands to run or edit this notebook. This way, you can maintain multiple versions of your dashboard or other Marimo apps in your project:

```sh
# this will run a local dashboard
marimo run dlt_dashboard.py

# this will run the marimo in edit mode
marimo edit dlt_dashboard.py
```

## Further reading

If you are running `dlt` in Python interactively or in a notebook, read the [Accessing loaded data in Python](./dataset-access/dataset.md) guide.
