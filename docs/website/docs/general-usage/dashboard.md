---
title: Inspect your pipeline with the workspace dashboard
description: Open a comprehensive dashboard with information about your pipeline
keywords: [pipeline, schema, data, inspect]
---

# Inspect your pipeline with the workspace dashboard

Once you have run a pipeline locally, you can launch a web app that displays detailed information about your pipeline. This app is built with the marimo Python notebook framework. For this to work, you will need to have the `marimo` package installed.

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

You can even eject the code for the dashboard app into your current working directory and start editing it either in your code editor or in marimo edit mode to create your own custom dashboard app!

![Dashboard overview](https://storage.googleapis.com/dlt-blog-images/dashboard-overview.png)


## Prerequisites

To install marimo, run the following command:
```sh
pip install marimo
```

## Launching the dashboard

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

This section is a development checklist for validating a new REST API pipeline using the dashboard.

:::tip Quick checklist
1. **Row counts look right?** → Dataset Browser
2. **Incremental cursor advancing?** → Pipeline State
3. **Schema structure correct?** → Schema Explorer
4. **Business entities present?** → Dataset Browser
5. **Data types correct?** → Schema Explorer
:::

### 1) Am I grabbing data correctly?

#### Pagination sanity

**What to look for**

- If your first successful run shows a suspiciously round count (e.g., **10 / 20 / 100**) for a resource whose page size matches that number, you probably captured only page 1.
- Cross-check the source's expected volume (API docs, admin UI, or a known "ground truth") vs. what landed.

To do so, navigate to the Dataset Browser and load row counts:

![Dataset Browser row counts](https://storage.googleapis.com/dlt-blog-images/docs-dashboard-dq1.png)

**Typical failure modes**

- The wrong paginator pattern was chosen vs. the API docs (cursor vs. offset vs. page/size).
- The API supports multiple pagination styles per endpoint and you implemented the wrong one.

**What to do:** Re-read the endpoint's official documentation (or ask your agent to do so) and confirm the exact pagination contract (parameter names, response fields, end-of-data signal).

### 2) Am I loading data correctly?

#### Full loads (non-incremental)

Using `replace` write disposition is slower but simpler. If you are using it to do full loads, you do not need to troubleshoot incremental loading.

#### Incremental loads

- Run **multiple increments** and check that each run only brings the delta.
- Validate in two places:
    - **Pipeline State**: the resource's cursor (e.g., `last_extracted_at` / `last_value`) should advance to the end of the previous run. This is how it looks in the raw pipeline state:

    ![Pipeline state cursor](https://storage.googleapis.com/dlt-blog-images/docs-dashboard-dq2.png)

    - **Pipeline Loads row counts**: Check how many rows each run loaded by running a query in the Dataset Browser:

    ```sql
    SELECT _dlt_load_id, COUNT(*) FROM items GROUP BY 1
    ```

- Optionally, check for continuity in the data. If you have a time axis or incremental IDs with normal distribution, plot them on a line chart to spot any anomalies. For easy plotting, we suggest using our integrated [marimo notebook](./dataset-access/marimo.md).

### 3) Is my normalized schema the one I actually want?

We generally recommend you let `dlt` fully manage your schema for two reasons: you do less work, and the entire schema is discovered and explicit. But sometimes your data might also contain metadata that is not useful and should be removed.

To inspect the schema, check the Schema Explorer:

![Schema Explorer](https://storage.googleapis.com/dlt-blog-images/docs-dashboard-dq3.png)

**Options to change your schema**

- [**Filter at extraction** to drop unneeded fields](../dlt-ecosystem/transformations/add-map.md)
- [**Reduce unnesting**](../reference/frequently-asked-questions.md#can-i-configure-different-nesting-levels-for-each-resource) or keep some payloads as **complex (nested) types** if your destination supports it.

**Example:** Using `json` type with REST API:

```py
import dlt
from dlt.sources.rest_api import rest_api_source

def example_complex_unnesting():
    # Default behavior: dlt creates 'orders__items' child table.
    # Desired behavior: 'items' column in 'orders' table with JSON/Struct type.

    config = {
        "client": {
            "base_url": "https://jaffle-shop.dlthub.com/api/v1/",
            "paginator": {
                "type": "page_number",
                "page_param": "page",
                "base_page": 1,
                "maximum_page": 1,  # Just 1 page for testing
                "total_path": None
            }
        },
        "resources": [
            {
                "name": "orders_complex_test",
                "endpoint": {
                    "path": "orders",
                    "params": {"page_size": 5}
                },
                "write_disposition": "replace",  # clean start
                "columns": {
                    "items": {"data_type": "json"}  # keep this table nested
                }
            }
        ]
    }

    pipeline = dlt.pipeline(
        pipeline_name="test_complex_pipeline",
        destination="duckdb",
        dataset_name="test_data"
    )

    source = rest_api_source(config)
    load_info = pipeline.run(source)
```

### 4) Do I have the right business data?

**What to look for**

Open the Dataset Browser and run a query to see what you actually have:

```sql
SELECT * FROM {your_table} LIMIT 10
```

- Do the tables contain the entities and attributes your use case needs?
- Are key columns present (IDs, timestamps, status fields)?
- Is the data complete or are important fields showing up as `NULL`?

**Typical failure modes**

- The API returns a **summary view** by default; you need extra parameters (e.g., `expand`, `include=changes`, `since=`) to get full details.
- **Related data lives in separate endpoints** that you haven't added yet (e.g., orders exist but order line items are a different endpoint).
- **PII columns** (emails, phones, names) are present and need to be hashed or removed before analytics.

**What to do**

1. Check the API docs for expansion parameters that return nested/related data.
2. Add additional endpoints to your source if you need related entities.
3. Use [`add_map`](../dlt-ecosystem/transformations/add-map.md) to hash PII or reshape records before loading.

Use the Dataset Browser to explore the data, or the [marimo notebook](./dataset-access/marimo.md) for more complex analysis.

### 5) Are my data types correct?

**What to look for**

Open the Schema Explorer and check the `data_type` column for each field:

- Numbers should be `bigint` or `double`, not `text`
- Dates should be `timestamp` or `date`, not `text`
- Boolean fields should be `bool`, not `text`

**Typical failure modes**

- Numbers arrive as strings (`"amount": "100.00"`) because the API returns them quoted.
- Timestamps in non-standard formats (e.g., `"12/25/2024"` or Unix epochs) aren't auto-detected.
- Boolean values come as `"true"`/`"false"` strings or `0`/`1` integers.

**What to do**

1. **Enable additional autodetectors** to catch more types automatically. Add to your config:

```toml
[schema]
detections = ["iso_timestamp", "timestamp", "large_integer"]
```

2. **Transform at extraction** using [`add_map`](../dlt-ecosystem/transformations/add-map.md) to cast values before they hit the schema.

**Docs:**
- [Schema → Data type autodetectors](schema.md#data-type-autodetectors)
- [`add_map` for custom record transformations](../dlt-ecosystem/transformations/add-map.md)

## Understanding the dashboard

The dashboard app should mostly be self-explanatory. Go to the section that corresponds to your task and click the toggle to open and use it. The dashboard app also refreshes all data when a new local pipeline run is detected for your selected pipeline. You can switch between pipelines on your machine using the pipeline dropdown in the top-right.

The following sections are available:


### Pipeline Info

The overview section gives you a general sense of the state of your pipeline and will also display exception information if the last run of your pipeline failed.

![Pipeline Info](https://storage.googleapis.com/dlt-blog-images/dashboard-pipeline-overview.png)

### Schema explorer

The schema explorer allows you to dive deep into your pipeline's schemas and see all tables, columns, and hints.

![Schema explorer](https://storage.googleapis.com/dlt-blog-images/dashboard-schema-explorer.png)

### Dataset Browser

The dataset browser allows you to query data from your destination and inspect the state of each of your resources, which contain information about their incremental status and other custom state values.

![Dataset Browser](https://storage.googleapis.com/dlt-blog-images/dashboard-dataset-browser.png)

### Querying your Dataset

This is what a query on the fruitshop dataset looks like:

![Querying Data](https://storage.googleapis.com/dlt-blog-images/dashboard-query.png)

### Pipeline state

This is a raw view of the full pipeline state.

![Pipeline state](https://storage.googleapis.com/dlt-blog-images/dashboard-state.png)

### Last run trace

This is an overview of the results from your last local pipeline run.

![Last run trace](https://storage.googleapis.com/dlt-blog-images/dashboard-trace.png)

### Pipeline loads

This provides an overview and detailed information about loads found in the _dlt_loads table and their associated rows.

![Pipeline loads](https://storage.googleapis.com/dlt-blog-images/dashboard-loads.png)

## Creating your own workspace dashboard

You can eject the code for the workspace dashboard into your current working directory and start editing it to create a custom version that fits your needs. To do this, run the `show` command with the `--edit` flag:

```sh
dlt pipeline {pipeline_name} show --edit
# or for the overview
dlt dashboard --edit
```

This will copy the dashboard code to the local folder and start marimo in edit mode. If a local copy already exists, it will not overwrite it but will start it in edit mode. Once you have the local version, you can also use the regular marimo commands to run or edit this notebook. This way, you can maintain multiple versions of your dashboard or other marimo apps in your project:

```sh
# this will run a local dashboard
marimo run dlt_dashboard.py

# this will run the marimo edit mode
marimo edit dlt_dashboard.py
```

## Further reading

If you are running `dlt` in Python interactively or in a notebook, read the [Accessing loaded data in Python](./dataset-access/dataset.md) guide.
