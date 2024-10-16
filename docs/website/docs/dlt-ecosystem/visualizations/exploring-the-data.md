---
title: Explore the loaded data
description: How to explore the data that has been loaded
keywords: [exploring, loaded data, data quality]
---

# Explore the loaded data

Once you have run a pipeline locally, you can launch a web app that displays the loaded data.

To do so, run the [cli command](../../reference/command-line-interface.md#show-tables-and-data-in-the-destination)
below with your pipeline name. The pipeline name is the name of the
Python file where your pipeline is defined and also displayed in your terminal when loading:

```sh
dlt pipeline {pipeline_name} show
```

This will open a streamlit app with:

- Information about the loads.
- Tables and sample data.
- A SQL client that you can use to run queries.

## Exploring the data in Python

You can quickly fetch loaded data from a destination using SQL. The data will be available as a
stream of rows or a data frame. Both methods use the same credentials that you set up for your
pipeline and hide many intricacies of correctly setting up the connection to your destination.

### Querying the data using the `dlt` SQL client

Execute any SQL query and get results following the Python
[dbapi](https://peps.python.org/pep-0249/) spec. Below, we fetch data from the customers table:

```py
pipeline = dlt.pipeline(destination="bigquery", dataset_name="crm")
with pipeline.sql_client() as client:
    with client.execute_query(
        "SELECT id, name, email FROM customers WHERE id = %s",
        10
    ) as cursor:
        # get all data from the cursor as a list of rows
        print(cursor.fetchall())
```

In the above, we used `dbapi` parameter placeholders and fetched the data using the `fetchall` method
that reads all the rows from the cursor.

### Querying data into a data frame

You can fetch the results of any SQL query as a data frame. If the destination supports that
natively (i.e., BigQuery and DuckDB), `dlt` uses the native method. Thanks to that, reading data
frames may be really fast! The example below reads GitHub reactions data from the `issues` table and
counts reaction types.

```py
pipeline = dlt.pipeline(
    pipeline_name="github_pipeline",
    destination="duckdb",
    dataset_name="github_reactions",
    dev_mode=True
)
with pipeline.sql_client() as client:
    with client.execute_query(
        'SELECT "reactions__+1", "reactions__-1", reactions__laugh, reactions__hooray, reactions__rocket FROM issues'
    ) as table:
        # calling `df` on a cursor, returns the data as a pandas DataFrame
        reactions = table.df()
counts = reactions.sum(0).sort_values(0, ascending=False)
```

The `df` method above returns all the data in the cursor as a data frame. You can also fetch data in
chunks by passing the `chunk_size` argument to the `df` method.

### Access destination native connection

The native connection to your destination like BigQuery `Client` or DuckDB `DuckDBPyConnection` is
available in case you want to do anything special. Below, we take the native connection to `duckdb`
to get `DuckDBPyRelation` from a query:

```py
import dlt
import duckdb

pipeline = dlt.pipeline(destination="duckdb", dataset_name="github_reactions")
with pipeline.sql_client() as client:
    conn = client.native_connection
    rel = conn.sql('SELECT * FROM issues')
    rel.limit(3).show()
```

## Data quality dashboards

After deploying a `dlt` pipeline, you might ask yourself: How can we know if the data is and remains
high quality?

There are two ways to catch errors:

1. Tests.
1. People [monitoring.](../../running-in-production/monitoring.md)

## Tests

The first time you load data from a pipeline you have built, you will likely want to test it. Plot
the data on time series line charts and look for any interruptions or spikes, which will highlight
any gaps or loading issues.

### Data usage as monitoring

Setting up monitoring is a good idea. However, in practice, often by the time you notice something is wrong through reviewing charts, someone in the business has likely already noticed something is wrong. That is, if there is usage of the data, then that usage will act as a sort of monitoring.

### Plotting main metrics on line charts

In cases where data is not being used much (e.g., only one marketing analyst is using some data alone), then it is a good idea to have them plot their main metrics on "last 7 days" line charts, so it's visible to them that something may be off when they check their metrics.

It's important to think about granularity here. A daily line chart, for example, would not catch hourly issues well. Typically, you will want to match the granularity of the time dimension (day/hour/etc.) of the line chart with the things that could go wrong, either in the loading process or in the tracked process.

If a dashboard is the main product of an analyst, they will generally watch it closely. Therefore, it's probably not necessary for a data engineer to include monitoring in their daily activities in these situations.

## Tools to create dashboards

[Metabase](https://www.metabase.com/), [Looker Studio](https://lookerstudio.google.com/u/0/), and [Streamlit](https://streamlit.io/) are some common tools that you might use to set up dashboards to explore data. It's worth noting that while many tools are suitable for exploration, different tools enable your organization to achieve different things. Some organizations use multiple tools for different scopes:

- Tools like [Metabase](https://www.metabase.com/) are intended for data democratization, where the business user can change the dimension or granularity to answer follow-up questions.
- Tools like [Looker Studio](https://lookerstudio.google.com/u/0/) and [Tableau](https://www.tableau.com/) are intended for minimal interaction curated dashboards that business users can filter and read as-is with limited training.
- Tools like [Streamlit](https://streamlit.io/) enable powerful customizations and the building of complex apps by Python-first developers, but they generally do not support self-service out of the box.

