---
title: Viewing the tables
description: Viewing tables that have been loaded
keywords: [viewing tables, loaded data, data structure]
---

# Viewing the tables

## Show tables and data in the destination

```
dlt pipeline <pipeline name> show
```

This command generates and launches a simple Streamlit app that you can use to inspect the schemas and data in the destination as well as your pipeline state and loading status / stats. It should be executed from the same folder where you ran the pipeline script to access destination credentials. It requires `streamlit` and `pandas` to be installed.

## Table and column names
- We [normalize table and column names](../general-usage/schema.md#naming-convention) so they fit what your destination database allows. We convert all the names in your source data into `snake_case`, alphanumeric identifiers. Mind that in many cases the names you had in your input document will be (slightly) different from identifiers you see in the database!

## Children and parent tables

When creating a schema during normalization, `dlt` recursively unpacks this nested structure into relational tables, creating and linking children and parent tables.
----------
how linking works:
1. each row in all data tables (top level and child) created by `dlt` contains UNIQUE column named `_dlt_id`
2. each child table contains FOREIGN KEY column `_dlt_parent_id` linking to a particular row (`_dlt_id`) of a parent table
3. rows in child tables come from the lists. `dlt` stores the position of each item in the list in `_dlt_list_idx`.
4. for tables that are loaded with `merge` write disposition we add a ROOT KEY column `_dlt_root_id` which links child table to a row in top level table

Note: if you define your own primary key on a child table, it will be used to link to parent table and the `_dlt_parent_id` and `_dlt_list_idx` will not be added. `_dlt_id` is always added even in case the primary key or other unique columns are defined.

Example:
[maybe you have something in the weather api]

## Load IDs

Load IDs are important and present in all of the top tables (`_dlt_loads`, `load_id`, etc).

1. Each pipeline run creates one or more load packages, each identified by a `load_id`. A load package typically contains data from all [resources](link) of a particular [source].
2. The `load_id` of a particular package is added to to the top data tables and when the load process is fully completed - to `_dlt_loads` table with a status 0.
3. [you can save](../running-in-production/running.md#inspect-and-save-the-load-info-and-trace) a complete lineage information for a particular `load_id` including list of loaded files, error messages (if any), elapsed times, schema changes etc. so you can troubleshoot any problem see data lineage below.
4. you can test and alert on anomalies ie. no data or too much loaded to a table in a particular load
5. see our Streamlit app, `Load info` tab for some useful load stats.

The `_dlt_loads` is a table that tracks complete loads and allows to chain transformations on top of them.
1. many destinations do not support distributed and long running transactions (ie. `Redshift`). in that case the user may see the partially loaded data. it is easy to filter such data out - any row with a `load_id` that does not exist in `_dlt_loads` is not yet completed.
2. you can easily remove data for a particular load
3. you can add transformations ie. using `dbt` and chain them. for that you can use `status` column. you start your transformation for all the data with `load_id` with status 0 and add/update status to 1. the next transformation starts with date of status 1 and so forth

### Data lineage

Data lineage can be super relevant when troubleshooting or even for some architectures like the data vault architecture.

The [data vault architecture](https://www.data-vault.co.uk/what-is-data-vault/) is a data warehouse that large organizations use when representing the same process across multiple systems, which adds data lineage requirements.

Using the pipeline name and `load_id` provided out of the box by `dlt`, you are able to identify the source and time of data