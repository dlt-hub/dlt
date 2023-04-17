---
title: Understanding the tables
description: Understanding the tables that have been loaded
keywords: [understanding tables, loaded data, data structure]
---

# Understanding the tables

## Show tables and data in the destination

```
dlt pipeline <pipeline name> show
```

This command generates and launches a simple Streamlit app that you can use to inspect the schemas and data in the destination as well as your pipeline state and loading status / stats. It should be executed from the same folder where you ran the pipeline script to access destination credentials. It requires `streamlit` and `pandas` to be installed.

## Table and column names

We [normalize table and column names](../general-usage/schema.md#naming-convention) so they fit what the destination database allows. We convert all the names in your source data into `snake_case`, alphanumeric identifiers. Please note that in many cases the names you had in your input document will be (slightly) different from identifiers you see in the database.

## Child and parent tables

When creating a schema during normalization, `dlt` recursively unpacks this nested structure into relational tables, creating and linking childs and parent tables.

This is how table linking works:
1. Each row in all (top level and child) data tables created by `dlt` contains UNIQUE column named `_dlt_id`
2. Each child table contains FOREIGN KEY column `_dlt_parent_id` linking to a particular row (`_dlt_id`) of a parent table
3. Rows in child tables come from the lists: `dlt` stores the position of each item in the list in `_dlt_list_idx`
4. For tables that are loaded with the `merge` write disposition, we add a ROOT KEY column `_dlt_root_id`, which links child table to a row in top level table

:::note
If you define your own primary key in a child table, it will be used to link to parent table and the `_dlt_parent_id` and `_dlt_list_idx` will not be added. `_dlt_id` is always added even in case the primary key or other unique columns are defined.
:::

## Load IDs

Load IDs are important and present in all of the top tables (`_dlt_loads`, `load_id`, etc). Each pipeline run creates one or more load packages, which can be identified by their `load_id`. A load package typically contains data from all [resources](../general-usage/glossary.md#resource) of a particular [source](../general-usage/glossary.md#source). The `load_id` of a particular package is added to the top data tables and to the `_dlt_loads` table with a status 0 (when the load process is fully completed).

For each load, you can test and [alert](../running-in-production/alerting.md) on anomalies (e.g. no data, too much loaded to a table). There are also some useful load stats in the `Load info` tab of the [Streamlit app](understanding-the-tables.md#show-tables-and-data-in-the-destination) mentioned above.

The `_dlt_loads` table tracks complete loads and allows to chain transformations on top of them. Many destinations do not support distributed and long running transactions (e.g. Amazon Redshift). In that case, the user may see the partially loaded data. It is possible to filter such data out—any row with a `load_id` that does not exist in `_dlt_loads` is not yet completed.

You can add [transformations](./transforming-the-data.md) and chain them together using the `status` column. You start the transformation for all of the data with a particular `load_id` with a status of 0 and then update it to 1. The next transformation starts with the status of 1 and is then updated to 2. This can repeated for every additional transformation.

### Data lineage

Data lineage can be super relevant for architectures like the [data vault architecture](https://www.data-vault.co.uk/what-is-data-vault/) or when troubleshooting. The data vault architecture is a data warehouse that large organizations use when representing the same process across multiple systems, which adds data lineage requirements. Using the pipeline name and `load_id` provided out of the box by `dlt`, you are able to identify the source and time of data.

You can [save](../running-in-production/running.md#inspect-and-save-the-load-info-and-trace) complete lineage info for a particular `load_id` including a list of loaded files, error messages (if any), elapsed times, schema changes, This can be helpful, for example, when troubleshooting problems.