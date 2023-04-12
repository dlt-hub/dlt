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

## Children and parent tables

When creating a schema during normalization, `dlt` recursively unpacks this nested structure into relational tables, creating and linking children and parent tables.

## Load IDs

Load IDs are important and present in all of the top tables (`_dlt_loads`, `load_id`, etc).

### Data lineage

Data lineage can be super relevant when troubleshooting or even for some architectures like the data vault architecture.

The [data vault architecture](https://www.data-vault.co.uk/what-is-data-vault/) is a data warehouse that large organizations use when representing the same process across multiple systems, which adds data lineage requirements.

Using the pipeline name and `load_id` provided out of the box by `dlt`, you are able to identify the source and time of data