---
title: Analytics Engineer
description: A guide to using dlt for Analytics Engineers
keywords: [analytics engineer, analytics, business intelligence]
---

# Analytics Engineer

## Use Case #1: dbt packages for existing `dlt` pipelines

### Using dbt packages with dlt pipelines

`dlt` automatically structures unstructured data by default. This means that typically the loaded data structure is a cleaned, typed, and normalized version of the initial unstructured data.

As a result, this data usually needs to be rearranged to get it to a structure that analysts and other business users can use to answer questions. For example, you will often want to consolidate this data into tables and entities that represent the business process makes it easier for downstream consumers.

To make this easier, `dlt` supports a dbt runner that allows you to create a virtual environment, install dbt on it, pass it credentials, and run a dbt package from a local or online location. You can read more about this [here](./using-loaded-data/transforming-the-data).

### Contributing dbt packages

#### What to contribute?

Great data models require a deep understanding of the data and are not easy to build. If you take the time to create a dbt package for a `dlt` pipeline, we encourage you to contribute it, so the community can benefit from your hard work.

We are esspecially interested in dbt packages that
- transition the pipeline data into 3rd normal form, supporting a more simple use of the data and the creation of an Inmon architecture data warehouse
- transition the pipeline data into a dimensional model, supporting pivot-table style usage via query builders by business users

#### How to contribute?

1. Fork the [pipelines repo](https://github.com/dlt-hub/pipelines)
2. Add the dbt package in a sub-folder of a pipeline folder
3. Include information about it in the pipeline folder README
4. Open a pull request (PR) on GitHub [here](https://github.com/dlt-hub/pipelines/pulls)

If you run into issues or have questions, please reach out in the `#technical-help` channel on [Slack](https://join.slack.com/t/dlthub-community/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g).

## Use Case #2: Clean, type, and customize how data is produced and loaded

`dlt` allows you to customize how data is produced, enabling you to rename, filter, and modify the data that arrives at your destination. Before you pass the data to `dlt` for normalization, you can transform the data in Python. After `dlt` has normalized and loaded the data, you can also further transform it using SQL. You can learn more about possible customizations in the docs (e.g. [pseudonymizing_columns](./customizations/customizing-pipelines/pseudonymizing_columns)).

## Use Case #3: Create your own pipelines in a declarative fashion

`dlt` was designed from the start for the open source data tool users. It enables people who have never before built a data pipeline to go from raw data in Python to structured data in a database in minutes. For example, using DuckDB, you can easily develop your pipeline locally without needing to set up and connect to your your production warehouse.

It features a declarative approach to configuring loading modes, handling all of the engineering for you by default, allowing you to leverage a great loader that comes complete with support for schema migrations, data typing, performance hint declarations, schema management, etc.

If you combine it with a `dbt` package, then you use it end to end to deliver analytics.

Read more about [running dbt packages](./using-loaded-data/transforming-the-data), [incremental loading](./general-usage/incremental-loading), and [performance hints and schemas](./walkthroughs/adjust-a-schema).
