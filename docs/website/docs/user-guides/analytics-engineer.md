---
title: Analytics Engineer
description: A guide to using dlt for Analytics Engineers
keywords: [analytics engineer, analytics, business intelligence]
---

# Analytics Engineer

## Use Case #1: dbt packages for existing `dlt` pipelines

### Using dbt packages with dlt pipelines

By default`dlt` automatically structures unstructured data. This means that the loaded data structure is a cleaned, typed, and normalized version of the initial unstructured data.

Since the loaded data is just a reflexion of the input raw structure, this data usually needs to be rearranged to get it to a structure that analysts and other business users can use to answer questions.
For example, you will often want to consolidate this data into table that represent the business processes and entities involved, to make it easier for downstream consumers.

To enable analysts to easily run dbt packages, `dlt` supports a dbt runner that allows you to create a virtual environment, install dbt on it, pass it credentials, and run a dbt package from a local or online location. You can read more about this [here](../dlt-ecosystem/transformations/transforming-the-data).

### Contributing dbt packages

#### What to contribute?

Great data models require a deep understanding of the data and are not easy to build. If you take the time to create a dbt package for a `dlt` pipeline, we encourage you to contribute it, so the community can benefit from your hard work.

We are especially interested in dbt packages that
- transition the pipeline data into 3rd normal form, supporting a more simple use of the data and the creation of an Inmon architecture data warehouse
- transition the pipeline data into a dimensional model, supporting pivot-table style usage via query builders by business users

#### How to contribute?

Follow the [CONTRIBUTING](https://github.com/dlt-hub/verified-sources/blob/master/CONTRIBUTING.md) guide in `verified-pipelines` repo.

## Use Case #2: Clean, type, and customize how data is produced and loaded

`dlt` allows you to customize how data is produced, enabling you to rename, filter, and modify the data that arrives at your destination. Before you pass the data to `dlt` for normalization, you can transform the data in Python. After `dlt` has normalized and loaded the data, you can also further transform it using SQL. You can learn more about possible customizations in the docs (e.g. [pseudonymizing_columns](../general-usage/customising-pipelines/pseudonymizing_columns.md)).

## Use Case #3: Create your own pipelines in a declarative fashion and supercharge your dbt packages

dbt is always going to be dependant on the upstream pipeline creating a schema - so for the first time you can supercharge dbt users with a pipeline to make them independent.

`dlt` follows similar user paradigms as dbt, enabling instant recognition and a zero learning curve to use the declarative loading style.

`dlt` was designed from the start for the open source data tool users. It enables people who have never before built a data pipeline to go from raw data in Python to structured data in a database in minutes. For example, using DuckDB, you can easily develop your pipeline locally without needing to set up and connect to your your production warehouse.

It features a declarative approach to configuring loading modes, handling all of the engineering for you by default, allowing you to leverage a great loader that comes complete with support for schema migrations, data typing, performance hint declarations, schema management, etc.

If you combine it with a `dbt` package, you enable end to end analytics engineering - if the dbt package is the main citizen, then let dlt be it's rocket engine.

Read more about [running dbt packages](../dlt-ecosystem/transformations/transforming-the-data), [incremental loading](../general-usage/incremental-loading), and [performance hints and schemas](../walkthroughs/adjust-a-schema).
