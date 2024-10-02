---
title: Usage
description: basic usage of the sql_database source
keywords: [sql connector, sql database pipeline, sql database]
---

import Header from '../_source-info-header.md';

# Usage

<Header/>

## Applying column-wise filtering on the data being ingested

By default, the existing source and resource functions, `sql_database` and `sql_table`, ingest all of the records from the source table. However, by using `query_adapter_callback`, it is possible to pass a `WHERE` clause inside the underlying `SELECT` statement using the [SQLAlchemy syntax](https://docs.sqlalchemy.org/en/14/core/selectable.html#). This enables filtering the data based on specific columns before extraction.

The example below uses `query_adapter_callback` to filter on the column `customer_id` for the table `orders`:

```py
from dlt.sources.sql_database import sql_database

def query_adapter_callback(query, table):
    if table.name == "orders":
        # Only select rows where the column customer_id has value 1
        return query.where(table.c.customer_id==1)
    # Use the original query for other tables
    return query

source = sql_database(
    query_adapter_callback=query_adapter_callback
).with_resources("orders")
```

## Transforming the data before load
You have direct access to the extracted data through the resource objects (`sql_table()` or `sql_database().with_resource())`), each of which represents a single SQL table. These objects are generators that yield individual rows of the table, which can be modified by using custom Python functions. These functions can be applied to the resource using `add_map`.

:::note
The PyArrow backend does not yield individual rows but loads chunks of data as `ndarray`. In this case, the transformation function that goes into `add_map` should be configured to expect an `ndarray` input.
:::


Examples:
1. Pseudonymizing data to hide personally identifiable information (PII) before loading it to the destination. (See [here](../../../general-usage/customising-pipelines/pseudonymizing_columns) for more information on pseudonymizing data with `dlt`)

    ```py
    import dlt
    import hashlib
    from dlt.sources.sql_database import sql_database

    def pseudonymize_name(doc):
        '''
        Pseudonymization is a deterministic type of PII-obscuring.
        Its role is to allow identifying users by their hash,
        without revealing the underlying info.
        '''
        # add a constant salt to generate
        salt = 'WI@N57%zZrmk#88c'
        salted_string = doc['rfam_acc'] + salt
        sh = hashlib.sha256()
        sh.update(salted_string.encode())
        hashed_string = sh.digest().hex()
        doc['rfam_acc'] = hashed_string
        return doc

    pipeline = dlt.pipeline(
        # Configure the pipeline
    )
    # using sql_database source to load family table and pseudonymize the column "rfam_acc"
    source = sql_database().with_resources("family")
    # modify this source instance's resource
    source.family.add_map(pseudonymize_name)
    # Run the pipeline. For a large db this may take a while
    info = pipeline.run(source, write_disposition="replace")
    print(info)
    ```

2. Excluding unnecessary columns before load

    ```py
    import dlt
    from dlt.sources.sql_database import sql_database

    def remove_columns(doc):
        del doc["rfam_id"]
        return doc

    pipeline = dlt.pipeline(
        # Configure the pipeline
    )
    # using sql_database source to load family table and remove the column "rfam_id"
    source = sql_database().with_resources("family")
    # modify this source instance's resource
    source.family.add_map(remove_columns)
    # Run the pipeline. For a large db this may take a while
    info = pipeline.run(source, write_disposition="replace")
    print(info)
    ```

## Deploying the sql_database pipeline

You can deploy the `sql_database` pipeline with any of the `dlt` deployment methods, such as [GitHub Actions](../../../walkthroughs/deploy-a-pipeline/deploy-with-github-actions), [Airflow](../../../walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer), [Dagster](../../../walkthroughs/deploy-a-pipeline/deploy-with-dagster), etc. See [here](../../../walkthroughs/deploy-a-pipeline) for a full list of deployment methods.

### Running on Airflow
When running on Airflow:
1. Use the `dlt` [Airflow Helper](../../../walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer.md#2-modify-dag-file) to create tasks from the `sql_database` source. (If you want to run table extraction in parallel, you can do this by setting `decompose = "parallel-isolated"` when doing the source->DAG conversion. See [here](../../../walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer#2-modify-dag-file) for a code example.)
2. Reflect tables at runtime with the `defer_table_reflect` argument.
3. Set `allow_external_schedulers` to load data using [Airflow intervals](../../../general-usage/incremental-loading.md#using-airflow-schedule-for-backfill-and-incremental-loading).

