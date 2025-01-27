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

## Write custom SQL custom queries
We recommend that you create a SQL VIEW in your source database and extract data from it. In that case `dlt` will infer all column types and read data in
shape you define in a view without any further customization.

If creating a view is not feasible, you can fully rewrite the automatically generated query with extended version of `query_adapter_callback`:

```py
import sqlalchemy as sa

def query_adapter_callback(
      query, table, incremental=None, engine=None
  ) -> TextClause:

      if incremental and incremental.start_value is not None:
          t_query = sa.text(
              f"SELECT *, 1 as add_int, 'const' as add_text FROM {table.fullname} WHERE"
              f" {incremental.cursor_path} > :start_value"
          ).bindparams(**{"start_value": incremental.start_value})
      else:
          t_query = sa.text(f"SELECT *, 1 as add_int, 'const' as add_text FROM {table.fullname}")

      return t_query
```
In the snippet above we do a few interesting things:
1. We create a text query with `sa.text`
2. We change the condition on selecting incremental column from the default `ge` to `greater` (f" {incremental.cursor_path} > :start_value")
3. We add additional computed columns: `1 as add_int, 'const' as add_text`. You can also join other table here.

We recommend that you explicitly type additional columns that you added with `table_adapter_callback`:

```py
from sqlalchemy.sql import sqltypes

def add_new_columns(table) -> None:
    required_columns = [
        ("add_int", sqltypes.BigInteger, {"nullable": True}),
        ("add_text", sqltypes.Text, {"default": None, "nullable": True}),
    ]
    for col_name, col_type, col_kwargs in required_columns:
        if col_name not in table.c:
            table.append_column(sa.Column(col_name, col_type, **col_kwargs))
```
Otherwise `dlt` will attempt to infer the types from the extracted data.

Here's how you call `sql_table` with those adapters:
```py
import dlt
from dlt.sources.sql_database import sql_table

table = sql_table(
  table="chat_channel",
  table_adapter_callback=add_new_columns,
  query_adapter_callback=query_adapter_callback,
  incremental=dlt.sources.incremental("updated_at"),
)
```

## Add computed columns and custom incremental clauses

You can add computed columns to the table definition by converting it into a subquery:
```py
def add_max_timestamp(table):
    computed_max_timestamp = sa.sql.type_coerce(
        sa.func.greatest(table.c.created_at, table.c.updated_at),
        sqltypes.DateTime,
    ).label("max_timestamp")
    subquery = sa.select(*table.c, computed_max_timestamp).subquery()
    return subquery
```
We add new `max_timestamp` column that is a MAX of `created_at` and `updated_at` columns and then we convert it into a subquery
because we intend to use it for incremental loading which will attach a `WHERE` clause to it.

```py
import dlt
from dlt.sources.sql_database import sql_table

read_table = sql_table(
    table="chat_message",
    table_adapter_callback=add_max_timestamp,
    incremental=dlt.sources.incremental("max_timestamp"),
)
```
`dlt` will use your subquery instead of original `chat_message` table to generate incremental query. Note that you can further
customize subquery with query adapter as in the example above.

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

