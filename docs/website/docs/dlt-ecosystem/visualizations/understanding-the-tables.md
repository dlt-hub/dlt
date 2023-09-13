---
title: Understanding the tables
description: Understanding the tables that have been loaded
keywords: [understanding tables, loaded data, data structure]
---

# Understanding the tables

In [Exploring the data](./exploring-the-data.md) you have seen the data that has been loaded into the
database. Let's take a closer look at the tables that have been created.

We start with a simple dlt pipeline:

```py
import dlt

data = [
    {'id': 1, 'name': 'Alice'},
    {'id': 2, 'name': 'Bob'}
]

pipeline = dlt.pipeline(
    pipeline_name='quick_start',
    destination='duckdb',
    dataset_name='mydata'
)
load_info = pipeline.run(data, table_name="users")
```

:::note

Here we are using the `duckdb` destination, which is an in-memory database. Other database [destinations](../destinations)
will behave similarly and have similar concepts.

:::

## Schema

When you run the pipeline, dlt creates a schema in the destination database. The schema is a
collection of tables that represent the data you loaded. The schema name is the same as the
`dataset_name` you provided in the pipeline definition. In the example above, we explicitly set the
`dataset_name` to `mydata`, if you don't set it, it will be set to the pipeline name with a suffix `_dataset`.

## Tables

Each [resource](../../general-usage/resource.md) in your pipeline definition will be represented by a table in
the destination. In the example above, we have one resource, `users`, so we will have one table, `users`,
in the destination. Here also, we explicitly set the `table_name` to `users`, if you don't set it, it will be
set to the resource name.

For example, we can rewrite the pipeline above as:

```py
@dlt.resource
def users():
    yield [
        {'id': 1, 'name': 'Balice'},
        {'id': 2, 'name': 'Bob'}
    ]

pipeline = dlt.pipeline(
    pipeline_name='quick_start',
    destination='duckdb',
    dataset_name='mydata'
)
load_info = pipeline.run(users)
```

The result will be the same, but the table is implicitly named `users` based on the resource name.

::: note

Special tables are created to track the pipeline state. These tables are prefixed with `_dlt_`
and are not shown in the `show` command of the `dlt pipeline` CLI. However, you can see them when
connecting to the database directly.

:::

## Child and parent tables

Now let's look at a more complex example:

```py
import dlt

data = [
    {
        'id': 1,
        'name': 'Alice',
        'pets': [
            {'id': 1, 'name': 'Fluffy', 'type': 'cat'},
            {'id': 2, 'name': 'Spot', 'type': 'dog'}
        ]
    },
    {
        'id': 2,
        'name': 'Bob',
        'pets': [
            {'id': 3, 'name': 'Fido', 'type': 'dog'}
        ]
    }
]

pipeline = dlt.pipeline(
    pipeline_name='quick_start',
    destination='duckdb',
    dataset_name='mydata'
)
load_info = pipeline.run(data, table_name="users")
```

Running this pipeline will create two tables in the destination, `users` and `users__pets`. The
`users` table will contain the top level data, and the `users__pets` table will contain the child
data. Here is what the tables may look like:

**users**

| id | name | _dlt_id | _dlt_load_id |
| --- | --- | --- | --- |
| 1 | Alice | wX3f5vn801W16A | 1234562350.98417 |
| 2 | Bob | rX8ybgTeEmAmmA | 1234562350.98417 |

**users__pets**

| id | name | type | _dlt_id | _dlt_parent_id | _dlt_list_idx |
| --- | --- | --- | --- | --- | --- |
| 1 | Fluffy | cat | w1n0PEDzuP3grw | wX3f5vn801W16A | 0 |
| 2 | Spot | dog | 9uxh36VU9lqKpw | wX3f5vn801W16A | 1 |
| 3 | Fido | dog | pe3FVtCWz8VuNA | rX8ybgTeEmAmmA | 0 |

When creating a database schema, dlt recursively unpacks nested structures into relational tables,
creating and linking children and parent tables.

This is how it works:

1. Each row in all (top level and child) data tables created by `dlt` contains UNIQUE column named
   `_dlt_id`.
1. Each child table contains FOREIGN KEY column `_dlt_parent_id` linking to a particular row
   (`_dlt_id`) of a parent table.
1. Rows in child tables come from the lists: `dlt` stores the position of each item in the list in
   `_dlt_list_idx`.
1. For tables that are loaded with the `merge` write disposition, we add a ROOT KEY column
   `_dlt_root_id`, which links child table to a row in top level table.


:::note

If you define your own primary key in a child table, it will be used to link to parent table
and the `_dlt_parent_id` and `_dlt_list_idx` will not be added. `_dlt_id` is always added even in
case the primary key or other unique columns are defined.

:::

## Naming convention: tables and columns

During a pipeline run, dlt [normalizes both table and column names](../../general-usage/schema.md#naming-convention) to ensure compatibility with the destination database's accepted format. All names from your source data will be transformed into snake_case and will only include alphanumeric characters. Please be aware that the names in the destination database may differ somewhat from those in your original input.

## Load IDs

Each pipeline run creates one or more load packages, which can be identified by their `load_id`. A load
package typically contains data from all [resources](../../general-usage/glossary.md#resource) of a
particular [source](../../general-usage/glossary.md#source). The `load_id` of a particular package
is added to the top data tables (`_dlt_load_id` column) and to the `_dlt_loads` table with a status 0 (when the load process
is fully completed).

The `_dlt_loads` table tracks complete loads and allows chaining transformations on top of them.
Many destinations do not support distributed and long-running transactions (e.g. Amazon Redshift).
In that case, the user may see the partially loaded data. It is possible to filter such data out—any
row with a `load_id` that does not exist in `_dlt_loads` is not yet completed. The same procedure may be used to delete and identify
and delete data for packages that never got completed.

For each load, you can test and [alert](../../running-in-production/alerting.md) on anomalies (e.g.
no data, too much loaded to a table). There are also some useful load stats in the `Load info` tab
of the [Streamlit app](understanding-the-tables.md#show-tables-and-data-in-the-destination)
mentioned above.

You can add [transformations](../transformations) and chain them together
using the `status` column. You start the transformation for all the data with a particular
`load_id` with a status of 0 and then update it to 1. The next transformation starts with the status
of 1 and is then updated to 2. This can be repeated for every additional transformation.

### Data lineage

Data lineage can be super relevant for architectures like the
[data vault architecture](https://www.data-vault.co.uk/what-is-data-vault/) or when troubleshooting.
The data vault architecture is a data warehouse that large organizations use when representing the
same process across multiple systems, which adds data lineage requirements. Using the pipeline name
and `load_id` provided out of the box by `dlt`, you are able to identify the source and time of
data.

You can [save](../../running-in-production/running.md#inspect-and-save-the-load-info-and-trace)
complete lineage info for a particular `load_id` including a list of loaded files, error messages
(if any), elapsed times, schema changes. This can be helpful, for example, when troubleshooting
problems.
