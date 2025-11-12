---
title: "Source: MS SQL replication"
description: MS SQL replication
keywords: [MSSQL, CDC, Change Tracking, MSSQL replication]
---

# MS SQL replication

import { DltHubFeatureAdmonition } from '@theme/DltHubFeatureAdmonition';

<DltHubFeatureAdmonition />

dltHub provides a comprehensive solution for syncing an MS SQL Server table using [Change Tracking](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-tracking-sql-server), a solution similar to CDC. By leveraging SQL Server's native Change Tracking feature, you can efficiently load incremental data changes — including inserts, updates, and deletes — into your destination.

## Prerequisites

Before you begin, ensure that Change Tracking is enabled on both your database and the tables you wish to track, as it is a feature that must be explicitly activated.

### Enable Change Tracking on the database

Run the following SQL command to enable Change Tracking on your database:

```sql
ALTER DATABASE [YourDatabaseName]
SET CHANGE_TRACKING = ON
(CHANGE_RETENTION = 7 DAYS, AUTO_CLEANUP = ON);
```

- `[YourDatabaseName]`: Replace with the name of your database.
- `CHANGE_RETENTION`: Specifies how long Change Tracking information is retained. In this example, it’s set to 7 days.
- `AUTO_CLEANUP`: When set to ON, Change Tracking information older than the retention period is automatically removed.

### Enable Change Tracking on the table

For each table you want to track, execute:

```sql
ALTER TABLE [YourSchemaName].[YourTableName]
ENABLE CHANGE_TRACKING
WITH (TRACK_COLUMNS_UPDATED = ON);
```

- `[YourSchemaName].[YourTableName]`: Replace with your schema and table names.
- `TRACK_COLUMNS_UPDATED`: When set to ON, allows you to see which columns were updated in a row. Set to OFF if you don’t need this level of detail.

### Set up dlthub and drivers

* Make sure dltHub is installed according to the [installation guide](../getting-started/installation.md).

* Install the Microsoft ODBC Driver for SQL Server according to the official [instructions](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16). If you prefer, there is also a [Python library alternative](https://www.pymssql.org/).

* Specify the credentials for your SQL Server connection according to the [sql_database source instructions](../../dlt-ecosystem/verified-sources/sql_database/setup)


## Setting up the pipeline

The process involves two main steps:

1. **Initial full load**: Use the `sql_table` function to perform a full backfill of your table data.
2. **Incremental loading**: Use the `create_change_tracking_table` function to load incremental changes using SQL Server's Change Tracking.

This approach ensures that you have a complete dataset from the initial load and efficiently keep it updated with subsequent changes.

### Initial full load

Get the Change Tracking version **before you execute the initial load** to make sure you do not miss any updates that may happen during it. This may result in "replaying" a few changes that happen during the load, but this will not have any impact on the destination data due to the `merge` write disposition.

```py
from dlt.hub.sources.mssql import get_current_change_tracking_version
from sqlalchemy import create_engine

connection_url = "mssql+pyodbc://username:password@your_server:port/YourDatabaseName?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"

engine = create_engine(connection_url)

tracking_version = get_current_change_tracking_version(engine)
```

To fully avoid any duplication, you may completely lock the table during the initial load.

Now you can use the `sql_table` resource to perform the initial backfill:

```py
import dlt
from dlt.sources.sql_database import sql_table

# Initial full load
initial_resource = sql_table(
    credentials=engine,
    schema=schema_name,
    table=table_name,
    reflection_level="full",
    write_disposition="merge",
)

pipeline = dlt.pipeline(
    pipeline_name='sql_server_sync_pipeline',
    destination='your_destination',
    dataset_name='destination_dataset',
)

# Run the pipeline for initial load
pipeline.run(initial_resource)
```

By default, the `_DLT_DELETED` or the `_DLT_SYS_CHANGE_VERSION` columns are only created by the incremental change tracking resource when there are changes. If you want these to be created during the initial load, you can configure this with `apply_hints` before running the pipeline as follows:

```py
initial_resource.apply_hints(
        columns=[
            {"name": "_dlt_sys_change_version", "data_type": "bigint"},
            {"name": "_dlt_deleted", "data_type": "text", "precision": 10},
        ]
    )
```

Next, configure the incremental resource for the first run with the `create_change_tracking_table` function and run it **once**:

```py
from dlthub.sources.mssql import create_change_tracking_table

# Optional: Configure engine isolation level
# use it if you create an Engine implicitly
def configure_engine_isolation_level(engine):
    return engine.execution_options(isolation_level="SERIALIZABLE")

incremental_resource = create_change_tracking_table(
    credentials=engine,
    table=table_name,
    schema=schema_name,
    initial_tracking_version=tracking_version,
    engine_adapter_callback=configure_engine_isolation_level,
)

pipeline.run(incremental_resource)
```
When running for the first time, it is necessary to pass the `tracking_version` in the `initial_tracking_version` argument. This will initialize incremental loading and keep the updated tracking version in the dlt state. In subsequent runs, you do not need to provide the initial value anymore.

### Incremental loading

After the initial load, you can run the `create_change_tracking_table` resource on a schedule to load only the changes since the last tracking version using SQL Server’s `CHANGETABLE` function.
You do not need to pass `initial_tracking_version` anymore, since this is automatically stored in the dlt state.

```py
from dlthub.sources.mssql import create_change_tracking_table

incremental_resource = create_change_tracking_table(
    credentials=engine,
    table=table_name,
    schema=schema_name,
)
pipeline.run(incremental_resource)
```

:::note
 The `write_disposition` is by default set to `merge`, which handles upserts based on primary keys. This determines the behavior when new data is loaded, especially regarding duplicates, updates, and deletes.
:::

## Full code example

<details>

<summary>Show full code example</summary>

```py
import dlt

from sqlalchemy import create_engine

from dlt.sources.sql_database import sql_table
from dlthub.sources.mssql import (
    create_change_tracking_table,
    get_current_change_tracking_version,
)


def single_table_initial_load(connection_url: str, schema_name: str, table_name: str) -> None:
    """Performs an initial full load and sets up tracking version and incremental loads"""
    # Create a new pipeline
    pipeline = dlt.pipeline(
        pipeline_name=f"{schema_name}_{table_name}_sync",
        destination="duckdb",
        dataset_name=schema_name,
    )

    # Explicit database connection
    engine = create_engine(connection_url, isolation_level="SNAPSHOT")

    # Initial full load
    initial_resource = sql_table(
        credentials=engine,
        schema=schema_name,
        table=table_name,
        reflection_level="full",
        write_disposition="merge",
    )

    # Get the current tracking version before you run the pipeline to make sure
    # you do not miss any records
    tracking_version = get_current_change_tracking_version(engine)
    print(f"will track from: {tracking_version}")  # noqa

    # Apply hints to create _DLT_DELETED and _DLT_SYS_CHANGE_VERSION columns on the initial load
    # This is an optional step
    initial_resource.apply_hints(
        columns=[
            {"name": "_dlt_sys_change_version", "data_type": "bigint"},
            {"name": "_dlt_deleted", "data_type": "text", "precision": 10},
        ]
    )

    # Run the pipeline for the initial load
    # NOTE: we always drop data and state from the destination on initial load
    print(pipeline.run(initial_resource, refresh="drop_resources"))  # noqa

    # Incremental loading resource
    incremental_resource = create_change_tracking_table(
        credentials=engine,
        table=table_name,
        schema=schema_name,
        initial_tracking_version=tracking_version,
    )

    # Run the pipeline for incremental load
    print(pipeline.run(incremental_resource))  # noqa


def single_table_incremental_load(connection_url: str, schema_name: str, table_name: str) -> None:
    """Continues loading incrementally"""
    # Make sure you use the same pipeline and dataset names in order to continue incremental
    # loading.
    pipeline = dlt.pipeline(
        pipeline_name=f"{schema_name}_{table_name}_sync",
        destination="duckdb",
        dataset_name=schema_name,
    )

    engine = create_engine(connection_url, isolation_level="SNAPSHOT")
    # We do not need to pass the tracking version anymore
    incremental_resource = create_change_tracking_table(
        credentials=engine,
        table=table_name,
        schema=schema_name,
    )
    print(pipeline.run(incremental_resource))  # noqa


if __name__ == "__main__":
    # Change Tracking already enabled here
    test_db = "my_database83ed099d2d98a3ccfa4beae006eea44c"
    # A test run with a local mssql instance
    connection_url = (
        f"mssql+pyodbc://sa:Strong%21Passw0rd@localhost:1433/{test_db}"
        "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    )
    single_table_initial_load(
        connection_url,
        "my_dlt_source",
        "app_user",
    )
    single_table_incremental_load(
        connection_url,
        "my_dlt_source",
        "app_user",
    )
```

</details>



## Understanding the change tracking query

The incremental loading process uses a SQL query that joins the CHANGETABLE function with the source table to fetch the latest changes. Here’s a simplified version of the query:

```sql
SELECT
    [Columns],
    ct.SYS_CHANGE_VERSION AS _dlt_sys_change_version,
    CASE WHEN ct.SYS_CHANGE_OPERATION = 'D' THEN 'DELETE' ELSE NULL END AS _dlt_deleted
FROM
    CHANGETABLE(CHANGES [YourSchemaName].[YourTableName], @last_version) AS ct
    LEFT JOIN [YourSchemaName].[YourTableName] AS t
        ON ct.[PrimaryKey] = t.[PrimaryKey]
WHERE
    ct.SYS_CHANGE_VERSION > @last_version
ORDER BY
    ct.SYS_CHANGE_VERSION ASC
```

- *CHANGETABLE*: Retrieves changes for the specified table since the last tracking version.
- **Join with source table**: The join retrieves the current data for the changed rows.
- *SYS_CHANGE_VERSION*: Used to track and order changes.
- *_dlt_deleted*: Indicates if a row was deleted.

:::note
 Since the query joins with the production table, there may be implications for locking and performance. Ensure your database can handle the additional load, and consider isolation levels if necessary.
:::


## Full refresh

:::warning
Doing a full refresh will drop the destination table, i.e., delete data from the destination, and reset the state holding the tracking version.
:::
You can trigger a full refresh by performing a full load again and passing `drop_resources` to the run method (as described in the [pipeline configuration](../../general-usage/pipeline#selectively-drop-tables-and-resource-state-with-drop_resources)):
```py
pipeline.run(initial_resource, refresh="drop_resources")
```


## Handling deletes

There is an optional parameter that can be passed to `create_change_tracking_table` for configuring how to handle deletes:

```py
from dlthub.sources.mssql import create_change_tracking_table

incremental_resource = create_change_tracking_table(
    credentials=engine,
    table=table_name,
    schema=schema_name,
    hard_delete=True,
)
pipeline.run(incremental_resource)
```

### Hard deletes

By default, `hard_delete` is set to `True`, meaning hard deletes are performed, i.e., rows deleted in the source will be permanently removed from the destination.

Replicated data allows for NULLs for not nullable columns when a record is deleted. To avoid additional tables that hold deleted rows and additional merge steps, dlt emits placeholder values that are stored in the staging dataset only.

### Soft deletes

If `hard_delete` is set to `False`, soft deletes are performed, i.e., rows deleted in the source will be marked as deleted but not physically removed from the destination.

In this case, the destination schema must accept NULLs for the replicated columns, so make sure you pass the `remove_nullability_adapter` adapter to the `sql_table` resource:

```py
from dlthub.sources.mssql import remove_nullability_adapter

table = sql_table(
    table_adapter_callback=remove_nullability_adapter,
)
```

