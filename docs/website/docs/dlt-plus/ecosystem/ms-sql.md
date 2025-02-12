---
title: MSSQL replication
description: MSSQL replication and helpers documentation
---

# MSSQL Replication and helpers

## Syncing SQL Server Tables with Change Tracking using DLT
This guide provides a comprehensive solution for syncing a SQL Server table using change tracking with **dlt**. By leveraging SQL Server's native change tracking feature, you can efficiently load incremental data changes - including inserts, updates, and deletes into your destination.

## Overview

The process involves two main steps:

1. **Initial Full Load**: Use the `sql_table` function to perform a full backfill of your table data.
2. **Incremental Loading**: Use the `create_change_tracking_table` function to load incremental changes using SQL Server's change tracking.

This approach ensures that you have a complete dataset from the initial load and efficiently keep it updated with subsequent changes.

## Prerequisites

### Enabling Change Tracking in SQL Server

Before you begin, ensure that change tracking is enabled on both your database and the tables you wish to track. Change tracking is a feature that must be explicitly activated.

#### Enable Change Tracking on the Database

Run the following SQL command to enable change tracking on your database:

```sql
ALTER DATABASE [YourDatabaseName]
SET CHANGE_TRACKING = ON
(CHANGE_RETENTION = 7 DAYS, AUTO_CLEANUP = ON);
```

- *[YourDatabaseName]*: Replace with the name of your database.
- *CHANGE_RETENTION*: Specifies how long change tracking information is retained. In this example, it’s set to 7 days.
- *AUTO_CLEANUP*: When set to ON, change tracking information older than the retention period is automatically removed.

#### Enable Change Tracking on the Table

For each table you want to track, execute:

```sql
ALTER TABLE [YourSchemaName].[YourTableName]
ENABLE CHANGE_TRACKING
WITH (TRACK_COLUMNS_UPDATED = ON);
```

- *[YourSchemaName].[YourTableName]*: Replace with your schema and table names.
- *TRACK_COLUMNS_UPDATED*: When set to ON, allows you to see which columns were updated in a row. Set to OFF if you don’t need this level of detail.

## Concept and Data Flow

### Initial Full Load with sql_table

The initial full load does the following:
1. Obtains current change tracking version
2. Uses `sql_table` resource, to extracts all data from the specified table. This step ensures that your destination dataset starts with a complete snapshot of your source table.
3. Initializes the incremental resource `create_change_tracking_table` with the tracking version from step 1 by running it once.

From that moment, incremental resource will manage the tracking version across the subsequent runs internally.

#### Obtain the Current Change Tracking Version

Get the change tracking version **before you execute initial load** to make
sure you do not miss any updates that may happen during it. This may result in
"replaying" a few changes that happen during the load, but that will not have
impact on the destination data due to `merge` write disposition. 

To fully avoid any duplication you may completely lock the table during an initial load. Ping our solution engineering for details.

### Incremental Loading with create_change_tracking_table

After the initial load, subsequent runs use the `create_change_tracking_table` resource to load only the changes since the last tracking version. This function leverages SQL Server’s CHANGETABLE function to efficiently retrieve changes.

**Note**: The `write_disposition` parameter is crucial for controlling how data is written to the destination table during incremental loads. It determines the behavior when new data is loaded, especially regarding duplicates, updates, and deletes. Default Value: The default `write_disposition` is "merge", which handles upserts based on primary keys.

#### Understanding the Change Tracking Query

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
- **Join with Source Table**: The join retrieves the current data for the changed rows.
- *SYS_CHANGE_VERSION*: Used to track and order changes.
- *_dlt_deleted*: Indicates if a row was deleted.

**Note**: Since the query joins with the production table, there may be implications for locking and performance. Ensure your database can handle the additional load, and consider isolation levels if necessary.


## Step-by-Step Recommended Usage

1. Set up `dlt` project.
You can follow the documentation for `sql_database` in order to setup credentials:

https://dlthub.com/docs/devel/dlt-ecosystem/verified-sources/sql_database/setup

You'll also need a working ODBC driver:

https://dlthub.com/docs/dlt-ecosystem/destinations/mssql

2. Set Up the Pipeline

```py
import dlt

pipeline = dlt.pipeline(
    pipeline_name='sql_server_sync_pipeline',
    destination='your_destination',
    dataset_name='destination_dataset',
)
```

2. Configure the Database Connection

In our example we create an explicit SQLAlchemy Engine. This is not required, you are free to
follow the instructions to create credentials in `secrets.toml` (or env variables) instead: 

```py
from sqlalchemy import create_engine

connection_url = "mssql+pyodbc://username:password@your_server:port/YourDatabaseName?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"

engine = create_engine(connection_url)
```

4. Obtain the Current Change Tracking Version

Get the change tracking version **before you execute initial load** to make
sure you do not miss any updates that may happen during it.

```py
from dlt_plus.sources.mssql import get_current_change_tracking_version

# Get current tracking version before you run the pipeline to make sure
# you do not miss any records,
tracking_version = get_current_change_tracking_version(engine)
```

3. Perform the Initial Full Load

Use the sql_table function to perform the initial backfill:

```py
from dlt.sources.sql_database import sql_table

# Initial full load
initial_resource = sql_table(
    credentials=engine,
    schema=schema_name,
    table=table_name,
    reflection_level="full",
    write_disposition="merge",
)

# Run the pipeline for initial load
pipeline.run(initial_resource)
```

4. Configure the Incremental Resource For the First Run

Use the `create_change_tracking_table` function to set up incremental loading:

```py
from dlt_plus.sources.mssql import create_change_tracking_table

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
```
When running for a first time your should pass `tracking_version` in `initial_tracking_version` argument.
This will initialize incremental loading and keep the updated tracking version in the `dlt` state.
In subsequent run, you do not need to provide the initial value anymore.

5. Run Incremental Loading for a First Time.

Run the pipeline with the incremental resource immediately after the initial load with `sql_table`:
```py
pipeline.run(incremental_resource)
```

6. Schedule incremental loading.
With initial load completed and tracking version in `dlt` state, you may run `create_change_tracking_table` on schedule.
You do not need to pass `initial_tracking_version` anymore (state takes precedence and this value will be ignored).

Note that `dlt` restores the state and the schemas using data in the destination. You do not need to store any
state yourself.

```py
incremental_resource = create_change_tracking_table(
    credentials=engine,
    table=table_name,
    schema=schema_name,
)
pipeline.run(incremental_resource)
```

## Full Refresh
You can trigger a full refresh by performing a full load again and passing to the run method
```py
pipeline.run(initial_resource, refresh="drop_resources")
```
This option drops the destination table (just before loading data again) and resets the state holding
the tracking version.

https://dlthub.com/docs/general-usage/pipeline#selectively-drop-tables-and-resource-state-with-drop_resources


## Full Code Example

Example table sync script is available via `dlt-plus` package in `sources._core_source_templates/mssql_pipeline.py`
```py
from dlt_plus.sources._core_source_templates import mssql_pipeline
```

* `single_table_initial_load` does full initial load, it also drops existing and state
* `single_table_incremental_load` continues incremental loading via the incremental state saved in the destination


## Hard vs. Soft Deletes

- **Hard Delete** (hard_delete=True): this flag is set to True by default. When hard_delete=True, any rows deleted in the source SQL Server table will be permanently removed from the destination dataset. This ensures that the destination mirrors the source exactly.

Replicated data allows for NULLs for not nullable columns when record is deleted. To avoid additional tables that hold deleted rows and additional merge steps,
`dlt` emits placeholder values that are stored in staging dataset only.

- **Soft Delete** (hard_delete=False): When set to False, deleted rows in the source will be marked as deleted in the destination but not physically removed. All
columns from replicated table (except primary key) will be set to NULL and `_dlt_deleted` set to `D`.

To use to soft delete, destination schema must accept NULLs for the replicated columns. `remove_nullability_adapter` table adapter will do it when
passed to `sql_table`:
```py
from dlt_plus.sources.mssql import remove_nullability_adapter

table = sql_table(
    table_adapter_callback=remove_nullability_adapter,
    ...
)
```

## License checks

You'll need to have a license in your environment to call `create_change_tracking_table` function and `get_current_change_tracking_version`.