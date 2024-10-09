---
title: Azure Synapse
description: Azure Synapse `dlt` destination
keywords: [synapse, destination, data warehouse]
---

# Synapse

## Install dlt with Synapse
**To install the dlt library with Synapse dependencies:**
```sh
pip install "dlt[synapse]"
```

## Setup guide

### Prerequisites

* **Microsoft ODBC Driver for SQL Server**

    The _Microsoft ODBC Driver for SQL Server_ must be installed to use this destination.
    This cannot be included with `dlt`'s Python dependencies, so you must install it separately on your system. You can find the official installation instructions [here](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16).

    Supported driver versions:
    * `ODBC Driver 18 for SQL Server`

    > 💡 Older driver versions do not work properly because they do not support the `LongAsMax` keyword that was [introduced](https://learn.microsoft.com/en-us/sql/connect/odbc/windows/features-of-the-microsoft-odbc-driver-for-sql-server-on-windows?view=sql-server-ver15#microsoft-odbc-driver-180-for-sql-server-on-windows) in `ODBC Driver 18 for SQL Server`. Synapse does not support the legacy ["long data types"](https://learn.microsoft.com/en-us/sql/t-sql/data-types/ntext-text-and-image-transact-sql), and requires "max data types" instead. `dlt` uses the `LongAsMax` keyword to automatically do the conversion.
* **Azure Synapse Workspace and dedicated SQL pool**

    You need an Azure Synapse workspace with a dedicated SQL pool to load data into. If you do not have one yet, you can use this [quickstart](https://learn.microsoft.com/en-us/azure/synapse-analytics/quickstart-create-sql-pool-studio).

### Steps

**1. Initialize a project with a pipeline that loads to Synapse by running**
```sh
dlt init chess synapse
```

**2. Install the necessary dependencies for Synapse by running**
```sh
pip install -r requirements.txt
```
This will install `dlt` with the **synapse** extra that contains all dependencies required for the Synapse destination.

**3. Create a loader user**

Execute the following SQL statements to set up the [loader](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/data-loading-best-practices#create-a-loading-user) user. Change the password and replace `yourpool` with the name of your dedicated SQL pool:
```sql
-- on master database, using a SQL admin account

CREATE LOGIN loader WITH PASSWORD = 'your_loader_password';
```

```sql
-- on yourpool database

CREATE USER loader FOR LOGIN loader;

-- DDL permissions
GRANT CREATE SCHEMA ON DATABASE :: yourpool TO loader;
GRANT CREATE TABLE ON DATABASE :: yourpool TO loader;
GRANT CREATE VIEW ON DATABASE :: yourpool TO loader;

-- DML permissions
GRANT ADMINISTER DATABASE BULK OPERATIONS TO loader; -- only required when loading from staging Storage Account
```

Optionally, you can create a `WORKLOAD GROUP` and add the `loader` user as a member to manage [workload isolation](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-workload-isolation). See the [instructions](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/data-loading-best-practices#create-a-loading-user) on setting up a loader user for an example of how to do this.

**4. Enter your credentials into `.dlt/secrets.toml`.**

Example, replace with your database connection info:
```toml
[destination.synapse.credentials]
database = "yourpool"
username = "loader"
password = "your_loader_password"
host = "your_synapse_workspace_name.sql.azuresynapse.net"
```

Equivalently, you can also pass a connection string as follows:

```toml
# Keep it at the top of your TOML file, before any section starts
destination.synapse.credentials = "synapse://loader:your_loader_password@your_synapse_workspace_name.azuresynapse.net/yourpool"
```

To pass credentials directly you can use the `credentials` argument of `dlt.destinations.synapse(...)`:
```py
pipeline = dlt.pipeline(
    pipeline_name='chess',
    destination=dlt.destinations.synapse(
        credentials='synapse://loader:your_loader_password@your_synapse_workspace_name.azuresynapse.net/yourpool'
    ),
    dataset_name='chess_data'
)
```
To use **Active Directory Principal**, you can use the `sqlalchemy.engine.URL.create` method to create the connection URL using your Active Directory Service Principal credentials. First, create the connection string as:
```py
conn_str = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={server_name};"
    f"DATABASE={database_name};"
    f"UID={service_principal_id}@{tenant_id};"
    f"PWD={service_principal_secret};"
    f"Authentication=ActiveDirectoryServicePrincipal"
)
```

Next, create the connection URL:
```py
connection_url = URL.create(
    "mssql+pyodbc",
    query={"odbc_connect": conn_str}
)
```

Once you have the connection URL, you can directly use it in your pipeline configuration or convert it to a string.
```py
pipeline = dlt.pipeline(
    pipeline_name='chess',
    destination=dlt.destinations.synapse(
        credentials=connection_url.render_as_string(hide_password=False)
    ),
    dataset_name='chess_data'
)
```

## Write disposition
All write dispositions are supported.

> ❗ The `staging-optimized` [`replace` strategy](../../general-usage/full-loading.md) is **not** implemented for Synapse.

## Data loading
Data is loaded via `INSERT` statements by default.

> 💡 Multi-row `INSERT INTO ... VALUES` statements are **not** possible in Synapse, because it doesn't support the [Table Value Constructor](https://learn.microsoft.com/en-us/sql/t-sql/queries/table-value-constructor-transact-sql). `dlt` uses `INSERT INTO ... SELECT ... UNION` statements as described [here](https://stackoverflow.com/a/73579830) to work around this limitation.

## Supported file formats
* [insert-values](../file-formats/insert-format.md) is used by default
* [parquet](../file-formats/parquet.md) is used when [staging](#staging-support) is enabled

## Data type limitations
* **Synapse cannot load `TIME` columns from `parquet` files**. `dlt` will fail such jobs permanently. Use the `insert_values` file format instead, or convert `datetime.time` objects to `str` or `datetime.datetime` to load `TIME` columns.
* **Synapse does not have a nested/JSON/struct data type**. The `dlt` `json` data type is mapped to the `nvarchar` type in Synapse.

## Table index type
The [table index type](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-tables-index) of the created tables can be configured at the resource level with the `synapse_adapter`:

```py
from dlt.destinations.adapters import synapse_adapter

info = pipeline.run(
    synapse_adapter(
        data=your_resource,
        table_index_type="clustered_columnstore_index",
    )
)
```

Possible values:
* `heap`: create [HEAP](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-tables-index#heap-tables) tables that do not have an index **(default)**
* `clustered_columnstore_index`: create [CLUSTERED COLUMNSTORE INDEX](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-tables-index#clustered-columnstore-indexes) tables


> ❗ Important:
>* **Set `default_table_index_type` to `"clustered_columnstore_index"` if you want to change the default** (see [additional destination options](#additional-destination-options)).
>* **CLUSTERED COLUMNSTORE INDEX tables do not support the `varchar(max)`, `nvarchar(max)`, and `varbinary(max)` data types.** If you don't specify the `precision` for columns that map to any of these types, `dlt` will use the maximum lengths `varchar(4000)`, `nvarchar(4000)`, and `varbinary(8000)`.
>* **While Synapse creates CLUSTERED COLUMNSTORE INDEXES by default, `dlt` creates HEAP tables by default.** HEAP is a more robust choice because it supports all data types and doesn't require conversions.
>* **When using the `insert-from-staging` [`replace` strategy](../../general-usage/full-loading.md), the staging tables are always created as HEAP tables**—any configuration of the table index types is ignored. The HEAP strategy makes sense for staging tables for reasons explained [here](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-tables-index#heap-tables).
>* **`dlt` system tables are always created as HEAP tables, regardless of any configuration.** This is in line with Microsoft's recommendation that "for small lookup tables, less than 60 million rows, consider using HEAP or clustered index for faster query performance."
>* Child tables, if any, inherit the table index type of their parent table.

## Supported column hints

Synapse supports the following [column hints](../../general-usage/schema#tables-and-columns):

* `primary_key` - creates a `PRIMARY KEY NONCLUSTERED NOT ENFORCED` constraint on the column.
* `unique` - creates a `UNIQUE NOT ENFORCED` constraint on the column.

> ❗ These hints are **disabled by default**. This is because the `PRIMARY KEY` and `UNIQUE` [constraints](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-table-constraints) are tricky in Synapse: they are **not enforced** and can lead to inaccurate results if the user does not ensure all column values are unique. For the column hints to take effect, the `create_indexes` configuration needs to be set to `True`, see [additional destination options](#additional-destination-options).

## Staging support
Synapse supports Azure Blob Storage (both standard and [ADLS Gen2](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)) as a file staging destination. `dlt` first uploads Parquet files to the blob container, and then instructs Synapse to read the Parquet file and load its data into a Synapse table using the [COPY INTO](https://learn.microsoft.com/en-us/sql/t-sql/statements/copy-into-transact-sql) statement.

Please refer to the [Azure Blob Storage filesystem documentation](./filesystem.md#azure-blob-storage) to learn how to configure credentials for the staging destination. By default, `dlt` will use these credentials for both the write into the blob container, and the read from it to load into Synapse. Managed Identity authentication can be enabled through the `staging_use_msi` option (see [additional destination options](#additional-destination-options)).

To run Synapse with staging on Azure Blob Storage:

```py
# Create a dlt pipeline that will load
# chess player data to the Synapse destination
# via staging on Azure Blob Storage
pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline',
    destination='synapse',
    staging='filesystem', # add this to activate the staging location
    dataset_name='player_data'
)
```

## Additional destination options
The following settings can optionally be configured:
```toml
[destination.synapse]
default_table_index_type = "heap"
create_indexes = "false"
staging_use_msi = "false"

[destination.synapse.credentials]
port = "1433"
connect_timeout = 15
```

`port` and `connect_timeout` can also be included in the connection string:

```toml
# Keep it at the top of your TOML file, before any section starts
destination.synapse.credentials = "synapse://loader:your_loader_password@your_synapse_workspace_name.azuresynapse.net:1433/yourpool?connect_timeout=15"
```

Descriptions:
- `default_table_index_type` sets the [table index type](#table-index-type) that is used if no table index type is specified on the resource.
- `create_indexes` determines if `primary_key` and `unique` [column hints](#supported-column-hints) are applied.
- `staging_use_msi` determines if the Managed Identity of the Synapse workspace is used to authorize access to the [staging](#staging-support) Storage Account. Ensure the Managed Identity has the [Storage Blob Data Reader](https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles#storage-blob-data-reader) role (or a higher-privileged role) assigned on the blob container if you set this option to `"true"`.
- `port` is used for the ODBC connection.
- `connect_timeout` sets the timeout for the `pyodbc` connection attempt, in seconds.

### dbt support
Integration with [dbt](../transformations/dbt/dbt.md) is currently not supported.

### Syncing of `dlt` state
This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

<!--@@@DLT_TUBA synapse-->

