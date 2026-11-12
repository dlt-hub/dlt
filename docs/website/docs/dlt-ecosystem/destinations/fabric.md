---
title: Fabric
description: Microsoft Fabric Warehouse `dlt` destination
keywords: [fabric, microsoft fabric, warehouse, destination, data warehouse, synapse]
---
# Microsoft Fabric Warehouse

## Install dlt with Fabric

To install the dlt library with Fabric Warehouse dependencies, use:

```cli
pip install "dlt[fabric]"
```

This will install `dlt` with the `fabric` extra, which contains the [`mssql-python`](https://github.com/microsoft/mssql-python) driver that `dlt` uses to connect to Fabric.

<!--@@@DLT_DESTINATION_CAPABILITIES fabric-->

## Setup guide

### Prerequisites

`mssql-python` comes with the _Microsoft ODBC Driver 18 for SQL Server_, so you do not need to install the driver on your system.

### Service Principal Authentication

Fabric Warehouse requires Azure Active Directory Service Principal authentication. You'll need:

1. **Tenant ID**: Your Azure AD tenant ID (GUID)
2. **Client ID**: Application (service principal) client ID (GUID)
3. **Client Secret**: Application client secret
4. **Host**: Your Fabric warehouse SQL endpoint
5. **Database**: The database name within your warehouse

**Finding your SQL endpoint:**

- In the Fabric portal, go to your warehouse **Settings**
- Select **SQL endpoint**
- Copy the **SQL connection string** - it should be in the format: `<guid>.datawarehouse.fabric.microsoft.com`

### Create a pipeline

1. Initialize a project with a pipeline that loads to Fabric by running:

```cli
dlt init chess fabric
```

2. Install the necessary dependencies for Fabric by running:

```cli
pip install -r requirements.txt
```

or run:

```cli
pip install "dlt[fabric]"
```

3. Enter your credentials into `.dlt/secrets.toml`.

```toml
[destination.fabric.credentials]
host = "<your-warehouse-guid>.datawarehouse.fabric.microsoft.com"
database = "mydb"
azure_tenant_id = "your-azure-tenant-id"
azure_client_id = "your-client-id"
azure_client_secret = "your-client-secret"
port = 1433
connect_timeout = 30
```

## Write disposition

All write dispositions are supported, including the [`upsert`](../../general-usage/merge-loading.md#upsert-strategy) and [`insert-only`](../../general-usage/merge-loading.md#insert-only-strategy) merge strategies.

If you set the [`replace` strategy](../../general-usage/full-loading.md) to `staging-optimized`, the destination tables will be dropped and recreated with an `ALTER SCHEMA ... TRANSFER`. The operation is atomic: Fabric supports DDL transactions.

## Staging support

Fabric Warehouse supports staging data via **OneLake Lakehouse** or **Azure Blob / Data Lake Storage** using the `COPY INTO` command for efficient bulk loading. This is the recommended approach for large datasets.

### Examples

```py
import dlt

pipeline = dlt.pipeline(
    destination="fabric",
    staging="filesystem",
    dataset_name='my_dataset'
)
```

#### `.dlt/secrets.toml` when using OneLake

```toml
[destination.fabric.credentials]
# your fabric credentials

[destination.filesystem]
bucket_url = "abfss://<your-workspace-guid>@onelake.dfs.fabric.microsoft.com/<your-lakehouse-guid>/Files"

[destination.filesystem.credentials]
azure_storage_account_name = "onelake"
azure_account_host = "onelake.blob.fabric.microsoft.com"
# use same Service Principal credentials as in [destination.fabric.credentials]
azure_tenant_id = "your-tenant-id"
azure_client_id = "your-client-id"
azure_client_secret = "your-client-secret"
```

**Finding your GUIDs**:

1. Navigate to your Fabric workspace in the browser
2. The workspace GUID is in the URL: `https://fabric.microsoft.com/groups/<workspace_guid>/...`
3. Open your Lakehouse
4. The lakehouse GUID is in the URL: `https://fabric.microsoft.com/.../lakehouses/<lakehouse_guid>`

#### `.dlt/secrets.toml` when using Azure Blob / Data Lake Storage

```toml
[destination.fabric.credentials]
# your fabric credentials

[destination.filesystem]
bucket_url = "az://your-container-name"

[destination.filesystem.credentials]
azure_storage_account_name = "your-storage-account-name"
azure_storage_account_key = "your-storage-account-key"
```

## Data loading

Data is loaded via INSERT statements by default. Fabric Warehouse has a limit of 1000 rows per INSERT, and this is what we use.

## Supported file formats

* [insert-values](../file-formats.md#sql-insert) is the default and currently only supported format

## Supported column hints

**fabric** will create unique indexes for all columns with `unique` hints. This behavior **is disabled by default**.

### Table and column identifiers

Fabric Warehouse (like [SQL Server](./mssql.md)) uses **case-insensitive identifiers** but preserves the casing of identifiers stored in the INFORMATION SCHEMA. You can use [case-sensitive naming conventions](../../general-usage/naming-convention.md#case-sensitive-and-insensitive-destinations) to keep the identifier casing. Note that you risk generating identifier collisions, which are detected by `dlt` and will fail the load process.

## Syncing of `dlt` state

This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

## Data types

Fabric Warehouse differs from standard SQL Server in several important ways:

### VARCHAR vs NVARCHAR

Fabric Warehouse uses `varchar` for text columns instead of `nvarchar`. Because `varchar` lengths are counted in
bytes while `precision` counts characters, the precision is multiplied by 4 (the worst case for UTF-8):

- `text` → `varchar(max)`
- `text` with `precision` → `varchar(precision * 4)`, for example `precision=25` → `varchar(100)`
- `text` with `precision` above 2000 → `varchar(max)`, since 8000 is the longest length Fabric accepts

### DATETIME2 vs DATETIMEOFFSET

Fabric uses `datetime2` for timestamps instead of `datetimeoffset`:

- `timestamp` → `datetime2(6)` (precision limited to 0-6, not 0-7)
- `time` → `time(6)` (explicit precision required)

### JSON Storage

Fabric does not support native JSON columns. JSON objects are stored as `varchar(max)` columns.

## Collation Support

Fabric Warehouse supports UTF-8 collations.

**Default collation**: `Latin1_General_100_BIN2_UTF8` (case-sensitive, UTF-8)

You can specify a different collation:

```toml
[destination.fabric]
collation = "Latin1_General_100_CI_AS_KS_WS_SC_UTF8"  # case-insensitive
```

Or in code:

```py
pipeline = dlt.pipeline(
    destination=dlt.destinations.fabric(
        credentials={},
        collation="Latin1_General_100_CI_AS_KS_WS_SC_UTF8"
    )
)
```

## Additional destination options

The **fabric** destination **does not** create UNIQUE indexes by default on columns with the `unique` hint (i.e., `_dlt_id`). To enable this behavior:

```toml
[destination.fabric]
create_indexes=true
```

## Differences from MSSQL Destination

While Fabric Warehouse is based on SQL Server, there are key differences:

1. **Authentication**: Fabric requires Service Principal; username/password auth is not supported
2. **Type System**: Uses `varchar` and `datetime2` instead of `nvarchar` and `datetimeoffset`
3. **Collation**: Uses UTF-8 collations
4. **SQL Dialect**: Uses `fabric` SQLglot dialect for proper SQL generation
5. **Driver**: Connects with `mssql-python` instead of `pyodbc`

### dbt support

Integration with [dbt](../transformations/dbt/dbt.md) is supported via [dbt-fabric](https://github.com/Microsoft/dbt-fabric). Both Service Principal and default Azure credentials are supported and shared with dbt runners.

Note: `dbt-fabric` versions that connect with `pyodbc` still need the _Microsoft ODBC Driver for SQL Server_ [installed on your system](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).

## Troubleshooting

### Authentication Failures

Ensure your Service Principal has:

- Proper permissions on the Fabric workspace
- Access to the target database/warehouse
- Correct tenant ID (your Azure AD tenant, not the workspace/capacity ID)

### UTF-8 Character Issues

If you experience character encoding issues:

1. Verify your warehouse uses a UTF-8 collation
2. Consider using the case-insensitive UTF-8 collation if needed

## Additional Resources

- [Microsoft Fabric Documentation](https://learn.microsoft.com/en-us/fabric/)
- [Fabric Warehouse Documentation](https://learn.microsoft.com/en-us/fabric/data-warehouse/)
- [Service Principal Setup Guide](https://learn.microsoft.com/en-us/fabric/security/service-principals)

<!--@@@DLT_TUBA fabric-->
