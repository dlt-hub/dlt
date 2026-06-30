---
title: Fabric
description: Microsoft Fabric Warehouse `dlt` destination
keywords: [fabric, microsoft fabric, warehouse, destination, data warehouse, synapse]
---

# Microsoft Fabric Warehouse

## Install dlt with Fabric
**To install the dlt library with Fabric Warehouse dependencies, use:**
```sh
pip install "dlt[fabric]"
```

This will install `dlt` with the `mssql` extra, which contains all the dependencies required by the SQL Server client that Fabric uses.

<!--@@@DLT_DESTINATION_CAPABILITIES fabric-->

## Setup guide

### Prerequisites

This destination uses the [mssql-python](https://github.com/microsoft/mssql-python) driver, which is
installed automatically with `dlt[fabric]` and bundles the SQL Server client libraries. No separate
ODBC driver installation is required.

### Authentication

Fabric Warehouse authenticates with Microsoft Entra ID. Whichever method you choose, you always set the warehouse SQL endpoint as the `host` (`<guid>.datawarehouse.fabric.microsoft.com`) and the warehouse name as the `database`.

**Finding your SQL endpoint:**
- In the Fabric portal, go to your warehouse **Settings**
- Select **SQL endpoint**
- Copy the **SQL connection string** - it should be in the format: `<guid>.datawarehouse.fabric.microsoft.com`

The method is selected with the `authentication` credential option. Fabric Warehouse accepts these
authentication types:

- Service Principal, and the other methods the ODBC driver signs in with itself
- [azure-identity](https://learn.microsoft.com/python/api/overview/azure/identity-readme) methods, where `dlt` acquires the token
- `fab_notebookutils`, for pipelines running inside a Fabric notebook

With the **driver-native** methods the ODBC driver performs the Entra ID sign-in. `ActiveDirectoryServicePrincipal` is the default and needs `azure_tenant_id`, `azure_client_id` and `azure_client_secret`; `ActiveDirectoryPassword` needs `username` and `password`. `ActiveDirectoryIntegrated`, `ActiveDirectoryInteractive` and `ActiveDirectoryMsi` need no further fields.

With the **azure-identity** methods `dlt` acquires an access token and injects it into the connection, so no secret is needed in `secrets.toml`. These work cross-platform, including macOS, where the ODBC driver's built-in Entra ID modes are unreliable. Use `ActiveDirectoryDefault` (alias `default`) for `DefaultAzureCredential`, or `ActiveDirectoryDeviceCode` for `DeviceCodeCredential`. When `authentication` is left at its default but no Service Principal secret is configured, `dlt` falls back to `ActiveDirectoryDefault`.

**`fab_notebookutils`** authenticates as whoever runs the notebook through [NotebookUtils](https://learn.microsoft.com/fabric/data-engineering/notebookutils/notebookutils-credentials), so that identity needs write access to the warehouse. A Fabric notebook has no environment variables, managed identity or Azure CLI login, so `DefaultAzureCredential` cannot sign in there:

```toml
[destination.fabric.credentials]
host = "<your-warehouse-guid>.datawarehouse.fabric.microsoft.com"
database = "mydb"
authentication = "fab_notebookutils"
```

The `notebookutils` module ships with the Fabric runtime and is not installed by `dlt`, so it is imported only when this method is used. Using it outside the Fabric runtime raises a configuration error rather than falling back silently. Staging through OneLake or Azure Blob Storage picks the same identity up automatically — see [OneLake staging from a Fabric notebook](#onelake-staging-from-a-fabric-notebook).

### Create a pipeline

**1. Initialize a project with a pipeline that loads to Fabric by running:**
```sh
dlt init chess fabric
```

**2. Install the necessary dependencies for Fabric by running:**
```sh
pip install -r requirements.txt
```
or run:
```sh
pip install "dlt[fabric]"
```

**3. Enter your credentials into `.dlt/secrets.toml`.**

Service Principal (default):

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

azure-identity, e.g. `DefaultAzureCredential` after `az login`, which needs no secret:

```toml
[destination.fabric.credentials]
host = "<your-warehouse-guid>.datawarehouse.fabric.microsoft.com"
database = "mydb"
authentication = "default"
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

#### `.dlt/secrets.toml` when using OneLake:

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

#### OneLake staging from a Fabric notebook

Inside a Fabric notebook you can drop the Service Principal from the staging credentials entirely.
Leave out `azure_storage_account_key`, `azure_storage_sas_token` and the principal fields, and `dlt`
authenticates to blob storage with the notebook identity through NotebookUtils, the same way the
warehouse connection does:

```toml
[destination.fabric.credentials]
host = "<your-warehouse-guid>.datawarehouse.fabric.microsoft.com"
database = "mydb"
authentication = "fab_notebookutils"

[destination.filesystem]
bucket_url = "abfss://<your-workspace-guid>@onelake.dfs.fabric.microsoft.com/<your-lakehouse-guid>/Files"

[destination.filesystem.credentials]
azure_storage_account_name = "onelake"
azure_account_host = "onelake.blob.fabric.microsoft.com"
```

Outside the Fabric runtime the same configuration keeps using `DefaultAzureCredential`, so a static
secret or an explicitly passed credential always takes precedence over NotebookUtils.

#### `.dlt/secrets.toml` when using Azure Blob / Data Lake Storage:

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
Fabric Warehouse (like SQL Server) uses **case-insensitive identifiers** but preserves the casing of identifiers stored in the INFORMATION SCHEMA. You can use [case-sensitive naming conventions](../../general-usage/naming-convention.md#case-sensitive-and-insensitive-destinations) to keep the identifier casing. Note that you risk generating identifier collisions, which are detected by `dlt` and will fail the load process.

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

Fabric Warehouse supports UTF-8 collations. Long/max types (e.g. `varchar(max)`) are handled natively by the mssql-python driver, so no extra configuration is needed for UTF-8 collations to work properly.

**Default collation**: `Latin1_General_100_BIN2_UTF8` (case-sensitive, UTF-8)

You can specify a different collation:
```toml
[destination.fabric]
collation = "Latin1_General_100_CI_AS_KS_WS_SC_UTF8"  # case-insensitive
```

Or in code:
```py
pipeline = dlt.pipeline(
    destination=fabric(
        credentials=my_credentials,
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

The `driver` credential option is deprecated and ignored: mssql-python bundles its own driver, so
no ODBC driver name needs to be configured.

## Differences from MSSQL Destination

While Fabric Warehouse is based on SQL Server, there are key differences:

1. **Authentication**: Fabric uses Entra ID; in addition to Service Principal, `dlt` supports several azure-identity methods (see [Authentication](#authentication))
2. **Type System**: Uses `varchar` and `datetime2` instead of `nvarchar` and `datetimeoffset`
3. **Collation**: Optimized for UTF-8 collations, with long/max types handled natively by the driver
4. **SQL Dialect**: Uses `fabric` SQLglot dialect for proper SQL generation

### dbt support
Integration with [dbt](../transformations/dbt/dbt.md) is supported via [dbt-fabric](https://github.com/Microsoft/dbt-fabric). Both Service Principal and default Azure credentials are supported and shared with dbt runners.

## Troubleshooting

### Authentication Failures

Ensure your Service Principal has:
- Proper permissions on the Fabric workspace
- Access to the target database/warehouse  
- Correct tenant ID (your Entra ID tenant, not the workspace/capacity ID)

### UTF-8 Character Issues

If you experience character encoding issues:
1. Verify your warehouse uses a UTF-8 collation
2. Consider using the case-insensitive UTF-8 collation if needed

## Additional Resources

- [Microsoft Fabric Documentation](https://learn.microsoft.com/en-us/fabric/)
- [Fabric Warehouse Documentation](https://learn.microsoft.com/en-us/fabric/data-warehouse/)
- [Service Principal Setup Guide](https://learn.microsoft.com/en-us/fabric/security/service-principals)

<!--@@@DLT_TUBA fabric-->
