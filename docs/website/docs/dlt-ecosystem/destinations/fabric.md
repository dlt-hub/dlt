---
title: Microsoft Fabric Warehouse
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

The _Microsoft ODBC Driver for SQL Server_ must be installed to use this destination.
This cannot be included with `dlt`'s Python dependencies, so you must install it separately on your system. You can find the official installation instructions [here](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16).

Supported driver versions:
* `ODBC Driver 18 for SQL Server` (recommended)
* `ODBC Driver 17 for SQL Server`

You can also [configure the driver name](#additional-destination-options) explicitly.

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

For example, replace with your Fabric Warehouse connection info:
```toml
[destination.fabric.credentials]
host = "abc12345-6789-def0-1234-56789abcdef0.datawarehouse.fabric.microsoft.com"
database = "mydb"
tenant_id = "12345678-1234-1234-1234-123456789012"
client_id = "87654321-4321-4321-4321-210987654321"
client_secret = "your-client-secret-here"
port = 1433
connect_timeout = 30
```

**To pass credentials directly**, use the [explicit instance of the destination](../../general-usage/destination.md#pass-explicit-credentials)
```py
import dlt
from dlt.destinations import fabric

pipeline = dlt.pipeline(
    pipeline_name='chess',
    destination=fabric(
        credentials={
            "host": "abc12345-6789-def0-1234-56789abcdef0.datawarehouse.fabric.microsoft.com",
            "database": "mydb",
            "tenant_id": "your-tenant-id",
            "client_id": "your-client-id",
            "client_secret": "your-client-secret",
        }
    ),
    dataset_name='chess_data'
)
```

## Write disposition
All write dispositions are supported.

If you set the [`replace` strategy](../../general-usage/full-loading.md) to `staging-optimized`, the destination tables will be dropped and recreated with an `ALTER SCHEMA ... TRANSFER`. The operation is atomic: Fabric supports DDL transactions.

## Staging support (OneLake)

Fabric Warehouse supports staging data via **OneLake Lakehouse** using the `COPY INTO` command for efficient bulk loading. This is the recommended approach for large datasets.

### OneLake Configuration

**IMPORTANT**: OneLake bucket URLs **must use GUIDs** for both the workspace and lakehouse, not their display names.

**Format**: `abfss://<workspace_guid>@onelake.dfs.fabric.microsoft.com/<lakehouse_guid>/Files`

**Finding your GUIDs**:
1. Navigate to your Fabric workspace in the browser
2. The workspace GUID is in the URL: `https://fabric.microsoft.com/groups/<workspace_guid>/...`
3. Open your Lakehouse
4. The lakehouse GUID is in the URL: `https://fabric.microsoft.com/.../lakehouses/<lakehouse_guid>`

### Example with OneLake staging

```py
import dlt
from dlt.destinations import fabric, filesystem

pipeline = dlt.pipeline(
    destination=fabric(
        credentials={
            "host": "abc12345-6789-def0-1234-56789abcdef0.datawarehouse.fabric.microsoft.com",
            "database": "mydb",
            "tenant_id": "your-tenant-id",
            "client_id": "your-client-id",
            "client_secret": "your-client-secret",
        },
        staging_config=filesystem(
            # Use workspace and lakehouse GUIDs (not names!)
            bucket_url="abfss://12345678-1234-1234-1234-123456789012@onelake.dfs.fabric.microsoft.com/87654321-4321-4321-4321-210987654321/Files",
            credentials={
                "azure_storage_account_name": "onelake",
                "azure_account_host": "onelake.blob.fabric.microsoft.com",
                # Must specify the same Service Principal credentials as the warehouse
                "azure_tenant_id": "your-tenant-id",
                "azure_client_id": "your-client-id",
                "azure_client_secret": "your-client-secret",
            },
        ),
    ),
    dataset_name='my_dataset'
)
```

Or using `.dlt/secrets.toml`:

```toml
[destination.fabric.credentials]
host = "abc12345-6789-def0-1234-56789abcdef0.datawarehouse.fabric.microsoft.com"
database = "mydb"
tenant_id = "your-tenant-id"
client_id = "your-client-id"
client_secret = "your-client-secret"

[destination.fabric.staging_config]
# Replace with your actual workspace and lakehouse GUIDs
bucket_url = "abfss://12345678-1234-1234-1234-123456789012@onelake.dfs.fabric.microsoft.com/87654321-4321-4321-4321-210987654321/Files"

[destination.fabric.staging_config.credentials]
azure_storage_account_name = "onelake"
azure_account_host = "onelake.blob.fabric.microsoft.com"
# Must specify the same Service Principal credentials
azure_tenant_id = "your-tenant-id"
azure_client_id = "your-client-id"
azure_client_secret = "your-client-secret"
```

**Note**: When using environment variables or the simplified `destination='fabric'` with `staging='filesystem'`, configure the staging credentials separately:

```bash
# Fabric warehouse credentials
export DESTINATION__FABRIC__CREDENTIALS__HOST="..."
export DESTINATION__FABRIC__CREDENTIALS__DATABASE="..."
export DESTINATION__FABRIC__CREDENTIALS__AZURE_TENANT_ID="..."
export DESTINATION__FABRIC__CREDENTIALS__AZURE_CLIENT_ID="..."
export DESTINATION__FABRIC__CREDENTIALS__AZURE_CLIENT_SECRET="..."

# Filesystem staging credentials (must specify explicitly)
export DESTINATION__FILESYSTEM__BUCKET_URL="abfss://..."
export DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME="onelake"
export DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_ACCOUNT_HOST="onelake.blob.fabric.microsoft.com"
export DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_TENANT_ID="..."
export DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_CLIENT_ID="..."
export DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_CLIENT_SECRET="..."
```

## Data loading
Data is loaded via INSERT statements by default. Fabric Warehouse has a limit of 1000 rows per INSERT, and this is what we use.

## Supported file formats
* [insert-values](../file-formats/insert-format.md) is the default and currently only supported format

## Supported column hints
**fabric** will create unique indexes for all columns with `unique` hints. This behavior **is disabled by default**.

### Table and column identifiers
Fabric Warehouse (like SQL Server) uses **case-insensitive identifiers** but preserves the casing of identifiers stored in the INFORMATION SCHEMA. You can use [case-sensitive naming conventions](../../general-usage/naming-convention.md#case-sensitive-and-insensitive-destinations) to keep the identifier casing. Note that you risk generating identifier collisions, which are detected by `dlt` and will fail the load process.

## Syncing of `dlt` state
This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

## Data types

Fabric Warehouse differs from standard SQL Server in several important ways:

### VARCHAR vs NVARCHAR
Fabric Warehouse uses `varchar` for text columns instead of `nvarchar`. This destination automatically maps:
- `text` → `varchar(max)`
- `text` (with unique hint) → `varchar(900)` (limited for index support)

### DATETIME2 vs DATETIMEOFFSET
Fabric uses `datetime2` for timestamps instead of `datetimeoffset`:
- `timestamp` → `datetime2(6)` (precision limited to 0-6, not 0-7)
- `time` → `time(6)` (explicit precision required)

### JSON Storage
Fabric does not support native JSON columns. JSON objects are stored as `varchar(max)` columns.

## Collation Support

Fabric Warehouse supports UTF-8 collations. The destination automatically configures `LongAsMax=yes` which is required for UTF-8 collations to work properly.

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
        credentials={...},
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

You can explicitly set the ODBC driver name:
```toml
[destination.fabric.credentials]
driver="ODBC Driver 18 for SQL Server"
```

## Differences from MSSQL Destination

While Fabric Warehouse is based on SQL Server, there are key differences:

1. **Authentication**: Fabric requires Service Principal; username/password auth is not supported
2. **Type System**: Uses `varchar` and `datetime2` instead of `nvarchar` and `datetimeoffset`
3. **Collation**: Optimized for UTF-8 collations with automatic `LongAsMax` configuration
4. **SQL Dialect**: Uses `fabric` SQLglot dialect for proper SQL generation

## Troubleshooting

### ODBC Driver Not Found

If you see "No supported ODBC driver found", install the Microsoft ODBC Driver 18 for SQL Server:

```bash
# Ubuntu/Debian
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

### Authentication Failures

Ensure your Service Principal has:
- Proper permissions on the Fabric workspace
- Access to the target database/warehouse  
- Correct tenant ID (your Azure AD tenant, not the workspace/capacity ID)

### UTF-8 Character Issues

If you experience character encoding issues:
1. Verify your warehouse uses a UTF-8 collation
2. Check that `LongAsMax=yes` is in the connection (automatically added by this destination)
3. Consider using the case-insensitive UTF-8 collation if needed

## Additional Resources

- [Microsoft Fabric Documentation](https://learn.microsoft.com/en-us/fabric/)
- [Fabric Warehouse Documentation](https://learn.microsoft.com/en-us/fabric/data-warehouse/)
- [Service Principal Setup Guide](https://learn.microsoft.com/en-us/fabric/security/service-principals)

<!--@@@DLT_TUBA fabric-->
