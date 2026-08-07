---
title: MS SQL
description: Microsoft SQL Server `dlt` destination
keywords: [mssql, sqlserver, destination, data warehouse]
---

# Microsoft SQL Server

## Install dlt with MS SQL
**To install the dlt library with MS SQL dependencies, use:**
```sh
pip install "dlt[mssql]"
```

<!--@@@DLT_DESTINATION_CAPABILITIES mssql-->

## Setup guide

### Prerequisites

This destination uses the [mssql-python](https://github.com/microsoft/mssql-python) driver, which is
installed automatically with `dlt[mssql]` together with its `mssql-python-odbc` dependency, providing
the SQL Server client libraries. No separate ODBC driver installation is required.

:::warning
**`dlt[mssql]`, `dlt[synapse]` and `dlt[fabric]` install `mssql-python` instead of `pyodbc`.** Existing
credentials keep working: a `driver` option, whether set directly or as a `?driver=` connection string
parameter, is accepted and ignored with a deprecation warning, since mssql-python installs and manages
its own driver dependency. Two things do change for code that reaches past the destination: `to_odbc_dsn()`
no longer emits a `DRIVER=` key, and `MsSqlCredentials.SUPPORTED_DRIVERS` is gone, along with the error
it raised for unrecognized driver names.

If you install `mssql-python` with `pip install --no-deps`, or from a private/mirrored index, make
sure `mssql-python-odbc` is installed or mirrored alongside it — the driver binaries live in that
companion package, not in `mssql-python` itself, and a plain `pip install mssql-python` only pulls it
in when normal dependency resolution runs.
:::

:::note Connection pooling and token expiry
mssql-python pools connections by default. Since v1.13 the pool key is identity-aware for the
`Authentication=` methods where the driver acquires the token itself (`ActiveDirectoryMsi`,
`ActiveDirectoryDeviceCode`, and off Windows `ActiveDirectoryInteractive`), and those pooled
connections are refreshed by the driver when their token nears expiry. `ActiveDirectoryDefault`
and the raw tokens passed through `access_token`/`azure_credential` are pooled by token hash
instead — distinct tokens never share a connection — but are **not** refreshed on expiry: if a
long-running pipeline's token expires while its connection sits pooled, the next use fails and
dlt does not renew it.
:::

### Create a pipeline

**1. Initialize a project with a pipeline that loads to MS SQL by running:**
```sh
dlt init chess mssql
```

**2. Install the necessary dependencies for MS SQL by running:**
```sh
pip install -r requirements.txt
```
or run:
```sh
pip install "dlt[mssql]"
```
This will install `dlt` with the `mssql` extra, which contains all the dependencies required by the SQL server client.

**3. Enter your credentials into `.dlt/secrets.toml`.**

For example, replace with your database connection info:
```toml
[destination.mssql.credentials]
database = "dlt_data"
username = "loader"
password = "<password>"
host = "loader.database.windows.net"
port = 1433
connect_timeout = 15
[destination.mssql.credentials.query]
# trust self-signed SSL certificates
TrustServerCertificate="yes"
# require SSL connection
Encrypt="yes"
```

You can also pass a SQLAlchemy-like database connection:
```toml
# Keep it at the top of your TOML file, before any section starts
destination.mssql.credentials="mssql://loader:<password>@loader.database.windows.net/dlt_data?TrustServerCertificate=yes&Encrypt=yes"
```

You can place any ODBC-specific settings into the query string or **destination.mssql.credentials.query** TOML table as in the example above.

**To connect to an `mssql` server using Windows authentication**, include `trusted_connection=yes` in the connection string.

```toml
destination.mssql.credentials="mssql://loader.database.windows.net/dlt_data?trusted_connection=yes"
```

:::note
If you encounter missing credentials errors when using Windows authentication, set the 'username' and 'password' as empty strings in the TOML file.
:::

**To connect to a local SQL server instance running without SSL**, pass the `encrypt=no` parameter:
```toml
destination.mssql.credentials="mssql://loader:loader@localhost/dlt_data?encrypt=no"
```

**To allow a self-signed SSL certificate** when you are getting `certificate verify failed: unable to get local issuer certificate`:
```toml
destination.mssql.credentials="mssql://loader:loader@localhost/dlt_data?TrustServerCertificate=yes"
```

Long strings (>8k) are handled automatically by the driver, no extra configuration is needed.

### Microsoft Entra ID authentication

For Azure-hosted SQL Server (Azure SQL Database, Managed Instance) you can authenticate with
Entra ID instead of a SQL login. Set the `authentication` credential option; `dlt` writes it to the
connection string as `Authentication=` and the
[mssql-python](https://github.com/microsoft/mssql-python) driver performs the sign-in, so no
separate `azure-identity` install is needed.

Leaving `authentication` empty keeps the plain SQL login with `username` and `password`. `ActiveDirectoryServicePrincipal` needs `azure_tenant_id`, `azure_client_id` and `azure_client_secret`; `ActiveDirectoryPassword` needs `username` and `password`. `ActiveDirectoryIntegrated`, `ActiveDirectoryInteractive`, `ActiveDirectoryMsi`, `ActiveDirectoryDefault` (alias `default`, which covers managed identity, environment and Azure CLI) and `ActiveDirectoryDeviceCode` need no further fields.

Passwordless example (e.g. after `az login`):
```toml
[destination.mssql.credentials]
database = "dlt_data"
host = "loader.database.windows.net"
authentication = "default"
```

Service Principal example:
```toml
[destination.mssql.credentials]
database = "dlt_data"
host = "loader.database.windows.net"
authentication = "ActiveDirectoryServicePrincipal"
azure_tenant_id = "your-tenant-id"
azure_client_id = "your-client-id"
azure_client_secret = "your-client-secret"
```

**To pass credentials directly**, use the [explicit instance of the destination](../../general-usage/destination.md#pass-explicit-credentials)
```py
pipeline = dlt.pipeline(
  pipeline_name='chess',
  destination=dlt.destinations.mssql("mssql://loader:<password>@loader.database.windows.net/dlt_data?connect_timeout=15"),
  dataset_name='chess_data')
```

## Write disposition
All write dispositions are supported.

If you set the [`replace` strategy](../../general-usage/full-loading.md) to `staging-optimized`, the destination tables will be dropped and
recreated with an `ALTER SCHEMA ... TRANSFER`. The operation is atomic: MSSQL supports DDL transactions.

## Data loading

:::tip
We recommend using ADBC + parquet to load data. We observed 10x - 100x increase in loading speed compared to the INSERT method. **parquet** file format
will activate automatically if the right driver is present in the system. 
:::

### Fast loading with parquet

[parquet](../file-formats.md#parquet) file format is supported via [ADBC driver](https://arrow.apache.org/adbc/). **mssql** driver is provided by
[Columnar](https://columnar.tech/). To install it you'll need `dbc` which is a tool to manage ADBC drivers:
```sh
pip install adbc-driver-manager dbc
dbc install mssql
```

with `uv` you can run `dbc` directly:
```sh
uv tool run dbc search
```
`dlt` will make **parquet** the preferred file format once driver is detected at runtime. This method is 10x-70x faster than INSERT and
we make it a default for all input data types.

Not all arrow data types are supported by the driver, see driver docs for more details:
* fixed length binary
* time with precision different than microseconds

We copy parquet files with batches of size of 1 row group. All groups are copied in a single transaction.

:::caution
It looks like ADBC driver is based on [go-mssqldb](https://github.com/denisenkom/go-mssqldb?tab=readme-ov-file)

DSN format is different. We translate a few overlapping keys. `pyodbc` and `adbc` ignore unknown keys so you can specify keys for both in the same string.
:::

You can go back to `insert_values` by passing `loader_file_format` to a resource or pipeline
```py
# revert to INSERT statements
pipeline.run(data_iter, dataset_name="speed_test_2", write_disposition="replace", table_name="unsw_flow", loader_file_format="insert_values")
```

### Loading with INSERT statements

Data is loaded via INSERT statements by default. MSSQL has a limit of 1000 rows per INSERT, and this is what we use. We send multiple
sql statements in a single batch. In case you observe odbc driver locking (i.e. when connection with open transaction leaks into the pool) you can:

1. disable `pyodbc` connection pool.
```py
import pyodbc
pyodbc.pooling = False
```

2. disable batching of multiple statements in `dlt`
```py
dlt.destinations.mssql("mssql://loader:<password>@loader.database.windows.net/dlt_data?connect_timeout=15", supports_multiple_statements=False)
```


## Supported file formats
* [insert-values](../file-formats.md#sql-insert) is used by default
* [parquet](../file-formats.md#parquet) is used if mssql ADBC driver is installed

## Supported column hints
**mssql** will create unique indexes for all columns with `unique` hints. This behavior **is disabled by default**.

### Table and column identifiers
SQL Server **with the default collation** uses case-insensitive identifiers but will preserve the casing of identifiers that are stored in the INFORMATION SCHEMA. You can use [case-sensitive naming conventions](../../general-usage/naming-convention.md#case-sensitive-and-insensitive-destinations) to keep the identifier casing. Note that you risk generating identifier collisions, which are detected by `dlt` and will fail the load process.

If you change the SQL Server server/database collation to case-sensitive, this will also affect the identifiers. Configure your destination as below in order to use case-sensitive naming conventions without collisions:
```toml
[destination.mssql]
has_case_sensitive_identifiers=true
```

## Syncing of `dlt` state
This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

## Data types
MS SQL does not support JSON columns, so JSON objects are stored as strings in `nvarchar` columns.

## Additional destination options
The **mssql** destination **does not** create UNIQUE indexes by default on columns with the `unique` hint (i.e., `_dlt_id`). To enable this behavior:
```toml
[destination.mssql]
create_indexes=true
```

### dbt support
This destination [integrates with dbt](../transformations/dbt/dbt.md) via [dbt-sqlserver](https://github.com/dbt-msft/dbt-sqlserver).

<!--@@@DLT_TUBA mssql-->
