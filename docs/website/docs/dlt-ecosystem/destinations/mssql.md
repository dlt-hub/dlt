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
connections are refreshed by the driver when their token nears expiry. `ActiveDirectoryDefault`,
`access_token` and `azure_credential` are pooled by token hash instead — distinct tokens never
share a connection — but are **not** refreshed on expiry: if a long-running pipeline's token
expires while its connection sits pooled, the next use fails and dlt does not renew it. Handing
`azure_credential` to the driver does not change this; a custom provider is keyed on the token it
mints, not on the provider object.
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

:::note
`authentication`, `uid`, `pwd` and `trusted_connection` query keys are dropped from the connection
string when `access_token` or `azure_credential` is set — otherwise the driver would sign in as that
identity and ignore the token you configured. Pick one or the other. Every other query key is passed
through untouched.
:::

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

#### Passing a credential object or a token yourself

Instead of naming a method, you can hand `dlt` a credential object through `azure_credential`. It is
passed straight to the driver, which acquires the token, so any object with a `get_token(scope)`
method works — every `azure-identity` credential, or your own wrapper:

```py
from azure.identity import DefaultAzureCredential

pipeline = dlt.pipeline(
  pipeline_name='chess',
  destination=dlt.destinations.mssql(credentials={
    "host": "loader.database.windows.net",
    "database": "dlt_data",
    "azure_credential": DefaultAzureCredential(),
  }),
  dataset_name='chess_data')
```

A pre-acquired token goes in `access_token` instead, which wins over both `azure_credential` and
`authentication`.

:::note Sovereign clouds
The driver requests the token for the Azure **commercial** SQL scope
(`https://database.windows.net/.default`). Azure US Government, Azure China and Azure Germany need a
different audience, and a token minted for the wrong one is rejected at login. For those, acquire
the token yourself for the right scope and pass it as `access_token`.
:::

:::warning Not every credential works with parquet
[Fast loading with parquet](#fast-loading-with-parquet) opens its **own** connection and signs in
again, and that connection supports fewer methods than the ODBC one. It cannot use:

* `access_token` — a pre-acquired token is never handed to it,
* `authentication = "ActiveDirectoryPassword"` and `"ActiveDirectoryIntegrated"` — it does not
  implement them.

A parquet load job configured with any of these fails immediately with a terminal error, before any
row is sent, rather than signing in as the wrong identity. Use `azure_credential`, another
`ActiveDirectory*` method, or keep that pipeline on `insert_values`.

`authentication = "ActiveDirectoryInteractive"` does work, but the bulk copy connection acquires its
own token — off Windows that can open a browser prompt in the middle of a load.
:::

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

Data is loaded with INSERT statements by default. The [parquet](../file-formats.md#parquet) file
format is much faster and is available whenever `pyarrow` is installed, but you have to ask for it —
see [fast loading with parquet](#fast-loading-with-parquet) for the trade-off.

### Fast loading with parquet

Parquet load files are streamed straight into SQL Server with mssql-python's native Arrow bulk copy,
which needs no additional driver — just `pyarrow`:

```sh
pip install "dlt[mssql,parquet]"
```

Select it per pipeline or per resource:

```py
pipeline.run(data_iter, table_name="unsw_flow", loader_file_format="parquet")
```

`dlt` reads the load file one row group at a time and hands the driver a `pyarrow.RecordBatchReader`,
so peak memory does not grow with the file size. Source columns are mapped to destination columns by
name rather than by ordinal position, which keeps loads correct after a schema evolution appends a
column to the table.

Each load file is sent as a **single transactional batch**, so a job that fails part-way commits
nothing and is retried like any other load job. The flip side is that one file is one transaction:
a very large load file means a long-lived transaction and a correspondingly large log. Control it
with the [parquet writer's](../file-formats.md#parquet) `file_max_items` / `file_max_bytes` rather
than by splitting the batch.

A bulk copy is given one hour to complete. Change it with `bulk_copy_timeout` (seconds):

```toml
[destination.mssql]
bulk_copy_timeout = 7200
```

:::caution Parquet does not fire triggers or check constraints
Bulk copy uses SQL Server's bulk-load path, which by default skips INSERT triggers and CHECK/FOREIGN
KEY constraints. `insert_values` fires and checks both. Switching a table to parquet therefore
changes its semantics if you rely on either. UNIQUE indexes are always enforced.

This — together with the credential limits above and the fact that end-to-end validation of the
native path is still young — is why `insert_values` stays the preferred format and parquet is opt-in.
:::

Arrow dictionary-encoded arrays are not supported, so `dlt` writes plain columns for this
destination. Everything else in `dlt`'s type matrix is passed through natively.

### Loading with INSERT statements

Data is loaded via INSERT statements by default. MSSQL has a limit of 1000 rows per INSERT, and this is what we use. We send multiple
sql statements in a single batch. In case you observe driver locking (i.e. when connection with open transaction leaks into the pool) you can:

1. disable the connection pool that `mssql-python` enables by default.
```py
import mssql_python
mssql_python.pooling(enabled=False)
```

2. disable batching of multiple statements in `dlt`
```py
dlt.destinations.mssql("mssql://loader:<password>@loader.database.windows.net/dlt_data?connect_timeout=15", supports_multiple_statements=False)
```


## Supported file formats
* [insert-values](../file-formats.md#sql-insert) is used by default
* [parquet](../file-formats.md#parquet) is available when `pyarrow` is installed, and is opt-in

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
