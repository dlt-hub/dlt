---
title: Microsoft SQL Server
description: Microsoft SQL Server `dlt` destination
keywords: [mssql, sqlserver, destination, data warehouse]
---

# Microsoft SQL Server

## Install dlt with MS SQL
**To install the dlt library with MS SQL dependencies, use:**
```sh
pip install "dlt[mssql]"
```

## Setup guide

### Prerequisites

The _Microsoft ODBC Driver for SQL Server_ must be installed to use this destination.
This cannot be included with `dlt`'s Python dependencies, so you must install it separately on your system. You can find the official installation instructions [here](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16).

Supported driver versions:
* `ODBC Driver 18 for SQL Server`
* `ODBC Driver 17 for SQL Server`

You can also [configure the driver name](#additional-destination-options) explicitly.

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
# send large string as VARCHAR, not legacy TEXT
LongAsMax="yes"
```

You can also pass a SQLAlchemy-like database connection:
```toml
# Keep it at the top of your TOML file, before any section starts
destination.mssql.credentials="mssql://loader:<password>@loader.database.windows.net/dlt_data?TrustServerCertificate=yes&Encrypt=yes&LongAsMax=yes"
```

You can place any ODBC-specific settings into the query string or **destination.mssql.credentials.query** TOML table as in the example above.

**To connect to an `mssql` server using Windows authentication**, include `trusted_connection=yes` in the connection string.

```toml
destination.mssql.credentials="mssql://loader.database.windows.net/dlt_data?trusted_connection=yes"
```

**To connect to a local SQL server instance running without SSL**, pass the `encrypt=no` parameter:
```toml
destination.mssql.credentials="mssql://loader:loader@localhost/dlt_data?encrypt=no"
```

**To allow a self-signed SSL certificate** when you are getting `certificate verify failed: unable to get local issuer certificate`:
```toml
destination.mssql.credentials="mssql://loader:loader@localhost/dlt_data?TrustServerCertificate=yes"
```

**To use long strings (>8k) and avoid collation errors**:
```toml
destination.mssql.credentials="mssql://loader:loader@localhost/dlt_data?LongAsMax=yes"
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
Data is loaded via INSERT statements by default. MSSQL has a limit of 1000 rows per INSERT, and this is what we use.

## Supported file formats
* [insert-values](../file-formats/insert-format.md) is used by default

## Supported column hints
**mssql** will create unique indexes for all columns with `unique` hints. This behavior **may be disabled**.

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

You can explicitly set the ODBC driver name:
```toml
[destination.mssql.credentials]
driver="ODBC Driver 18 for SQL Server"
```

When using a SQLAlchemy connection string, replace spaces with `+`:

```toml
# Keep it at the top of your TOML file, before any section starts
destination.mssql.credentials="mssql://loader:<password>@loader.database.windows.net/dlt_data?driver=ODBC+Driver+18+for+SQL+Server"
```

### dbt support
This destination [integrates with dbt](../transformations/dbt/dbt.md) via [dbt-snowflake](https://github.com/dbt-msft/dbt-sqlserver).

<!--@@@DLT_TUBA mssql-->

