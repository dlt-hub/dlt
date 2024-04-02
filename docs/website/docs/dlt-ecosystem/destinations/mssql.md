---
title: Microsoft SQL Server
description: Microsoft SQL Server `dlt` destination
keywords: [mssql, sqlserver, destination, data warehouse]
---

# Microsoft SQL Server

## Install dlt with MS SQL
**To install the DLT library with MS SQL dependencies, use:**
```sh
pip install dlt[mssql]
```

## Setup guide

### Prerequisites

The _Microsoft ODBC Driver for SQL Server_ must be installed to use this destination.
This cannot be included with `dlt`'s python dependencies, so you must install it separately on your system. You can find the official installation instructions [here](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16).

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
pip install dlt[mssql]
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
```

You can also pass a SQLAlchemy-like database connection:
```toml
# keep it at the top of your toml file! before any section starts
destination.mssql.credentials="mssql://loader:<password>@loader.database.windows.net/dlt_data?connect_timeout=15"
```

To connect to an `mssql` server using Windows authentication, include `trusted_connection=yes` in the connection string. This method is useful when SQL logins aren't available, and you use Windows credentials.

```toml
destination.mssql.credentials="mssql://username:password@loader.database.windows.net/dlt_data?trusted_connection=yes"
```
> The username and password must be filled out with the appropriate login credentials or left untouched. Leaving these empty is not recommended.

To pass credentials directly, you can use the `credentials` argument passed to `dlt.pipeline` or `pipeline.run` methods.
```py
pipeline = dlt.pipeline(pipeline_name='chess', destination='postgres', dataset_name='chess_data', credentials="mssql://loader:<password>@loader.database.windows.net/dlt_data?connect_timeout=15")
```

## Write disposition
All write dispositions are supported.

If you set the [`replace` strategy](../../general-usage/full-loading.md) to `staging-optimized`, the destination tables will be dropped and
recreated with an `ALTER SCHEMA ... TRANSFER`. The operation is atomic: mssql supports DDL transactions.

## Data loading
Data is loaded via INSERT statements by default. MSSQL has a limit of 1000 rows per INSERT, and this is what we use.

## Supported file formats
* [insert-values](../file-formats/insert-format.md) is used by default

## Supported column hints
**mssql** will create unique indexes for all columns with `unique` hints. This behavior **may be disabled**.

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
# keep it at the top of your toml file! before any section starts
destination.mssql.credentials="mssql://loader:<password>@loader.database.windows.net/dlt_data?driver=ODBC+Driver+18+for+SQL+Server"
```

### dbt support
No dbt support yet.

<!--@@@DLT_TUBA mssql-->

