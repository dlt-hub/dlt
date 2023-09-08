---
title: Microsoft SQL Server
description: Microsoft SQL Server `dlt` destination
keywords: [mssql, sqlserver, destination, data warehouse]
---

# Microsoft SQL Server

## Setup guide

### Prerequisites

Microsoft ODBC driver for SQL Server must be installed to use this destination.
This can't be included with `dlt`s python dependencies so you must installed it separately on your system.

See instructions here to [install Microsoft ODBC Driver 18 for SQL Server on Windows, Mac and Linux](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16)

### Create a pipeline

**1. Initalize a project with a pipeline that loads to MS SQL by running**
```
dlt init chess mssql
```

**2. Install the necessary dependencies for BigQuery by running**
```
pip install -r requirements.txt
```
or run:
```
pip install dlt[mssql]
```
This will install dlt with **mssql** extra which contains all the dependencies required by the sql server client.

**3. Enter your credentials into `.dlt/secrets.toml`.**

Example, replace with your database connection info:
```toml
[destination.mssql.credentials]
database = "dlt_data"
username = "loader"
password = "<password>"
host = "loader.database.windows.net"
port = 1433
connect_timeout = 15
```

## Supported file formats
* [insert-values](../file-formats/insert-format.md) is used by default

## Syncing of `dlt` state
This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination)

## Data types

MS SQL does not support JSON columns, so JSON objects are stored as strings in `nvarchar` column.
