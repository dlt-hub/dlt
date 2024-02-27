---
title: Microsoft SQL Server
description: Microsoft SQL Server `dlt` destination
keywords: [mssql, sqlserver, destination, data warehouse]
---

# Microsoft SQL Server

## Install dlt with MS SQL
**To install the DLT library with MS SQL dependencies:**
```
pip install dlt[mssql]
```

## Setup guide

### Prerequisites

_Microsoft ODBC Driver for SQL Server_ must be installed to use this destination.
This can't be included with `dlt`'s python dependencies, so you must install it separately on your system. You can find the official installation instructions [here](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16).

Supported driver versions:
* `ODBC Driver 18 for SQL Server`
* `ODBC Driver 17 for SQL Server`

You can [configure driver name](#additional-destination-options) explicitly as well.

### Create a pipeline

**1. Initalize a project with a pipeline that loads to MS SQL by running**
```
dlt init chess mssql
```

**2. Install the necessary dependencies for MS SQL by running**
```
pip install -r requirements.txt
```
or run:
```
pip install dlt[mssql]
```
This will install dlt with **mssql** extra which contains all the dependencies required by the SQL server client.

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

You can also pass a SQLAlchemy-like database connection:
```toml
# keep it at the top of your toml file! before any section starts
destination.mssql.credentials="mssql://loader:<password>@loader.database.windows.net/dlt_data?connect_timeout=15"
```

To pass credentials directly you can use `credentials` argument passed to `dlt.pipeline` or `pipeline.run` methods.
```python
pipeline = dlt.pipeline(pipeline_name='chess', destination='postgres', dataset_name='chess_data', credentials="mssql://loader:<password>@loader.database.windows.net/dlt_data?connect_timeout=15")
```

## Write disposition
All write dispositions are supported

If you set the [`replace` strategy](../../general-usage/full-loading.md) to `staging-optimized` the destination tables will be dropped and
recreated with an `ALTER SCHEMA ... TRANSFER`. The operation is atomic: mssql supports DDL transactions.

## Data loading
Data is loaded via INSERT statements by default. MSSQL has a limit of 1000 rows per INSERT and this is what we use.

## Supported file formats
* [insert-values](../file-formats/insert-format.md) is used by default

## Supported column hints
**mssql** will create unique indexes for all columns with `unique` hints. This behavior **may be disabled**

## Syncing of `dlt` state
This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination)

## Data types
MS SQL does not support JSON columns, so JSON objects are stored as strings in `nvarchar` column.

## Additional destination options
**mssql** destination **does not** creates UNIQUE indexes by default on columns with `unique` hint (ie. `_dlt_id`). To enable this behavior
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
No dbt support yet

<!--@@@DLT_SNIPPET_START tuba::mssql-->
## Additional Setup guides

- [Load data from Stripe to Microsoft SQL Server in python with dlt](https://dlthub.com/docs/pipelines/stripe_analytics/load-data-with-python-from-stripe_analytics-to-mssql)
- [Load data from Google Analytics to Microsoft SQL Server in python with dlt](https://dlthub.com/docs/pipelines/google_analytics/load-data-with-python-from-google_analytics-to-mssql)
- [Load data from Google Sheets to Microsoft SQL Server in python with dlt](https://dlthub.com/docs/pipelines/google_sheets/load-data-with-python-from-google_sheets-to-mssql)
- [Load data from Chess.com to Microsoft SQL Server in python with dlt](https://dlthub.com/docs/pipelines/chess/load-data-with-python-from-chess-to-mssql)
- [Load data from GitHub to Microsoft SQL Server in python with dlt](https://dlthub.com/docs/pipelines/github/load-data-with-python-from-github-to-mssql)
- [Load data from Notion to Microsoft SQL Server in python with dlt](https://dlthub.com/docs/pipelines/notion/load-data-with-python-from-notion-to-mssql)
- [Load data from HubSpot to Microsoft SQL Server in python with dlt](https://dlthub.com/docs/pipelines/hubspot/load-data-with-python-from-hubspot-to-mssql)
<!--@@@DLT_SNIPPET_END tuba::mssql-->