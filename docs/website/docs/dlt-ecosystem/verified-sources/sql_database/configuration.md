---
title: Configuring the SQL Database source
description: configuring the pipeline script, connection, and backend settings in the sql_database source
keywords: [sql connector, sql database pipeline, sql database]
---

import Header from '../_source-info-header.md';

# Configuration

<Header/>

## Configuring the SQL database source

`dlt` sources are Python scripts made up of source and resource functions that can be easily customized. The SQL Database verified source has the following built-in source and resource:
1. `sql_database`: a `dlt` source that can be used to load multiple tables and views from a SQL database.
2. `sql_table`: a `dlt` resource that loads a single table from the SQL database.

Read more about sources and resources here: [General usage: source](../../../general-usage/source.md) and [General usage: resource](../../../general-usage/resource.md).

### Example usage:

1. **Load all the tables from a database**
    Calling `sql_database()` loads all tables from the database.

    ```py
    import dlt
    from dlt.sources.sql_database import sql_database

    def load_entire_database() -> None:
        # Define the pipeline
        pipeline = dlt.pipeline(
            pipeline_name="rfam",
            destination='synapse',
            dataset_name="rfam_data"
        )

        # Fetch all the tables from the database
        source = sql_database()

        # Run the pipeline
        info = pipeline.run(source, write_disposition="replace")

        # Print load info
        print(info)
    ```

2. **Load select tables from a database**
    Calling `sql_database().with_resources("family", "clan")` loads only the tables `"family"` and `"clan"` from the database.

    ```py
    import dlt
    from dlt.sources.sql_database import sql_database

    def load_select_tables_from_database() -> None:
        # Define the pipeline
        pipeline = dlt.pipeline(
            pipeline_name="rfam",
            destination="postgres",
            dataset_name="rfam_data"
        )

        # Fetch tables "family" and "clan"
        source = sql_database().with_resources("family", "clan")

        # Run the pipeline
        info = pipeline.run(source)

        # Print load info
        print(info)

    ```

3. **Load a standalone table**
    Calling `sql_table(table="family")` fetches only the table `"family"`

    ```py
    import dlt
    from dlt.sources.sql_database import sql_table

    def load_select_tables_from_database() -> None:
        # Define the pipeline
        pipeline = dlt.pipeline(
            pipeline_name="rfam",
            destination="duckdb",
            dataset_name="rfam_data"
        )

        # Fetch the table "family"
        table = sql_table(table="family")

        # Run the pipeline
        info = pipeline.run(table)

        # Print load info
        print(info)

    ```

:::tip
We intend our sources to be fully hackable. Feel free to change the source code of the sources and resources to customize it to your needs.
:::


## Configuring the connection

### Connection string format
`sql_database` uses SQLAlchemy to create database connections and reflect table schemas. You can pass credentials using
[database URLs](https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls), which have the general format:

```py
"dialect+database_type://username:password@server:port/database_name"
```

For example, to connect to a MySQL database using the `pymysql` dialect, you can use the following connection string:
```py
"mysql+pymysql://rfamro:PWD@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
```

Database-specific drivers can be passed into the connection string using query parameters. For example, to connect to Microsoft SQL Server using the ODBC Driver, you would need to pass the driver as a query parameter as follows:

```py
"mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server"
```

### Passing connection credentials to the `dlt` pipeline

There are several options for adding your connection credentials into your `dlt` pipeline:

#### 1. Setting them in `secrets.toml` or as environment variables (recommended)

You can set up credentials using [any method](../../../general-usage/credentials/setup#available-config-providers) supported by `dlt`. We recommend using `.dlt/secrets.toml` or the environment variables. See Step 2 of the [setup](./setup) for how to set credentials inside `secrets.toml`. For more information on passing credentials, read [here](../../../general-usage/credentials/setup).

#### 2. Passing them directly in the script

It is also possible to explicitly pass credentials inside the source. Example:

```py
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.sources.sql_database import sql_database

credentials = ConnectionStringCredentials(
    "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
)

source = sql_database(credentials).with_resource("family")
```

:::note
It is recommended to configure credentials in `.dlt/secrets.toml` and to not include any sensitive information in the pipeline code.
:::

### Other connection options

#### Using SqlAlchemy Engine as credentials

You are able to pass an instance of SqlAlchemy Engine instead of credentials:

```py
from dlt.sources.sql_database import sql_table
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam")
table = sql_table(engine, table="chat_message", schema="data")
```

This engine is used by `dlt` to open database connections and can work across multiple threads, so it is compatible with the `parallelize` setting of dlt sources and resources.

## Configuring the backend

Table backends convert streams of rows from database tables into batches in various formats. The default backend, `SQLAlchemy`, follows standard `dlt` behavior of extracting and normalizing Python dictionaries. We recommend this for smaller tables, initial development work, and when minimal dependencies or a pure Python environment is required. This backend is also the slowest. Other backends make use of the structured data format of the tables and provide significant improvement in speeds. For example, the `PyArrow` backend converts rows into `Arrow` tables, which results in good performance and preserves exact data types. We recommend using this backend for larger tables.

### SQLAlchemy

The `SQLAlchemy` backend (the default) yields table data as a list of Python dictionaries. This data goes through the regular extract and normalize steps and does not require additional dependencies to be installed. It is the most robust (works with any destination, correctly represents data types) but also the slowest. You can set `reflection_level="full_with precision"` to pass exact data types to the `dlt` schema.

### PyArrow

The `PyArrow` backend yields data as `Arrow` tables. It uses `SQLAlchemy` to read rows in batches but then immediately converts them into `ndarray`, transposes it, and sets it as columns in an `Arrow` table. This backend always fully reflects the database table and preserves original types (i.e., **decimal** / **numeric** data will be extracted without loss of precision). If the destination loads parquet files, this backend will skip the `dlt` normalizer, and you can gain two orders of magnitude (20x - 30x) speed increase.

Note that if `pandas` is installed, we'll use it to convert `SQLAlchemy` tuples into `ndarray` as it seems to be 20-30% faster than using `numpy` directly.

```py
import dlt
import sqlalchemy as sa
from dlt.sources.sql_database import sql_database

pipeline = dlt.pipeline(
    pipeline_name="rfam_cx", destination="postgres", dataset_name="rfam_data_arrow"
)

def _double_as_decimal_adapter(table: sa.Table) -> None:
    """Emits decimals instead of floats."""
    for column in table.columns.values():
        if isinstance(column.type, sa.Float):
            column.type.asdecimal = False

sql_alchemy_source = sql_database(
    "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam?&binary_prefix=true",
    backend="pyarrow",
    backend_kwargs={"tz": "UTC"},
    table_adapter_callback=_double_as_decimal_adapter
).with_resources("family", "genome")

info = pipeline.run(sql_alchemy_source)
print(info)
```
For more information on the `tz` parameter within `backend_kwargs` supported by PyArrow, please refer to the 
[official documentation.](https://arrow.apache.org/docs/python/generated/pyarrow.timestamp.html)

### Pandas

The `pandas` backend yields data as DataFrames using the `pandas.io.sql` module. `dlt` uses `PyArrow` dtypes by default as they generate more stable typing.

With the default settings, several data types will be coerced to dtypes in the yielded data frame:
* **decimal** is mapped to double, so it is possible to lose precision
* **date** and **time** are mapped to strings
* all types are nullable

:::note
`dlt` will still use the data types reflected from the source database when creating destination tables. How the type differences resulting from the `pandas` backend are reconciled/parsed is up to the destination. Most of the destinations will be able to parse date/time strings and convert doubles into decimals (Please note that you'll still lose precision on decimals with default settings.). **However, we strongly suggest not to use the** `pandas` **backend if your source tables contain date, time, or decimal columns.**
:::

Internally, `dlt` uses `pandas.io.sql._wrap_result` to generate `pandas` frames. To adjust [pandas-specific settings,](https://pandas.pydata.org/docs/reference/api/pandas.read_sql_table.html) pass it in the `backend_kwargs` parameter. For example, below we set `coerce_float` to `False`:

```py
import dlt
import sqlalchemy as sa
from dlt.sources.sql_database import sql_database

pipeline = dlt.pipeline(
    pipeline_name="rfam_cx", destination="postgres", dataset_name="rfam_data_pandas_2"
)

def _double_as_decimal_adapter(table: sa.Table) -> None:
    """Emits decimals instead of floats."""
    for column in table.columns.values():
        if isinstance(column.type, sa.Float):
            column.type.asdecimal = True

sql_alchemy_source = sql_database(
    "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam?&binary_prefix=true",
    backend="pandas",
    table_adapter_callback=_double_as_decimal_adapter,
    chunk_size=100000,
    # set coerce_float to False to represent them as string
    backend_kwargs={"coerce_float": False, "dtype_backend": "numpy_nullable"},
).with_resources("family", "genome")

info = pipeline.run(sql_alchemy_source)
print(info)
```

### ConnectorX
The [`ConnectorX`](https://sfu-db.github.io/connector-x/intro.html) backend completely skips `SQLALchemy` when reading table rows, in favor of doing that in Rust. This is claimed to be significantly faster than any other method (validated only on PostgreSQL). With the default settings, it will emit `PyArrow` tables, but you can configure this by specifying the `return_type` in `backend_kwargs`. (See the [`ConnectorX` docs](https://sfu-db.github.io/connector-x/api.html) for a full list of configurable parameters.)

There are certain limitations when using this backend:
* It will ignore `chunk_size`. `ConnectorX` cannot yield data in batches.
* In many cases, it requires a connection string that differs from the `SQLAlchemy` connection string. Use the `conn` argument in `backend_kwargs` to set this.
* It will convert **decimals** to **doubles**, so you will lose precision.
* Nullability of the columns is ignored (always true).
* It uses different mappings for each data type. (Check [here](https://sfu-db.github.io/connector-x/databases.html) for more details.)
* JSON fields (at least those coming from PostgreSQL) are double-wrapped in strings. To unwrap this, you can pass the in-built transformation function `unwrap_json_connector_x` (for example, with `add_map`):

    ```py
    from dlt.sources.sql_database.helpers import unwrap_json_connector_x
    ```

:::note
`dlt` will still use the data types reflected from the source database when creating destination tables. It is up to the destination to reconcile/parse type differences. Please note that you'll still lose precision on decimals with default settings.
:::

```py
"""This example is taken from the benchmarking tests for ConnectorX performed on the UNSW_Flow dataset (~2mln rows, 25+ columns). Full code here: https://github.com/dlt-hub/sql_database_benchmarking"""
import os
import dlt
from dlt.destinations import filesystem
from dlt.sources.sql_database import sql_table

unsw_table = sql_table(
    "postgresql://loader:loader@localhost:5432/dlt_data",
    "unsw_flow_7",
    "speed_test",
    # this is ignored by connectorx
    chunk_size=100000,
    backend="connectorx",
    # keep source data types
    reflection_level="full_with_precision",
    # just to demonstrate how to set up a separate connection string for connectorx
    backend_kwargs={"conn": "postgresql://loader:loader@localhost:5432/dlt_data"}
)

pipeline = dlt.pipeline(
    pipeline_name="unsw_download",
    destination=filesystem(os.path.abspath("../_storage/unsw")),
    progress="log",
    dev_mode=True,
)

info = pipeline.run(
    unsw_table,
    dataset_name="speed_test",
    table_name="unsw_flow",
    loader_file_format="parquet",
)
print(info)
```
With the dataset above and a local PostgreSQL instance, the `ConnectorX` backend is 2x faster than the `PyArrow` backend.

