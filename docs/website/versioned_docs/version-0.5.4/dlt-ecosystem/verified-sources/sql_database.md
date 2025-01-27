---
title: 30+ SQL Databases
description: dlt pipeline for SQL Database
keywords: [sql connector, sql database pipeline, sql database]
---
import Header from './_source-info-header.md';

# 30+ SQL Databases

<Header/>

SQL databases are management systems (DBMS) that store data in a structured format, commonly used
for efficient and reliable data retrieval.

Our SQL Database verified source loads data to your specified destination using SQLAlchemy, pyarrow, pandas or ConnectorX

:::tip
View the pipeline example [here](https://github.com/dlt-hub/verified-sources/blob/master/sources/sql_database_pipeline.py).
:::

Sources and resources that can be loaded using this verified source are:

| Name         | Description                                                          |
| ------------ | -------------------------------------------------------------------- |
| sql_database | Reflects the tables and views in SQL database and retrieves the data |
| sql_table    | Retrieves data from a particular SQL database table                  |
|              |                                                                      |

### Supported databases

We support all [SQLAlchemy dialects](https://docs.sqlalchemy.org/en/20/dialects/), which include, but are not limited to, the following database engines:

* PostgreSQL
* MySQL
* SQLite
* Oracle
* Microsoft SQL Server
* MariaDB
* IBM DB2 and Informix
* Google BigQuery
* Snowflake
* Redshift
* Apache Hive and Presto
* SAP Hana
* CockroachDB
* Firebird
* Teradata Vantage

:::note
Note that there many unofficial dialects, such as [DuckDB](https://duckdb.org/).
:::

## Setup Guide

1. ### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init sql_database duckdb
   ```

   It will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/sql_database_pipeline.py)
   with an SQL database as the [source](../../general-usage/source) and
   [DuckDB](../destinations/duckdb.md) as the [destination](../destinations).

   :::tip
   If you'd like to use a different destination, simply replace `duckdb` with the name of your preferred [destination](../destinations).
   :::

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the guide on [how to add a verified source](../../walkthroughs/add-a-verified-source).

2. ### Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe.

   Here's what the `secrets.toml` looks like:

   ```toml
   [sources.sql_database.credentials]
   drivername = "mysql+pymysql" # driver name for the database
   database = "Rfam" # database name
   username = "rfamro" # username associated with the database
   host = "mysql-rfam-public.ebi.ac.uk" # host address
   port = "4497" # port required for connection
   ```

1. Alternatively, you can also provide credentials in "secrets.toml" as:

   ```toml
   [sources.sql_database]
   credentials="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
   ```
   > See
   > [pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/sql_database_pipeline.py)
   > for details.

1. Finally, follow the instructions in [Destinations](../destinations/) to add credentials for your chosen destination. This will ensure that your data is properly routed.

For more information, read the [General Usage: Credentials.](../../general-usage/credentials)

#### Credentials format

`sql_database` uses SQLAlchemy to create database connections and reflect table schemas. You can pass credentials using
[database urls](https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls). For example:

"mysql+pymysql://rfamro:PWD@mysql-rfam-public.ebi.ac.uk:4497/Rfam"`

will connect to `myssql` database with a name `Rfam` using `pymysql` dialect. The database host is at `mysql-rfam-public.ebi.ac.uk`, port `4497`.
User name is `rfmaro` and password  is `PWD`.

3. ### Run the pipeline

1. Install the necessary dependencies by running the following command:

   ```sh
   pip install -r requirements.txt
   ```

1. Run the verified source by entering:

   ```sh
   python sql_database_pipeline.py
   ```

1. Make sure that everything is loaded as expected with:

   ```sh
   dlt pipeline <pipeline_name> show
   ```

   :::note
   The pipeline_name for the above example is `rfam`, you may also use any
   custom name instead.
   :::

## Source and resource functions
Import `sql_database` and `sql_table` functions as follows:
```py
from sql_database import sql_database, sql_table
```
and read the docstrings to learn about available options.

:::tip
We intend our sources to be fully hackable. Feel free to change the code of the source to customize it to your needs
:::

## Pick the right backend to load table data
Table backends convert stream of rows from database tables into batches in various formats. The default backend **sqlalchemy** is following standard `dlt` behavior of
extracting and normalizing Python dictionaries. We recommend it for smaller tables, initial development work and when minimal dependencies or pure Python environment is required. This backend is also the slowest.
Database tables are structured data and other backends speed up dealing with such data significantly. The **pyarrow** will convert rows into `arrow` tables, has
good performance, preserves exact database types and we recommend it for large tables.

### **sqlalchemy** backend

**sqlalchemy** (the default) yields table data as list of Python dictionaries. This data goes through regular extract
and normalize steps and does not require additional dependencies to be installed. It is the most robust (works with any destination, correctly represents data types) but also the slowest. You can use `reflection_level="full_with_precision"` to pass exact database types to `dlt` schema.

### **pyarrow** backend

**pyarrow** yields data as Arrow tables. It uses **SqlAlchemy** to read rows in batches but then immediately converts them into `ndarray`, transposes it and uses to set columns in an arrow table. This backend always fully
reflects the database table and preserves original types ie. **decimal** / **numeric** will be extracted without loss of precision. If the destination loads parquet files, this backend will skip `dlt` normalizer and you can gain two orders of magnitude (20x - 30x) speed increase.

Note that if **pandas** is installed, we'll use it to convert SqlAlchemy tuples into **ndarray** as it seems to be 20-30% faster than using **numpy** directly.

```py
import sqlalchemy as sa
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
    table_adapter_callback=_double_as_decimal_adapter
).with_resources("family", "genome")

info = pipeline.run(sql_alchemy_source)
print(info)
```

### **pandas** backend

**pandas** backend yield data as data frames using the `pandas.io.sql` module. `dlt` use **pyarrow** dtypes by default as they generate more stable typing.

With default settings, several database types will be coerced to dtypes in yielded data frame:
* **decimal** are mapped to doubles so it is possible to lose precision.
* **date** and **time** are mapped to strings
* all types are nullable.

Note: `dlt` will still use the reflected source database types to create destination tables. It is up to the destination to reconcile / parse
type differences. Most of the destinations will be able to parse date/time strings and convert doubles into decimals (Please note that you' still lose precision on decimals with default settings.). **However we strongly suggest
not to use pandas backend if your source tables contain date, time or decimal columns**

Example: Use `backend_kwargs` to pass [backend-specific settings](https://pandas.pydata.org/docs/reference/api/pandas.read_sql_table.html) ie. `coerce_float`. Internally dlt uses `pandas.io.sql._wrap_result` to generate panda frames.

```py
import sqlalchemy as sa
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

### **connectorx** backend
[connectorx](https://sfu-db.github.io/connector-x/intro.html) backend completely skips **sqlalchemy** when reading table rows, in favor of doing that in rust. This is claimed to be significantly faster than any other method (confirmed only on postgres - see next chapter). With the default settings it will emit **pyarrow** tables, but you can configure it via **backend_kwargs**.

There are certain limitations when using this backend:
* it will ignore `chunk_size`. **connectorx** cannot yield data in batches.
* in many cases it requires a connection string that differs from **sqlalchemy** connection string. Use `conn` argument in **backend_kwargs** to set it up.
* it will convert **decimals** to **doubles** so you'll will lose precision.
* nullability of the columns is ignored (always true)
* it uses different database type mappings for each database type. [check here for more details](https://sfu-db.github.io/connector-x/databases.html)
* JSON fields (at least those coming from postgres) are double wrapped in strings. Here's a transform to be added with `add_map` that will unwrap it:

```py
from sources.sql_database.helpers import unwrap_json_connector_x
```

Note: dlt will still use the reflected source database types to create destination tables. It is up to the destination to reconcile / parse type differences. Please note that you' still lose precision on decimals with default settings.

```py
"""Uses unsw_flow dataset (~2mln rows, 25+ columns) to test connectorx speed"""
import os
from dlt.destinations import filesystem

unsw_table = sql_table(
    "postgresql://loader:loader@localhost:5432/dlt_data",
    "unsw_flow_7",
    "speed_test",
    # this is ignored by connectorx
    chunk_size=100000,
    backend="connectorx",
    # keep source data types
    reflection_level="full_with_precision",
    # just to demonstrate how to setup a separate connection string for connectorx
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
With dataset above and local postgres instance, connectorx is 2x faster than pyarrow backend.

### Notes on source databases

#### Oracle
1. When using **oracledb** dialect in thin mode we are getting protocol errors. Use thick mode or **cx_oracle** (old) client.
2. Mind that **sqlalchemy** translates Oracle identifiers into lower case! Keep the default `dlt` naming convention (`snake_case`) when loading data. We'll support more naming conventions soon.
3. Connectorx is for some reason slower for Oracle than `pyarrow` backend.

#### DB2
1. Mind that **sqlalchemy** translates DB2 identifiers into lower case! Keep the default `dlt` naming convention (`snake_case`) when loading data. We'll support more naming conventions soon.
2. DB2 `DOUBLE` type is mapped to `Numeric` SqlAlchemy type with default precision, still `float` python types are returned. That requires `dlt` to perform additional casts. The cost of the cast however is minuscule compared to the cost of reading rows from database

#### MySQL
1. SqlAlchemy dialect converts doubles to decimals, we disable that behavior via table adapter in our demo pipeline

#### Postgres / MSSQL
No issues found. Postgres is the only backend where we observed 2x speedup with connector x. On other db systems it performs same as `pyarrrow` backend or slower.

### Notes on data types

#### JSON
JSON data type is represented as Python object for the **sqlalchemy** backend and as JSON string for the **pyarrow** backend. Currently it does not work correctly
with **pandas** and **connector-x** which cast Python objects to str generating invalid JSON strings that cannot be loaded into destination.

#### UUID
UUIDs are represented as string by default. You can switch that behavior by using table adapter callback and modifying properties of the UUID type for a particular column.


## Incremental Loading
Efficient data management often requires loading only new or updated data from your SQL databases, rather than reprocessing the entire dataset. This is where incremental loading comes into play.

Incremental loading uses a cursor column (e.g., timestamp or auto-incrementing ID) to load only data newer than a specified initial value, enhancing efficiency by reducing processing time and resource use.


### Configuring Incremental Loading
1. **Choose a Cursor Column**: Identify a column in your SQL table that can serve as a reliable indicator of new or updated rows. Common choices include timestamp columns or auto-incrementing IDs.
1. **Set an Initial Value**: Choose a starting value for the cursor to begin loading data. This could be a specific timestamp or ID from which you wish to start loading data.
1. **Deduplication**: When using incremental loading, the system automatically handles the deduplication of rows based on the primary key (if available) or row hash for tables without a primary key.
1. **Set end_value for backfill**: Set `end_value` if you want to backfill data from
certain range.
1. **Order returned rows**. Set `row_order` to `asc` or `desc` to order returned rows.

#### Incremental Loading Example
1. Consider a table with a `last_modified` timestamp column. By setting this column as your cursor and specifying an
   initial value, the loader generates a SQL query filtering rows with `last_modified` values greater than the specified initial value.

   ```py
   from sql_database import sql_table
   from datetime import datetime

   # Example: Incrementally loading a table based on a timestamp column
   table = sql_table(
       table='your_table_name',
       incremental=dlt.sources.incremental(
           'last_modified',  # Cursor column name
           initial_value=datetime(2024, 1, 1)  # Initial cursor value
       )
   )

   info = pipeline.extract(table, write_disposition="merge")
   print(info)
   ```

1. To incrementally load the "family" table using the sql_database source method:

   ```py
   source = sql_database().with_resources("family")
   #using the "updated" field as an incremental field using initial value of January 1, 2022, at midnight
   source.family.apply_hints(incremental=dlt.sources.incremental("updated"),initial_value=pendulum.DateTime(2022, 1, 1, 0, 0, 0))
   #running the pipeline
   info = pipeline.run(source, write_disposition="merge")
   print(info)
   ```
   In this example, we load data from the `family` table, using the `updated` column for incremental loading. In the first run, the process loads all data starting from midnight (00:00:00) on January 1, 2022. Subsequent runs perform incremental loading, guided by the values in the `updated` field.

1. To incrementally load the "family" table using the 'sql_table' resource.

   ```py
   family = sql_table(
       table="family",
       incremental=dlt.sources.incremental(
           "updated", initial_value=pendulum.datetime(2022, 1, 1, 0, 0, 0)
       ),
   )
   # Running the pipeline
   info = pipeline.extract(family, write_disposition="merge")
   print(info)
   ```

   This process initially loads all data from the `family` table starting at midnight on January 1, 2022. For later runs, it uses the `updated` field for incremental loading as well.

   :::info
   * For merge write disposition, the source table needs a primary key, which `dlt` automatically sets up.
   * `apply_hints` is a powerful method that enables schema modifications after resource creation, like adjusting write disposition and primary keys. You can choose from various tables and use `apply_hints` multiple times to create pipelines with merged, appended, or replaced resources.
   :::

## Run on Airflow
When running on Airflow
1. Use `dlt` [Airflow Helper](../../walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer.md#2-modify-dag-file) to create tasks from `sql_database` source. You should be able to run table extraction in parallel with `parallel-isolated` source->DAG conversion.
2. Reflect tables at runtime with `defer_table_reflect` argument.
3. Set `allow_external_schedulers` to load data using [Airflow intervals](../../general-usage/incremental-loading.md#using-airflow-schedule-for-backfill-and-incremental-loading).

## Parallel extraction
You can extract each table in a separate thread (no multiprocessing at this point). This will decrease loading time if your queries take time to execute or your network latency/speed is low.
```py
database = sql_database().parallelize()
table = sql_table().parallelize()
```

## Column reflection

Columns and their data types are reflected with SQLAlchemy. The SQL types are then mapped to `dlt` types.
Most types are supported.

The `reflection_level` argument controls how much information is reflected:

- `reflection_level = "minimal"`: Only column names and nullability are detected. Data types are inferred from the data.
- `reflection_level = "full"`: Column names, nullability, and data types are detected. For decimal types we always add precision and scale. **This is the default.**
- `reflection_level = "full_with_precision"`: Column names, nullability, data types, and precision/scale are detected, also for types like text and binary. Integer sizes are set to bigint and to int for all other types.

If the SQL type is unknown or not supported by `dlt` the column is skipped when using the `pyarrow` backend.
In other backend the type is inferred from data regardless of `reflection_level`, this often works, some types are coerced to strings
and `dataclass` based values from sqlalchemy are inferred as `complex` (JSON in most destinations).

:::tip
If you use **full** (and above) reflection level you may encounter a situation where the data returned by sql alchemy or pyarrow backend
does not match the reflected data types. Most common symptoms are:
1. The destination complains that it cannot cast one type to another for a certain column. For example `connector-x` returns TIME in nanoseconds
and BigQuery sees it as bigint and fails to load.
2. You get `SchemaCorruptedException` or other coercion error during `normalize` step.
In that case you may try **minimal** reflection level where all data types are inferred from the returned data. From our experience this prevents
most of the coercion problems.
:::

You can also override the sql type by passing a `type_adapter_callback` function.
This function takes an `sqlalchemy` data type and returns a new type (or `None` to force the column to be inferred from the data).

This is useful for example when:
- You're loading a data type which is not supported by the destination (e.g. you need JSON type columns to be coerced to string)
- You're using an sqlalchemy dialect which uses custom types that don't inherit from standard sqlalchemy types.
- For certain types you prefer `dlt` to infer data type from the data and you return `None`

Example, when loading timestamps from Snowflake you can make sure they translate to `timestamp` columns in the result schema:

```py
import dlt
from snowflake.sqlalchemy import TIMESTAMP_NTZ
import sqlalchemy as sa

def type_adapter_callback(sql_type):
    if isinstance(sql_type, TIMESTAMP_NTZ):  # Snowflake does not inherit from sa.DateTime
        return sa.DateTime(timezone=True)
    return sql_type  # Use default detection for other types

source = sql_database(
    "snowflake://user:password@account/database?&warehouse=WH_123",
    reflection_level="full",
    type_adapter_callback=type_adapter_callback,
    backend="pyarrow"
)

dlt.pipeline("demo").run(source)
```

## Extended configuration
You are able to configure most of the arguments to `sql_database` and `sql_table` via toml files and environment variables. This is particularly useful with `sql_table`
because you can maintain a separate configuration for each table (below we show **secrets.toml** and **config.toml**, you are free to combine them into one.):
```toml
[sources.sql_database]
credentials="mssql+pyodbc://loader.database.windows.net/dlt_data?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
```

```toml
[sources.sql_database.chat_message]
backend="pandas"
chunk_size=1000

[sources.sql_database.chat_message.incremental]
cursor_path="updated_at"
```
Example above will setup **backend** and **chunk_size** for a table with name **chat_message**. It will also enable incremental loading on a column named **updated_at**.
Table resource is instantiated as follows:
```py
table = sql_table(table="chat_message", schema="data")
```

Similarly, you can configure `sql_database` source.
```toml
[sources.sql_database]
credentials="mssql+pyodbc://loader.database.windows.net/dlt_data?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
schema="data"
backend="pandas"
chunk_size=1000

[sources.sql_database.chat_message.incremental]
cursor_path="updated_at"
```
Note that we are able to configure incremental loading per table, even if it is a part of a dlt source. Source below will extract data using **pandas** backend
with **chunk_size** 1000. **chat_message** table will load data incrementally using **updated_at** column. All other tables will load fully.
```py
database = sql_database()
```

You can configure all the arguments this way (except adapter callback function). [Standard dlt rules apply](https://dlthub.com/docs/general-usage/credentials/configuration#configure-dlt-sources-and-resources). You can use environment variables [by translating the names properly](https://dlthub.com/docs/general-usage/credentials/config_providers#toml-vs-environment-variables) ie.
```sh
SOURCES__SQL_DATABASE__CREDENTIALS="mssql+pyodbc://loader.database.windows.net/dlt_data?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
SOURCES__SQL_DATABASE__BACKEND=pandas
SOURCES__SQL_DATABASE__CHUNK_SIZE=1000
SOURCES__SQL_DATABASE__CHAT_MESSAGE__INCREMENTAL__CURSOR_PATH=updated_at
```

### Configuring incremental loading
`dlt.sources.incremental` class is a [config spec](https://dlthub.com/docs/general-usage/credentials/config_specs) and can be configured like any other spec, here's an example that sets all possible options:
```toml
[sources.sql_database.chat_message.incremental]
cursor_path="updated_at"
initial_value=2024-05-27T07:32:00Z
end_value=2024-05-28T07:32:00Z
row_order="asc"
allow_external_schedulers=false
```
Please note that we specify date times in **toml** as initial and end value. For env variables only strings are currently supported.


### Use SqlAlchemy Engine as credentials
You are able to pass an instance of **SqlAlchemy** `Engine` instance instead of credentials:
```py
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam")
table = sql_table(engine, table="chat_message", schema="data")
```
Engine is used by `dlt` to open database connections and can work across multiple threads so is compatible with `parallelize` setting of dlt sources and resources.


## Troubleshooting

### Connect to mysql with SSL
Here, we use the `mysql` and `pymysql` dialects to set up an SSL connection to a server, with all information taken from the [SQLAlchemy docs](https://docs.sqlalchemy.org/en/14/dialects/mysql.html#ssl-connections).

1. To enforce SSL on the client without a client certificate you may pass the following DSN:

   ```toml
   sources.sql_database.credentials="mysql+pymysql://root:<pass>@<host>:3306/mysql?ssl_ca="
   ```

1. You can also pass the server's public certificate (potentially bundled with your pipeline) and disable host name checks:

   ```toml
   sources.sql_database.credentials="mysql+pymysql://root:<pass>@<host>:3306/mysql?ssl_ca=server-ca.pem&ssl_check_hostname=false"
   ```

1. For servers requiring a client certificate, provide the client's private key (a secret value). In Airflow, this is usually saved as a variable and exported to a file before use. The server certificate is omitted in the example below:

   ```toml
   sources.sql_database.credentials="mysql+pymysql://root:<pass>@35.203.96.191:3306/mysql?ssl_ca=&ssl_cert=client-cert.pem&ssl_key=client-key.pem"
   ```

### SQL Server connection options

**To connect to an `mssql` server using Windows authentication**, include `trusted_connection=yes` in the connection string.

```toml
sources.sql_database.credentials="mssql+pyodbc://loader.database.windows.net/dlt_data?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
```

**To connect to a local sql server instance running without SSL** pass `encrypt=no` parameter:
```toml
sources.sql_database.credentials="mssql+pyodbc://loader:loader@localhost/dlt_data?encrypt=no&driver=ODBC+Driver+17+for+SQL+Server"
```

**To allow self signed SSL certificate** when you are getting `certificate verify failed:unable to get local issuer certificate`:
```toml
sources.sql_database.credentials="mssql+pyodbc://loader:loader@localhost/dlt_data?TrustServerCertificate=yes&driver=ODBC+Driver+17+for+SQL+Server"
```

***To use long strings (>8k) and avoid collation errors**:
```toml
sources.sql_database.credentials="mssql+pyodbc://loader:loader@localhost/dlt_data?LongAsMax=yes&driver=ODBC+Driver+17+for+SQL+Server"
```

## Customizations
### Transform the data in Python before it is loaded

You have direct access to all resources (that represent tables) and you can modify hints, add python transforms, parallelize execution etc. as for any other
resource. Below we show you an example on how to pseudonymize the data before it is loaded by using deterministic hashing.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```py
   pipeline = dlt.pipeline(
        pipeline_name="rfam",  # Use a custom name if desired
        destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
        dataset_name="rfam_data"  # Use a custom name if desired
   )
   ```

1. Pass your credentials using any of the methods [described above](#add-credentials).

1. To load the entire database, use the `sql_database` source as:

   ```py
   source = sql_database()
   info = pipeline.run(source, write_disposition="replace")
   print(info)
   ```

1. If you just need the "family" table, use:

   ```py
   source = sql_database().with_resources("family")
   #running the pipeline
   info = pipeline.run(source, write_disposition="replace")
   print(info)
   ```

1. To pseudonymize columns and hide personally identifiable information (PII), refer to the
   [documentation](https://dlthub.com/docs/general-usage/customising-pipelines/pseudonymizing_columns).
   As an example, here's how to pseudonymize the "rfam_acc" column in the "family" table:

   ```py
   import hashlib

   def pseudonymize_name(doc):
      '''
      Pseudonmyisation is a deterministic type of PII-obscuring
      Its role is to allow identifying users by their hash,
      without revealing the underlying info.
      '''
      # add a constant salt to generate
      salt = 'WI@N57%zZrmk#88c'
      salted_string = doc['rfam_acc'] + salt
      sh = hashlib.sha256()
      sh.update(salted_string.encode())
      hashed_string = sh.digest().hex()
      doc['rfam_acc'] = hashed_string
      return doc

   pipeline = dlt.pipeline(
       # Configure the pipeline
   )
   # using sql_database source to load family table and pseudonymize the column "rfam_acc"
   source = sql_database().with_resources("family")
   # modify this source instance's resource
   source = source.family.add_map(pseudonymize_name)
   # Run the pipeline. For a large db this may take a while
   info = pipeline.run(source, write_disposition="replace")
   print(info)
   ```

1. To exclude columns, such as the "rfam_id" column from the "family" table before loading:

   ```py
   def remove_columns(doc):
       del doc["rfam_id"]
       return doc

   pipeline = dlt.pipeline(
       # Configure the pipeline
   )
   # using sql_database source to load family table and remove the column "rfam_id"
   source = sql_database().with_resources("family")
   # modify this source instance's resource
   source = source.family.add_map(remove_columns)
   # Run the pipeline. For a large db this may take a while
   info = pipeline.run(source, write_disposition="replace")
   print(info)
   ```

1. Remember to keep the pipeline name and destination dataset name consistent. The pipeline name is crucial for retrieving the [state](https://dlthub.com/docs/general-usage/state) from the last run, which is essential for incremental loading. Altering these names could initiate a "[dev_mode](https://dlthub.com/docs/general-usage/pipeline#do-experiments-with-dev-mode)", interfering with the metadata tracking necessary for [incremental loads](https://dlthub.com/docs/general-usage/incremental-loading).

## Additional Setup guides
- [Load data from IBM Db2 to Google Cloud Storage in python with dlt](https://dlthub.com/docs/pipelines/sql_database_db2/load-data-with-python-from-sql_database_db2-to-filesystem-gcs)
- [Load data from PostgreSQL to AlloyDB in python with dlt](https://dlthub.com/docs/pipelines/sql_database_postgres/load-data-with-python-from-sql_database_postgres-to-alloydb)
- [Load data from MySQL to Dremio in python with dlt](https://dlthub.com/docs/pipelines/sql_database_mysql/load-data-with-python-from-sql_database_mysql-to-dremio)
- [Load data from IBM Db2 to AWS Athena in python with dlt](https://dlthub.com/docs/pipelines/sql_database_db2/load-data-with-python-from-sql_database_db2-to-athena)
- [Load data from SAP HANA to AWS S3 in python with dlt](https://dlthub.com/docs/pipelines/sql_database_hana/load-data-with-python-from-sql_database_hana-to-filesystem-aws)
- [Load data from Microsoft SQL Server to AWS S3 in python with dlt](https://dlthub.com/docs/pipelines/sql_database_mssql/load-data-with-python-from-sql_database_mssql-to-filesystem-aws)
- [Load data from PostgreSQL to CockroachDB in python with dlt](https://dlthub.com/docs/pipelines/sql_database_postgres/load-data-with-python-from-sql_database_postgres-to-cockroachdb)
- [Load data from MySQL to CockroachDB in python with dlt](https://dlthub.com/docs/pipelines/sql_database_mysql/load-data-with-python-from-sql_database_mysql-to-cockroachdb)
- [Load data from IBM Db2 to BigQuery in python with dlt](https://dlthub.com/docs/pipelines/sql_database_db2/load-data-with-python-from-sql_database_db2-to-bigquery)
- [Load data from IBM Db2 to YugabyteDB in python with dlt](https://dlthub.com/docs/pipelines/sql_database_db2/load-data-with-python-from-sql_database_db2-to-yugabyte)
