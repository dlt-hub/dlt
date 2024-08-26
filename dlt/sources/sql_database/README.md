# SQL Database
SQL database, or Structured Query Language database, are a type of database management system (DBMS) that stores and manages data in a structured format.  The SQL Database `dlt` is a verified source and pipeline example that makes it easy to load data from your SQL database to a destination of your choice. It offers flexibility in terms of loading either the entire database or specific tables to the target.

## Initialize the pipeline with SQL Database verified source
```bash
dlt init sql_database bigquery
```
Here, we chose BigQuery as the destination. Alternatively, you can also choose redshift, duckdb, or any of the other [destinations.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Setup verified source

To setup the SQL Database Verified Source read the [full documentation here.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database)

## Add credentials
1. Open `.dlt/secrets.toml`.
2. In order to continue, we will use the supplied connection URL to establish credentials. The connection URL is associated with a public database and looks like this:
    ```bash
    connection_url = "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
    ```
    Here's what the `secrets.toml` looks like:
    ```toml
    # Put your secret values and credentials here. do not share this file and do not upload it to github.
    # We will set up creds with the following connection URL, which is a public database

    # The credentials are as follows
    drivername = "mysql+pymysql" # Driver name for the database
    database = "Rfam # Database name
    username = "rfamro" # username associated with the database
    host = "mysql-rfam-public.ebi.ac.uk" # host address
    port = "4497 # port required for connection
    ```
3. Enter credentials for your chosen destination as per the [docs.](https://dlthub.com/docs/dlt-ecosystem/destinations/)

## Running the pipeline example

1. Install the required dependencies by running the following command:
    ```bash
    pip install -r requirements.txt
    ```

2. Now you can build the verified source by using the command:
    ```bash
    python3 sql_database_pipeline.py
    ```

3. To ensure that everything loads as expected, use the command:
    ```bash
    dlt pipeline <pipeline_name> show
    ```

    For example, the pipeline_name for the above pipeline example is `rfam`, you can use any custom name instead.


## Pick the right table backend
Table backends convert stream of rows from database tables into batches in various formats. The default backend **sqlalchemy** is following standard `dlt` behavior of
extracting and normalizing Python dictionaries. We recommend it for smaller tables, initial development work and when minimal dependencies or pure Python environment is required. It is also the slowest.
Database tables are structured data and other backends speed up dealing with such data significantly. The **pyarrow** will convert rows into `arrow` tables, has
good performance, preserves exact database types and we recommend it for large tables.

### **sqlalchemy** backend

**sqlalchemy** (the default) yields table data as list of Python dictionaries. This data goes through regular extract
and normalize steps and does not require additional dependencies to be installed. It is the most robust (works with any destination, correctly represents data types) but also the slowest. You can use `detect_precision_hints` to pass exact database types to `dlt` schema.

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
    """Return double as double, not decimals, this is mysql thing"""
    for column in table.columns.values():
        if isinstance(column.type, sa.Double):
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
    detect_precision_hints=True,
    # just to demonstrate how to setup a separate connection string for connectorx
    backend_kwargs={"conn": "postgresql://loader:loader@localhost:5432/dlt_data"}
)

pipeline = dlt.pipeline(
    pipeline_name="unsw_download",
    destination=filesystem(os.path.abspath("../_storage/unsw")),
    progress="log",
    full_refresh=True,
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

## Notes on source databases

### Oracle
1. When using **oracledb** dialect in thin mode we are getting protocol errors. Use thick mode or **cx_oracle** (old) client.
2. Mind that **sqlalchemy** translates Oracle identifiers into lower case! Keep the default `dlt` naming convention (`snake_case`) when loading data. We'll support more naming conventions soon.
3. Connectorx is for some reason slower for Oracle than `pyarrow` backend.

### DB2
1. Mind that **sqlalchemy** translates DB2 identifiers into lower case! Keep the default `dlt` naming convention (`snake_case`) when loading data. We'll support more naming conventions soon.
2. DB2 `DOUBLE` type is mapped to `Numeric` SqlAlchemy type with default precision, still `float` python types are returned. That requires `dlt` to perform additional casts. The cost of the cast however is minuscule compared to the cost of reading rows from database

### MySQL
1. SqlAlchemy dialect converts doubles to decimals, we disable that behavior via table adapter in our demo pipeline

### Postgres / MSSQL
No issues found. Postgres is the only backend where we observed 2x speedup with connector x. On other db systems it performs same as `pyarrrow` backend or slower.

## Learn more
💡 To explore additional customizations for this pipeline, we recommend referring to the official DLT SQL Database verified documentation. It provides comprehensive information and guidance on how to further customize and tailor the pipeline to suit your specific needs. You can find the DLT SQL Database documentation in [Setup Guide: SQL Database.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database)
