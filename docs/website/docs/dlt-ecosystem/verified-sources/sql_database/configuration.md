---
title: Configuration
description: configuring the pipeline script, connection, and backend settings in the sql_database source
keywords: [sql connector, sql database pipeline, sql database]
---

import Header from '../_source-info-header.md';

# Configuration

<Header/>

## Select tables to load

`dlt` sources are Python scripts made up of source and resource functions that can be easily customized. The SQL Database verified source has the following built-in source and resource:
1. `sql_database`: a `dlt` source that can be used to load multiple tables and views from a SQL database.
2. `sql_table`: a `dlt` resource that loads a single table from the SQL database.

Read more about sources and resources here: [General usage: source](../../../general-usage/source.md) and [General usage: resource](../../../general-usage/resource.md).


### Example usage:

:::tip
We intend our sources to be fully hackable. `dlt init` command allows you to eject the source code of the core source and modify it
according to your needs. For example

```sh
 dlt init sql_database duckdb --eject
 ```

will create `sql_database` folder with the source code that you can import and use.
:::

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

    Calling `sql_database(table_names=["family", "clan"])` or `sql_database().with_resources("family", "clan")` loads only the tables `"family"` and `"clan"` from the database.

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
        source = sql_database(table_names=['family', 'clan'])
        # or
        # source = sql_database().with_resources("family", "clan")

        # Run the pipeline
        info = pipeline.run(source)

        # Print load info
        print(info)

    ```

    :::note
    When using the `sql_database` source, specifying table names directly in the source arguments (e.g., `sql_database(table_names=["family", "clan"])`) ensures that only those tables are reflected and turned into resources. In contrast, if you use `.with_resources("family", "clan")`, the entire schema is reflected first, and resources are generated for all tables before filtering for the specified ones. For large schemas, specifying `table_names` can improve performance.
    :::

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
4. **Prefix table names using `apply_hints`**

   You can rename tables before loading them into the destination by applying the `apply_hints` method to each resource. This is useful for avoiding naming collisions or organizing data.

   ```py
   import dlt
   from dlt.sources.sql_database import sql_database
   
   def load_prefixed_tables_from_database() -> None:
       
       # Define the pipeline
       pipeline = dlt.pipeline(
           pipeline_name="rfam",
           destination="duckdb",
           dataset_name="rfam_data",
       )
       
       # Fetch specific tables from the database
       source = sql_database(table_names=["family", "clan"])
       
       # Prefix tables before loading to avoid collisions
       source_system = "prefix"  # Your desired prefix
       for _resource_name, resource in source.resources.items():
           resource.apply_hints(table_name=f"{source_system}__{resource.name}")
       
       # Run the pipeline
       load_info = pipeline.run(source)
       print(load_info)

   ```
   This renames the tables before insertion. For example, the table "family" will be loaded as "prefix__family".
   
5. **Configuring table and column selection in `config.toml`**

   To manage table and column selections outside of your Python scripts, you can configure them directly in the `config.toml` file. This approach is especially beneficial when dealing with multiple tables or when you prefer to keep configuration separate from code.

   Below is an example of how to define table and column selections in the `config.toml` file:
   ```toml
   # to select tables names
   [sources.sql_database]
   table_names = [
       "Table_Name_1",  
   ]

   # to select specific columns from table "Table_Name_1"
   [sources.sql_database.Table_Name_1]
   included_columns = [
       "Column_Name_1",
       "Column_Name_2"
   ]
   ```
   :::note
   *Case-Sensitivity:* 
   
   Table and column names specified in `config.toml` must exactly match their counterparts in the SQL database, as they are case-sensitive.
   :::

## Incremental loading
Incremental loading uses a cursor column (e.g., timestamp or auto-incrementing ID) to load only new or updated data. In essence, arguments that you pass
to [dlt.sources.incremental](../../../general-usage/incremental/cursor) are used by `dlt` to generate SQL query that will select the rows that you need. 

Read [step by step guide on how to use incremental with sql_database](../../../walkthroughs/sql-incremental-configuration).

### How to configure
1. **Choose a cursor column**: Identify a column in your SQL table that can serve as a reliable indicator of new or updated rows. Common choices include timestamp columns or auto-incrementing IDs.
2. **Set an initial value(optional)**: Choose an initial value for the cursor to begin loading data. This could be a specific timestamp or ID from which you wish to start loading. After first run it will be replaced with the maximum cursor value from the selected rows.
3. **Set the comparison direction in the query**. By default greater than or equal op (**>=**) is used to compare initial/previous value with row column value. You can change it with `last_value_func` argument (**max**/**min**).
4. **Set if the comparison is inclusive or exclusive**. By default the range is closed (equal values are included). [Look here for explanation and examples](advanced.md#inclusive-and-exclusive-filtering). Note that for closed ranges `dlt` will use [internal deduplication](../../../general-usage/incremental/cursor.md#deduplicate-overlapping-ranges) which adds some processing cost.
4. **Configure backfill options(optional)**. You can use `end_value` with `range_end` to read data from specified range. You can also control **order returned rows**
to split long incremental loading into many chunks by time and row count. [Look here for details and examples]

:::info Special characters in the cursor column name
If your cursor column name contains special characters (e.g., `$`) you need to escape it when passing it to the `incremental` function. For example, if your cursor column is `example_$column`, you should pass it as `"'example_$column'"` or `'"example_$column"'` to the `incremental` function: `incremental("'example_$column'", initial_value=...)`.
:::

### Configure timezone-aware and naive timestamp cursors
If your cursor is on a timestamp/datetime column, make sure you set up your initial and end values correctly. This will help you avoid implicit type conversions, invalid datetime literals, or column comparisons in database queries. Note that implicit conversions may result in data loss, for example if a naive datetime has a different local timezone on the machine where Python is executing versus your DBMS.

* If your datetime column is naive, use naive Python datetime. Note that `pendulum` datetime is timezone-aware by default while standard `datetime` is naive.
* Use `full` reflection level or above to reflect the `timezone` (awareness hint) on datetime columns.
* Read about [timestamp handling](../../../general-usage/schema.md#handling-of-timestamp-and-time-zones) in `dlt`


### Examples

1. **Incremental loading with the resource `sql_table`**.

  Consider a table "family" with a timestamp column `last_modified` that indicates when a row was last modified. To ensure that only rows modified after midnight (00:00:00) on January 1, 2024, are loaded, you would set the `last_modified` timestamp as the cursor as follows:

  ```py
  import dlt
  from dlt.sources.sql_database import sql_table
  from dlt.common.pendulum import pendulum

  # Example: Incrementally loading a table based on a timestamp column
  table = sql_table(
     table='family',
     incremental=dlt.sources.incremental(
         'last_modified',  # Cursor column name
         initial_value=pendulum.DateTime(2024, 1, 1, 0, 0, 0)  # Initial cursor value
     )
  )

  pipeline = dlt.pipeline(destination="duckdb")
  extract_info = pipeline.extract(table, write_disposition="merge")
  print(extract_info)
  ```

  Behind the scene, the loader generates a SQL query filtering rows with `last_modified` values greater or equal to the incremental value. In the first run, this is the initial value (midnight (00:00:00) January 1, 2024).
  ```sql
  SELECT * FROM family WHERE last_modified >= '2024-01-01T00:00:00Z'
  ```
  In subsequent runs, it is the latest value of `last_modified` that `dlt` stores in [state](../../../general-usage/state).

2. **Incremental loading with the source `sql_database`**.

  To achieve the same using the `sql_database` source, you would specify your cursor as follows:

  ```py
  import dlt
  from dlt.sources.sql_database import sql_database

  source = sql_database().with_resources("family")
  # Using the "last_modified" field as an incremental field using initial value of midnight January 1, 2024 and exclusive comparison
  source.family.apply_hints(
    incremental=dlt.sources.incremental("updated", initial_value=pendulum.DateTime(2022, 1, 1, 0, 0, 0), range_start="open")
    )

  # Running the pipeline
  pipeline = dlt.pipeline(destination="duckdb")
  load_info = pipeline.run(source, write_disposition="merge")
  print(load_info)
  ```
Which generates the following query:
  ```sql
  -- mind the exclusive comparison with > due to range being open
  SELECT * FROM family WHERE last_modified > '2024-01-01T00:00:00Z'
  ```

  :::info
    * When using "merge" write disposition, the source table needs a primary key, which `dlt` automatically sets up.
    * `apply_hints` is a powerful method that enables schema modifications after resource creation, like adjusting write disposition and primary keys. You can choose from various tables and use `apply_hints` multiple times to create pipelines with merged, appended, or replaced resources.
  :::

## Limit number of items returned by the query
If you specified a limit on `sql_table` resource with [add_limit](../../../general-usage/resource.md#sample-from-large-data), this limit will be forwarded 
to the query. Note that limit works in the multiples of `chunk_size`. For example if the `chunk_size` is 1000 and you set `max_items` in `add_limit` to
2, your query will return 2000 rows.

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

You can set up credentials using [any method](../../../general-usage/credentials/setup) supported by `dlt`. We recommend using `.dlt/secrets.toml` or the environment variables. See Step 2 of the [setup](./setup) for how to set credentials inside `secrets.toml`. For more information on passing credentials, read [here](../../../general-usage/credentials/setup).

#### 2. Passing them directly in the script

It is also possible to explicitly pass credentials inside the source. Example:

```py
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.sources.sql_database import sql_database

credentials = ConnectionStringCredentials(
    "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
)

source = sql_database(credentials).with_resources("family")
```

:::note
It is recommended to configure credentials in `.dlt/secrets.toml` and to not include any sensitive information in the pipeline code.
:::

## Other connection options

### Using SQLAlchemy Engine as credentials

You are able to pass an instance of SQLAlchemy Engine instead of credentials:

```py
from dlt.sources.sql_database import sql_table
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam")
table = sql_table(engine, table="chat_message", schema="data")
```

This engine is used by `dlt` to open database connections and can work across multiple threads, so it is compatible with the `parallelize` setting of dlt sources and resources.

### Connecting to a remote database over SSH

To access a remote database securely through an SSH tunnel, you can use the `sshtunnel` library to create a connection and a SQLAlchemy engine. This approach is useful when the database is behind a firewall or requires secure SSH access.

**Step 1: Store SSH and database credentials**

First, store your SSH and database credentials in a configuration file like ".dlt/secrets.toml" or manage them securely through environment variables. For example:

```toml
# .dlt/secrets.toml
[destination.sqlalchemy.credentials]
database = "mydb"
username = "myuser"
password = "mypassword"
host = "please set me up!"
port = 5432
driver_name = "postgresql"

[ssh]
server_ip_address = "please set me up!"
username = "ssh_user_name"
private_key_path = "/path/to/private_key_file"
private_key_password = "optional_key_password" # Leave empty if not needed
```
**Step 2: Set up the SSH tunnel and create the SQLAlchemy engine**

The following script demonstrates the process of establishing an SSH tunnel, creating a SQLAlchemy engine, and utilizing it to configure and run a data pipeline:

```py
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine

from dlt.sources.sql_database import sql_table
import dlt

ssh_creds = dlt.secrets["ssh"]
db_creds = dlt.secrets["destination.sqlalchemy.credentials"]

with SSHTunnelForwarder(
    (ssh_creds["server_ip_address"], 22),
    ssh_username=ssh_creds["username"],
    ssh_pkey=ssh_creds["private_key_path"],
    ssh_private_key_password=ssh_creds.get("private_key_password"),
    remote_bind_address=("127.0.0.1", 5432),
) as tunnel:
    engine = create_engine(
        f"postgresql://{db_creds['username']}:{db_creds['password']}"
        f"@127.0.0.1:{tunnel.local_bind_port}/{db_creds['database']}"
    )

    # Access database table as a dlt resource
    table_resource = sql_table(engine, table="employees", schema="public")

    # Define and run the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="remote_db_pipeline_2",
        destination="duckdb",
        dataset_name="remote_dataset",
    )

    print(pipeline.run(table_resource))
```
Establishing an SSH tunnel and using a SQLAlchemy engine allows secure access to remote databases, ensuring compatibility with dlt pipelines. Always secure credentials and close the tunnel after use.

## Configuring the backend

Table backends convert streams of rows from database tables into batches in various formats. The default backend, `SQLAlchemy`, follows standard `dlt` behavior of extracting and normalizing Python dictionaries. We recommend this for smaller tables, initial development work, and when minimal dependencies or a pure Python environment is required. This backend is also the slowest. Other backends make use of the structured data format of the tables and provide significant improvement in speeds. For example, the `PyArrow` backend converts rows into `Arrow` tables, which results in good performance and preserves exact data types. We recommend using this backend for larger tables.

### SQLAlchemy

The `SQLAlchemy` backend (the default) yields table data as a list of Python dictionaries. This data goes through the regular extract and normalize steps and does not require additional dependencies to be installed. It is the most robust (works with any destination, correctly represents data types) but also the slowest. You can set `reflection_level="full_with precision"` to pass exact data types to the `dlt` schema.

### PyArrow

The `PyArrow` backend yields data as `Arrow` tables. It uses `SQLAlchemy` to read rows in batches but then immediately converts them into `ndarray`, transposes it, and sets it as columns in an `Arrow` table. This backend always fully reflects the database table and preserves original types (i.e., **decimal** / **numeric** data will be extracted without loss of precision). If the destination loads parquet files, this backend will skip the `dlt` normalizer, and you can gain two orders of magnitude (20x - 30x) speed increase.

:::note
To use the `backend="arrow"` configuration, you will need `numpy` installed. You can get another 20-30% speed increase by having `pandas` installed.
The library `numpy` is a required dependency of `pandas` and `pyarrow<18.0.0`. To have all required dependencies, we suggest using this command:

```sh
pip install dlt[sql_database] pyarrow numpy pandas
```
:::

```py
import dlt
import sqlalchemy as sa
from dlt.sources.sql_database import sql_database

pipeline = dlt.pipeline(
    pipeline_name="rfam_cx", destination="postgres", dataset_name="rfam_data_arrow"
)

def _double_as_decimal_adapter(table: sa.Table) -> sa.Table:
    """Emits decimals instead of floats."""
    for column in table.columns.values():
        if isinstance(column.type, sa.Float):
            column.type.asdecimal = False
    return table

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

def _double_as_decimal_adapter(table: sa.Table) -> sa.Table:
    """Emits decimals instead of floats."""
    for column in table.columns.values():
        if isinstance(column.type, sa.Float):
            column.type.asdecimal = True
    return table

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
* Unless `return_type` is set to `arrow_stream` in `backend_kwargs`, it will ignore `chunk_size`. Please note that certain data types such as arrays and high-precision time types are not supported in streaming mode by `ConnectorX`. We also observer that timestamps are not properly returned: tz-aware timestamps are passed without timezone, naive timestamps are passed as date64 which we internally cast back to naive timestamps.
* In many cases, it requires a connection string that differs from the `SQLAlchemy` connection string. Use the `conn` argument in `backend_kwargs` to set this.
* For `connectorx>=0.4.2`, on `reflection_level="minimal"`, `connectorx` can return decimal values. On higher `reflection_level`, dlt will coerce the data type (e.g., modify the decimal `precision` and `scale`, convert to `float`).
    * For `connectorx<0.4.2`, dlt will convert decimals to doubles, thus losing numerical precision.
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
