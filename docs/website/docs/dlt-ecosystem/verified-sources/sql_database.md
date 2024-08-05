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

Our SQL Database verified source loads data to your specified destination using SQLAlchemy, pyarrow, pandas, or ConnectorX

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

* [PostgreSQL](#postgres--mssql)
* [MySQL](#mysql)
* SQLite
* [Oracle](#oracle)
* [Microsoft SQL Server](#postgres--mssql)
* MariaDB
* [IBM DB2 and Informix](#db2)
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

## Setup

To connect to your SQL database using `dlt` follow these steps:

1.  Initialize a `dlt` project in the current working directory by running the following command:

    ```sh 
    dlt init sql_database duckdb
    ```

    This will add necessary files and configurations for a `dlt` pipeline with SQL database as the [source](../../general-usage/source) and
   [DuckDB](../destinations/duckdb.md) as the [destination](../destinations).
   
    :::tip
    If you'd like to use a different destination, simply replace `duckdb` with the name of your preferred [destination](../destinations).
    :::

2. Add credentials for your SQL database

    To connect to your SQL database, `dlt` would need to authenticate using necessary credentials. To enable this, paste your credentials in the `secrets.toml` file created inside the `.dlt/` folder in the following format:
    ```toml
    [sources.sql_database.credentials]
    drivername = "mysql+pymysql" # driver name for the database
    database = "Rfam" # database name
    username = "rfamro" # username associated with the database
    host = "mysql-rfam-public.ebi.ac.uk" # host address
    port = "4497" # port required for connection
    ```

    Alternatively, you can also authenticate using connection strings:
    ```toml
    [sources.sql_database.credentials]
    credentials="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
    ```

    To learn more about how to pass credentials into your `dlt` pipeline see [here](../../walkthroughs/add_credentials.md).  

3. Add credentials for your destination (if necessary)  

    Depending on which [destination](../destinations) you're loading into, you might also need to add your destination credentials. For more information read the [General Usage: Credentials.](../../general-usage/credentials)

4. Install any necessary dependencies  

    ```sh
    pip install -r requirements.txt
    ```

5. Run the pipeline  

    ```sh
    python sql_database_pipeline.py
    ```


6. Make sure everything is loaded as expected with  
    ```sh
    dlt pipeline <pipeline_name> show
    ```

   :::note
   The pipeline_name for the above example is `rfam`, you may also use any
   custom name instead. :::  


## How to use

The SQL Database verified source has two in-built sources and resources:  
1. `sql_database`: a `dlt` source which can be used to load multiple tables and views from a SQL database
2. `sql_table`: a `dlt` resource that loads a single table from the SQL database

Read more about sources and resources here: [General Usage: Source](../../general-usage/source.md) and [General Usage: Resource](../../general-usage/resource.md).

#### Examples:

1. **Load all the tables from a database**  
Calling `sql_database()` loads all tables from the database.

    ```py
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


## Configuring connection to the SQL database

### Connection string format
`sql_database` uses SQLAlchemy to create database connections and reflect table schemas. You can pass credentials using
[database urls](https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls). For example, to use the `pymysql` dialect to connect to a `myssql` database `Rfam` with user name `rfmaro` password `PWD` host `mysql-rfam-public.ebi.ac.uk` and port `4497`, you would construct your connection string as follows:

"mysql+pymysql://rfamro:PWD@mysql-rfam-public.ebi.ac.uk:4497/Rfam"`

Database-specific drivers can be passed into the connection string using query parameters. For example, to connect to Microsoft SQL Server using the ODBC Driver, you would need to pass the driver as a query parameter as follows:  

"mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server"


### Adding credentials to the `dlt` pipeline

#### Setting them in `secrets.toml` or as environment variables (Recommended)

By default, `dlt` looks for credentials inside `.dlt/secrets.toml` or in the environment variables. See Step 2 of the [setup](#setup) for how to set credentials inside `secrets.toml`. For more information on passing credentials through `.toml` or as enviroment variables, read [here](../../walkthroughs/add_credentials.md).  


#### Passing them directly in the script 
It is also possible to explicitly pass credentials inside the source. Example:
```py
from dlt.sources.credentials import ConnectionStringCredentials
from sql_database import sql_table

credentials = ConnectionStringCredentials(
    "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
)

source = sql_table(credentials).with_resource("family")
```
:::
Note: It is recommended to configure credentials in `.dlt/secrets.toml` and to not include any sensitive information in the pipeline code. :::

### Other connection options
#### Using SqlAlchemy Engine as credentials  
You are able to pass an instance of SqlAlchemy Engine instead of credentials:
```py
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam")
table = sql_table(engine, table="chat_message", schema="data")
```
This engine is used by `dlt` to open database connections and can work across multiple threads so is compatible with `parallelize` setting of dlt sources and resources.

#### Connect to mysql with SSL 
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

#### SQL Server connection options

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

**To use long strings (>8k) and avoid collation errors**:
```toml
sources.sql_database.credentials="mssql+pyodbc://loader:loader@localhost/dlt_data?LongAsMax=yes&driver=ODBC+Driver+17+for+SQL+Server"
```

## Configuring the backend

Table backends convert streams of rows from database tables into batches in various formats. The default backend **sqlalchemy** follows standard `dlt` behavior of
extracting and normalizing Python dictionaries. We recommend this for smaller tables, initial development work, and when minimal dependencies or a pure Python environment is required. This backend is also the slowest. Other backends make use of the structured data format of the tables and provide significant improvement in speeds. For example, the **pyarrow** backend converts rows into `arrow` tables, which results in
good performance and preserves exact database types. We recommend using this backend for larger tables.

### **sqlalchemy**

The **sqlalchemy** backend (the default) yields table data as a list of Python dictionaries. This data goes through regular extract
and normalize steps and does not require additional dependencies to be installed. It is the most robust (works with any destination, correctly represents data types) but also the slowest. You can set `reflection_level="full_with_precision"` to pass exact database types to `dlt` schema.

### **pyarrow**

The **pyarrow** backend yields data as Arrow tables. It uses **SqlAlchemy** to read rows in batches but then immediately converts them into `ndarray`, transposes it, and sets it as columns in an arrow table. This backend always fully
reflects the database table and preserves original types (i.e. **decimal** / **numeric** data will be extracted without loss of precision). If the destination loads parquet files, this backend will skip `dlt` normalizer and you can gain two orders of magnitude (20x - 30x) speed increase.

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

### **pandas**

The **pandas** backend yields data as data frames using the `pandas.io.sql` module. `dlt` uses **pyarrow** dtypes by default as they generate more stable typing.

With the default settings, several database types will be coerced to dtypes in the yielded data frame:
* **decimal** is mapped to doubles so it is possible to lose precision
* **date** and **time** are mapped to strings
* all types are nullable

Note: `dlt` will still use the reflected source database types to create destination tables. It is up to the destination to reconcile / parse
type differences. Most of the destinations will be able to parse date/time strings and convert doubles into decimals (Please note that you'll still lose precision on decimals with default settings.). **However we strongly suggest
not to use pandas backend if your source tables contain date, time or decimal columns**

To adjust [backend-specific settings,](https://pandas.pydata.org/docs/reference/api/pandas.read_sql_table.html) pass it in the `backend_kwargs` parameter. For example, below we set `coerce_float` to `False`: 

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

### **connectorx**
The [connectorx](https://sfu-db.github.io/connector-x/intro.html) backend completely skips **sqlalchemy** when reading table rows, in favor of doing that in rust. This is claimed to be significantly faster than any other method (validated only on postgres). With the default settings it will emit **pyarrow** tables, but you can configure it via **backend_kwargs**.

There are certain limitations when using this backend:
* it will ignore `chunk_size`. **connectorx** cannot yield data in batches.
* in many cases it requires a connection string that differs from the **sqlalchemy** connection string. Use the `conn` argument in **backend_kwargs** to set this.
* it will convert **decimals** to **doubles**, so you'll will lose precision.
* nullability of the columns is ignored (always true)
* it uses different database type mappings for each database type. [check here for more details](https://sfu-db.github.io/connector-x/databases.html)
* JSON fields (at least those coming from postgres) are double wrapped in strings. To unwrap this, you can pass the in-built transformation function `unwrap_json_connector_x` (for example, with `add_map`):

    ```py
    from sources.sql_database.helpers import unwrap_json_connector_x
    ```

Note: `dlt` will still use the reflected source database types to create destination tables. It is up to the destination to reconcile / parse type differences. Please note that you'll still lose precision on decimals with default settings.

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
With the dataset above and a local postgres instance, the connectorx backend is 2x faster than the pyarrow backend.

### Specific database notes

#### Oracle
1. When using the **oracledb** dialect in thin mode we are getting protocol errors. Use thick mode or **cx_oracle** (old) client.
2. Mind that **sqlalchemy** translates Oracle identifiers into lower case! Keep the default `dlt` naming convention (`snake_case`) when loading data. We'll support more naming conventions soon.
3. Connectorx is for some reason slower for Oracle than the `pyarrow` backend.  
  
See [here](https://github.com/dlt-hub/sql_database_benchmarking/tree/main/oracledb#installing-and-setting-up-oracle-db) for information and code on setting up and benchmarking on Oracle.

#### DB2
1. Mind that **sqlalchemy** translates DB2 identifiers into lower case! Keep the default `dlt` naming convention (`snake_case`) when loading data. We'll support more naming conventions soon.
2. The DB2 type `DOUBLE` gets incorrectly mapped to the python type `float` (instead of the SqlAlchemy type `Numeric` with default precision). This requires `dlt` to perform additional casts. The cost of the cast, however, is minuscule compared to the cost of reading rows from database.  

See [here](https://github.com/dlt-hub/sql_database_benchmarking/tree/main/db2#installing-and-setting-up-db2) for information and code on setting up and benchmarking on db2.

#### MySQL
1. The **SqlAlchemy** dialect converts doubles to decimals. (This can be disabled via the table adapter argument as shown in the code example [here](#pyarrow))

#### Postgres / MSSQL
No issues were found for these databases. Postgres is the only backend where we observed 2x speedup with connectorx (see [here](https://github.com/dlt-hub/sql_database_benchmarking/tree/main/postgres) for the benchmarking code). On other db systems it performs the same as (or some times worse than) the `pyarrrow` backend.

## Advanced configuration

### Incremental Loading

Efficient data management often requires loading only new or updated data from your SQL databases, rather than reprocessing the entire dataset. This is where incremental loading comes into play.

Incremental loading uses a cursor column (e.g., timestamp or auto-incrementing ID) to load only data newer than a specified initial value, enhancing efficiency by reducing processing time and resource use.


#### How to configure
1. **Choose a Cursor Column**: Identify a column in your SQL table that can serve as a reliable indicator of new or updated rows. Common choices include timestamp columns or auto-incrementing IDs.
1. **Set an Initial Value**: Choose a starting value for the cursor to begin loading data. This could be a specific timestamp or ID from which you wish to start loading data.
1. **Deduplication**: When using incremental loading, the system automatically handles the deduplication of rows based on the primary key (if available) or row hash for tables without a primary key.
1. **Set end_value for backfill**: Set `end_value` if you want to backfill data from
certain range.
1. **Order returned rows**. Set `row_order` to `asc` or `desc` to order returned rows.

#### Examples

1. Incremental loading with the resource `sql_table`  
    Conside a table "family" with a timestamp column "last_modified" that indicates when a row was last modified. To ensure that only rows modified after midnight (00:00:00) on January 1, 2024, are loaded, you would set "last_modified" timestamp as the cursor as follows:
   ```py
   from sql_database import sql_table
   from datetime import datetime

   # Example: Incrementally loading a table based on a timestamp column
   table = sql_table(
       table='family',
       incremental=dlt.sources.incremental(
           'last_modified',  # Cursor column name
           initial_value=pendulum.DateTime(2024, 1, 1, 0, 0, 0)  # Initial cursor value
       )
   )

   info = pipeline.extract(table, write_disposition="merge")
   print(info)
   ```
    Behind the scene, the loader generates a SQL query filtering rows with `last_modified` values greater than the incremental value. In the first run, this is the initial value (midnight (00:00:00) January 1, 2024). 
    In subsequent runs, it is the latest value of "last_modified" that `dlt` stores in [state](https://dlthub.com/docs/general-usage/state).

2. Incremental loading with the source `sql_database`  
    To achieve the same using the `sql_database` source, you would specify your cursor as follows:

    ```py
    source = sql_database().with_resources("family")
    #using the "last_modified" field as an incremental field using initial value of midnight January 1, 2024
    source.family.apply_hints(incremental=dlt.sources.incremental("updated"),initial_value=pendulum.DateTime(2024, 1, 1, 0, 0, 0))
    #running the pipeline
    info = pipeline.run(source, write_disposition="merge")
    print(info)
    ```  

   :::info
   * For merge write disposition, the source table needs a primary key, which `dlt` automatically sets up. 
   * `apply_hints` is a powerful method that enables schema modifications after resource creation, like adjusting write disposition and primary keys. You can choose from various tables and use `apply_hints` multiple times to create pipelines with merged, appended, or replaced resources.
   :::

### Parallelize extraction

You can extract each table in a separate thread (no multiprocessing at this point). This will decrease loading time if your queries take time to execute or your network latency/speed is low. To enable this, declare your sources/resources as follows:
```py
database = sql_database().parallelize()
table = sql_table().parallelize()
```

### Column reflection
Columns and their data types are reflected with SQLAlchemy. The SQL types are then mapped to `dlt` types.
Most types are supported.

The `reflection_level` argument controls how much information is reflected:

- `reflection_level = "minimal"`: Only column names and nullability are detected. Data types are inferred from the data.
- `reflection_level = "full"`: Column names, nullability, and data types are detected. For decimal types we always add precision and scale. **This is the default.**
- `reflection_level = "full_with_precision"`: Column names, nullability, data types, and precision/scale are detected, also for types like text and binary. Integer sizes are set to bigint and to int for all other types.

If the SQL type is unknown or not supported by `dlt`, then, in the pyarrow backend, the column will be skipped, whereas in the other backends the type will be inferred directly from the data irrespective of the `reflection_level` specified. In the latter case, this often means that some types are coerced to strings and  `dataclass` based values from sqlalchemy are inferred as `complex` (JSON in most destinations).  
:::tip
If you use reflection level **full** / **full_with_precision** you may encounter a situation where the data returned by sqlalchemy or pyarrow backend does not match the reflected data types. Most common symptoms are:
1. The destination complains that it cannot cast one type to another for a certain column. For example `connector-x` returns TIME in nanoseconds
and BigQuery sees it as bigint and fails to load.
2. You get `SchemaCorruptedException` or other coercion error during the `normalize` step.
In that case you may try **minimal** reflection level where all data types are inferred from the returned data. From our experience this prevents
most of the coercion problems.
:::

You can also override the sql type by passing a `type_adapter_callback` function. This function takes a `sqlalchemy` data type as input and returns a new type (or `None` to force the column to be inferred from the data) as output.

This is useful, for example, when:
- You're loading a data type which is not supported by the destination (e.g. you need JSON type columns to be coerced to string)
- You're using a sqlalchemy dialect which uses custom types that don't inherit from standard sqlalchemy types.
- For certain types you prefer `dlt` to infer data type from the data and you return `None`

Example, when loading timestamps from Snowflake, you ensure that they get translated into standard sqlalchemy `timestamp` columns in the resultant schema:

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

### Configuring with toml/environment variables
You can set most of the arguments of `sql_database()` and `sql_table()` directly in the `.toml` files and/or as environment variables. `dlt` automatically injects these values into the pipeline script.

This is particularly useful with `sql_table()` because you can maintain a separate configuration for each table (below we show **secrets.toml** and **config.toml**, you are free to combine them into one):

The examples below show how you can set arguments in any of the `.toml` files (`secrets.toml` or `config.toml`):
1. Specifying connection string:
    ```toml
    [sources.sql_database]
    credentials="mssql+pyodbc://loader.database.windows.net/dlt_data?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
    ```
2. Setting parameters like backend, chunk_size, and incremental column for the table `chat_message`:
    ```toml
    [sources.sql_database.chat_message]
    backend="pandas"
    chunk_size=1000

    [sources.sql_database.chat_message.incremental]
    cursor_path="updated_at"
    ```
    This is especially useful with `sql_table()` in a situation where you may want to run this resource for multiple tables. Setting parameters like this would then give you a clean way of maintaing separate configurations for each table.  

3. Handling separate configurations for database and individual tables  
    When using the `sql_database()` source, you can separately configure the parameters for the database and for the individual tables.
    ```toml
    [sources.sql_database]
    credentials="mssql+pyodbc://loader.database.windows.net/dlt_data?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
    schema="data"
    backend="pandas"
    chunk_size=1000

    [sources.sql_database.chat_message.incremental]
    cursor_path="updated_at"
    ```    

    The resulting source created below will extract data using **pandas** backend with **chunk_size** 1000. The table **chat_message** will load data incrementally using **updated_at** column. All the other tables will not use incremental loading, and will instead load the full data.

    ```py
    database = sql_database()
    ```

You'll be able to configure all the arguments this way (except adapter callback function). [Standard dlt rules apply](https://dlthub.com/docs/general-usage/credentials/configuration#configure-dlt-sources-and-resources).  
  
It is also possible to set these arguments as environment variables [using the proper naming convention](https://dlthub.com/docs/general-usage/credentials/config_providers#toml-vs-environment-variables):
```sh
SOURCES__SQL_DATABASE__CREDENTIALS="mssql+pyodbc://loader.database.windows.net/dlt_data?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
SOURCES__SQL_DATABASE__BACKEND=pandas
SOURCES__SQL_DATABASE__CHUNK_SIZE=1000
SOURCES__SQL_DATABASE__CHAT_MESSAGE__INCREMENTAL__CURSOR_PATH=updated_at
```

## Extended Usage

### Running on Airflow
When running on Airflow:
1. Use the `dlt` [Airflow Helper](../../walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer.md#2-modify-dag-file) to create tasks from the `sql_database` source. You should be able to run table extraction in parallel with `parallel-isolated` source->DAG conversion.
2. Reflect tables at runtime with `defer_table_reflect` argument.
3. Set `allow_external_schedulers` to load data using [Airflow intervals](../../general-usage/incremental-loading.md#using-airflow-schedule-for-backfill-and-incremental-loading).

### Transforming the data before load
You have direct access to the extracted data through the resource objects (`sql_table()` or `sql_database().with_resource())`), each of which represents a single SQL table. These objects are generators that yield 
individual rows of the table which can be modified by using custom python functions. These functions can be applied to the resource using `add_map`.  
  

Examples:
1. Pseudonymizing data to hide personally identifiable information (PII) before loading it to the destination. (See [here](https://dlthub.com/docs/general-usage/customising-pipelines/pseudonymizing_columns) for more information on pseudonymizing data with `dlt`)

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

2. Excluding unnecessary columns before load

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

<!--@@@DLT_TUBA sql_database-->
