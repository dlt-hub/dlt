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

Our SQL Database verified source loads data to your specified destination using SQLAlchemy.

:::tip
View the pipeline example [here](https://github.com/dlt-hub/verified-sources/blob/master/sources/sql_database_pipeline.py).
:::

Sources and resources that can be loaded using this verified source are:

| Name         | Description                               |
| ------------ | ----------------------------------------- |
| sql_database | Retrieves data from an SQL database       |
| sql_table    | Retrieves data from an SQL database table |

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

### Grab credentials

This verified source utilizes SQLAlchemy for database connectivity. Let's take a look at the following public database example:

`connection_url = "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"`

The database above doesn't require a password.

The connection URL can be broken down into:

```py
connection_url = connection_string = f"{drivername}://{username}:{password}@{host}:{port}{database}"
```

`drivername`: Indicates both the database system and driver used.

- E.g., "mysql+pymysql" uses MySQL with the pymysql driver. Alternatives might include mysqldb and
  mysqlclient.

`username`: Used for database authentication.

- E.g., "rfamro" as a possible read-only user.

`password`: The password for the given username.

`host`: The server's address or domain where the database is hosted.

- E.g., A public database at "mysql-rfam-public.ebi.ac.uk" hosted by EBI.

`port`: The port for the database connection.

- E.g., "4497", in the above connection URL.
`port`: The port for the database connection.

- E.g., "4497", in the above connection URL.

`database`: The specific database on the server.

- E.g., Connecting to the "Rfam" database.

### Configure connection

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

### Initialize the verified source

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

### Add credentials

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
   sources.sql_database.credentials="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
   ```

1. You can also pass credentials in the pipeline script the following way:

   ```py
   credentials = ConnectionStringCredentials(
       "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
   )
   ```

   > See
   > [pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/sql_database_pipeline.py)
   > for details.

1. Finally, follow the instructions in [Destinations](../destinations/) to add credentials for your chosen destination. This will ensure that your data is properly routed.

For more information, read the [General Usage: Credentials.](../../general-usage/credentials)

## Run the pipeline

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


## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Source `sql_database`:

This function loads data from an SQL database via SQLAlchemy and auto-creates resources for each
table or from a specified list of tables.

```py
@dlt.source
def sql_database(
    credentials: Union[ConnectionStringCredentials, Engine, str] = dlt.secrets.value,
    schema: Optional[str] = dlt.config.value,
    metadata: Optional[MetaData] = None,
    table_names: Optional[List[str]] = dlt.config.value,
    chunk_size: int = 1000,
    detect_precision_hints: Optional[bool] = dlt.config.value,
    defer_table_reflect: Optional[bool] = dlt.config.value,
    table_adapter_callback: Callable[[Table], None] = None,
) -> Iterable[DltResource]:
   ...
```

`credentials`: Database details or an 'sqlalchemy.Engine' instance.

`schema`: Database schema name (default if unspecified).

`metadata`: Optional SQLAlchemy.MetaData; takes precedence over schema.

`table_names`: List of tables to load; defaults to all if not provided.

`chunk_size`: Number of records in a batch. Internally SqlAlchemy maintains a buffer twice that size

`detect_precision_hints`: Infers full schema for columns including data type, precision and scale

`defer_table_reflect`: Will connect to the source database and reflect the tables
only at runtime. Use when running on Airflow

`table_adapter_callback`: A callback with SQLAlchemy `Table` where you can, for example,
remove certain columns to be selected.

### Resource `sql_table`

This function loads data from specific database tables.

```py
@dlt.common.configuration.with_config(
    sections=("sources", "sql_database"), spec=SqlTableResourceConfiguration
)
def sql_table(
    credentials: Union[ConnectionStringCredentials, Engine, str] = dlt.secrets.value,
    table: str = dlt.config.value,
    schema: Optional[str] = dlt.config.value,
    metadata: Optional[MetaData] = None,
    incremental: Optional[dlt.sources.incremental[Any]] = None,
    chunk_size: int = 1000,
    detect_precision_hints: Optional[bool] = dlt.config.value,
    defer_table_reflect: Optional[bool] = dlt.config.value,
    table_adapter_callback: Callable[[Table], None] = None,
) -> DltResource:
   ...
```
`incremental`: Optional, enables incremental loading.

`write_disposition`: Can be "merge", "replace", or "append".

for other arguments, see `sql_database` source above.

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
   * `apply_hints` is a powerful method that enables schema modifications after resource creation, like adjusting write disposition and primary keys. You can choose from various tables and use `apply_hints` multiple times to create pipelines with merged, appendend, or replaced resources.
   :::

### Run on Airflow
When running on Airflow
1. Use `dlt` [Airflow Helper](../../walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer.md#2-modify-dag-file) to create tasks from `sql_database` source. You should be able to run table extraction in parallel with `parallel-isolated` source->DAG conversion.
2. Reflect tables at runtime with `defer_table_reflect` argument.
3. Set `allow_external_schedulers` to load data using [Airflow intervals](../../general-usage/incremental-loading.md#using-airflow-schedule-for-backfill-and-incremental-loading).

### Parallel extraction
You can extract each table in a separate thread (no multiprocessing at this point). This will decrease loading time if your queries take time to execute or your network latency/speed is low.
```py
database = sql_database().parallelize()
table = sql_table().parallelize()
```


### Troubleshooting
If you encounter issues where the expected WHERE clause for incremental loading is not generated, ensure your configuration aligns with the `sql_table` resource rather than applying hints post-resource creation. This ensures the loader generates the correct query for incremental loading.

## Customization
### Create your own pipeline

To create your own pipeline, use source and resource methods from this verified source.

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

1. Remember to keep the pipeline name and destination dataset name consistent. The pipeline name is crucial for retrieving the [state](https://dlthub.com/docs/general-usage/state) from the last run, which is essential for incremental loading. Altering these names could initiate a "[full_refresh](https://dlthub.com/docs/general-usage/pipeline#do-experiments-with-full-refresh)", interfering with the metadata tracking necessary for [incremental loads](https://dlthub.com/docs/general-usage/incremental-loading).

<!--@@@DLT_TUBA sql_database-->
