# SQL Database

:::info Need help deploying these sources, or figuring out how to run them in your data stack?

[Join our Slack community](https://dlthub-community.slack.com/join/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g)
or [book a call](https://calendar.app.google/kiLhuMsWKpZUpfho6) with our support engineer Adrian.
:::

SQL databases are management systems (DBMS) that store data in a structured format, commonly used
for efficient and reliable data retrieval.

This SQL database `dlt` verified source and
[pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/sql_database_pipeline.py)
loads data using SqlAlchemy to the destination of your choice.

Sources and resources that can be loaded using this verified source are:

| Name         | Description                               |
| ------------ | ----------------------------------------- |
| sql_database | Retrieves data from an SQL database       |
| sql_table    | Retrieves data from an SQL database table |

## Setup Guide

### Grab credentials

This verified source utilizes SQLAlchemy for database connectivity. Let us consider this public
database example:

`connection_url = "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"`

> This public database doesn't require a password.

Connection URL can be broken down into:

```python
connection_url = "connection_string = f"{drivername}://{username}:{password}@{host}:{port}/{database}"
```

`drivername`: Indicates both the database system and driver used.

- E.g., "mysql+pymysql" uses MySQL with the pymysql driver. Alternatives might include mysqldb and
  mysqlclient.

`username`: Used for database authentication.

- E.g., "rfamro" as a possible read-only user.

`password`: The password for the given username.

`host`: The server's address or domain where the database is hosted.

- E.g., A public database at "mysql-rfam-public.ebi.ac.uk" hosted by EBI.

`port`: The port for the database connection. E.g., "4497", in the above connection URL.

`database`: The specific database on the server.

- E.g., Connecting to the "Rfam" database.

### Provide special options in connection string

Here we use `mysql` and `pymysql` dialect to set up SSL connection to a server. All information
taken from the
[SQLAlchemy docs](https://docs.sqlalchemy.org/en/14/dialects/mysql.html#ssl-connections)

1. To force SSL on the client without a client certificate you may pass the following DSN:

   ```toml
   sources.sql_database.credentials="mysql+pymysql://root:<pass>@<host>:3306/mysql?ssl_ca="
   ```

1. You can also pass server public certificate as a file. For servers with a public certificate
   (potentially bundled with your pipeline) and disabling host name checks:

   ```toml
   sources.sql_database.credentials="mysql+pymysql://root:<pass>@<host>:3306/mysql?ssl_ca=server-ca.pem&ssl_check_hostname=false"
   ```

1. For servers requiring a client certificate, provide the client's private key (a secret value). In
   Airflow, this is usually saved as a variable and exported to a file before use. Server cert is
   omitted in the example below:

   ```toml
   sources.sql_database.credentials="mysql+pymysql://root:8P5gyDPNo9zo582rQG6a@35.203.96.191:3306/mysql?ssl_ca=&ssl_cert=client-cert.pem&ssl_key=client-key.pem"
   ```

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```bash
   dlt init sql_database duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/sql_database_pipeline.py)
   with SQL database as the [source](../../general-usage/source) and
   [duckdb](../destinations/duckdb.md) as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the
[Walkthrough: Add a verified source.](../../walkthroughs/add-a-verified-source)

### Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe.

   Here's what the `secrets.toml` looks like

   You can pass the credentials

   ```toml
   [sources.sql_database.credentials]
   drivername = "please set me up!" # driver name for the database
   database = "please set me up!" # database name
   username = "please set me up!" # username associated with the database
   host = "please set me up!" # host address
   port = "please set me up!" # port required for connection
   ```

1. Alternatively, you can also provide credentials in "secrets.toml" as:

   ```toml
   sources.sql_database.credentials="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
   ```

1. Alternatively, you can also pass credentials in the pipeline script like this:

   ```python
   credentials = ConnectionStringCredentials(
       "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
   )
   ```

   > See
   > [pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/sql_database_pipeline.py)
   > for details.

1. Finally, follow the instructions in [Destinations](../destinations/) to add credentials for your
   chosen destination. This will ensure that your data is properly routed to its final destination.

## Run the pipeline example

1. Install the necessary dependencies by running the following command:

   ```bash
   pip install -r requirements.txt
   ```

1. Now the verified source can be run by using the command:

   ```bash
   python3 sql_database_pipeline.py
   ```

1. To make sure that everything is loaded as expected, use the command:

   ```bash
   dlt pipeline <pipeline_name> show
   ```

   For example, the pipeline_name for the above pipeline example is `rfam`, you may also use any
   custom name instead)

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Source `sql_database`:

This function loads data from an SQL database via SQLAlchemy and auto-creates resources for each
table or from a specified list.

```python
@dlt.source
def sql_database(
    credentials: Union[ConnectionStringCredentials, Engine, str] = dlt.secrets.value,
    schema: Optional[str] = dlt.config.value,
    metadata: Optional[MetaData] = None,
    table_names: Optional[List[str]] = dlt.config.value,
) -> Iterable[DltResource]:
```

`credentials`: Database details or an 'sqlalchemy.Engine' instance.

`schema`: Database schema name (default if unspecified).

`metadata`: Optional, sqlalchemy.MetaData takes precedence over schema.

`table_names`: List of tables to load. Defaults to all if not provided.

### Resource `sql_table`

This function loads data from specific database tables.

```python
@dlt.common.configuration.with_config(
    sections=("sources", "sql_database"), spec=SqlTableResourceConfiguration
)
def sql_table(
    credentials: Union[ConnectionStringCredentials, Engine, str] = dlt.secrets.value,
    table: str = dlt.config.value,
    schema: Optional[str] = dlt.config.value,
    metadata: Optional[MetaData] = None,
    incremental: Optional[dlt.sources.incremental[Any]] = None,
) -> DltResource:
```

`credentials`: Database info or an Engine instance.

`table`: Table to load, set in code or default from "config.toml".

`schema`: Optional, name of table schema.

`metadata`: Optional, sqlalchemy.MetaData takes precedence over schema.

`incremental`: Optional, enables incremental loading.

`write_disposition`: Can be "merge", "replace", or "append".

### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this
verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```python
   pipeline = dlt.pipeline(
        pipeline_name="rfam",  # Use a custom name if desired
        destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
        dataset_name="rfam_data"  # Use a custom name if desired
   )
   ```

1. You can pass credentials using any of the methods discussed above.

1. To load the entire database, use the `sql_database` source as:

   ```python
   source = sql_database()
   info = pipeline.run(source, write_disposition="replace")
   print(info)
   ```

   > Use one method from the methods [described above](#add-credentials) to pass credentials.

1. To load just the "family" table using the `sql_database` source:

   ```python
   source = sql_database().with_resources("family")
   #running the pipeline
   info = pipeline.run(source, write_disposition="replace")
   print(info)
   ```

1. To pseudonymize columns and hide personally identifiable information (PII), refer to the
   [documentation](https://dlthub.com/docs/general-usage/customising-pipelines/pseudonymizing_columns).
   For example, to pseudonymize the "rfam_acc" column in the "family" table:

   ```python
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

   def load_table_with_pseudonymized_columns():

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

1. To exclude the columns, for eg. "rfam_id" column from the "family" table before loading:

   ```python
   def remove_columns(doc):
       del doc["rfam_id"]
       return doc

   def load_table_with_deleted_columns():
       
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

1. To incrementally load the "family" table using the sql_database source method:

   ```python
   source = sql_database().with_resources("family")
   #using the "updated" field as an incremental field using initial value of January 1, 2022, at midnight
   source.family.apply_hints(incremental=dlt.sources.incremental("updated"),initial_value=pendulum.DateTime(2022, 1, 1, 0, 0, 0))
   #running the pipeline
   info = pipeline.run(source, write_disposition="merge")
   print(info)
   ```

   > In this example, we load the "family" table and set the "updated" column for incremental
   > loading. In first run it loads all the data from January 1, 2022, at midnight (00:00:00) and
   > then loads incrementally in subsequent runs using "updated" field.

1. To incrementally load the "family" table using the 'sql_table' resource.

   ```python
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

   > Loads all data from "family" table from January 1, 2022, at midnight (00:00:00) and then loads
   > incrementally in subsequent runs using "updated" field.

   > 💡 Please note that to use merge write disposition a primary key must exist in the source table.
   > `dlt` finds and sets up primary keys automatically.

   > 💡 `apply_hints` is a powerful method that allows to modify the schema of the resource after it
   > was created: including the write disposition and primary keys. You are free to select many
   > different tables and use `apply_hints` several times to have pipelines where some resources are
   > merged, appended or replaced.

1. Remember, to maintain the same pipeline name and destination dataset name. The pipeline name
   retrieves the [state](https://dlthub.com/docs/general-usage/state) from the last run, essential
   for incremental data loading. Changing these names might trigger a
   [“full_refresh”](https://dlthub.com/docs/general-usage/pipeline#do-experiments-with-full-refresh),
   disrupting metadata tracking for
   [incremental loads](https://dlthub.com/docs/general-usage/incremental-loading).
