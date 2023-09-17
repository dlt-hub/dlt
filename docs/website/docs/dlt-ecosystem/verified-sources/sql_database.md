# SQL Database

:::info
Need help deploying these sources, or figuring out how to run them in your data stack?

[Join our slack community](https://dlthub-community.slack.com/join/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g) or [book a call](https://calendar.app.google/kiLhuMsWKpZUpfho6) with our support engineer Adrian.
:::


SQL databases, or Structured Query Language databases, are a type of database management system (DBMS) that stores and manages data in a structured format. SQL databases are widely used for storing and retrieving structured data efficiently and reliably.

The SQL Database `dlt` verified source loads data from your SQL Database to a destination of your choosing. It offers flexibility in terms of loading either the entire database or specific tables to the destination. You have the option to perform a single load using the "replace" mode, or load data incrementally using the "merge" or "append" modes.

Internally we use `SqlAlchemy` to query the data. You may need to pip install the right dialect for your database. `dlt` will inform you on the missing dialect on the first run of the pipeline.

`dlt` understands the SqlAlchemy connection strings. Special options that are passed via query strings are supported. We give a few examples below.

## Initialize the SQL Database verified source and the pipeline example

To get started with this verified source, follow these steps:

1. Open up your terminal or command prompt and navigate to the directory where you'd like to create your project.
2. Enter the following command:

```properties
dlt init sql_database duckdb
```

This command will initialize your verified source with SQL Database and creates pipeline example with duckdb as the destination. If you'd like to use a different destination, simply replace **duckdb** with the name of your preferred destination. You can find supported destinations and their configuration options in our [documentation](../destinations/)

3. After running this command, a new directory will be created with the necessary files and configuration settings to get started.

```
sql_database
├── .dlt
│   ├── config.toml
│   └── secrets.toml
├── sql_database
│   └── __init__.py
│   └── helpers.py
│   └── settings.py
├── .gitignore
├── requirements.txt
└── sql_database_pipeline.py
```

## Add credentials

1. Inside the **`.dlt`** folder, you'll find a file called **`secrets.toml`**, which is where you can securely store credentials and other sensitive information. It's important to handle this file with care and keep it safe.
2. To proceed with this demo, we will establish credentials using the provided connection URL given below. The connection URL is associated with a public database and is: `mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam`

Here's what the `secrets.toml` looks like

```toml
# make sure this is at the top of toml file, not in any section like [sources]
sources.sql_database.credentials="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
```

You can set up the credentials by passing host, password etc. separately
```toml
[sources.sql_database.credentials]
drivername = "mysql+pymysql" # driver name for the database
database = "Rfam" # database name
username = "rfamro" # username associated with the database
host = "mysql-rfam-public.ebi.ac.uk" # host address
port = "4497" # port required for connection
```

4. Finally, follow the instructions in **[Destinations](../destinations/)** to add credentials for your chosen destination. This will ensure that your data is properly routed to its final destination.

## Run the pipeline example

1. Install the necessary dependencies by running the following command:

```properties
pip install -r requirements.txt
```

2. Now the verified source can be run by using the command:
```properties
python3 sql_database_pipeline.py
```

3. To make sure that everything is loaded as expected, use the command:

```properties
dlt pipeline <pipeline_name> show
```

(For example, the pipeline_name for the above pipeline example is `rfam`, you may also use any custom name instead)

## Customizations
This source let's you build your own pipeline where you pick the whole database or selected tables to be synchronized with the destination. The source and resource functions allow to set up write disposition (append or replace data) or incremental/merge loads.

The demo script that you get with `dlt init` contains several well commented examples that we explain further below.

### Include and configure the tables to be synchronized

**Source sql_database**:
```python
from sql_database import sql_database
```

This `sql_database` source uses SQLAlchemy to reflect the whole source database and create dlt resource for each table. You can also provide and explicit list of tables to be reflected.


**Resource sql_table:**

```python
from sql_database import sql_table
```
`sql_table` resource will reflect a single table with provided name.

### Usage examples

1. Declare the pipeline by specifying the pipeline name, destination, and dataset. To read more about pipeline configuration, please refer to our documentation [here](https://dlthub.com/docs/general-usage/pipeline).

```python
pipeline = dlt.pipeline(
     pipeline_name="rfam", destination='duckdb', dataset_name="rfam"
)
```

2. To load the entire database, you can use the sql_database source function as follows:

```python
load_source = sql_database()
load_info = pipeline.run(load_source, write_disposition="replace")
print(info)
```

3. To load data from the "family" table in incremental mode, utilizing the "updated" field, you can employ the `sql_table` resource. In the provided code, the last value of the "updated" field is stored in the dlt state, initially set to January 1, 2022, at midnight (00:00:00). During subsequent runs, only the new data created after the last recorded "updated" value will be loaded. This ensures efficient and targeted data retrieval, optimizing the processing of your pipeline.

```python
family = sql_table(
        table="family",
        incremental=dlt.sources.incremental(
            "updated", initial_value=pendulum.DateTime(2022, 1, 1, 0, 0, 0)
        ),
)
#running the pipeline
load_info = pipeline.extract(family, write_disposition="merge")
print(info)
```

In the provided code, the "_merge_" mode write deposition is utilized. This mode ensures that only unique rows are added to the destination. Had the "_append_" mode been used instead, all rows, regardless of their uniqueness, would have been added to the destination after the last "updated" field. By employing the merge mode, data integrity is maintained, and only distinct records are included.

> 💡 Please note that to use merge write disposition a primary key must exist in the source table. `dlt` finds and sets up primary keys automatically.


4. You can also load data from the “family”  table in incremental mode, using the "sql_database" source method:

```python
load_data = sql_database().with_resources("family")
#using the "updated" field as an incremental field using initial value of January 1, 2022, at midnight
load_data.family.apply_hints(incremental=dlt.sources.incremental("updated"),initial_value=pendulum.DateTime(2022, 1, 1, 0, 0, 0))

#running the pipeline
load_info = pipeline.run(load_data, write_disposition="merge")
print(load_info)
```

In the example above first we reflect the whole source database and then select only the **family** table to be loaded (`with_resources()`). Then we use `apply_hints` method to set up incremental load on **updated** column.

> 💡 `apply_hints` is a powerful method that allows to modify the schema of the resource after it was created: including the write disposition and primary keys. You are free to select many different tables and use `apply_hints` several times to have pipelines where some resources are merged, appended or replaced.

5. It's important to keep the pipeline name and destination dataset name unchanged. The pipeline name is crucial for retrieving the [state](https://dlthub.com/docs/general-usage/state) of the last pipeline run, which includes the end date needed for loading data incrementally. Modifying these names can lead to [“full_refresh”](https://dlthub.com/docs/general-usage/pipeline#do-experiments-with-full-refresh) which will disrupt the tracking of relevant metadata(state) for [incremental data loading](https://dlthub.com/docs/general-usage/incremental-loading).

### Provide special options in connection string
Here we use `mysql` and `pymysql` dialect to set up ssl connection to a server. All information taken from the [SQLAlchemy docs](https://docs.sqlalchemy.org/en/14/dialects/mysql.html#ssl-connections)

1. If your server accepts clients without client certificate but exclusively over ssl you may try to force ssl on the client by passing the following DSN to `dlt`: `mysql+pymysql://root:<pass>@<host>:3306/mysql?ssl_ca=`
2. You can also pass server public certificate as a file. Possibly bundled with your pipeline as this is not any secret. Below we also disable the host name check on the client: most certificates are self-issued and host name typically does not match: `mysql+pymysql://root:<pass>@<host>:3306/mysql?ssl_ca=server-ca.pem&ssl_check_hostname=false`
3. If your server requires client certificate, you must pass the private key of the client which is a secret value. In Airflow we typically paste it into a variable and then dump it to file before use. We do not provide server cert below.
```toml
sources.sql_database.credentials="'mysql+pymysql://root:8P5gyDPNo9zo582rQG6a@35.203.96.191:3306/mysql?ssl_ca=&ssl_cert=client-cert.pem&ssl_key=client-key.pem')
```

That's it! Enjoy running your SQL Database dlt pipeline!
