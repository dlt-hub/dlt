# SQL Database

SQL databases, or Structured Query Language databases, are a type of database management system (DBMS) that stores and manages data in a structured format. SQL databases are widely used for storing and retrieving structured data efficiently and reliably.

The SQL Database `dlt` verified source and pipeline example that facilitates the loading of data from your SQL Database to a destination of your choosing. It offers flexibility in terms of loading either the entire database or specific tables to the destination. You have the option to perform a single load using the "replace" mode, or load data incrementally using the "merge" or "append" modes.

## Initialize the SQL Database verified source and the pipeline example

To get started with this verified source, follow these steps:

1. Open up your terminal or command prompt and navigate to the directory where you'd like to create your project.
2. Enter the following command:

```properties
dlt init sql_database bigquery
```

This command will initialize your verified source with SQL Database and creates pipeline example with BigQuery as the destination. If you'd like to use a different destination, simply replace **`bigquery`** with the name of your preferred destination. You can find supported destinations and their configuration options in our [documentation](https://dlthub.com/docs/destinations/duckdb)

3. After running this command, a new directory will be created with the necessary files and configuration settings to get started.

```
sql_database_source
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

## **Add credential**

1. Inside the **`.dlt`** folder, you'll find a file called **`secrets.toml`**, which is where you can securely store credentials and other sensitive information. It's important to handle this file with care and keep it safe.
2. To proceed with this demo, we will establish credentials using the provided connection URL given below. The connection URL is associated with a public database and is as follows:

    `connection_url = "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"`

   Here's what the `secrets.toml` looks like

  ```toml
  # Put your secret values and credentials here. do not share this file and do not push it to github
  # We will set up creds with the connection URL given below, which is a public database

  # The credentials are as:
  drivername = "mysql+pymysql" # driver name for the database
  database = "Rfam" # database name
  username = "rfamro" # username associated with the database
  host = "mysql-rfam-public.ebi.ac.uk" # host address
  port = "4497" # port required for connection

  [destination.bigquery.credentials]
  project_id = "project_id" # GCP project ID
  private_key = "private_key" # Unique private key (including `BEGIN and END PRIVATE KEY`)
  client_email = "client_email" # Service account email
  location = "US" # Project location (e.g. “US”)
  ```

4. Finally, follow the instructions in **[Destinations](https://dlthub.com/docs/destinations/duckdb)** to add credentials for your chosen destination. This will ensure that your data is properly routed to its final destination.

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

To load data to the destination using this verified source, you have the option to write your own methods.

### **Source and resource methods**

`dlt` works on the principle of [sources](https://dlthub.com/docs/general-usage/source) and [resources](https://dlthub.com/docs/general-usage/resource) that for this verified source are found in the `__init__.py` file within the *sql_database* directory. This SQL Database verified source has the following default methods that form the basis of loading. The methods are:

**Source sql_database**:

```python
@dlt.source
def sql_database(
    credentials: Union[ConnectionStringCredentials, Engine, str] = dlt.secrets.value,
    schema: Optional[str] = dlt.config.value,
    metadata: Optional[MetaData] = None,
    table_names: Optional[List[str]] = dlt.config.value,
) -> Iterable[DltResource]:
```

- **`credentials`:** This parameter represents the credentials needed to establish a connection with the SQL database. Database credentials or an Engine instance representing the database connection.
- **`schema`:** This optional parameter specifies the schema to be used within the SQL database. If not provided, the value from _dlt.config.value_ will be used, indicating that the schema can be configured externally.
- **`metadata`:** This optional parameter allows passing a metadata object that provides information about the database's structure, such as table names, column names, and relationships. If not provided, it defaults to “None”.
- **`table_names`:** This optional parameter specifies a list of table names to be used as the data source. If not provided, the value from _dlt.config.value_ will be used, suggesting that the list of table names can be configured externally.

This function utilizes SQLAlchemy to load data from an SQL database. It automatically generates resources for each table within the specified schema or based on a provided list of tables.

**Resource sql_table:**

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

- **`@dlt.common.configuration.with_config`**: This decorator indicates that the function will be using a configuration from the specified sections ("sources" and "sql_database") and will be using the _SqlTableResourceConfiguration_ specification for validation.
- **`credentials`**: This parameter represents the credentials required to establish a connection with the SQL database. It can be either database credentials or an Engine instance.
- **`table`**: This parameter specifies the name of the SQL table to be used as the data source. The value can be configured, using _dlt.config.value_.
- **`schema`**: This optional parameter specifies the schema of the SQL table. If not provided, the value from _dlt.config.value_ will be used.
- **`metadata`**: This optional parameter allows passing a metadata object that provides information about the database's structure, such as table names, column names, and relationships. If not provided, it defaults to “None”.
- **`incremental`**: This optional parameter is related to the incremental loading of data and is of type _dlt.sources.incremental[Any]_. It allows specifying an incremental loading for the data source, such as keeping track of the last processed record. If not provided, it defaults to “None”.

The provided code defines a `dlt` resource that utilizes SQLAlchemy to extract data from an SQL database table.  The above code allows incremental loading using any parameter that is passed. By preserving the most recent parameter value from the previous pipeline run, in the next pipeline run, it fetches the latest values from the database that were created after the previous pipeline run. This ensures a seamless progression.

### **Create Your Data Loading Pipeline using SQL Database verified source**

To create your data pipeline using single loading and [incremental data loading](https://dlthub.com/docs/general-usage/incremental-loading), follow these steps:

1. Configure the pipeline by specifying the pipeline name, destination, and dataset. To read more about pipeline configuration, please refer to our documentation [here](https://dlthub.com/docs/general-usage/pipeline).

```python
pipeline = dlt.pipeline(
     pipeline_name="rfam", destination='bigquery', dataset_name="rfam"
)
```

2. To load the entire database, you can use the sql_database source function as follows:

```python
load_source = sql_database()
load_info = pipeline.run(load_source, write_disposition="replace")
print(info)
```

3. To load data from the "family" table in incremental mode, utilizing the "updated" field, you can employ the `sql_table` resource. In the provided code, the last value of the "updated" field is stored in the DLT state, initially set to January 1, 2022, at midnight (00:00:00). During subsequent runs, only the new data created after the last recorded "updated" value will be loaded. This ensures efficient and targeted data retrieval, optimizing the processing of your pipeline.

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
>
In the provided code, the "_merge_" mode write deposition is utilized. This mode ensures that only unique rows are added to the destination. Had the "_append_" mode been used instead, all rows, regardless of their uniqueness, would have been added to the destination after the last "updated" field. By employing the merge mode, data integrity is maintained, and only distinct records are included.
>

4. You can also achieve loading data from the “family”  table in incremental mode, using the "sql_database"  source method, as follows

```python
load_data = sql_database().with_resources("family")
#using the "updated" field as an incremental field using initial value of January 1, 2022, at midnight
load_data.family.apply_hints(incremental=dlt.sources.incremental("updated"),initial_value=pendulum.DateTime(2022, 1, 1, 0, 0, 0))

#running the pipeline
load_info = pipeline.run(load_data, write_disposition="merge")
print(load_info)
```
>
The method depicted above loads data using the "_sql_database_" source using the “_with_resources_” function. It uses the "updated" field for incremental loading, ensuring that only newly updated data is retrieved. Moreover, the method utilizes the "_merge_" mode to load the new data into the destination everytime pipeline runs.
>

5. It's important to keep the pipeline name and destination dataset name unchanged. The pipeline name is crucial for retrieving the [state](https://dlthub.com/docs/general-usage/state) of the last pipeline run, which includes the end date needed for loading data incrementally. Modifying these names can lead to [“full_refresh”](https://dlthub.com/docs/general-usage/pipeline#do-experiments-with-full-refresh) which will disrupt the tracking of relevant metadata(state) for [incremental data loading](https://dlthub.com/docs/general-usage/incremental-loading).

That's it! Enjoy running your SQL Database DLT pipeline!
