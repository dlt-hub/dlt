---
title: Destination
description: Declare and configure destinations to which to load data
keywords: [destination, load data, configure destination, name destination]
---

# Destination

[Destination](glossary.md#destination) is a location in which `dlt` creates and maintains the current version of the schema and loads your data. Destinations come in various forms: databases, datalakes, vector stores, or files. `dlt` deals with this variety via destination type modules which you declare when creating a pipeline.

We maintain a set of [built-in destinations](../dlt-ecosystem/destinations/) that you can use right away.

## Declare the destination type
We recommend that you declare the destination type when creating a pipeline instance with `dlt.pipeline`. This allows the `run` method to synchronize your local pipeline state with the destination and `extract` and `normalize` to create compatible load packages and schemas. You can also pass the destination to the `run` and `load` methods.

* Use destination **shorthand type**
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::shorthand-->

Above, we want to use the **filesystem** built-in destination. You can use shorthand types only for built-ins.

* Use a [**named destination**](#use-named-destinations) with a configured type
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::custom_destination_name-->

Above, we use a custom destination name and configure the destination type to **filesystem** using an environment variable. This approach is especially useful when switching between destinations without modifying the actual pipeline code. See details in the [section on using named destinations](#use-named-destinations-to-switch-destinations-without-changing-code).

* Use full **destination factory type**
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::class_type-->

Above, we use the built-in **filesystem** destination by providing a factory type `filesystem` from the module `dlt.destinations`. You can implement [your own destination](../walkthroughs/create-new-destination.md) and pass this external module as well.

* Import **destination factory**
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::class-->

Above, we import the destination factory for **filesystem** and pass it to the pipeline.

All examples above will create the same destination class with default parameters and pull required config and secret values from [configuration](credentials/index.md) - they are equivalent.

### Pass explicit parameters and a name to a destination factory
You can instantiate the **destination factory** yourself to configure it explicitly. When doing this, you work with destinations the same way you work with [sources](source.md)
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::instance-->

Above, we import and instantiate the `filesystem` destination factory. We pass the explicit URL of the bucket and name the destination `production_az_bucket`.

If a destination is not named, its shorthand type (the Python factory name) serves as the destination name. Name your destination explicitly if you need several separate configurations for destinations of the same type (i.e., when you wish to maintain credentials for development, staging, and production storage buckets in the same config file). The destination name is also stored in the [load info](../running-in-production/running.md#inspect-and-save-the-load-info-and-trace) and pipeline traces, so use explicit names when you need more descriptive identifiers (rather than generic names like `filesystem`).


## Configure a destination
We recommend passing the credentials and other required parameters to configuration via TOML files, environment variables, or other [config providers](credentials/setup). This allows you, for example, to easily switch to production destinations after deployment.

Use the [default config section layout](credentials/advanced#organize-configuration-and-secrets-with-sections) as shown below:
<!--@@@DLT_SNIPPET ./snippets/destination-toml.toml::default_layout-->

Alternatively, you can use environment variables:
```sh
DESTINATION__FILESYSTEM__BUCKET_URL=az://dlt-azure-bucket
DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME=dltdata
DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY="storage key"
```

When using destination factories, use the destination name in the config section:
<!--@@@DLT_SNIPPET ./snippets/destination-toml.toml::name_layout-->

For custom destination names passed to your pipeline (e.g., `destination="my_destination"`), dlt resolves the destination type from configuration. Add `destination_type` to specify which destination type to use:
<!--@@@DLT_SNIPPET ./snippets/destination-toml.toml::custom_name_layout-->


Note that when you use the `dlt init` command to create or add a data source, `dlt` creates a sample configuration for the selected destination.



### Pass explicit credentials
You can pass credentials explicitly when creating a destination factory instance. This replaces the `credentials` argument in `dlt.pipeline` and `pipeline.load` methods, which is now deprecated. You can pass the required credentials object, its dictionary representation, or the supported native form like below:
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::config_explicit-->


:::tip
You can create and pass partial credentials, and `dlt` will fill in the missing data. Below, we pass a PostgreSQL connection string but without a password and expect that it will be present in environment variables (or any other [config provider](credentials/setup))
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::config_partial-->


<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::config_partial_spec-->


Please read how to use [various built-in credentials types](credentials/complex_types).
:::

### Inspect destination capabilities
[Destination capabilities](../walkthroughs/create-new-destination.md#3-set-the-destination-capabilities) tell `dlt` what a given destination can and cannot do. For example, it tells which file formats it can load, what the maximum query or identifier length is. Inspect destination capabilities as follows:
```py
import dlt
pipeline = dlt.pipeline("snowflake_test", destination="snowflake")
print(dict(pipeline.destination.capabilities()))
```

### Pass additional parameters and change destination capabilities
The destination factory accepts additional parameters that will be used to pre-configure it and change destination capabilities.
```py
import dlt
duck_ = dlt.destinations.duckdb(naming_convention="duck_case", recommended_file_size=120000)
print(dict(duck_.capabilities()))
```
The example above is overriding the `naming_convention` and `recommended_file_size` in the destination capabilities.


## Use named destinations

Named destinations are destinations with a custom name, the type of which can be explicitly provided or be resolved from the toml files or equivalent environment variables. There are multiple ways to use named destinations:

- Use a named destination string reference with type configured via an environment variable

<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::custom_destination_name-->

- Use `dlt.destination()` with the type configured via an environment variable

<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::named_destination_dlt_destination-->

- Use `dlt.destination()` with the type explicitly configured

<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::named_destination_dlt_destination_explicit_type-->

For all of the above, the destination type can alternatively be configured in the `secrets.toml` file as follows:

```toml
[destination.my_destination]
destination_type = "filesystem"
```

:::note
When resolving non-module destination string references (e.g., `"bigquery"` or `"my_destination"`, not `"dlt.destinations.bigquery"`), dlt first attempts to resolve the reference as a named destination with a valid destination type configured, then falls back to shorthand type resolution.

This means that, in the examples above, if the destination type was not properly configured or was not a valid destination type, dlt would have attempted to resolve `"my_destination"` as a shorthand for a built-in type and would have eventually failed.

As another example, the following:
<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::avoid_example-->
will be resolved as a DuckDB destination that is named `"bigquery"`, because a valid destination type `"duckdb"` is configured and dlt does not attempt to resolve the name `"bigquery"` as a shorthand for a built-in type!

**Exception:** If `dlt.destination()` is used and the `destination_type` is explicitly provided as an argument, dlt will skip the shorthand fallback and only attempt named destination resolution.

:::


### Configure multiple destinations of the same type

One of the benefits of named destinations is the ability to configure multiple destinations of the same type for use across different pipelines in a single script. For example, if you have two BigQuery destinations, you could define the following in the `secrets.toml` file:

```toml
[destination.my_destination]
location = "US"
[destination.my_destination.credentials]
project_id = "please set me up!"
private_key = "please set me up!"
client_email = "please set me up!"

[destination.my_other_destination]
location = "EU"
[destination.my_other_destination.credentials]
project_id = "please set me up!"
private_key = "please set me up!"
client_email = "please set me up!"
```

And use it in the pipeline code as follows:
```py
import dlt

# Configure the pipeline to use the "my_destination" BigQuery destination
my_pipeline = dlt.pipeline(
    pipeline_name='my_pipeline',
    destination=dlt.destination("my_destination", destination_type="bigquery"),
    dataset_name='dataset_name'
)

# Configure the pipeline to use the "my_other_destination" BigQuery destination
my_other_pipeline = dlt.pipeline(
    pipeline_name='my_other_pipeline',
    destination=dlt.destination("my_other_destination", destination_type="bigquery"),
    dataset_name='dataset_name'
)
```

### Use named destinations to switch destinations without changing code

Another advantage of named destinations is environment-based switching. When you need to use different destinations across development, staging, and production environments, named destinations allow you to switch destinations by modifying only the toml configuration files without changing the pipeline code. For example, if you are developing with DuckDB, you would first have your `secrets.toml` file configured as follows:

```toml
[destination.my_destination]
destination_type = "duckdb"
```

With the pipeline code being:

```py
import dlt

pipeline = dlt.pipeline(
    pipeline_name='my_pipeline',
    destination='my_destination',
    dataset_name='dataset_name'
)
```

Then when you deploy the script, you would simply adjust your toml file with:

```toml
[destination.my_destination]
destination_type = "bigquery"
[destination.my_destination.credentials]
project_id = "please set me up!"
private_key = "please set me up!"
client_email = "please set me up!"
```

And keep the pipeline code intact.


## Access a destination
When loading data, `dlt` will access the destination in two cases:
1. At the beginning of the `run` method to sync the pipeline state with the destination (or if you call `pipeline.sync_destination` explicitly).
2. In the `pipeline.load` method - to migrate the schema and load the load package.

`dlt` will also access the destination when you instantiate [sql_client](../dlt-ecosystem/transformations/sql.md).

:::note
`dlt` will not import the destination dependencies or access destination configuration if access is not needed. You can build multi-stage pipelines where steps are executed in separate processes or containers - the `extract` and `normalize` step do not need destination dependencies, configuration, and actual connection.

<!--@@@DLT_SNIPPET ./snippets/destination-snippets.py::late_destination_access-->

:::

## Control how `dlt` creates table, column, and other identifiers
`dlt` maps identifiers found in the source data into destination identifiers (i.e., table and column names) using [naming conventions](naming-convention.md) which ensure that
character set, identifier length, and other properties fit into what the given destination can handle. For example, our [default naming convention (**snake case**)](./naming-convention.md#use-default-naming-convention-snake_case) converts all names in the source (i.e., JSON document fields) into snake case, case-insensitive identifiers.

Each destination declares its preferred naming convention, support for case-sensitive identifiers, and case folding function that case-insensitive identifiers follow. For example:
1. Redshift - by default, does not support case-sensitive identifiers and converts all of them to lower case.
2. Snowflake - supports case-sensitive identifiers and considers upper-cased identifiers as case-insensitive (which is the default case folding).
3. DuckDb - does not support case-sensitive identifiers but does not case fold them, so it preserves the original casing in the information schema.
4. Athena - does not support case-sensitive identifiers and converts all of them to lower case.
5. BigQuery - all identifiers are case-sensitive; there's no case-insensitive mode available via case folding (but it can be enabled at the dataset level).

You can change the naming convention used in [many different ways](naming-convention.md#configure-naming-convention). Below, we set the preferred naming convention on the Snowflake destination to `sql_cs` to switch Snowflake to case-sensitive mode:
```py
import dlt
snow_ = dlt.destinations.snowflake(naming_convention="sql_cs_v1")
```
Setting the naming convention will impact all new schemas being created (i.e., on the first pipeline run) and will re-normalize all existing identifiers.

:::warning
`dlt` prevents re-normalization of identifiers in tables that were already created at the destination. Use [refresh](pipeline.md#refresh-pipeline-data-and-state) mode to drop the data. You can also disable this behavior via [configuration](naming-convention.md#avoid-identifier-collisions).
:::

:::note
Destinations that support case-sensitive identifiers but use a case folding convention to enable case-insensitive identifiers are configured in case-insensitive mode by default. Examples: Postgres, Snowflake, Oracle.
:::

:::warning
If you use a case-sensitive naming convention with a case-insensitive destination, `dlt` will:
1. Fail the load if it detects an identifier collision due to case folding.
2. Warn if any case folding is applied by the destination.
:::

### Enable case-sensitive identifiers support
Selected destinations may be configured so they start accepting case-sensitive identifiers. For example, it is possible to set case-sensitive collation on an **mssql** database and then tell `dlt` about it.
```py
from dlt.destinations import mssql
dest_ = mssql(has_case_sensitive_identifiers=True, naming_convention="sql_cs_v1")
```
Above, we can safely use a case-sensitive naming convention without worrying about name collisions.

You can configure the case sensitivity, **but configuring destination capabilities is not currently supported**.
```toml
[destination.mssql]
has_case_sensitive_identifiers=true
```

:::note
In most cases, setting the flag above just indicates to `dlt` that you switched the case-sensitive option on a destination. `dlt` will not do that for you. Refer to the destination documentation for details.
:::

## Writing to the destination
### Destination schema

The destination schema (i.e., database schema) is a collection of tables that represent the data you loaded into the database.
The schema name is the same as the `dataset_name` you provided in the pipeline definition.
In the example above, we explicitly set the `dataset_name` to `mydata`. If you don't set it,
it will be set to the pipeline name with a suffix `_dataset`.

Be aware that the schema referred to in this section is distinct from the [dlt Schema](schema.md).
The database schema pertains to the structure and organization of data within the database, including table
definitions and relationships. On the other hand, the "dlt Schema" specifically refers to the format
and structure of normalized data within the dlt pipeline.

### Tables

Each [resource](resource.md) in your pipeline definition will be represented by a table in
the destination. In the example above, we have one resource, `users`, so we will have one table, `mydata.users`,
in the destination. Here, `mydata` is the schema name, and `users` is the table name. Here also, we explicitly set
the `table_name` to `users`. When `table_name` is not set, the table name will be set to the resource name.

For example, we can rewrite the pipeline above as:

```py
@dlt.resource
def users():
    yield [
        {'id': 1, 'name': 'Alice'},
        {'id': 2, 'name': 'Bob'}
    ]

pipeline = dlt.pipeline(
    pipeline_name='quick_start',
    destination='duckdb',
    dataset_name='mydata'
)
load_info = pipeline.run(users)
```

The result will be the same; note that we do not explicitly pass `table_name="users"` to `pipeline.run`, and the table is implicitly named `users` based on the resource name (e.g., `users()` decorated with `@dlt.resource`).

:::note

Special tables are created to track the pipeline state. These tables are prefixed with `_dlt_`
and are not shown in the `show` command of the `dlt pipeline` CLI. However, you can see them when
connecting to the database directly.

:::

## Nested tables

Now let's look at a more complex example:

```py
import dlt

data = [
    {
        'id': 1,
        'name': 'Alice',
        'pets': [
            {'id': 1, 'name': 'Fluffy', 'type': 'cat'},
            {'id': 2, 'name': 'Spot', 'type': 'dog'}
        ]
    },
    {
        'id': 2,
        'name': 'Bob',
        'pets': [
            {'id': 3, 'name': 'Fido', 'type': 'dog'}
        ]
    }
]

pipeline = dlt.pipeline(
    pipeline_name='quick_start',
    destination='duckdb',
    dataset_name='mydata'
)
load_info = pipeline.run(data, table_name="users")
```

Running this pipeline will create two tables in the destination, `users` (**root table**) and `users__pets` (**nested table**). The `users` table will contain the top-level data, and the `users__pets` table will contain the data nested in the Python lists. Here is what the tables may look like:

**mydata.users**

| id | name | _dlt_id | _dlt_load_id |
| --- | --- | --- | --- |
| 1 | Alice | wX3f5vn801W16A | 1234562350.98417 |
| 2 | Bob | rX8ybgTeEmAmmA | 1234562350.98417 |

**mydata.users__pets**

| id | name | type | _dlt_id | _dlt_parent_id | _dlt_list_idx |
| --- | --- | --- | --- | --- | --- |
| 1 | Fluffy | cat | w1n0PEDzuP3grw | wX3f5vn801W16A | 0 |
| 2 | Spot | dog | 9uxh36VU9lqKpw | wX3f5vn801W16A | 1 |
| 3 | Fido | dog | pe3FVtCWz8VuNA | rX8ybgTeEmAmmA | 0 |

When inferring a database schema, dlt maps the structure of Python objects (i.e., from parsed JSON files) into nested tables and creates references between them.

This is how it works:

1. Each row in all (root and nested) data tables created by dlt contains a unique column named `_dlt_id` (**row key**).
2. Each nested table contains a column named `_dlt_parent_id` referencing a particular row (`_dlt_id`) of a parent table (**parent key**).
3. Rows in nested tables come from the Python lists: `dlt` stores the position of each item in the list in `_dlt_list_idx`.
4. For nested tables that are loaded with the `merge` write disposition, we add a **root key** column `_dlt_root_id`, which references the child table to a row in the root table.

[Learn more about nested references, row keys, and parent keys](schema.md#nested-references-root-and-nested-tables)

## Naming convention: tables and columns

During a pipeline run, dlt [normalizes both table and column names](schema.md#naming-convention) to ensure compatibility with the destination database's accepted format. All names from your source data will be transformed into snake_case and will only include alphanumeric characters. Please be aware that the names in the destination database may differ somewhat from those in your original input.

## Variant columns
If your data has inconsistent types, `dlt` will dispatch the data to several **variant columns**. For example, if you have a resource (i.e., a JSON file) with a field named `answer` and your data contains boolean values, you will get a column named `answer` of type `BOOLEAN` in your destination. If, for some reason, on the next load, you get integer and string values in `answer`, the inconsistent data will go to `answer__v_bigint` and `answer__v_text` columns respectively.
The general naming rule for variant columns is `<original name>__v_<type>` where `original_name` is the existing column name (with data type clash) and `type` is the name of the data type stored in the variant.

## Load packages and load IDs

Each execution of the pipeline generates one or more load packages. A load package typically contains data retrieved from all the [resources](glossary.md#resource) of a particular [source](glossary.md#source). These packages are uniquely identified by a `load_id`. The `load_id` of a particular package is added to the top data tables (referenced as `_dlt_load_id` column in the example above) and to the special `_dlt_loads` table with a status of 0 (when the load process is fully completed).

To illustrate this, let's load more data into the same destination:

```py
data = [
    {
        'id': 3,
        'name': 'Charlie',
        'pets': []
    },
]
```

The rest of the pipeline definition remains the same. Running this pipeline will create a new load package with a new `load_id` and add the data to the existing tables. The `users` table will now look like this:

**mydata.users**

| id | name | _dlt_id | _dlt_load_id |
| --- | --- | --- | --- |
| 1 | Alice | wX3f5vn801W16A | 1234562350.98417 |
| 2 | Bob | rX8ybgTeEmAmmA | 1234562350.98417 |
| 3 | Charlie | h8lehZEvT3fASQ | **1234563456.12345** |

The `_dlt_loads` table will look like this:

**mydata._dlt_loads**

| load_id | schema_name | status | inserted_at | schema_version_hash |
| --- | --- | --- | --- | --- |
| 1234562350.98417 | quick_start | 0 | 2023-09-12 16:45:51.17865+00 | aOEb...Qekd/58= |
| **1234563456.12345** | quick_start | 0 | 2023-09-12 16:46:03.10662+00 | aOEb...Qekd/58= |

The `_dlt_loads` table tracks complete loads and allows chaining transformations on top of them. Many destinations do not support distributed and long-running transactions (e.g., Amazon Redshift). In that case, the user may see the partially loaded data. It is possible to filter such data out: any row with a `load_id` that does not exist in `_dlt_loads` is not yet completed. The same procedure may be used to identify and delete data for packages that never got completed.

For each load, you can test and [alert](../running-in-production/alerting.md) on anomalies (e.g., no data, too much loaded to a table). There are also some useful load stats in [dashboard app](../general-usage/dashboard) mentioned above.

You can add [transformations](../dlt-ecosystem/transformations/) and chain them together using the `status` column. You start the transformation for all the data with a particular `load_id` with a status of 0 and then update it to 1. The next transformation starts with the status of 1 and is then updated to 2. This can be repeated for every additional transformation.

### Data lineage

Data lineage can be super relevant for architectures like the [data vault architecture](https://www.data-vault.co.uk/what-is-data-vault/) or when troubleshooting. The data vault architecture is a data warehouse that large organizations use when representing the same process across multiple systems, which adds data lineage requirements. Using the pipeline name and `load_id` provided out of the box by `dlt`, you are able to identify the source and time of data.

You can [save](../running-in-production/running.md#inspect-and-save-the-load-info-and-trace) complete lineage info for a particular `load_id` including a list of loaded files, error messages (if any), elapsed times, schema changes. This can be helpful, for example, when troubleshooting problems.

## SQL Client

:::note
The SQL client is a low-level API. If you simply want to query your data, refer to the [dataset interface](./dataset-access/dataset.md).
:::

Most `dlt` destinations use an implementation of the `SqlClientBase` class to connect to the physical destination to which your data is loaded. DDL statements, data insert or update commands, as well as SQL merge and replace queries, are executed via a connection on this client. It also is used for reading data for the [dashboard app](./dashboard.md) and [data access via `dlt` datasets](./dataset-access/dataset.md).

All SQL destinations make use of an SQL client; additionally, the filesystem has a special implementation of the SQL client which you can read about [below](#the-filesystem-sql-client).

### Execute a query

You can access the SQL client of your destination via the `sql_client` method on your pipeline. The code below shows how to use the SQL client to execute a query.

```py
pipeline = dlt.pipeline(destination="bigquery", dataset_name="crm")
with pipeline.sql_client() as client:
    with client.execute_query(
        "SELECT id, name, email FROM customers WHERE id = %s",
        10
    ) as cursor:
        # get all data from the cursor as a list of tuples
        print(cursor.fetchall())
```

### Query result format

The cursor returned by `execute_query` has several methods for retrieving the data. The supported formats are Python tuples, Pandas DataFrame, and Arrow table.

The code below shows how to retrieve the data as a Pandas DataFrame and then manipulate it in memory:

```py
pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
with pipeline.sql_client() as client:
    with client.execute_query(
        'SELECT "reactions__+1", "reactions__-1", reactions__laugh, reactions__hooray, reactions__rocket FROM issues'
    ) as cursor:
        # calling `df` on a cursor, returns the data as a pandas DataFrame
        reactions = cursor.df()
counts = reactions.sum(0).sort_values(0, ascending=False)
```

#### Retrieval methods

- `fetchall()`: returns all rows as a list of tuples;
- `fetchone()`: returns a single row as a tuple;
- `fetchmany(size=None)`: returns a number of rows as a list of tuples; if no size is provided, all rows are returned;
- `df(chunk_size=None, **kwargs)`: returns the data as a Pandas DataFrame; if `chunk_size` is provided, the data is retrieved in chunks of the given size;
- `arrow(chunk_size=None, **kwargs)`: returns the data as an Arrow table; if `chunk_size` is provided, the data is retrieved in chunks of the given size;
- `iter_fetch(chunk_size: int)`: iterates over the data in chunks of the given size as lists of tuples;
- `iter_df(chunk_size: int)`: iterates over the data in chunks of the given size as Pandas DataFrames;
- `iter_arrow(chunk_size: int)`: iterates over the data in chunks of the given size as Arrow tables.

:::info
Which retrieval method you should use very much depends on your use case and the destination you are using. Some drivers for our destinations provided by their vendors natively support Arrow or Pandas DataFrames; in these cases, we will use that interface. If they do not, `dlt` will convert lists of tuples into these formats.
:::

### Filesystem SQL client

The filesystem destination implements a special but extremely useful version of the SQL client. While during a normal pipeline run, the filesystem does not make use of an SQL client but rather copies the files resulting from a load into the folder or bucket you have specified, it is possible to query this data using SQL via this client. For this to work, `dlt` uses an in-memory `DuckDB` database instance and makes your filesystem tables available as views on this database. For the most part, you can use the filesystem SQL client just like any other SQL client. `dlt` uses sqlglot to discover which tables you are trying to access and, as mentioned above, `DuckDB` to make them queryable.

The code below shows how to use the filesystem SQL client to query the data:

```py
pipeline = dlt.pipeline(destination="filesystem", dataset_name="my_dataset")
with pipeline.sql_client() as client:
    with client.execute_query("SELECT * FROM my_table") as cursor:
        print(cursor.fetchall())
```

A few things to know or keep in mind when using the filesystem SQL client:

- The SQL database you are actually querying is an in-memory database, so if you do any kind of mutating queries, these will not be persisted to your folder or bucket.
- You must have loaded your data as `JSONL`, `Parquet`, `CSV` files or `delta`/`iceberg` tables for this SQL client to work. For optimal performance, you should use `Parquet` files or open table formats, as `DuckDB` is able to only read the bytes needed to execute your query from a folder or bucket in this case.
- Keep in mind that if you do any filtering, sorting, or full table loading with the SQL client, the in-memory `DuckDB` instance will have to download and query a lot of data from your bucket or folder if you have a large table.
- If you are accessing data on a bucket, `dlt` will temporarily store your credentials in `DuckDB` to let it connect to the bucket.
- Some combinations of buckets and table formats may not be fully supported at this time.
- Multi-schema support (dlt 1.25.0+): When a dataset includes multiple schemas, the filesystem SQL client creates views that span all schemas. If the same table name exists in multiple schemas at different physical locations (e.g. when the layout includes `{schema_name}/`), views are combined. If they share the same location, columns are merged into a single view. This means queries may return rows from multiple schemas — use `pipeline.dataset(schema="name")` to restrict to one schema.

#### Refresh SQL client data view
`sqlclient` creates views in which the data is immutable (each next query will access the same data). Such "snapshots" are created by:
* globbing the table files once - when view is created
* using the newest iceberg metadata to create view

Updating views may be costly (globbing, re-reading iceberg metadata) so your best option is to create new `sql_client` (or `pipeline.dataset()`) instance
when you need fresh data. Alternatively you can enable autorefresh mode which will re-create view on each query:

```py
from dlt.destination import filesystem

pipeline = dlt.pipeline(destination=filesystem(always_refresh_views=True), dataset_name="my_dataset")
with pipeline.sql_client() as client:
    with client.execute_query("SELECT * FROM my_table") as cursor:
        print(cursor.fetchall())
        # pipeline.run() here and get updated data
        print(cursor.fetchall())
```

Note: `delta` tables are by default on autorefresh which is implemented by delta core and seems to be pretty efficient.

## Loading data into existing tables not created by dlt

You can also load data from `dlt` into tables that already exist in the destination dataset and were not created by `dlt`.
There are a few things to keep in mind when doing this:

If you load data into a table that exists but does not contain any data, in most cases, your load will succeed without problems.
`dlt` will create the needed columns and insert the incoming data. `dlt` will only be aware of columns that exist on the
discovered or provided internal schema, so if you have columns in your destination that are not anticipated by `dlt`, they
will remain in the destination but stay unknown to `dlt`. This generally will not be a problem.

If your destination table already exists and contains columns that have the same name as columns discovered by `dlt` but
do not have matching datatypes, your load will fail, and you will have to fix the column on the destination table first,
or change the column name in your incoming data to something else to avoid a collision.

If your destination table exists and already contains data, your load might also initially fail, since `dlt` creates
special `non-nullable` columns that contain required mandatory metadata. Some databases will not allow you to create
`non-nullable` columns on tables that have data, since the initial value for these columns of the existing rows cannot
be inferred. You will have to manually create these columns with the correct type on your existing tables and
make them `nullable`, then fill in values for the existing rows. Some databases may allow you to create a new column
that is `non-nullable` and take a default value for existing rows in the same command. The columns you will need to
create are:

| name | type |
| --- | --- |
| _dlt_load_id | text/string/varchar |
| _dlt_id | text/string/varchar |

For nested tables, you may also need to create:

| name | type |
| --- | --- |
| _dlt_parent_id | text/string/varchar |
| _dlt_root_id | text/string/varchar |

## Create a new destination
You have two ways to implement a new destination:
1. You can use the `@dlt.destination` decorator and [implement a sink function](../dlt-ecosystem/destinations/destination.md). This is a perfect way to implement reverse ETL destinations that push data back to REST APIs.
2. You can implement [a full destination](../walkthroughs/create-new-destination.md) where you have full control over load jobs and schema migration.
