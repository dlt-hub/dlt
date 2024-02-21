# Create new destination

:::caution
This guide is compatible with `dlt` **0.3.x**. Version **0.4.x** has a different module layout. We are working on an update.
:::

`dlt` can import destinations from external python modules. Below we show how to quickly add a [dbapi](https://peps.python.org/pep-0249/) based destination. `dbapi` is a standardized interface to access
databases in Python. If you used ie. postgres (ie. `psycopg2`) you are already familiar with it.

> 🧪 This guide is not comprehensive. The internal interfaces are still evolving. Besides reading info below, you should check out [source code of existing destinations](https://github.com/dlt-hub/dlt/tree/devel/dlt/destinations)

## 1. Copy existing destination to your `dlt` project
Initialize a new project with [dlt init](../reference/command-line-interface.md#dlt-init)
```shell
dlt init github postgres
```
This adds `github` verified source (it produces quite complicated datasets and that good for testing, does not require credentials to use) and `postgres` credentials (connection-string-like) that we'll repurpose later.

Clone [dlt](https://github.com/dlt-hub/dlt) repository to a separate folder. In the repository look for **dlt/destinations** folder and copy one of the destinations to your project. Pick your starting point:
* **postgres** - a simple destination without staging storage support and COPY jobs
* **redshift** - based on postgres, adds staging storage support and remote COPY jobs
* **snowflake** - a destination supporting additional authentication schemes, local and remote COPY jobs and no support for direct INSERTs

Below we'll use **postgres** as starting point.


## 2. Adjust the destination configuration and credentials
`dbapi` based destinations use `ConnectionStringCredentials` as a credentials base which accepts SQLAlchemy style connection strings. Typically you should derive from it to change the `drivername` and make desired properties (like `host` or `password`) mandatory.

We keep config and credentials in `configuration.py`. You should:
- rename the classes properly to match your destination name
- if you need more properties (ie. look at `iam_role` in `redshift` credentials) then add them, remember about typing. Behind the hood credentials and configs are **dataclasses**.
- import and use new configuration class in `_configure()` method in `__init__.py`
- tell `dlt` the default configuration section by placing your destination name in `sections` argument of `@with_config` decorator.
- expose the configuration type in `spec` method in `__init__.py`

> 💡 Each destination module implements `DestinationReference` protocol defined in [reference.py](https://github.com/dlt-hub/dlt/blob/devel/dlt/common/destination/reference.py).

> 💡 See how `snowflake` destination adds additional authorization methods and configuration options.

## 3. Set the destination capabilities
`dlt` needs to know a few things about the destination to correctly work with it. Those are stored in `capabilities()` function in `__init__.py`.

* supported loader file formats both for direct and staging loading (see below)
* `escape_identifier` a function that escapes database identifiers ie. table or column name. provided implementation for postgres should work for you.
* `escape_literal` a function that escapes string literal. it is only used if destination supports **insert-values** loader format
* `decimal_precision` precision and scale of decimal/numeric types. also used to create right decimal types in loader files ie. parquet
* `wei_precision` precision and scale of decimal/numeric to store very large (up to 2**256) integers. specify maximum precision for scale 0
* `max_identifier_length` max length of table and schema/dataset names
* `max_column_identifier_length` max length of column name
* `naming_convention` a name or naming convention module that maps the input alphabet (ie. JSON identifiers) to destination alphabet. leave the default - it is [very conservative](../general-usage/schema.md#naming-convention)
* `max_query_length`, `is_max_query_length_in_bytes`, `max_text_data_type_length`, `is_max_text_data_type_length_in_bytes` - tells `dlt` the maximum length of text query and of text data types.
* `supports_transactions` tells if destination supports transactions
* `timestamp_precision` sets fidelity of timestamp/datetime type: 0 - 9 (from seconds to nanoseconds), default is 6
* `supports_ddl_transactions` tells if the destination supports ddl transactions.
* `alter_add_multi_column` tells if destination can add multiple columns in **ALTER** statement
* `supports_truncate_command` tells dlt if **truncate** command is used, otherwise it will use **DELETE** to clear tables.

### Supported loader file formats
Specify which [loader file formats](../dlt-ecosystem/file-formats/) your destination will support directly and via [storage staging](../dlt-ecosystem/staging.md). Direct support means that destination is able to load a local file or supports INSERT command. Loading via staging is using `filesystem` to send load package to a (typically) bucket storage and then load from there.

> 💡 the [insert-values](../dlt-ecosystem/file-formats/insert-format.md) data format generates large INSERT statement that are executed on the destination. If you have any other option for local loading, avoid using this format. It is typically slower and requires the use of bullet-proof `escape_literal` function.

* `preferred_loader_file_format` - a file format that will be used by default to load data from local file system. Set to `None` if direct loading is not supported.
* `supported_loader_file_formats` - file formats that can be loaded from local file system to destination. Set to `[]` if direct loading is not supported.
* `preferred_staging_file_format` - a file format that will be used by default when `staging` is enabled. Set to `None` if destination can't load from staging.
* `supported_staging_file_formats` - file formats that are supported to be loaded staging storage. Set to `[]` if destination can't load from staging.

> 💡 Mind that for each file type you'll need to implement a load job (which in most cases is a `COPY` command to which you pass a file path and file type)

> 💡 Postgres does not support staging and any other file format beyond **insert-values**. Check the `snowflake` capabilities for a destination that supports all possible formats.

### Escape identifiers and literals
The default `escape_identifier` function identifier escapes `"` and '\' and quotes identifier with `"`. This is standard SQL behavior. Mind that if you use default naming convention, `dlt` normalizes identifiers to an alphabet that does not accept any special characters. Users are able to change the naming convention in the configuration so correct escape function is still important.

> 💡 **postgres** destination that you modify is using standard implementation that you may keep.

You should avoid providing a custom `escape_literal` function by not enabling `insert-values` for your destination.

## 4. Adjust the SQL client
**sql client** is a wrapper over `dbapi` and its main role is to provide consistent interface for executing SQL statements, managing transactions and (probably the most important) to help handling errors via classifying exceptions. Here's a few things you should pay attention to:

- When opening the connection: add current dataset name to search path, set session timezone to UTC.
- Transactions: typically to begin a transaction, you need to disable the auto-commit (like `postgres` implementation does)
- `execute_query`: dlt uses `%s` to represent dbi api query parameters. see `duckdb sql_client for a crude way to align your `dbapi` client if it uses other parameter placeholders.
- `execute_fragments`: if your `dbapi` client does not provide a method to join SQL fragments without full string copy, just delete `postgres` override. The base class just joins strings.

### Fully qualified names
When created, `sql_client` is bound to particular dataset name (which typically corresponds to a database schema). Most of the database engines follow usual rules of qualifying and quoting (**"schema"."table"."column"**) but there are exceptions like `BigQuery` or `Motherduck`. You have full control over generating identifiers via:
* `fully_qualified_dataset_name` returns a fully qualified dataset name.
* `make_qualified_table_name` same but for a given table name

### `dbapi` exceptions
`dlt` must be able to distinct a few error cases for the loading to work properly. Unfortunately error reporting is not very well defined by `dbapi` and even the existing exception tree is not used consistently across implementations.

`_make_database_exception` method wraps incoming `Exception` in one of exception types required by `dlt`:
- `DatabaseUndefinedRelation`: raised when schema or table that `dlt` tries to reference is undefined. It is important to detect this case exactly: via specific `dbapi` exceptions (like in case of `postgres` and `duckdb`) or via detecting proper category of exceptions and inspecting the error codes or messages (see. `redshift` and `snowflake`)
- `DatabaseTerminalException`: errors during loading that will permanently fail a job and should not retry. `IntegrityError`, `ProgrammingError` and most of the `DataError` belong to this class. (example: decimal value out of range, insert NULL in non NULL columns)
- `DatabaseTransientException`: all other exceptions. we also include `SyntaxError` (if exists in particular `dbapi` implementation) here

> 💡 How this works in practice: we have a set of tests for all relevant error cases in [test_sql_client.py](https://github.com/dlt-hub/dlt/blob/devel/tests/load/test_sql_client.py), this way we make sure that new sql_client behaves correctly.

### What base class assumes
 - that `INFORMATION_SCHEMA` exists from which we can take basic information on `SCHEMATA` and `COLUMNS`
 - `CREATE SCHEMA` and `DROP SCHEMA` (see how `BigQuery` overrides that)
 - `DELETE` or `TRUNCATE` is available to clear tables without dropping
 - `DROP TABLE` only for CLI command (`pipeline drop`)

## 5. Adjust the job client
Job client is responsible for creating/starting load jobs and managing the schema updates. Here we'll adjust the `SqlJobClientBase` base class which uses the `sql_client` to manage the destination. Typically only a few methods needs to be overridden by a particular implementation. The job client code customarily resides in a file with name `<destination_name>.py` ie. `postgres.py` and is exposed in `__init__.py` by `client` method.

### Database type mappings
You must map `dlt` data types to destination data types. This happens in `_to_db_type` and `_from_db_type` class methods. Typically just a mapping dictionary is enough. A few tricks to remember:
* the database types must be exactly those as used in `INFORMATION_SCHEMA.COLUMNS`
* decimal precision and scale are filled from the capabilities (in all our implementations)
* until now all destinations could handle binary types
* we always try to map the `complex` type into `JSON` type in the destination. if that does not work you can try mapping into a string. See how we do that for various destinations.
* the reverse mapping of types is sometimes tricky ie. you may not able to detect complex types (your destination lacks JSON support). this is not really needed during schema updates and loading (just for testing) so in general you should be fine.

### Table and column hints
You can map hints present for tables and columns (ie. `cluster`, `sort`, `partition`) to generate specific DDL for columns and tables. See `_get_column_def_sql` in various destinations.
You can also add hints (ie indexes, partition clauses) to tables via `_get_table_update_sql` - see `BigQuery` implementation for a good example.

### Participate in staging dataset merge and replace
`dlt` supports merging and transactional replace via **staging dataset** living along the destination dataset. `SqlJobClientBase` participates in this mechanism by default. In essence: each time when a job is completed, `dlt` checks which table got updated and if there are no remaining jobs for that table and its child and parent tables (all together called **table chain**). If table chain is fully loaded, `dlt` executes SQL transformations that move/merge data from staging dataset to destination dataset (that, as you can expect, happens also via jobs, of type `sql` that are dynamically created).

Generated SQL is quite simple and we were able to run it on all existing destinations (we may introduce `sqlglot` to handle future cases). The SQL used requires:
- SELECT, INSERT, DELETE/TRUNCATE statements
- WINDOW functions for merge.

In case of destinations that do not allow the data modifications you can opt out from both replace and merge:
- override `get_truncate_destination_table_dispositions` method and return empty list so your tables are never truncated
- override `get_stage_dispositions` and return empty list to opt out from any operations on staging dataset.

### What base class assumes
* DDL to create and add column to tables is available
* it is possible to SELECT data
* it is possible to INSERT data (in order to complete package and store the updated schema)

> 💡 talk to us on slack if your destination is fully read only.


## 6. Implement load jobs
Load jobs make sure that all files in load package are loaded to destination. `dlt` creates single job per file and [makes sure that it transitions to `completed` state. (look for `LoadJob`)](https://github.com/dlt-hub/dlt/blob/devel/dlt/common/destination/reference.py)

The file name of the job is used as the job id and both sync and async execution is supported. The executor is multi-threaded. Each job starts in separate thread and its completion status is checked from the main thread.

Jobs are typically very simple and just execute INSERT or COPY commands. They do not replace nor merge data themselves.

### Enable insert jobs
If you use **insert-values** loader file format then derive your job client from `InsertValuesJobClient`. `postgres.py` does exactly that.

Look at `snowflake.py` for a destination that does not use **insert-values**.

### Copy jobs from local and remote files
`dlt` allows to chain two destinations to create a [storage stage (typically on a bucket)](../dlt-ecosystem/staging.md). The staging destination (currently `filesystem`) will copy new files, complete the corresponding jobs and for each of them it will create **reference job** that will be passed to a destination to execute.

The `postgres` destination does not implement any copy jobs.
- See `RedshiftCopyFileLoadJob` in `redshift.py` how we create and start a copy job from a bucket. It uses `CopyRemoteFileLoadJob` base to handle the references and creates a `COPY` SQL statement in `execute()` method.
- See `SnowflakeLoadJob` in `snowflake.py` how to implement a job that can load local and reference files. It also forwards AWS credentials from staging destination. At the end the code just generates a COPY command for various loader file formats.

## Testing
We can quickly repurpose existing github source and `secrets.toml` already present in the project to test new destination. Let's assume that the module name is `presto`, same for the destination name and config section name. Here's our testing script `github_pipeline.py`
```python
import dlt

from github import github_repo_events
import presto  # importing destination module

def load_airflow_events() -> None:
    """Loads airflow events. Shows incremental loading. Forces anonymous access token"""
    pipeline = dlt.pipeline(
        "github_events", destination=presto, dataset_name="airflow_events"
    )
    data = github_repo_events("apache", "airflow", access_token="")
    print(pipeline.run(data))

if __name__ == "__main__":
    load_airflow_events()
```

Here's `secrets.toml`:
```toml
[destination.presto]
# presto config
[destination.presto.credentials]
database = "dlt_data"
password = "loader"
username = "loader"
host = "localhost"
port = 5432
```

Mind that in the script above we import the `presto` module and then pass it in `destination` argument to `dlt.pipeline`. Github pipeline will load the events in `append` mode. You may force `replace` and `merge` modes in `pipeline.run` to check more advanced behavior of the destination.

After executing the pipeline script:
```
python github_pipeline.py
got page https://api.github.com/repos/apache/airflow/events?per_page=100, requests left: 59
got page https://api.github.com/repositories/33884891/events?per_page=100&page=2, requests left: 58
got page https://api.github.com/repositories/33884891/events?per_page=100&page=3, requests left: 57
Pipeline github_events completed in 4.56 seconds
1 load package(s) were loaded to destination presto and into dataset airflow_events
The presto destination used postgres://loader:***@localhost:5432/dlt_data location to store data
Load package 1690628947.953597 is LOADED and contains no failed jobs
```

you can use `dlt pipeline show github_events` to view data in the destination.
