# Create new destination

:::tip
You can use the `@dlt.destination` decorator and [implement a sink function](../dlt-ecosystem/destinations/destination.md). This is a perfect way to implement reverse ETL components that push data back to REST APIs.
:::

`dlt` can import destinations from external Python modules. Below, we show how to quickly add a [dbapi](https://peps.python.org/pep-0249/) based destination. `dbapi` is a standardized interface for accessing
databases in Python. If you have used, for example, PostgreSQL (i.e., `psycopg2`), you are already familiar with it.

> 🧪 This guide is not comprehensive. The internal interfaces are still evolving. Besides reading the information below, you should check out the [source code of existing destinations](https://github.com/dlt-hub/dlt/tree/devel/dlt/destinations/impl).

## 0. Prerequisites

Destinations are implemented in Python packages under: `dlt.destinations.impl.<destination_name>`. Generally, a destination consists of the following modules:

* `__init__.py` - This module contains the destination capabilities.
* `<destination_name>.py` - This module contains the job client and load job implementations for the destination.
* `configuration.py` - This module contains the destination and credentials configuration classes.
* `sql_client.py` - This module contains the SQL client implementation for the destination, which is a wrapper over `dbapi` that provides a consistent interface to `dlt` for executing queries.
* `factory.py` - This module contains a `Destination` subclass that is the entry point for the destination.

## 1. Copy an existing destination to your `dlt` project
Initialize a new project with [dlt init](../reference/command-line-interface.md#dlt-init):
```sh
dlt init github postgres
```
This adds a `github` verified source (it produces quite complicated datasets and that's good for testing; it does not require credentials to use) and `postgres` credentials (connection-string-like) that we'll repurpose later.

Clone the [dlt](https://github.com/dlt-hub/dlt) repository to a separate folder. In the repository, look for the **dlt/destinations/impl** folder and copy one of the destinations to your project. Pick your starting point:
* **postgres** - A simple destination without staging storage support and COPY jobs.
* **redshift** - Based on postgres, adds staging storage support and remote COPY jobs.
* **snowflake** - A destination supporting additional authentication schemes, local and remote COPY jobs, and no support for direct INSERTs.

Below, we'll use **postgres** as the starting point.

## 2. Adjust the destination configuration and credentials
`dbapi` based destinations use `ConnectionStringCredentials` as a credentials base, which accepts SQLAlchemy-style connection strings. Typically, you should derive from it to change the `drivername` and make desired properties (like `host` or `password`) mandatory.

We keep the config and credentials in `configuration.py`. You should:
- Rename the classes properly to match your destination name.
- If you need more properties (e.g., look at `iam_role` in `redshift` credentials), then add them, remembering about typing. Behind the hood, credentials and configs are **dataclasses**.
- Adjust the `__init__` arguments in your `Destination` class in `factory.py` to match the new credentials and config classes.
- Expose the configuration type in the `spec` attribute in `factory.py`.

> 💡 Each destination implements the `Destination` abstract class defined in [reference.py](https://github.com/dlt-hub/dlt/blob/devel/dlt/common/destination/reference.py).

> 💡 See how the `snowflake` destination adds additional authorization methods and configuration options.

## 3. Set the destination capabilities
`dlt` needs to know a few things about the destination to correctly work with it. These are stored in the `capabilities()` function in `__init__.py`.

* Supported loader file formats both for direct and staging loading (see below).
* `escape_identifier`, a function that escapes database identifiers, i.e., table or column names. Look in the `dlt.common.data_writers.escape` module to see how this is implemented for existing destinations.
* `escape_literal`, a function that escapes string literals. It is only used if the destination supports the **insert-values** loader format (also see existing implementations in `dlt.common.data_writers.escape`).
* `decimal_precision`, precision and scale of decimal/numeric types. Also used to create the right decimal types in loader files, i.e., Parquet.
* `wei_precision`, precision and scale of decimal/numeric to store very large (up to 2**256) integers. Specify maximum precision for scale 0.
* `max_identifier_length`, maximum length of table and schema/dataset names.
* `max_column_identifier_length`, maximum length of column names.
* `naming_convention`, a name or naming convention module that maps the input alphabet (i.e., JSON identifiers) to the destination alphabet. Leave the default - it is [very conservative](../general-usage/schema.md#naming-convention).
* `max_query_length`, `is_max_query_length_in_bytes`, `max_text_data_type_length`, `is_max_text_data_type_length_in_bytes` - tells `dlt` the maximum length of text queries and of text data types.
* `supports_transactions` tells if the destination supports transactions.
* `timestamp_precision` sets the fidelity of the timestamp/datetime type: 0 - 9 (from seconds to nanoseconds), default is 6.
* `supports_ddl_transactions` tells if the destination supports DDL transactions.
* `alter_add_multi_column` tells if the destination can add multiple columns in an **ALTER** statement.
* `supports_truncate_command` tells `dlt` if the **truncate** command is used, otherwise it will use **DELETE** to clear tables.
* `schema_supports_numeric_precision` whether numeric data types support precision/scale configuration.
* `max_rows_per_insert`, maximum number of rows supported per insert statement, used with the `insert-values` loader file format (set to `None` for no limit). E.g., MS SQL has a limit of 1000 rows per statement, but most databases have no limit and the statement is divided according to `max_query_length`.

### Supported loader file formats
Specify which [loader file formats](../dlt-ecosystem/file-formats/) your destination will support directly and via [storage staging](../dlt-ecosystem/staging.md). Direct support means that the destination is able to load a local file or supports the INSERT command. Loading via staging involves using `filesystem` to send the load package to a (typically) bucket storage and then load from there.

> 💡 The [insert-values](../dlt-ecosystem/file-formats/insert-format.md) data format generates large INSERT statements that are executed on the destination. If you have any other option for local loading, avoid using this format. It is typically slower and requires the use of the bullet-proof `escape_literal` function.

* `preferred_loader_file_format` - a file format that will be used by default to load data from the local file system. Set to `None` if direct loading is not supported.
* `supported_loader_file_formats` - file formats that can be loaded from the local file system to the destination. Set to `[]` if direct loading is not supported.
* `preferred_staging_file_format` - a file format that will be used by default when `staging` is enabled. Set to `None` if the destination can't load from staging.
* `supported_staging_file_formats` - file formats that are supported to be loaded from staging storage. Set to `[]` if the destination can't load from staging.

> 💡 Mind that for each file type, you'll need to implement a load job (which in most cases is a `COPY` command to which you pass a file path and file type).

> 💡 Postgres does not support staging and any other file format beyond **insert-values**. Check the `snowflake` capabilities for a destination that supports all possible formats.

### Escape identifiers and literals
The default `escape_identifier` function escapes `"` and '\\' and quotes the identifier with `"`. This is standard SQL behavior. Mind that if you use the default naming convention, `dlt` normalizes identifiers to an alphabet that does not accept any special characters. Users are able to change the naming convention in the configuration, so the correct escape function is still important.

> 💡 The **postgres** destination that you modify is using a standard implementation that you may keep.

You should avoid providing a custom `escape_literal` function by not enabling `insert-values` for your destination.

### Enable / disable case-sensitive identifiers
Specify if the destination supports case-sensitive identifiers by setting `has_case_sensitive_identifiers` to `True` (or `False` if otherwise). Some case-sensitive destinations (i.e., **Snowflake** or **Postgres**) support case-insensitive identifiers via case folding; i.e., **Snowflake** considers all upper-case identifiers as case-insensitive (set `casefold_identifier` to `str.upper`), **Postgres** does the same with lower-case identifiers (`str.lower`).
Some case-insensitive destinations (i.e., **Athena** or **Redshift**) case-fold (i.e., lower case) all identifiers and store them as such. In that case, set `casefold_identifier` to `str.lower` as well.

## Adjust the SQL client
The **SQL client** is a wrapper over `dbapi` and its main role is to provide a consistent interface for executing SQL statements, managing transactions, and (probably the most important) helping handle errors via classifying exceptions. Here are a few things you should pay attention to:

- When opening the connection: add the current dataset name to the search path, set the session timezone to UTC.
- Transactions: typically, to begin a transaction, you need to disable auto-commit (like the `postgres` implementation does).
- `execute_query`: dlt uses `%s` to represent dbi API query parameters. See `duckdb sql_client` for a crude way to align your `dbapi` client if it uses other parameter placeholders.
- `execute_fragments`: if your `dbapi` client does not provide a method to join SQL fragments without a full string copy, just delete the `postgres` override. The base class just joins strings.

### Fully qualified names
When created, `sql_client` is bound to a particular dataset name (which typically corresponds to a database schema). Most of the database engines follow the usual rules of qualifying and quoting (**"schema"."table"."column"**), but there are exceptions like `BigQuery` or `Motherduck`. You have full control over generating identifiers via:
* `fully_qualified_dataset_name` returns a fully qualified dataset name.
* `make_qualified_table_name` does the same but for a given table name.

### `dbapi` exceptions
`dlt` must be able to distinguish a few error cases for the loading to work properly. Unfortunately, error reporting is not very well defined by `dbapi`, and even the existing exception tree is not used consistently across implementations.

`_make_database_exception` method wraps an incoming `Exception` in one of the exception types required by `dlt`:
- `DatabaseUndefinedRelation`: raised when a schema or table that `dlt` tries to reference is undefined. It is important to detect this case exactly: via specific `dbapi` exceptions (like in the case of `postgres` and `duckdb`) or by detecting the proper category of exceptions and inspecting the error codes or messages (see `redshift` and `snowflake`).
- `DatabaseTerminalException`: errors during loading that will permanently fail a job and should not retry. `IntegrityError`, `ProgrammingError`, and most of the `DataError` belong to this class. (example: decimal value out of range, insert NULL in non-NULL columns)
- `DatabaseTransientException`: all other exceptions. We also include `SyntaxError` (if it exists in a particular `dbapi` implementation) here.

> 💡 How this works in practice: we have a set of tests for all relevant error cases in [test_sql_client.py](https://github.com/dlt-hub/dlt/blob/devel/tests/load/test_sql_client.py), this way we make sure that the new sql_client behaves correctly.

### What the base class assumes
 - that `INFORMATION_SCHEMA` exists from which we can take basic information on `SCHEMATA` and `COLUMNS`
 - `CREATE SCHEMA` and `DROP SCHEMA` (see how `BigQuery` overrides that)
 - `DELETE` or `TRUNCATE` is available to clear tables without dropping
 - `DROP TABLE` only for CLI command (`pipeline drop`)

## Adjust the job client
The job client is responsible for creating/starting load jobs and managing the schema updates. Here we'll adjust the `SqlJobClientBase` base class which uses the `sql_client` to manage the destination. Typically, only a few methods need to be overridden by a particular implementation. The job client code customarily resides in a file named `<destination_name>.py`, i.e., `postgres.py`, and is exposed in `factory.py` by the `client_class` property on the destination class.

### Database type mappings
You must map `dlt` data types to destination data types. For this, you can implement a subclass of `TypeMapper`. You can specify there dicts to map `dlt` data types to destination data types, with or without precision. A few tricks to remember:
* the database types must be exactly those as used in `INFORMATION_SCHEMA.COLUMNS`
* decimal precision and scale are filled from the capabilities (in all our implementations)
* until now, all destinations could handle binary types
* we always try to map the `json` type into `JSON` type in the destination. If that does not work, you can try mapping it into a string. See how we do that for various destinations.
* the reverse mapping of types is sometimes tricky, i.e., you may not be able to detect nested types (your destination lacks JSON support). This is not really needed during schema updates and loading (just for testing), so in general, you should be fine.

### Table and column hints
You can map hints present for tables and columns (i.e., `cluster`, `sort`, `partition`) to generate specific DDL for columns and tables. See `_get_column_def_sql` in various destinations.
You can also add hints (i.e., indexes, partition clauses) to tables via `_get_table_update_sql` - see the `BigQuery` implementation for a good example.

### Participate in staging dataset merge and replace
`dlt` supports merging and transactional replace via a **staging dataset** living alongside the destination dataset. `SqlJobClientBase` participates in this mechanism by default. In essence, each time a job is completed, `dlt` checks which table was updated and if there are no remaining jobs for that table and its nested and root tables (all together called **table chain**). If the table chain is fully loaded, `dlt` executes SQL transformations that move/merge data from the staging dataset to the destination dataset (that, as you can expect, happens also via jobs, of type `sql` that are dynamically created).

The generated SQL is quite simple, and we were able to run it on all existing destinations (we may introduce `sqlglot` to handle future cases). The SQL used requires:
- SELECT, INSERT, DELETE/TRUNCATE statements
- WINDOW functions for merge.

In case of destinations that do not allow data modifications, you can opt out from both replace and merge:
- Override the `get_truncate_destination_table_dispositions` method and return an empty list so your tables are never truncated.
- Override the `get_stage_dispositions` and return an empty list to opt out from any operations on the staging dataset.

### What the base class assumes
* DDL to create and add columns to tables is available.
* It is possible to SELECT data.
* It is possible to INSERT data (in order to complete the package and store the updated schema).

> 💡 Talk to us on Slack if your destination is fully read-only.


## 6. Implement load jobs
Load jobs ensure that all files in the load package are loaded to the destination. `dlt` creates a single job per file and [makes sure that it transitions to the `completed` state. (look for `LoadJob`)](https://github.com/dlt-hub/dlt/blob/devel/dlt/common/destination/reference.py)

The file name of the job is used as the job ID, and both sync and async execution is supported. The executor is multi-threaded. Each job starts in a separate thread, and its completion status is checked from the main thread.

Jobs are typically very simple and just execute INSERT or COPY commands. They do not replace nor merge data themselves.

### Enable insert jobs
If you use the **insert-values** loader file format, then derive your job client from `InsertValuesJobClient`. `postgres.py` does exactly that.

Look at `snowflake.py` for a destination that does not use the **insert-values**.

### Copy jobs from local and remote files
`dlt` allows chaining two destinations to create a [storage stage (typically on a bucket)](../dlt-ecosystem/staging.md). The staging destination (currently `filesystem`) will copy new files, complete the corresponding jobs, and for each of them, it will create a **reference job** that will be passed to a destination to execute.

The `postgres` destination does not implement any copy jobs.
- See `RedshiftCopyFileLoadJob` in `redshift.py` for how we create and start a copy job from a bucket. It uses `CopyRemoteFileLoadJob` base to handle the references and creates a `COPY` SQL statement in the `execute()` method.
- See `SnowflakeLoadJob` in `snowflake.py` for how to implement a job that can load local and reference files. It also forwards AWS credentials from the staging destination. At the end, the code just generates a COPY command for various loader file formats.

## 7. Expose your destination to dlt

The `Destination` subclass in `dlt.destinations.impl.<destination_name>.factory` module is the entry point for the destination.
Add an import to your factory in [`dlt.destinations.__init__`](https://github.com/dlt-hub/dlt/blob/devel/dlt/destinations/__init__.py). `dlt` looks in this module when you reference a destination by name, i.e., `dlt.pipeline(..., destination="postgres")`.

## Testing
We can quickly repurpose existing GitHub source and `secrets.toml` already present in the project to test a new destination. Let's assume that the module name is `presto`, the same for the destination name and config section name. Here's our testing script `github_pipeline.py`:
```py
import dlt

from github import github_repo_events
from presto import presto  # importing destination factory

def load_airflow_events() -> None:
    """Loads airflow events. Shows incremental loading. Forces anonymous access token."""
    pipeline = dlt.pipeline(
        "github_events", destination=presto(), dataset_name="airflow_events"
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

Note that in the script above, we import the `presto` module and then pass it in the `destination` argument to `dlt.pipeline`. The GitHub pipeline will load the events in `append` mode. You may force `replace` and `merge` modes in `pipeline.run` to check more advanced behavior of the destination.

After executing the pipeline script:
```text
python github_pipeline.py
got page https://api.github.com/repos/apache/airflow/events?per_page=100, requests left: 59
got page https://api.github.com/repositories/33884891/events?per_page=100&page=2, requests left: 58
got page https://api.github.com/repositories/33884891/events?per_page=100&page=3, requests left: 57
Pipeline github_events completed in 4.56 seconds
1 load package(s) were loaded to destination presto and into dataset airflow_events
The presto destination used postgres://loader:***@localhost:5432/dlt_data location to store data
Load package 1690628947.953597 is LOADED and contains no failed jobs
```

You can use `dlt pipeline show github_events` to view data in the destination.

