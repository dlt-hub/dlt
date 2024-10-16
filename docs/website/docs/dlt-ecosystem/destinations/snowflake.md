---
title: Snowflake
description: Snowflake `dlt` destination
keywords: [Snowflake, destination, data warehouse]
---

# Snowflake

## Install `dlt` with Snowflake
**To install the `dlt` library with Snowflake dependencies, run:**
```sh
pip install "dlt[snowflake]"
```

## Setup guide

**1. Initialize a project with a pipeline that loads to Snowflake by running:**
```sh
dlt init chess snowflake
```

**2. Install the necessary dependencies for Snowflake by running:**
```sh
pip install -r requirements.txt
```
This will install `dlt` with the `snowflake` extra, which contains the Snowflake Python dbapi client.

**3. Create a new database, user, and give `dlt` access.**

Read the next chapter below.

**4. Enter your credentials into `.dlt/secrets.toml`.**
It should now look like this:
```toml
[destination.snowflake.credentials]
database = "dlt_data"
password = "<password>"
username = "loader"
host = "kgiotue-wn98412"
warehouse = "COMPUTE_WH"
role = "DLT_LOADER_ROLE"
```
In the case of Snowflake, the **host** is your [Account Identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier). You can get it in **Admin**/**Accounts** by copying the account URL: https://kgiotue-wn98412.snowflakecomputing.com and extracting the host name (**kgiotue-wn98412**).

The **warehouse** and **role** are optional if you assign defaults to your user. In the example below, we do not do that, so we set them explicitly.

### Set up the database user and permissions
The instructions below assume that you use the default account setup that you get after creating a Snowflake account. You should have a default warehouse named **COMPUTE_WH** and a Snowflake account. Below, we create a new database, user, and assign permissions. The permissions are very generous. A more experienced user can easily reduce `dlt` permissions to just one schema in the database.
```sql
-- create database with standard settings
CREATE DATABASE dlt_data;
-- create new user - set your password here
CREATE USER loader WITH PASSWORD='<password>';
-- we assign all permissions to a role
CREATE ROLE DLT_LOADER_ROLE;
GRANT ROLE DLT_LOADER_ROLE TO USER loader;
-- give database access to new role
GRANT USAGE ON DATABASE dlt_data TO DLT_LOADER_ROLE;
-- allow `dlt` to create new schemas
GRANT CREATE SCHEMA ON DATABASE dlt_data TO ROLE DLT_LOADER_ROLE;
-- allow access to a warehouse named COMPUTE_WH
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO DLT_LOADER_ROLE;
-- grant access to all future schemas and tables in the database
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE dlt_data TO DLT_LOADER_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE dlt_data TO DLT_LOADER_ROLE;
```

Now you can use the user named `LOADER` to access the database `DLT_DATA` and log in with the specified password.

You can also decrease the suspend time for your warehouse to 1 minute (**Admin**/**Warehouses** in Snowflake UI).

### Authentication types

Snowflake destination accepts three authentication types:
- Password authentication
- [Key pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth)
- OAuth authentication

The **password authentication** is not any different from other databases like Postgres or Redshift. `dlt` follows the same syntax as the [SQLAlchemy dialect](https://docs.snowflake.com/en/developer-guide/python-connector/sqlalchemy#required-parameters).

You can also pass credentials as a database connection string. For example:
```toml
# Keep it at the top of your TOML file, before any section starts
destination.snowflake.credentials="snowflake://loader:<password>@kgiotue-wn98412/dlt_data?warehouse=COMPUTE_WH&role=DLT_LOADER_ROLE"

```

In **key pair authentication**, you replace the password with a private key string that should be in Base64-encoded DER format ([dbt also recommends](https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup#key-pair-authentication) base64-encoded private keys for Snowflake connections). The private key may also be encrypted. In that case, you must provide a passphrase alongside the private key.
```toml
[destination.snowflake.credentials]
database = "dlt_data"
username = "loader"
host = "kgiotue-wn98412"
private_key = "LS0tLS1CRUdJTiBFTkNSWVBURUQgUFJJ....Qo="
private_key_passphrase="passphrase"
```
> You can easily get the base64-encoded value of your private key by running `base64 -i <path-to-private-key-file>.pem` in your terminal

If you pass a passphrase in the connection string, please URL encode it.
```toml
# Keep it at the top of your TOML file, before any section starts
destination.snowflake.credentials="snowflake://loader:<password>@kgiotue-wn98412/dlt_data?private_key=<base64 encoded pem>&private_key_passphrase=<url encoded passphrase>"
```

In **OAuth authentication**, you can use an OAuth provider like Snowflake, Okta, or an external browser to authenticate. In the case of Snowflake OAuth, you pass your `authenticator` and refresh `token` as below:
```toml
[destination.snowflake.credentials]
database = "dlt_data"
username = "loader"
authenticator="oauth"
token="..."
```
or in the connection string as query parameters.

In the case of external authentication, you need to find documentation for your OAuth provider. Refer to Snowflake [OAuth](https://docs.snowflake.com/en/user-guide/oauth-intro) for more details.

### Additional connection options

We pass all query parameters to the `connect` function of the Snowflake Python Connector. For example:

```toml
[destination.snowflake.credentials]
database = "dlt_data"
authenticator="oauth"
[destination.snowflake.credentials.query]
timezone="UTC"
# keep session alive beyond 4 hours
client_session_keep_alive=true
```

This will set the timezone and session keep alive. Mind that if you use TOML, your configuration is typed. The alternative:
`"snowflake://loader/dlt_data?authenticator=oauth&timezone=UTC&client_session_keep_alive=true"`
will pass `client_session_keep_alive` as a string to the connect method (which we didn't verify if it works).

### Write disposition

All write dispositions are supported.

If you set the [`replace` strategy](../../general-usage/full-loading.md) to `staging-optimized`, the destination tables will be dropped and recreated with a [clone command](https://docs.snowflake.com/en/sql-reference/sql/create-clone) from the staging tables.

### Data loading

The data is loaded using an internal Snowflake stage. We use the `PUT` command and per-table built-in stages by default. Stage files are kept by default, unless specified otherwise via the `keep_staged_files` parameter:

```toml
[destination.snowflake]
keep_staged_files = false
```

### Data types
`snowflake` supports various timestamp types, which can be configured using the column flags `timezone` and `precision` in the `dlt.resource` decorator or the `pipeline.run` method.

- **Precision**: Allows you to specify the number of decimal places for fractional seconds, ranging from 0 to 9. It can be used in combination with the `timezone` flag.
- **Timezone**:
  - Setting `timezone=False` maps to `TIMESTAMP_NTZ`.
  - Setting `timezone=True` (or omitting the flag, which defaults to `True`) maps to `TIMESTAMP_TZ`.

#### Example precision and timezone: TIMESTAMP_NTZ(3)
```py
@dlt.resource(
    columns={"event_tstamp": {"data_type": "timestamp", "precision": 3, "timezone": False}},
    primary_key="event_id",
)
def events():
    yield [{"event_id": 1, "event_tstamp": "2024-07-30T10:00:00.123"}]

pipeline = dlt.pipeline(destination="snowflake")
pipeline.run(events())
```

## Supported file formats
* [insert-values](../file-formats/insert-format.md) is used by default.
* [parquet](../file-formats/parquet.md) is supported.
* [jsonl](../file-formats/jsonl.md) is supported.
* [csv](../file-formats/csv.md) is supported.

When staging is enabled:
* [jsonl](../file-formats/jsonl.md) is used by default.
* [parquet](../file-formats/parquet.md) is supported.
* [csv](../file-formats/csv.md) is supported.

:::caution
When loading from `parquet`, Snowflake will store `json` types (JSON) in `VARIANT` as a string. Use the `jsonl` format instead or use `PARSE_JSON` to update the `VARIANT` field after loading.
:::

### Custom CSV formats
By default, we support the CSV format [produced by our writers](../file-formats/csv.md#default-settings), which is comma-delimited, with a header, and optionally quoted.

You can configure your own formatting, i.e., when [importing](../../general-usage/resource.md#import-external-files) external `csv` files.
```toml
[destination.snowflake.csv_format]
delimiter="|"
include_header=false
on_error_continue=true
```
This will read a `|` delimited file, without a header, and will continue on errors.

Note that we ignore missing columns `ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE` and we will insert NULL into them.

## Supported column hints
Snowflake supports the following [column hints](../../general-usage/schema#tables-and-columns):
* `cluster` - Creates a cluster column(s). Many columns per table are supported and only when a new table is created.

## Table and column identifiers
Snowflake supports both case-sensitive and case-insensitive identifiers. All unquoted and uppercase identifiers resolve case-insensitively in SQL statements. Case-insensitive [naming conventions](../../general-usage/naming-convention.md#case-sensitive-and-insensitive-destinations) like the default **snake_case** will generate case-insensitive identifiers. Case-sensitive (like **sql_cs_v1**) will generate
case-sensitive identifiers that must be quoted in SQL statements.

:::note
Names of tables and columns in [schemas](../../general-usage/schema.md) are kept in lowercase like for all other destinations. This is the pattern we observed in other tools, i.e., `dbt`. In the case of `dlt`, it is, however, trivial to define your own uppercase [naming convention](../../general-usage/schema.md#naming-convention).
:::

## Staging support

Snowflake supports S3 and GCS as file staging destinations. `dlt` will upload files in the parquet format to the bucket provider and will ask Snowflake to copy their data directly into the db.

Alternatively to parquet files, you can also specify jsonl as the staging file format. For this, set the `loader_file_format` argument of the `run` command of the pipeline to `jsonl`.

### Snowflake and Amazon S3

Please refer to the [S3 documentation](./filesystem.md#aws-s3) to learn how to set up your bucket with the bucket_url and credentials. For S3, the `dlt` Redshift loader will use the AWS credentials provided for S3 to access the S3 bucket if not specified otherwise (see config options below). Alternatively, you can create a stage for your S3 Bucket by following the instructions provided in the [Snowflake S3 documentation](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration).
The basic steps are as follows:

* Create a storage integration linked to GCS and the right bucket.
* Grant access to this storage integration to the Snowflake role you are using to load the data into Snowflake.
* Create a stage from this storage integration in the PUBLIC namespace, or the namespace of the schema of your data.
* Also grant access to this stage for the role you are using to load data into Snowflake.
* Provide the name of your stage (including the namespace) to `dlt` like so:

To prevent `dlt` from forwarding the S3 bucket credentials on every command, and set your S3 stage, change these settings:

```toml
[destination]
stage_name="PUBLIC.my_s3_stage"
```

To run Snowflake with S3 as the staging destination:

```py
# Create a `dlt` pipeline that will load
# chess player data to the Snowflake destination
# via staging on S3
pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline',
    destination='snowflake',
    staging='filesystem', # add this to activate the staging location
    dataset_name='player_data'
)
```

### Snowflake and Google Cloud Storage

Please refer to the [Google Storage filesystem documentation](./filesystem.md#google-storage) to learn how to set up your bucket with the bucket_url and credentials. For GCS, you can define a stage in Snowflake and provide the stage identifier in the configuration (see config options below). Please consult the Snowflake Documentation on [how to create a stage for your GCS Bucket](https://docs.snowflake.com/en/user-guide/data-load-gcs-config). The basic steps are as follows:

* Create a storage integration linked to GCS and the right bucket.
* Grant access to this storage integration to the Snowflake role you are using to load the data into Snowflake.
* Create a stage from this storage integration in the PUBLIC namespace, or the namespace of the schema of your data.
* Also grant access to this stage for the role you are using to load data into Snowflake.
* Provide the name of your stage (including the namespace) to `dlt` like so:

```toml
[destination]
stage_name="PUBLIC.my_gcs_stage"
```

To run Snowflake with GCS as the staging destination:

```py
# Create a `dlt` pipeline that will load
# chess player data to the Snowflake destination
# via staging on GCS
pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline',
    destination='snowflake',
    staging='filesystem', # add this to activate the staging location
    dataset_name='player_data'
)
```

### Snowflake and Azure Blob Storage

Please refer to the [Azure Blob Storage filesystem documentation](./filesystem.md#azure-blob-storage) to learn how to set up your bucket with the bucket_url and credentials. For Azure, the Snowflake loader will use the filesystem credentials for your Azure Blob Storage container if not specified otherwise (see config options below). Alternatively, you can define an external stage in Snowflake and provide the stage identifier. Please consult the Snowflake Documentation on [how to create a stage for your Azure Blob Storage Container](https://docs.snowflake.com/en/user-guide/data-load-azure). The basic steps are as follows:

* Create a storage integration linked to Azure Blob Storage and the right container.
* Grant access to this storage integration to the Snowflake role you are using to load the data into Snowflake.
* Create a stage from this storage integration in the PUBLIC namespace, or the namespace of the schema of your data.
* Also, grant access to this stage for the role you are using to load data into Snowflake.
* Provide the name of your stage (including the namespace) to `dlt` like so:

```toml
[destination]
stage_name="PUBLIC.my_azure_stage"
```

To run Snowflake with Azure as the staging destination:

```py
# Create a `dlt` pipeline that will load
# chess player data to the Snowflake destination
# via staging on Azure
pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline',
    destination='snowflake',
    staging='filesystem', # add this to activate the staging location
    dataset_name='player_data'
)
```

## Additional destination options

You can define your own stage to PUT files and disable the removal of the staged files after loading.

```toml
[destination.snowflake]
# Use an existing named stage instead of the default. Default uses the implicit table stage per table
stage_name="DLT_STAGE"
# Whether to keep or delete the staged files after COPY INTO succeeds
keep_staged_files=true
```

### Setting up CSV format

You can provide [non-default](../file-formats/csv.md#default-settings) csv settings via configuration file or explicitly.

```toml
[destination.snowflake.csv_format]
delimiter="|"
include_header=false
on_error_continue=true
```
or
```py
from dlt.destinations import snowflake
from dlt.common.data_writers.configuration import CsvFormatConfiguration

csv_format = CsvFormatConfiguration(delimiter="|", include_header=False, on_error_continue=True)

dest_ = snowflake(csv_format=csv_format)
```
Above, we set the CSV file format without a header, with **|** as a separator, and we request to ignore lines with errors.

:::tip
You'll need these settings when [importing external files](../../general-usage/resource.md#import-external-files).
:::

### Query tagging

`dlt` [tags sessions](https://docs.snowflake.com/en/sql-reference/parameters#query-tag) that execute loading jobs with the following job properties:
* **source** - name of the source (identical with the name of the `dlt` schema)
* **resource** - name of the resource (if known, else empty string)
* **table** - name of the table loaded by the job
* **load_id** - load id of the job
* **pipeline_name** - name of the active pipeline (or empty string if not found)

You can define a query tag by defining a query tag placeholder in Snowflake credentials:

```toml
[destination.snowflake]
query_tag='{{"source":"{source}", "resource":"{resource}", "table": "{table}", "load_id":"{load_id}", "pipeline_name":"{pipeline_name}"}}'
```
which contains Python named formatters corresponding to tag names i.e., `{source}` will assume the name of the dlt source.

:::note
1. Query tagging is off by default. The `query_tag` configuration field is `None` by default and must be set to enable tagging.
2. Only sessions associated with a job are tagged. Sessions that migrate schemas remain untagged.
3. Jobs processing table chains (i.e., SQL merge jobs) will use the top-level table as **table**.
:::

### dbt support
This destination [integrates with dbt](../transformations/dbt/dbt.md) via [dbt-snowflake](https://github.com/dbt-labs/dbt-snowflake). Both password and key pair authentication are supported and shared with dbt runners.

### Syncing of `dlt` state
This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

### Snowflake connection identifier
We enable Snowflake to identify that the connection is created by `dlt`. Snowflake will use this identifier to better understand the usage patterns associated with `dlt` integration. The connection identifier is `dltHub_dlt`.

<!--@@@DLT_TUBA snowflake-->

