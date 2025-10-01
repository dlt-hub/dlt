---
title: MotherDuck / DuckLake
description: MotherDuck and DuckLake `dlt` destination
keywords: [MotherDuck, duckdb, destination, data warehouse, DuckLake]
---

# MotherDuck / DuckLake

## Install dlt with MotherDuck
**To install the dlt library with MotherDuck dependencies:**
```sh
pip install "dlt[motherduck]"
```

:::tip
If you see a lot of retries in your logs with various timeouts, decrease the number of load workers to 3-5 depending on the quality of your internet connection. Add the following to your `config.toml`:
```toml
[load]
workers=3
```
or export the **LOAD__WORKERS=3** env variable. See more in [performance](../../reference/performance.md)
:::

## Setup guide

**1. Initialize a project with a pipeline that loads to MotherDuck by running**
```sh
dlt init chess motherduck
```

**2. Install the necessary dependencies for MotherDuck by running**
```sh
pip install -r requirements.txt
```

This will install dlt with the **motherduck** extra which contains **duckdb** and **pyarrow** dependencies.

**3. Add your MotherDuck token to `.dlt/secrets.toml`**
```toml
[destination.motherduck.credentials]
database = "dlt_data_3"
password = "<your token here>"
```
Paste your **service token** into the password field. The `database` field is optional, but we recommend setting it. MotherDuck will create this database (in this case `dlt_data_3`) for you.

Alternatively, you can use the connection string syntax.
```toml
[destination]
motherduck.credentials="md:dlt_data_3?motherduck_token=<my service token>"
```

:::tip
Motherduck now supports configurable **access tokens**. Please refer to the [documentation](https://motherduck.com/docs/key-tasks/authenticating-to-motherduck/#authentication-using-an-access-token)

You can pass token in a native Motherduck environment variable:
```sh
export MOTHERDUCK_TOKEN='<token>'
```
in that case you can skip **password** / **motherduck_token** secret.

**database** defaults to `my_db`.

More in Motherduck [documentation](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/authenticating-to-motherduck/#storing-the-access-token-as-an-environment-variable)
:::

**4. Run the pipeline**
```sh
python3 chess_pipeline.py
```

## Destination capabilities

The following table shows the capabilities of the Motherduck destination:

| Feature | Value | More |
|---------|-------|------|
| Preferred Loader File Format | parquet | [Data Types](../../general-usage/schema#data-types) |
| Supported Loader File Formats | insert_values, parquet, jsonl, model | [File Formats](../file-formats/) |
| Has Case Sensitive Identifiers | False | [Data Types](../../general-usage/schema#data-types) |
| Supported Merge Strategies | delete-insert, upsert, scd2 | [Merge Loading](../../general-usage/merge-loading#merge-strategies) |
| Supported Replace Strategies | truncate-and-insert, insert-from-staging | [Merge Loading](../../general-usage/merge-loading#merge-strategies) |
| Supports Tz Aware Datetime | True | [Data Types](../../general-usage/schema#data-types) |
| Supports Naive Datetime | True | [Data Types](../../general-usage/schema#data-types) |

*This table shows the supported features of the Motherduck destination in dlt.*


### DuckLake setup
DuckLake can be used to manage and persist your MotherDuck databases on external object storage like S3. This is especially useful if you want more control over where your data is stored or if you’re integrating with your own cloud infrastructure.
The steps below show how to set up a DuckLake-managed database backed by S3.

**1. Create the S3-Backed DuckLake Database**
You can create a DuckLake-managed database using the following SQL command, which should be run in the MotherDuck SQL Editor or any SQL client connected to your MotherDuck account:
```sql
CREATE DATABASE my_ducklake (
  TYPE DUCKLAKE,
  DATA_PATH 's3://mybucket/my_optional_path/'
);
```

**2. Register S3 Credentials with a MotherDuck Secret**
```sql
CREATE SECRET my_secret IN MOTHERDUCK (
  TYPE S3,
  KEY_ID 'my_s3_access_key',
  SECRET 'my_s3_secret_key',
  REGION 'my-bucket-region'
);
```

**3. Configure `secrets.toml` in your dlt project**
Your `secrets.toml` only needs to reference the DuckLake database and your service token:
```toml
[destination.motherduck.credentials]
database = "my_ducklake"
password = "<your token here>"
```
As long as the DuckLake database and the corresponding S3 secret have been set up in MotherDuck, dlt can load data into it without requiring direct access to the S3 credentials.

With this setup, you can now load data into DuckLake tables using dlt, just like with any DuckDB destination through MotherDuck.

### Motherduck connection identifier
We enable Motherduck to identify that the connection is created by `dlt`. Motherduck will use this identifier to better understand the usage patterns
associated with `dlt` integration. The connection identifier is `dltHub_dlt/DLT_VERSION(OS_NAME)`.

### Additional configuration
Query string will be passed to `duckdb` connection. Several global configs may be set using it:
```toml
[destination]
motherduck.credentials="md:dlt_data_3?dbinstance_inactivity_ttl=0s"
```
will disable connection caching.

Additional `duckdb` configuration, [where you can set up extensions, pragmas, global and local setting](duckdb.md#additional-configuration), is also supported.

## Write disposition
All write dispositions are supported.

## Data loading
By default, Parquet files and the `COPY` command are used to move files to the remote duckdb database. All write dispositions are supported.

The **INSERT** format is also supported and will execute large INSERT queries directly into the remote database. This method is significantly slower and may exceed the maximum query size, so it is not advised.

## dbt support
This destination [integrates with dbt](../transformations/dbt/dbt.md) via [dbt-duckdb](https://github.com/jwills/dbt-duckdb), which is a community-supported package. `dbt` version >= 1.7 is required.

## Multi-statement transaction support
Motherduck supports multi-statement transactions. This change happened with `duckdb 0.10.2`.

## Syncing of `dlt` state
This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

## Troubleshooting

### My database is attached in read-only mode
i.e., `Error: Invalid Input Error: Cannot execute statement of type "CREATE" on database "dlt_data" which is attached in read-only mode!`
We encountered this problem for databases created with `duckdb 0.9.x` and then migrated to `0.10.x`. After switching to `1.0.x` on Motherduck, all our databases had permission "read-only" visible in UI. We could not figure out how to change it, so we dropped and recreated our databases.

### I see some exception with home_dir missing when opening `md:` connection.
Some internal component (HTTPS) requires the **HOME** env variable to be present. Export such a variable to the command line. Here is what we do in our tests:
```py
os.environ["HOME"] = "/tmp"
```
before opening the connection.


## Additional Setup guides
- [Load data from Aladtec to MotherDuck in python with dlt](https://dlthub.com/docs/pipelines/aladtec/load-data-with-python-from-aladtec-to-motherduck)
- [Load data from DigitalOcean to MotherDuck in python with dlt](https://dlthub.com/docs/pipelines/digitalocean/load-data-with-python-from-digitalocean-to-motherduck)
- [Load data from Coinbase to MotherDuck in python with dlt](https://dlthub.com/docs/pipelines/coinbase/load-data-with-python-from-coinbase-to-motherduck)
- [Load data from PostgreSQL to MotherDuck in python with dlt](https://dlthub.com/docs/pipelines/sql_database_postgres/load-data-with-python-from-sql_database_postgres-to-motherduck)
- [Load data from Imgur to MotherDuck in python with dlt](https://dlthub.com/docs/pipelines/imgur/load-data-with-python-from-imgur-to-motherduck)
- [Load data from Shopify to MotherDuck in python with dlt](https://dlthub.com/docs/pipelines/shopify_dlt/load-data-with-python-from-shopify_dlt-to-motherduck)
- [Load data from GitHub to MotherDuck in python with dlt](https://dlthub.com/docs/pipelines/github/load-data-with-python-from-github-to-motherduck)
- [Load data from X to MotherDuck in python with dlt](https://dlthub.com/docs/pipelines/x/load-data-with-python-from-x-to-motherduck)
- [Load data from IBM Db2 to MotherDuck in python with dlt](https://dlthub.com/docs/pipelines/sql_database_db2/load-data-with-python-from-sql_database_db2-to-motherduck)
- [Load data from Harvest to MotherDuck in python with dlt](https://dlthub.com/docs/pipelines/harvest/load-data-with-python-from-harvest-to-motherduck)

