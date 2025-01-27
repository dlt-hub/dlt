---
title: MotherDuck
description: MotherDuck `dlt` destination
keywords: [MotherDuck, duckdb, destination, data warehouse]
---

# MotherDuck

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
export motherduck_token='<token>'
```
in that case you can skip **password** / **motherduck_token** secret.

**database** defaults to `my_db`.

More in Motherduck [documentation](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/authenticating-to-motherduck/#storing-the-access-token-as-an-environment-variable)
:::

**4. Run the pipeline**
```sh
python3 chess_pipeline.py
```

### Motherduck connection identifier
We enable Motherduck to identify that the connection is created by `dlt`. Motherduck will use this identifier to better understand the usage patterns
associated with `dlt` integration. The connection identifier is `dltHub_dlt/DLT_VERSION(OS_NAME)`.

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
- [Load data from Notion to MotherDuck in python with dlt](https://dlthub.com/docs/pipelines/notion/load-data-with-python-from-notion-to-motherduck)
- [Load data from Pinterest to MotherDuck in python with dlt](https://dlthub.com/docs/pipelines/pinterest/load-data-with-python-from-pinterest-to-motherduck)
- [Load data from MongoDB to MotherDuck in python with dlt](https://dlthub.com/docs/pipelines/mongodb/load-data-with-python-from-mongodb-to-motherduck)
- [Load data from Jira to MotherDuck in python with dlt](https://dlthub.com/docs/pipelines/jira/load-data-with-python-from-jira-to-motherduck)
- [Load data from Trello to MotherDuck in python with dlt](https://dlthub.com/docs/pipelines/trello/load-data-with-python-from-trello-to-motherduck)
- [Load data from HubSpot to MotherDuck in python with dlt](https://dlthub.com/docs/pipelines/hubspot/load-data-with-python-from-hubspot-to-motherduck)
- [Load data from Slack to MotherDuck in python with dlt](https://dlthub.com/docs/pipelines/slack/load-data-with-python-from-slack-to-motherduck)
- [Load data from Mux to MotherDuck in python with dlt](https://dlthub.com/docs/pipelines/mux/load-data-with-python-from-mux-to-motherduck)
- [Load data from Bitbucket to MotherDuck in python with dlt](https://dlthub.com/docs/pipelines/bitbucket/load-data-with-python-from-bitbucket-to-motherduck)
- [Load data from Aladtec to MotherDuck in python with dlt](https://dlthub.com/docs/pipelines/aladtec/load-data-with-python-from-aladtec-to-motherduck)

