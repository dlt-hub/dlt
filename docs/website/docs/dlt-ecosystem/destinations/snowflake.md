---
title: Snowflake
description: Snowflake `dlt` destination
keywords: [Snowflake, destination, data warehouse]
---

# Snowflake

**1. Initialize a project with a pipeline that loads to snowflake by running**

```
dlt init chess snowflake
```

**2. Install the necessary dependencies for snowflake by running**

```
pip install -r requirements.txt
```

This will install dlt with **snowflake** extra which contains Snowflake Python dbapi client.

**3. Create a new database, user and give dlt access**

Read the next chapter below.

**4. Enter your credentials into `.dlt/secrets.toml`.** It should now look like

```toml
[destination.snowflake.credentials]
database = "dlt_data"
password = "<password>"
username = "loader"
host = "kgiotue-wn98412"
warehouse = "COMPUTE_WH"
role = "DLT_LOADER_ROLE"
```

In case of snowflake **host** is your
[Account Identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier). You can get
in **Admin**/**Accounts** by copying account url: https://kgiotue-wn98412.snowflakecomputing.com and
extracting the host name (**kgiotue-wn98412**)

The **warehouse** and **role** are optional if you assign defaults to your user. In the example
below we do not do that, so we set them explicitly.

## Setup the database user and permissions

Instructions below assume that you use the default account setup that you get after creating
Snowflake account. You should have default warehouse named **COMPUTE_WH** and snowflake account.
Below we create a new database, user and assign permissions. The permissions are very generous. A
more experienced user can easily reduce `dlt` permissions to just one schema in the database.

```sql
--create database with standard settings
CREATE DATABASE dlt_data;
-- create new user - set your password here
CREATE USER loader WITH PASSWORD='<password>'
-- we assign all permission to a role
CREATE ROLE DLT_LOADER_ROLE;
GRANT ROLE DLT_LOADER_ROLE TO USER loader;
-- give database access to new role
GRANT USAGE ON DATABASE dlt_data TO DLT_LOADER_ROLE;
-- allow dlt to create new schemas
GRANT CREATE SCHEMA ON DATABASE dlt_data TO ROLE DLT_LOADER_ROLE
-- allow access to a warehouse named COMPUTE_WH
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO DLT_LOADER_ROLE;
-- grant access to all future schemas and tables in the database
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE dlt_data TO DLT_LOADER_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE dlt_data TO DLT_LOADER_ROLE;
```

Now you can use the user named `LOADER` to access database `DLT_DATA` and log in with specified
password.

You can also decrease the suspend time for your warehouse to 1 minute (**Admin**/**Warehouses** in
Snowflake UI)

## Authentication types

Snowflake destination accepts two authentication type

- password authentication
- [key pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth)

The **password authentication** is not any different from other databases like Postgres or Redshift.
`dlt` follows the same syntax as
[SQLAlchemy dialect](https://docs.snowflake.com/en/developer-guide/python-connector/sqlalchemy#required-parameters).

You can also pass credentials as a database connection string. For example:

```toml
destination.postgres.snowflake="snowflake://loader:<password>@kgiotue-wn98412/dlt_data?warehouse=COMPUTE_WH&role=DLT_LOADER_ROLE"
```

In **key pair authentication** you replace password with a private key exported in PEM format. The
key may be encrypted. In that case you must provide a passphrase.

```toml
[destination.snowflake.credentials]
database = "dlt_data"
username = "loader"
host = "kgiotue-wn98412"
private_key = """-----BEGIN ENCRYPTED PRIVATE KEY-----
    MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDz5LZoccgKZ4jH
    ...
-----END PRIVATE KEY-----
private_key_passphrase="passphrase"
"""
```

We allow to pass private key and passphrase in connection string. Please url encode the private key
and passphrase.

```toml
destination.postgres.snowflake="snowflake://loader:<password>@kgiotue-wn98412/dlt_data?private_key=<url encoded pem>&private_key_passphrase=<url encoded passphrase>"
```

## Data loading

The data is loaded using internal Snowflake stage. We use `PUT` command and per-table built-in
stages by default. Stage files are immediately removed (if not specified otherwise).

Currently **jsonl** (default) and **parquet** file formats are supported.

### Table and column identifiers

Snowflake makes all unquoted identifiers uppercase and then resolves them case-insensitive in SQL
statements. `dlt` (effectively) does not quote identifies in DDL preserving default behavior.

Names of tables and columns in [schemas](../../general-usage/schema.md) are kept in lower case like
for all other destinations. This is the pattern we observed in other tools ie. `dbt`. In case of
`dlt` it is however trivial to define your own uppercase
[naming convention](../../general-usage/schema.md#naming-convention)

## Additional destination options

You can define your own stage to PUT files and disable removing of the staged files after loading.

```toml
[destination.snowflake]
# Use an existing named stage instead of the default. Default uses the implicit table stage per table
stage_name="DLT_STAGE"
# Whether to keep or delete the staged files after COPY INTO succeeds
keep_staged_files=true
```

## dbt support

This destination
[integrates with dbt](../transformations/dbt.md)
via [dbt-snowflake](https://github.com/dbt-labs/dbt-snowflake). Both password and key pair
authentication is supported and shared with dbt runners.
