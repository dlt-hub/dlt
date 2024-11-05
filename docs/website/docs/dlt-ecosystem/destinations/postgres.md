---
title: Postgres
description: Postgres `dlt` destination
keywords: [postgres, destination, data warehouse]
---

# Postgres

## Install dlt with PostgreSQL
**To install the dlt library with PostgreSQL dependencies, run:**
```sh
pip install "dlt[postgres]"
```

## Setup guide

**1. Initialize a project with a pipeline that loads to Postgres by running:**
```sh
dlt init chess postgres
```

**2. Install the necessary dependencies for Postgres by running:**
```sh
pip install -r requirements.txt
```
This will install dlt with the `postgres` extra, which contains the `psycopg2` client.

**3. After setting up a Postgres instance and `psql` or a query editor, create a new database by running:**
```sql
CREATE DATABASE dlt_data;
```

Add the `dlt_data` database to `.dlt/secrets.toml`.

**4. Create a new user by running:**
```sql
CREATE USER loader WITH PASSWORD '<password>';
```

Add the `loader` user and `<password>` password to `.dlt/secrets.toml`.

**5. Give the `loader` user owner permissions by running:**
```sql
ALTER DATABASE dlt_data OWNER TO loader;
```

You can set more restrictive permissions (e.g., give user access to a specific schema).

**6. Enter your credentials into `.dlt/secrets.toml`.**
It should now look like this:
```toml
[destination.postgres.credentials]

database = "dlt_data"
username = "loader"
password = "<password>" # replace with your password
host = "localhost" # or the IP address location of your database
port = 5432
connect_timeout = 15
```

You can also pass a database connection string similar to the one used by the `psycopg2` library or [SQLAlchemy](https://docs.sqlalchemy.org/en/20/core/engines.html#postgresql). The credentials above will look like this:
```toml
# Keep it at the top of your TOML file, before any section starts
destination.postgres.credentials="postgresql://loader:<password>@localhost/dlt_data?connect_timeout=15"
```

To pass credentials directly, use the [explicit instance of the destination](../../general-usage/destination.md#pass-explicit-credentials)
```py
pipeline = dlt.pipeline(
  pipeline_name='chess',
  destination=dlt.destinations.postgres("postgresql://loader:<password>@localhost/dlt_data"),
  dataset_name='chess_data'
)
```

## Write disposition
All write dispositions are supported.

If you set the [`replace` strategy](../../general-usage/full-loading.md) to `staging-optimized`, the destination tables will be dropped and replaced by the staging tables.

## Data loading
`dlt` will load data using large INSERT VALUES statements by default. Loading is multithreaded (20 threads by default).

### Data types
`postgres` supports various timestamp types, which can be configured using the column flags `timezone` and `precision` in the `dlt.resource` decorator or the `pipeline.run` method.

- **Precision**: allows you to specify the number of decimal places for fractional seconds, ranging from 0 to 6. It can be used in combination with the `timezone` flag.
- **Timezone**:
  - Setting `timezone=False` maps to `TIMESTAMP WITHOUT TIME ZONE`.
  - Setting `timezone=True` (or omitting the flag, which defaults to `True`) maps to `TIMESTAMP WITH TIME ZONE`.

#### Example precision and timezone: TIMESTAMP (3) WITHOUT TIME ZONE
```py
@dlt.resource(
    columns={"event_tstamp": {"data_type": "timestamp", "precision": 3, "timezone": False}},
    primary_key="event_id",
)
def events():
    yield [{"event_id": 1, "event_tstamp": "2024-07-30T10:00:00.123"}]

pipeline = dlt.pipeline(destination="postgres")
pipeline.run(events())
```

### Fast loading with Arrow tables and CSV
You can use [Arrow tables](../verified-sources/arrow-pandas.md) and [CSV](../file-formats/csv.md) to quickly load tabular data. Pick the CSV loader file format like below:
```py
info = pipeline.run(arrow_table, loader_file_format="csv")
```
In the example above, `arrow_table` will be converted to CSV with **pyarrow** and then streamed into **postgres** with the COPY command. This method skips the regular `dlt` normalizer used for Python objects and is several times faster.

## Supported file formats
* [insert-values](../file-formats/insert-format.md) is used by default.
* [CSV](../file-formats/csv.md) is supported.

## Supported column hints
`postgres` will create unique indexes for all columns with `unique` hints. This behavior **may be disabled**.

### Table and column identifiers
Postgres supports both case-sensitive and case-insensitive identifiers. All unquoted and lowercase identifiers resolve case-insensitively in SQL statements. Case insensitive [naming conventions](../../general-usage/naming-convention.md#case-sensitive-and-insensitive-destinations) like the default **snake_case** will generate case-insensitive identifiers. Case sensitive (like **sql_cs_v1**) will generate case-sensitive identifiers that must be quoted in SQL statements.

## Additional destination options
The Postgres destination creates UNIQUE indexes by default on columns with the `unique` hint (i.e., `_dlt_id`). To disable this behavior:
```toml
[destination.postgres]
create_indexes=false
```

### Setting up CSV format
You can provide [non-default](../file-formats/csv.md#default-settings) CSV settings via a configuration file or explicitly.

```toml
[destination.postgres.csv_format]
delimiter="|"
include_header=false
```

or

```py
from dlt.destinations import postgres
from dlt.common.data_writers.configuration import CsvFormatConfiguration

csv_format = CsvFormatConfiguration(delimiter="|", include_header=False)

dest_ = postgres(csv_format=csv_format)
```
Above, we set the `CSV` file without a header, with **|** as a separator.

:::tip
You'll need those settings when [importing external files](../../general-usage/resource.md#import-external-files).
:::

### dbt support
This destination [integrates with dbt](../transformations/dbt/dbt.md) via dbt-postgres.

### Syncing of dlt state
This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

<!--@@@DLT_TUBA postgres-->

