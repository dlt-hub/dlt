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
query.options = "-ctimezone=Europe/Paris"
```

You can also pass a database connection string similar to the one used by the `psycopg2` library or [SQLAlchemy](https://docs.sqlalchemy.org/en/20/core/engines.html#postgresql). The credentials above will look like this:
```toml
# Keep it at the top of your TOML file, before any section starts
destination.postgres.credentials="postgresql://loader:<password>@localhost/dlt_data?connect_timeout=15&options=-ctimezone%3DEurope%2FParis"
```

To pass credentials directly, use the [explicit instance of the destination](../../general-usage/destination.md#pass-explicit-credentials)
```py
pipeline = dlt.pipeline(
  pipeline_name='chess',
  destination=dlt.destinations.postgres("postgresql://loader:<password>@localhost/dlt_data"),
  dataset_name='chess_data' #your destination schema name
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

### Fast loading with Arrow tables and parquet

[parquet](../file-formats/parquet.md) file format is supported via [ADBC driver](https://arrow.apache.org/adbc/current/driver/postgresql.html). Install the right driver to enable it:
```sh
pip install adbc-driver-postgresql
```
with this driver installed, you can set `parquet` as `loader_file_format` for `dlt` resources.

Not all `postgres` types are supported, see driver docs for more details:
* `JSONB` is not supported so we use `JSON` instead if the resource was explicitly decorated.
* `INT8` (128 bit) not supported and `TIME(3)` (millisecond precision) are not supported
* We observed problems with some decimal precision/scale ie. `decimal128(6, 2)` is not properly decoded.
* large decimals are not supported. `postgres` is the only destination that fully supports `wei` (256 bit) decimal precision, this does not work with ADBC.

We copy parquet files with batches of size of 1 row group. One files is copied in a single transaction.

## Supported file formats
* [insert-values](../file-formats/insert-format.md) is used by default.
* [CSV](../file-formats/csv.md) is supported.
* [parquet](../file-formats/parquet.md) is supported via [ADBC](https://arrow.apache.org/adbc/current/driver/postgresql.html)

## Supported column hints
`postgres` will create unique indexes for all columns with `unique` hints. This behavior **may be disabled**.

### Spatial Types

To enable GIS capabilities in your Postgres destination, use the `x-postgres-geometry` and `x-postgres-srid` hints for columns containing geometric data.
The `postgres_adapter` facilitates applying these hints conveniently, with a default SRID of `4326`.

**Supported Geometry Types:**

- WKT (Well-Known Text)
- Hex Representation

If you have geometry data in binary format, you will need to convert it to hexadecimal representation before loading.

**Example:** Using `postgres_adapter` with Different Geometry Types

```py
from dlt.destinations.impl.postgres.postgres_adapter import postgres_adapter

# Sample data with various geometry types
data_wkt = [
  {"type": "Point_wkt", "geom": "POINT (1 1)"},
  {"type": "Point_wkt", "geom": "Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])"},
  ]

data_wkb_hex = [
  {"type": "Point_wkb_hex", "geom": "0101000000000000000000F03F000000000000F03F"},
  {"type": "Point_wkb_hex", "geom": "01020000000300000000000000000000000000000000000000000000000000F03F000000000000F03F00000000000000400000000000000040"},
]



# Apply postgres_adapter to the 'geom' column with default SRID 4326
resource_wkt = postgres_adapter(data_wkt, geometry="geom")
resource_wkb_hex = postgres_adapter(data_wkb_hex, geometry="geom")

# If you need a different SRID
resource_wkt = postgres_adapter(data_wkt, geometry="geom", srid=3242)
```

Ensure that the PostGIS extension is enabled in your Postgres database:

```sql
CREATE EXTENSION postgis;
```

This configuration allows `dlt` to map the `geom` column to the PostGIS `geometry` type for spatial queries and analyses.

:::warning
`LinearRing` geometry type isn't supported.
:::

## Table and column identifiers
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

