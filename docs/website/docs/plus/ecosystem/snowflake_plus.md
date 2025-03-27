---
title: Snowflake Plus
description: Snowflake destination with Iceberg table support
keywords: [Snowflake, Iceberg, destination]
---

# Snowflake Plus

Snowflake Plus is an **experimental** extension of the [Snowflake destination](../../dlt-ecosystem/destinations/snowflake.md) that adds [Apache Iceberg tables](https://docs.snowflake.com/en/user-guide/tables-iceberg) creation and related features.
This destination is available starting from `dlt-plus==0.9.0`. It fully supports all the functionality of the standard Snowflake destination, plus:

1. The ability to create Iceberg tables in Snowflake by setting `force_iceberg` to `true` in your `config.toml` file or `dlt.yml` file.
2. Additional configuration for Iceberg tables in Snowflake via:
   - `external_volume`: The external volume name where Iceberg data is stored.
   - `catalog`: The catalog name in which Iceberg tables are created. Defaults to `"SNOWFLAKE"`.
   - `base_location`: The base path that Snowflake uses for storing the table. If omitted, the table name is used.

## Installation

Install the `dlt-plus` package with the `snowflake` extra:

```sh
pip install "dlt-plus[snowflake]"
```

## Setup

Once the `snowflake` extra is installed, you can configure a pipeline to use `snowflake_plus` exactly the same way you would use `snowflake` or initialize a dlt+ project with the `snowflake_plus` source.

### Use as a drop-in replacement for the `snowflake` destination

1. Make sure you have configured [Snowflake credentials](../../dlt-ecosystem/destinations/snowflake.md#setup-guide)
2. Make sure you [set up database user and permissions](../../dlt-ecosystem/destinations/snowflake.md#set-up-the-database-user-and-permissions)
3. [Configure an external volume in Snowflake](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume)
4. Grant usage on the external volume to the role you are using to load data:

```sql
GRANT USAGE ON EXTERNAL VOLUME <external_volume_name> TO ROLE <role_name>;
```

5. Configure the `snowflake_plus` destination in your `config.toml`:

```toml
[destination.snowflake]
external_volume = "<external_volume_name>"
force_iceberg = true
```

6. Use the `snowflake_plus` destination in your pipeline:

```python
import dlt

pipeline = dlt.pipeline(
    pipeline_name="my_snowflake_plus_pipeline",
    destination="snowflake_plus",
    dataset_name="my_dataset"
)

@dlt.resource
def my_iceberg_table():
    ...
```

### Use with `dlt+` project

Initialize a dlt+ project in the current working directory. Replace `sql_database` with the source of your choice:

```sh
dlt project init sql_database snowflake_plus
```

This will create a Snowflake Plus destination in your `dlt.yml` file.

```yaml
destinations:
  snowflake_plus:
    type: snowflake_plus
```

To enable Iceberg table creation, set the `force_iceberg` option to `true` and `external_volume` to the name of the external volume you created in step 3.

```yaml
destinations:
  snowflake_plus:
    type: snowflake_plus
    force_iceberg: true
    external_volume: "<external_volume_name>"
```

## Configuration

The `snowflake_plus` destination extends the standard Snowflake configuration with additional options:

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| `force_iceberg` | Whether to force the creation of Iceberg tables | No | `False` |
| `external_volume` | The external volume to store Iceberg metadata | Yes | None |
| `catalog` | The catalog to use for Iceberg tables | No | `"SNOWFLAKE"` |
| `base_location` | Custom base location for Iceberg data in the external volume | No | `<dataset_name>/<table_name>` |

You can configure these options in your `config.toml` file under the `[destination.snowflake]` section or in `dlt.yml` file under the `destinations.snowflake_plus` section.

## Write dispositions

All standard write dispositions (`append`, `replace`, and `merge`) are supported for both regular Snowflake tables and Iceberg tables.

## Data Types

The `snowflake_plus` destination extends the standard Snowflake data types with additional types:

Snowflake Plus supports all the data types from the standard Snowflake destination, with additional type mappings for Iceberg tables:

| dlt Type | Iceberg Type |
|----------|--------------|
| `text` | `string` |
| `bigint` | `long`, `int` |
| `double` | `double` |
| `bool` | `boolean` |
| `timestamp` | `timestamp` |
| `date` | `date` |
| `time` | `time` |
| `decimal` | `decimal` |
| `binary` | `binary` |
| `json` | `string` |

## Additional Resources

For more information on basic Snowflake destination functionality, please refer to the [Snowflake destination documentation](../../dlt-ecosystem/destinations/snowflake.md).
