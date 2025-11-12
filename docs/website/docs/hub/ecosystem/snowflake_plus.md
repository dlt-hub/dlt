---
title: "Destination: Snowflake+ Iceberg / Open Catalog"
description: Snowflake destination with Iceberg and Open Catalog
keywords: [Snowflake, Iceberg, destination]
---

# Snowflake+ Iceberg / Open Catalog

import { DltHubFeatureAdmonition } from '@theme/DltHubFeatureAdmonition';

<DltHubFeatureAdmonition />

Snowflake+ is a drop-in replacement for [OSS Snowflake destination](../../dlt-ecosystem/destinations/snowflake.md) that adds [Apache Iceberg tables](https://docs.snowflake.com/en/user-guide/tables-iceberg) creation and related features.

It uses Snowflake to manage Iceberg data - tables are created and data is copied via Snowflake SQL and automatically visible in Snowflake (HORIZON)
catalog as other (native) tables. On top of that, Snowflake provides table maintenance (like compacting, deleting snapshot etc.).

**Snowflake Open Catalog** (Polaris) is [fully supported](#syncing-snowflake-managed-iceberg-tables-to-snowflake-open-catalog) via `CATALOG SYNC` option. Both new data and all schema migrations performed by `dlt` are visible in it without any additional code or setup.

All [data access](../../general-usage/dataset-access/) methods (pandas, arrow, Ibis, SQL etc.) that `dlt` supports via `pipeline.dataset()` are available.

:::tip
You can [link](https://docs.snowflake.com/LIMITEDACCESS/iceberg/tables-iceberg-externally-managed-writes#label-tables-iceberg-external-writes-create-cld) any catalog (Lakekeeper, Glue, S3Tables or Open Catalog/Polaris) used by `dlt` [Iceberg](iceberg.md) destination to a Snowflake database.
:::

This destination is available starting from dltHub version 0.9.0. It fully supports all the functionality of the standard Snowflake destination, plus:

1. The ability to create Iceberg tables in Snowflake by configuring `iceberg_mode` in your `config.toml` file or `dlt.yml` file.
2. Additional configuration for Iceberg tables in Snowflake via:
   - `external_volume`: The external volume name where Iceberg data is stored.
   - `catalog`: The catalog name in which Iceberg tables are created. Defaults to `"SNOWFLAKE"`.
   - `base_location`: A template string for the base path that Snowflake uses for storing the table data in external storage, supporting placeholders.
   - `extra_placeholders`: Additional values that can be used in the `base_location` template.
   - `catalog_sync`: The name of a [catalog integration](https://docs.snowflake.com/en/user-guide/tables-iceberg#catalog-integration) configured for [Snowflake Open Catalog](https://other-docs.snowflake.com/en/opencatalog/overview). If specified, Snowflake syncs Snowflake-managed Iceberg tables in the database with an external catalog in your Snowflake Open Catalog account.

## Installation

Install the `dlthub` package with the `snowflake` extra:

```sh
pip install "dlthub[snowflake]"
```

Once the `snowflake` extra is installed, you can configure a pipeline to use `snowflake_plus` exactly the same way you would use the `snowflake` destination.

## Setup

1. [Configure your Snowflake credentials](../../dlt-ecosystem/destinations/snowflake.md#setup-guide)
2. [Set up a database user and permissions](../../dlt-ecosystem/destinations/snowflake.md#set-up-the-database-user-and-permissions)
3. [Configure an external volume in Snowflake](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume)
4. Grant usage on the external volume to the role you are using to load data:

```sql
GRANT USAGE ON EXTERNAL VOLUME <external_volume_name> TO ROLE <role_name>;
```

5. Configure the `snowflake_plus` destination. For a dltHub project (in `dlt.yml`) or for a Python script (in `config.toml`):

<Tabs
  groupId="config-format"
  defaultValue="dlt-yml"
  values={[
    {"label": "dlt.yml", "value": "dlt-yml"},
    {"label": "config.toml", "value": "config-toml"},
]}>
  <TabItem value="dlt-yml">

If you don't have a dltHub project yet, initialize one in the current working directory. Replace `sql_database` with the source of your choice:

```sh
dlt project init sql_database snowflake_plus
```

This will create a Snowflake Plus destination in your `dlt.yml` file:

```yaml
destinations:
  snowflake:
    type: snowflake_plus
```

To enable Iceberg table creation, set the `iceberg_mode` option and `external_volume` to the name of the external volume you created in step 3.

```yaml
destinations:
  snowflake:
    type: snowflake_plus
    external_volume: "<external_volume_name>"
    iceberg_mode: "all"
```

  </TabItem>
  <TabItem value="config-toml">

Add the configuration to your `config.toml` file:

```toml
[destination.snowflake]
external_volume = "<external_volume_name>"
iceberg_mode = "all"
```

Use the `snowflake_plus` destination in your pipeline:

```py
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

  </TabItem>
</Tabs>

## Configuration

The `snowflake_plus` destination extends the standard Snowflake configuration with additional options:

### `iceberg_mode`
Controls which tables are created as Iceberg tables.
- Possible values:
  - `"all"`: All tables including dlt system tables are created as Iceberg tables
  - `"data_tables"`: Only data tables (non-dlt system tables) are created as Iceberg tables
  - `"none"`: No tables are created as Iceberg tables
- Required: No
- Default: `"none"`

### `external_volume`
The external volume to store Iceberg metadata.
- Required: Yes
- Default: None

### `catalog`
The catalog to use for Iceberg tables.
- Required: No
- Default: `"SNOWFLAKE"`. This will use [Snowflake as the catalog](https://docs.snowflake.com/en/user-guide/tables-iceberg#label-tables-iceberg-snowflake-as-catalog) for the Iceberg tables.

### `base_location`
Template string for the base location where Iceberg data is stored in the external volume. Supports placeholders like `{dataset_name}` and `{table_name}`.
- Required: No
- Default: `"{dataset_name}/{table_name}"`

### `extra_placeholders`
Dictionary of additional values that can be used in the `base_location` template. The values can be static strings or functions that accept the dataset name and table name as arguments and return a string.
- Required: No
- Default: None

### `catalog_sync`
The name of a [catalog integration](https://docs.snowflake.com/en/user-guide/tables-iceberg#catalog-integration) for syncing Iceberg tables to an external catalog in [Snowflake Open Catalog](https://other-docs.snowflake.com/en/opencatalog/overview).
- Required: No
- Default: None

Configure these options in your `config.toml` file under the `[destination.snowflake]` section or in `dlt.yml` file under the `destinations.snowflake_plus` section.

## Base location templating

The `base_location` parameter controls where Snowflake stores your Iceberg table data and metadata in the external volume. It's a template string that supports the following built-in placeholders:

- `{dataset_name}`: The name of your dataset
- `{table_name}`: The name of the table

For more flexibility, you can also define custom placeholders using the `extra_placeholders` option.

### Examples

1. The default pattern `{dataset_name}/{table_name}` creates paths like `my_dataset/customers` in your external volume.

2. Custom static path:
   ```yaml
   base_location: "custom/static/path"
   ```
   This creates all tables in the same directory `custom/static/path`.

3. Using custom placeholders:
   ```yaml
   base_location: "{env}/{dataset_name}/{table_name}"
   extra_placeholders:
     env: "prod"
   ```
   This creates paths like `prod/my_dataset/customers`.

### How Snowflake uses the base location

When you provide a `base_location`, Snowflake uses it to create the paths where data and metadata are stored in your external cloud storage. The actual directory structure Snowflake creates follows this pattern:

```text
STORAGE_BASE_URL/BASE_LOCATION.<randomId>/[data | metadata]/
```

Where `<randomId>` is a random Snowflake-generated 8-character string appended to create a unique directory.

For more details on how Snowflake organizes Iceberg table files in external storage, see the [Snowflake documentation on data and metadata directories](https://docs.snowflake.com/en/user-guide/tables-iceberg-storage#data-and-metadata-directories).

## Table format for individual tables
You can specify table format (Iceberg/Native) for individual `dlt` resources. For example:
  ```py
  @dlt.resource(
    table_format="native"
  )
  def my_resource():
      ...

  pipeline = dlt.pipeline("loads_native", destination="snowflake_plus")
  ```
  Will create a native (non-iceberg) **my_resource** table, also when you set the [iceberg_mode](#iceberg_mode) to **all** or **data_tables**.

## Write dispositions

All standard write dispositions (`append`, `replace`, and `merge`) are supported for both regular Snowflake tables and Iceberg tables.

## Data types

The Snowflake Plus destination supports all standard Snowflake destination data types, with additional type mappings for Iceberg tables:

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

## Syncing Snowflake-managed Iceberg tables to Snowflake Open Catalog

To enable querying of Snowflake-managed Iceberg tables by third-party engines (e.g., Apache Spark) via an external catalog (Snowflake Open Catalog), use the `catalog_sync` configuration option. This setting specifies a [catalog integration](https://docs.snowflake.com/en/user-guide/tables-iceberg#catalog-integration) that syncs Iceberg tables to the external catalog.

### Setup

1. Create an [external catalog in Snowflake Open Catalog](https://other-docs.snowflake.com/en/opencatalog/create-catalog).

2. Create a catalog integration in Snowflake. Example:

```sql
  CREATE OR REPLACE CATALOG INTEGRATION my_open_catalog_int
    CATALOG_SOURCE = POLARIS
    TABLE_FORMAT = ICEBERG
    REST_CONFIG = (
      CATALOG_URI = 'https://<orgname>-<my-snowflake-open-catalog-account-name>.snowflakecomputing.com/polaris/api/catalog'
      CATALOG_NAME = 'myOpenCatalogExternalCatalogName'
    )
    REST_AUTHENTICATION = (
      TYPE = OAUTH
      OAUTH_CLIENT_ID = 'myClientId'
      OAUTH_CLIENT_SECRET = 'myClientSecret'
      OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:ALL')
    )
    ENABLED = TRUE;
```

Refer to the [Snowflake documentation](https://docs.snowflake.com/en/user-guide/tables-iceberg-open-catalog-sync#step-4-create-a-catalog-integration-for-open-catalog) for detailed setup instructions.

3. Configure the `catalog_sync` option:

<Tabs
  groupId="config-format"
  defaultValue="config-toml"
  values={[
    {"label": "dlt.yml", "value": "dlt-yml"},
    {"label": "config.toml", "value": "config-toml"},
]}>
  <TabItem value="dlt-yml">

```yaml
destinations:
  snowflake:
    type: snowflake_plus
    # ... other configuration
    catalog_sync: "my_open_catalog_int"
```

  </TabItem>
  <TabItem value="config-toml">

```toml
[destination.snowflake]
# ... other configuration
catalog_sync = "my_open_catalog_int"
```

  </TabItem>
</Tabs>

## Additional Resources

For more information on basic Snowflake destination functionality, please refer to the [Snowflake destination documentation](../../dlt-ecosystem/destinations/snowflake.md).
