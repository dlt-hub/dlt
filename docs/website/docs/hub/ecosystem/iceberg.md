---
title: "Destination: Iceberg"
description: Iceberg destination
keywords: [Iceberg, pyiceberg]
---

# Iceberg

import { DltHubFeatureAdmonition } from '@theme/DltHubFeatureAdmonition';

<DltHubFeatureAdmonition />

Apache Iceberg is an open table format designed for high-performance analytics on large datasets. It supports ACID transactions, schema evolution, and time travel.

The Iceberg destination in dlt allows you to load data into Iceberg tables using the [pyiceberg](https://py.iceberg.apache.org/) library. It supports multiple catalog types and both local and cloud storage backends.

## Features

* Compatible with SQL and REST catalogs (Lakekeeper, Polaris)
* Automatic schema evolution and table creation
* All write dispositions supported
* Works with local filesystems and cloud storage (S3, Azure, GCS)
* Exposes data via DuckDB views using `pipeline.dataset()`
* Supports partitioning

##  Prerequisites
Make sure you have installed the necessary dependencies:
```sh
pip install dlt[filesystem,pyiceberg]>=1.9.1
pip install dlthub>=0.9.1
```

## Configuration

### Overview

To configure Iceberg destination you need to choose and configure the catalog. The role of iceberg catalog is to:

* store metadata and coordinate transactions (required)
* generate and hand credentials to pyiceberg client (credentials vending)
* generate and hand locations for newly generated tables (rest catalogs)

Currently, the Iceberg destination supports two catalog types:
* SQL-based catalog. Ideal for local development; stores metadata in SQLite or PostgreSQL
* REST catalog. Used in production with systems like Lakekeeper or Polaris

### SQL catalog

The SQL catalog is ideal for development and testing. It does not provide credential or location vending, so these must be configured manually. It supports local storage paths, such as a file-based SQLite database, and is generally used for working with local filesystems.

To configure a SQL catalog, provide the following parameters:

<Tabs
  groupId="filesystem-type"
  defaultValue="toml"
  values={[
    {"label": "TOML files", "value": "toml"},
    {"label": "Environment variables", "value": "env"}
]}>

<TabItem value="toml">

```toml
[destination.iceberg]
catalog_type="sql"
credentials="sqlite:///catalog.db" # connection string for accessing the database

[destination.iceberg.filesystem]
bucket_url="path_to_data" # table location

# credentials section below is only needed if you're using the cloud storage (not local disk)
[destination.iceberg.filesystem.credentials]
aws_access_key_id = "please set me up!"
aws_secret_access_key = "please set me up!"

[destination.iceberg.capabilities]
# will register tables if found in storage but not found in the catalog (backward compatibility)
register_new_tables=true
table_location_layout="{dataset_name}/{table_name}"
```
</TabItem>

<TabItem value="env">

```sh
export DESTINATION__ICEBERG__CATALOG_TYPE=sql
export DESTINATION__ICEBERG__CREDENTIALS=sqlite:///catalog.db
export DESTINATION__ICEBERG__FILESYSTEM__BUCKET_URL=path_to_data
# credentials section below is only needed if you're using the cloud storage (not local disk)
export DESTINATION__ICEBERG__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID="please set me up!"
export DESTINATION__ICEBERG__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY="please set me up!"

export DESTINATION__ICEBERG__CAPABILITIES__REGISTER_NEW_TABLE=True
export DESTINATION__ICEBERG__CAPABILITIES__TABLE_LOCATION_LAYOUT={dataset_name}/{table_name}
```
</TabItem>
</Tabs>

* `catalog_type=sql` - this indicates, that you will use SQL-based catalog.
* `credentials=dialect+database_type://username:password@server:port/database_name` -  the connection string for your catalog database.
This can be any SQLAlchemy-compatible database such as SQLite or PostgreSQL. For local development, a simple SQLite file like `sqlite:///catalog.db` works well. dlt will create it automatically if it doesn't exist.
* `filesystem.bucket_url` - the physical location where Iceberg table data is stored.
This can be a local directory or any cloud storage supported by the [filesystem destination](../../dlt-ecosystem/destinations/filesystem.md). If you’re using cloud storage, be sure to include the appropriate credentials as explained in the
[credentials setup guide](../../dlt-ecosystem/destinations/filesystem.md#set-up-the-destination-and-credentials). For local filesystems, no additional credentials are needed.
* `capabilities.register_new_tables=true` - enables automatic registration of tables found in storage but missing in the catalog.
* `capabilities.table_location_layout` - controls the directory structure for Iceberg table files.
It supports two modes:
  * absolute - you provide a full URI that matches the catalog’s warehouse path, optionally including deeper subpaths.
  * relative - a path that’s appended to the catalog’s warehouse root. This is especially useful with catalogs like Lakekeeper.

The SQL catalog stores one table of the following schema:

| catalog_name | table_namespace     | table_name  | metadata_location                                 | previous_metadata_location                                                          |
|--------------|---------------------|-------------|---------------------------------------------------|---------------------------------------------------------------------------------------|
| default      | jaffle_shop_dataset | orders | path/to/files                                     | path/to/files |
| default      | jaffle_shop_dataset | _dlt_loads  | path/to/files  | path/to/files |

### Lakekeeper catalog

[Lakekeeper](https://docs.lakekeeper.io/) is an open-source, production-grade Iceberg catalog. It’s easy to set up, plays well with any cloud storage, and lets you build real
data platforms without needing to set up heavy-duty infrastructure. Lakeleeper also supports vended credentials, credential vending, removing the need to pass long-lived secrets directly to dlt.

To configure Lakekeeper, you need to specify both catalog and storage parameters. The catalog handles metadata and credential vending, while the `bucket_url` must align with the warehouse configured in Lakekeeper.

<Tabs
  groupId="filesystem-type"
  defaultValue="toml"
  values={[
    {"label": "TOML files", "value": "toml"},
    {"label": "Environment variables", "value": "env"}
]}>

<TabItem value="toml">

```toml
[destination.iceberg]
catalog_type="rest"

[destination.iceberg.credentials]
credential="my_lakekeeper_key"
uri="https://lakekeeper.path.to.host/catalog"
warehouse="warehouse"

[destination.iceberg.credentials.properties]
scope="lakekeeper"
oauth2-server-uri="https://keycloak.path.to.host/realms/master/protocol/openid-connect/token"

[destination.iceberg.filesystem]
bucket_url="s3://warehouse/"

[destination.iceberg.capabilities]
table_location_layout="lakekeeper-warehouse/dlthub_demo/lakekeeper_demo/{dataset_name}/{table_name}"
```
</TabItem>

<TabItem value="env">

```sh
export DESTINATION__ICEBERG__CATALOG_TYPE=rest
export DESTINATION__ICEBERG__CREDENTIALS__CREDENTIAL=my_lakekeeper_key
export DESTINATION__ICEBERG__CREDENTIALS__URI=https://lakekeeper.path.to.host/catalog
export DESTINATION__ICEBERG__CREDENTIALS__WAREHOUSE=warehouse
export DESTINATION__ICEBERG__CREDENTIALS__PROPERTIES__SCOPE=lakekeeper
export DESTINATION__ICEBERG__CREDENTIALS__PROPERTIES__OAUTH2-SERVER-URI=https://keycloak.path.to.host/realms/master/protocol/openid-connect/token
export DESTINATION__ICEBERG__FILESYSTEM__BUCKET_URL=s3://warehouse/
export DESTINATION__ICEBERG__CAPABILITIES__TABLE_LOCATION_LAYOUT=lakekeeper-warehouse/dlthub_demo/lakekeeper_demo/{dataset_name}/{table_name}
```
</TabItem>

</Tabs>

* `catalog_type=rest` - specifies that you're using a REST-based catalog implementation.
* `credentials.credential` - your Lakekeeper key or token used to authenticate with the catalog.
* `credentials.uri` - the URL of your Lakekeeper catalog endpoint.
* `credentials.warehouse` - the name of the warehouse configured in Lakekeeper, which defines the root location for all data tables.
* `credentials.properties.scope=lakekeeper` - the scope required for authentication.
* `credentials.properties.oauth2-server-uri` - he URL of your OAuth2 token endpoint used for Lakekeeper authentication.
* `filesystem.bucket_url` - the physical storage location for Iceberg table files. This can be any supported cloud storage backend listed in the [filesystem destination](../../dlt-ecosystem/destinations/filesystem.md).

:::warning
Currently, the following buckets and credentials combinations are well-tested:

* s3: STS and signer, s3 express
* azure: both access key and tenant-id (principal) based auth
* google storage
:::

* `capabilities.table_location_layout` - controls the directory structure for Iceberg table files.
It supports two modes:
  * absolute - you provide a full URI that matches the catalog’s warehouse path, optionally including deeper subpaths.
  * relative - a path that’s appended to the catalog’s warehouse root. This is especially useful with catalogs like Lakekeeper.

### Polaris catalog

[Polaris](https://polaris.apache.org/) - is an open-source, fully-featured catalog for Iceberg. Its configuration is similar to Lakekeeper, with some differences in credential scopes and URI.
<Tabs
  groupId="filesystem-type"
  defaultValue="toml"
  values={[
    {"label": "TOML files", "value": "toml"},
    {"label": "Environment variables", "value": "env"}
]}>

<TabItem value="toml">

```toml
[destination.iceberg]
catalog_type="rest"

[destination.iceberg.credentials]
credential="my_polaris_key"
uri="https://account.snowflakecomputing.com/polaris/api/catalog"
warehouse="warehouse"

[destination.iceberg.credentials.properties]
scope="PRINCIPAL_ROLE:ALL"

[destination.iceberg.filesystem]
bucket_url="s3://warehouse"

[destination.iceberg.capabilities]
table_location_layout="{dataset_name}/{table_name}"
```
</TabItem>

<TabItem value="env">

```sh
export DESTINATION__ICEBERG__CATALOG_TYPE=rest
export DESTINATION__ICEBERG__CREDENTIALS__CREDENTIAL=my_polaris_key
export DESTINATION__ICEBERG__CREDENTIALS__URI=https://account.snowflakecomputing.com/polaris/api/catalog
export DESTINATION__ICEBERG__CREDENTIALS__WAREHOUSE=warehouse
export DESTINATION__ICEBERG__CREDENTIALS__PROPERTIES__SCOPE=PRINCIPAL_ROLE:ALL
export DESTINATION__ICEBERG__FILESYSTEM__BUCKET_URL=s3://warehouse/
export DESTINATION__ICEBERG__CAPABILITIES__TABLE_LOCATION_LAYOUT={dataset_name}/{table_name}
```
</TabItem>

</Tabs>

For more information, refer to the [Lakekeeper section above](#lakekeeper-catalog).

### AWS Iceberg Catalogs

dlt supports three AWS-backed Iceberg catalogs.
Their names correspond to the `catalog_type` value you pass in your destination block:

| `catalog_type`      | What it talks to under the hood |
| ------------------- | ----------------------------------------------------------------------------------- |
| **`s3tables-rest`** | Uses the AWS S3 Tables Iceberg REST API endpoint, and S3 table buckets |
| **`glue-rest`**     | Uses the AWS Glue Iceberg REST API endpoint, Lake Formation, and S3 table buckets |
| **`glue`**          | Uses the AWS Glue Catalog, and normal S3 buckets |


#### Catalog `[s3tables-rest]`

Configure this catalog when you want to publish Iceberg tables directly into an S3 Table bucket via the Amazon S3 Tables Iceberg REST API endpoint.

To configure a `s3tables-rest` catalog, provide the following parameters and replace `<region>`, `<account-id>`, `<s3-table-bucket-name>`, and AWS keys with your actual values:

<Tabs
  groupId="filesystem-type"
  defaultValue="toml"
  values={[
    {"label": "TOML files", "value": "toml"},
    {"label": "Environment variables", "value": "env"}
]}>

<TabItem value="toml">

```toml
[destination.iceberg]
catalog_type = "s3tables-rest"
catalog_name = "iceberg_lake"

[destination.iceberg.credentials]
warehouse = "arn:aws:s3tables:<region>:<account-id>:bucket/<s3-table-bucket-name>"
uri       = "https://s3tables.<region>.amazonaws.com/iceberg"
aws_access_key_id     = "<aws_access_key_id>"
aws_secret_access_key = "<aws_secret_access_key>"
region_name           = "<region>"

[destination.iceberg.credentials.properties]
"rest.sigv4-enabled"  = "true"
"rest.signing-name"   = "s3tables"
"rest.signing-region" = "<region>"
```
</TabItem>

<TabItem value="env">

```sh
export DESTINATION__ICEBERG__CREDENTIALS__WAREHOUSE="arn:aws:s3tables:<region>:<account-id>:bucket/<s3-table-bucket-name>"
export DESTINATION__ICEBERG__CREDENTIALS__URI="https://s3tables.<region>.amazonaws.com/iceberg"
export DESTINATION__ICEBERG__CREDENTIALS__AWS_ACCESS_KEY_ID="<aws_access_key_id>"
export DESTINATION__ICEBERG__CREDENTIALS__AWS_SECRET_ACCESS_KEY="<aws_secret_access_key>"
export DESTINATION__ICEBERG__CREDENTIALS__REGION_NAME="<region>"

export DESTINATION__ICEBERG__CREDENTIALS__PROPERTIES='{
  "rest.sigv4-enabled": "true",
  "rest.signing-name" : "s3tables",
  "rest.signing-region": "<region>"
}'
```
</TabItem>

</Tabs>

##### Prerequisites

Create the S3 Table bucket first and grant the calling IAM principal s3tables:* actions read/write permissions on that bucket.
* `warehouse` – full bucket ARN for your S3 Tables catalog.
* `uri` – region-specific S3 Tables REST endpoint.
* `rest.*` properties – mandatory SigV4 settings for every call.

#### Catalog `[glue-rest]`
Configure this catalog when you want to publish Iceberg tables directly into an S3 Tables bucket via the AWS Glue Iceberg REST API endpoint.

:::note
`glue-rest` catalog involves the most complex IAM/Lake Formation setup.
:::
To configure a `glue-rest` catalog, provide the following parameters and replace `<region>`, `<account-id>`, `<s3-table-bucket-name>`, and AWS keys with real values:

<Tabs
  groupId="filesystem-type"
  defaultValue="toml"
  values={[
    {"label": "TOML files", "value": "toml"},
    {"label": "Environment variables", "value": "env"}
]}>

<TabItem value="toml">

```toml
[destination.iceberg]
catalog_type = "glue-rest"
catalog_name = "iceberg_lake"

[destination.iceberg.credentials]
warehouse = "<account-id>:s3tablescatalog/<s3-table-bucket-name>"
uri       = "https://glue.<region>.amazonaws.com/iceberg"
aws_access_key_id     = "<aws_access_key_id>"
aws_secret_access_key = "<aws_secret_access_key>"
region_name           = "<region>"

[destination.iceberg.credentials.properties]
"rest.sigv4-enabled"  = "true"
"rest.signing-name"   = "glue"
"rest.signing-region" = "<region>"
```
</TabItem>
<TabItem value="env">

```sh
export DESTINATION__ICEBERG__CREDENTIALS__WAREHOUSE="<account-id>:s3tablescatalog/<s3-table-bucket-name>"
export DESTINATION__ICEBERG__CREDENTIALS__URI="https://glue.<region>.amazonaws.com/iceberg"
export DESTINATION__ICEBERG__CREDENTIALS__AWS_ACCESS_KEY_ID="<aws_access_key_id>"
export DESTINATION__ICEBERG__CREDENTIALS__AWS_SECRET_ACCESS_KEY="<aws_secret_access_key>"
export DESTINATION__ICEBERG__CREDENTIALS__REGION_NAME="<region>"
export DESTINATION__ICEBERG__CREDENTIALS__PROPERTIES='{
  "rest.sigv4-enabled": "true",
  "rest.signing-name" : "glue",
  "rest.signing-region": "<region>"
}'
```

</TabItem>
</Tabs>

##### Prerequisites
Сreate the S3 Table bucket first and follow this AWS documentation to properly configure IAM, Glue, and Lake Formation: [Create an Iceberg catalog for S3 Tables via Glue REST](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-glue-endpoint.html)
* `warehouse` – glue catalog arn for your S3 Tables catalog.
* `uri` – region-specific Glue REST endpoint.
* `rest.*` properties – mandatory SigV4 settings for every call.

#### Catalog `[glue]`

Choose this when you simply want to use the Glue Catalog with a normal S3 bucket for table storage.

<Tabs
  groupId="filesystem-type"
  defaultValue="toml"
  values={[
    {"label": "TOML files", "value": "toml"},
    {"label": "Environment variables", "value": "env"}
]}>

<TabItem value="toml">

```toml
[destination.iceberg]
catalog_type = "glue"
catalog_name = "iceberg_lake"

[destination.iceberg.filesystem]
bucket_url = "<s3-bucket-url>"

[destination.iceberg.credentials]
aws_access_key_id     = "<aws_access_key_id>"
aws_secret_access_key = "<aws_secret_access_key>"
region_name           = "<region>"
```
</TabItem> <TabItem value="env">

```sh
export DESTINATION__ICEBERG__FILESYSTEM__BUCKET_URL="<s3-bucket-url>"
export DESTINATION__ICEBERG__CREDENTIALS__AWS_ACCESS_KEY_ID="<aws_access_key_id>"
export DESTINATION__ICEBERG__CREDENTIALS__AWS_SECRET_ACCESS_KEY="<aws_secret_access_key>"
export DESTINATION__ICEBERG__CREDENTIALS__REGION_NAME="<region>"
```

</TabItem>
</Tabs>

##### Prerequisites
An S3 bucket and an IAM principal allowed to read/write that bucket and access the Glue Data Catalog.

* `bucket_url` – S3 prefix where Iceberg data and metadata files will live.


### Unity Catalog

[Unity Catalog](https://www.databricks.com/product/unity-catalog) provides a REST API for Iceberg that allows external clients to access Databricks tables.

#### Prerequisites

Before using Unity Catalog with the Iceberg destination, ensure you have:

1. Unity Catalog enabled in your workspace
2. External data access enabled for your metastore
3. EXTERNAL USE SCHEMA privilege granted to your principal:
   ```sql
   GRANT EXTERNAL USE SCHEMA ON CATALOG <catalog_name> TO `user@company.com`;
   ```
   Where `<catalog_name>` is the name of the catalog you want to grant access to.

For detailed setup instructions, see Databricks guide on [accessing tables from Apache Iceberg clients](https://learn.microsoft.com/en-us/azure/databricks/external-access/iceberg).

#### Configuration

<Tabs
  groupId="filesystem-type"
  defaultValue="toml"
  values={[
    {"label": "TOML files", "value": "toml"},
    {"label": "Environment variables", "value": "env"}
]}>

<TabItem value="toml">

```toml
[destination.iceberg]
catalog_type = "rest"

[destination.iceberg.credentials]
uri = "https://<workspace-url>/api/2.1/unity-catalog/iceberg-rest"
warehouse = "dlt_ci"

[destination.iceberg.credentials.properties]
token = "please set me up!"
```
</TabItem>

<TabItem value="env">

```sh
export DESTINATION__ICEBERG__CATALOG_TYPE=rest
export DESTINATION__ICEBERG__CREDENTIALS__URI=https://<workspace-url>/api/2.1/unity-catalog/iceberg-rest
export DESTINATION__ICEBERG__CREDENTIALS__WAREHOUSE=dlt_ci
export DESTINATION__ICEBERG__CREDENTIALS__PROPERTIES__TOKEN=please set me up!
```
</TabItem>

</Tabs>

#### Configuration Parameters

* `catalog_type=rest` - specifies the REST catalog implementation
* `credentials.uri` - the Unity Catalog Iceberg REST API endpoint (format: `https://<workspace-url>/api/2.1/unity-catalog/iceberg-rest`)
* `credentials.warehouse` - the catalog name in Unity Catalog containing your tables
* `credentials.properties.token` - your Databricks personal access token for authentication

## Write dispositions

All [write dispositions](../../general-usage/incremental-loading.md) are supported.

## Merge strategies

The Iceberg destination supports two merge strategies when using `write_disposition="merge"`: `delete-insert` and `upsert`.
Both strategies use a single Iceberg transaction for delete and insert operations and both support [hard delete](../../general-usage/merge-loading.md#delete-records) functionality.

### Delete-insert strategy

The `delete-insert` strategy first deletes existing rows matching the key columns, then inserts new data.

```py
@dlt.resource(
    write_disposition={"disposition": "merge", "strategy": "delete-insert"},
    primary_key="id",
    table_format="iceberg"
)
def my_resource():
    yield [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
```

#### Key characteristics
- Requires primary key or merge key columns
- Supports only regular tables (nested tables not supported)
- Automatically deduplicates data based on primary keys
- Uses single Iceberg transaction for delete and insert operations

### Upsert strategy

The `upsert` strategy is similar to delete-insert strategy but with key differences in behavior and requirements:

- Supports nested data: unlike `delete-insert`, `upsert` handles nested data
    - Note: nested data requires a column with the `unique` property (dlt will use `_dlt_id` by default)
- Does not deduplicate records with duplicate primary keys in the incoming data
- Upsert _does not_ support merge keys

```py
@dlt.resource(
    write_disposition={"disposition": "merge", "strategy": "upsert"},
    primary_key="id",
    table_format="iceberg"
)
def my_upsert_resource():
    yield [{"id": 1, "name": "Alice Updated"}, {"id": 3, "name": "Charlie"}]
```

:::note
dltHub is not using PyIceberg's `Table.upsert` but implements its own method using delete and insert operations in a single transaction.
:::

:::info Performance Testing
Both `delete-insert` and `upsert` merge strategies have been stress tested with datasets containing tens of millions of rows without encountering any issues. Memory usage and processing time scale linearly with the size of the updated dataset.
:::

#### Known limitations
- Orphaned nested table records: updates to nested structures that remove elements do not delete them from the destination table.

## Data access

The Iceberg destination integrates with `pipeline.dataset()` to give users queryable access to their data.
When invoked, this creates an in-memory DuckDB database with views pointing to Iceberg tables.

The created views reflect the latest available snapshot. To ensure fresh data during development, use the `always_refresh_views` option. Views are materialized only on demand, based on query usage.

## Credentials for data access
By default, credentials for accessing data are vended by the catalog, and per-table secrets are created automatically. This works best with cloud storage providers like AWS S3 using STS credentials.
However, due to potential performance limitations with temporary credentials, we recommend defining the filesystem explicitly when working with `dataset()` or dltHub transformations.
This approach allows for native DuckDB filesystem access, persistent secrets, and faster data access. For example, when using AWS S3 as the storage location
for your Iceberg tables, you can provide explicit credentials in the destination configuration in `filesystem` section:

```toml
[destination.iceberg.filesystem.credentials]
aws_access_key_id = "please set me up!"
aws_secret_access_key = "please set me up!"
```

## Partitioning

Apache Iceberg supports [table partitioning](https://iceberg.apache.org/docs/latest/partitioning/) to optimize query performance.

There are two ways to configure partitioning in dltHub Iceberg destination:
* Using the [`iceberg_adapter`](#using-the-iceberg_adapter-function) function
* Using column-level [`partition`](#using-column-level-partition-property) property

### Using the `iceberg_adapter` function

The `iceberg_adapter` function allows you to configure partitioning for your Iceberg tables. This adapter supports various partition transformations that can be applied to your data columns.


```py
import dlt
from dlthub.destinations.adapters import iceberg_adapter, iceberg_partition

@dlt.resource
def my_data():
    yield [{"id": 1, "created_at": "2024-01-01", "category": "A"}]

# Apply partitioning to the resource
iceberg_adapter(
    my_data,
    partition=["category"]  # Simple identity partition
)
```

### Partition transformations

Iceberg supports several transformation functions for partitioning. Use the `iceberg_partition` helper class to create partition specifications:

#### Identity partitioning
Partition by the exact value of a column (default for string columns when specified by name):

```py
# These are equivalent:
iceberg_adapter(resource, partition=["region"])
iceberg_adapter(resource, partition=[iceberg_partition.identity("region")])
```

#### Temporal transformations
Extract time components from date/datetime columns:

* `iceberg_partition.year(column_name)`: Partition by year
* `iceberg_partition.month(column_name)`: Partition by month
* `iceberg_partition.day(column_name)`: Partition by day
* `iceberg_partition.hour(column_name)`: Partition by hour

```py
import dlt
from dlthub.destinations.adapters import iceberg_adapter, iceberg_partition

@dlt.resource
def events():
    yield [{"id": 1, "event_time": datetime.datetime(2024, 3, 15, 10, 30), "data": "..."}]

iceberg_adapter(
    events,
    partition=[iceberg_partition.month("event_time")]
)
```

#### Bucket partitioning
Distribute data across a fixed number of buckets using a hash function:

```py
iceberg_adapter(
    resource,
    partition=[iceberg_partition.bucket(16, "user_id")]
)
```

#### Truncate partitioning
Partition string values by a fixed prefix length:

```py
iceberg_adapter(
    resource,
    partition=[iceberg_partition.truncate(3, "category")]  # Groups "ELECTRONICS" → "ELE"
)
```

### Advanced partitioning examples

#### Multi-column partitioning
Combine multiple partition strategies:

```py
import dlt
from dlthub.destinations.adapters import iceberg_adapter, iceberg_partition

@dlt.resource
def sales_data():
    yield [
        {
            "id": 1,
            "timestamp": datetime.datetime(2024, 1, 15, 10, 30),
            "region": "US",
            "category": "Electronics",
            "amount": 1250.00
        },
        {
            "id": 2,
            "timestamp": datetime.datetime(2024, 1, 16, 14, 20),
            "region": "EU",
            "category": "Clothing",
            "amount": 890.50
        }
    ]

# Partition by month and region
iceberg_adapter(
    sales_data,
    partition=[
        iceberg_partition.month("timestamp"),
        "region"  # Identity partition on region
    ]
)

pipeline = dlt.pipeline("sales_pipeline", destination="iceberg")
pipeline.run(sales_data)
```

#### Custom partition field names
Specify custom names for partition fields to make them more descriptive:

```py
import dlt
from dlthub.destinations.adapters import iceberg_adapter, iceberg_partition

@dlt.resource
def user_activity():
    yield [{"user_id": 123, "activity_time": datetime.datetime.now(), "action": "click"}]

iceberg_adapter(
    user_activity,
    partition=[
        iceberg_partition.year("activity_time", "activity_year"),
        iceberg_partition.bucket(8, "user_id", "user_bucket")
    ]
)
```

### Using column-level partition property

You can configure identity partitioning directly at the column level using the `"partition": True` property in the column specification. This approach uses identity transformation (partitioning by exact column values).

#### Basic column-level partitioning

```py
import dlt

@dlt.resource(columns={"region": {"partition": True}})
def sales_data():
    yield [
        {"id": 1, "region": "US", "amount": 1250.00},
        {"id": 2, "region": "EU", "amount": 890.50},
        {"id": 3, "region": "APAC", "amount": 1100.00}
    ]

pipeline = dlt.pipeline("sales_pipeline", destination="iceberg")
pipeline.run(sales_data)
```

#### Multiple column partitioning

You can partition on multiple columns by setting `"partition": True` for each column:

```py
@dlt.resource(columns={
    "region": {"partition": True},
    "category": {"partition": True},
})
def multi_partition_data():
    yield [
        {"id": 1, "region": "US", "category": "Electronics", "amount": 1250.00},
        {"id": 2, "region": "EU", "category": "Clothing", "amount": 890.50}
    ]
```

### Partitioning by dlt load id

dlt [load id](../../general-usage/destination-tables.md#load-packages-and-load-ids) is a unique identifier for each load package (a batch of data processed by dlt). Each execution of a pipeline generates a unique load id that identifies all data loaded in that specific run. The `_dlt_load_id` is a system column automatically added by `dlt` to each row of data loaded in a table.

To partition by dlt load id, set the `partition` property to `_dlt_load_id` in the column specification:

```py
@dlt.resource(columns={"_dlt_load_id": {"partition": True}})
def load_partitioned_data():
    yield [
        {"id": 1, "data": "example1"},
        {"id": 2, "data": "example2"}
    ]
```
