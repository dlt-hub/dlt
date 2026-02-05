---
title: ClickHouse
description: ClickHouse `dlt` destination
keywords: [ clickhouse, destination, data warehouse ]
---

# ClickHouse

There are two separate destinations for ClickHouse:
1) `clickhouse`: for single-node ClickHouse and ClickHouse Cloud databases
2) `clickhouse_cluster`: for ClickHouse clusters

`clickhouse_cluster` is built on top of `clickhouse`, but adds additional functionality related to distributed tables.

The first part of this page applies to both `clickhouse` and `clickhouse_cluster`. The last section is specific to `clickhouse_cluster`

## Install dlt with ClickHouse

**To install the DLT library with ClickHouse dependencies:**

```sh
pip install "dlt[clickhouse]"
```

<!--@@@DLT_DESTINATION_CAPABILITIES clickhouse-->

## Setup guide

### 1. Initialize the dlt project

Let's start by initializing a new `dlt` project as follows:

```sh
dlt init chess clickhouse
```

`dlt init` command will initialize your pipeline with chess as the source and ClickHouse as the destination.

The above command generates several files and directories, including `.dlt/secrets.toml` and a requirements file for ClickHouse. You can install the necessary dependencies specified in the requirements file by executing it as follows:

```sh
pip install -r requirements.txt
```

or with `pip install "dlt[clickhouse]"`, which installs the `dlt` library and the necessary dependencies for working with ClickHouse as a destination.

### 2. Setup ClickHouse database

To load data into ClickHouse, you need to create a ClickHouse database. While we recommend asking our GPT-4 assistant for details, we've provided a general outline of the process below:

1. You can use an existing ClickHouse database or create a new one.

2. To create a new database, connect to your ClickHouse server using the `clickhouse-client` command line tool or a SQL client of your choice.

3. Run the following SQL commands to create a new database, user, and grant the necessary permissions:

   ```sql
   CREATE DATABASE IF NOT EXISTS dlt;
   CREATE USER dlt IDENTIFIED WITH sha256_password BY 'Dlt*12345789234567';
   GRANT CREATE, ALTER, SELECT, DELETE, DROP, TRUNCATE, OPTIMIZE, SHOW, INSERT, dictGet ON dlt.* TO dlt;
   GRANT SELECT ON INFORMATION_SCHEMA.COLUMNS TO dlt;
   GRANT CREATE TEMPORARY TABLE, S3 ON *.* TO dlt;
   ```

### 3. Add credentials

1. Next, set up the ClickHouse credentials in the `.dlt/secrets.toml` file as shown below:

   ```toml
   [destination.clickhouse.credentials]
   database = "dlt"                         # The database name you created.
   username = "dlt"                         # ClickHouse username, default is usually "default".
   password = "Dlt*12345789234567"          # ClickHouse password, if any.
   host = "localhost"                       # ClickHouse server host.
   port = 9000                              # ClickHouse native TCP protocol port, default is 9000.
   http_port = 8443                         # ClickHouse HTTP port, default is 9000.
   secure = 1                               # Set to 1 if using HTTPS, else 0.
   ```

   :::info Network Ports
    The `http_port` parameter specifies the port number to use when connecting to the ClickHouse server's HTTP interface.
    The default non-secure HTTP port for ClickHouse is `8123`.
    This is different from the default port `9000`, which is used for the native TCP protocol.

    You must additionaly set `http_port` if you are not using external staging (i.e., you don't set the `staging` destination parameter in your pipeline). This is because dlt's built-in ClickHouse local storage staging uses the [clickhouse-connect](https://github.com/ClickHouse/clickhouse-connect) library, which communicates with ClickHouse over HTTP.

    Make sure your ClickHouse server is configured to accept HTTP connections on the port specified by `http_port`. For example:

   - If you set `http_port = 8123` (default non-secure HTTP port), then ClickHouse should be listening for HTTP requests on port 8123.
   - If you set `http_port = 8443`, then ClickHouse should be listening for secure HTTPS requests on port 8443.

   If you're using external staging, you can omit the `http_port` parameter, since clickhouse-connect will not be used in this case.

   For local development and testing with ClickHouse running locally, it is recommended to use the default non-secure HTTP port `8123` by setting `http_port=8123` or omitting the parameter.

   Please see the [ClickHouse network port documentation](https://clickhouse.com/docs/en/guides/sre/network-ports) for further reference.
   :::

2. You can pass a database connection string similar to the one used by the `clickhouse-driver` library. The credentials above will look like this:

   ```toml
   # Keep it at the top of your TOML file, before any section starts
   destination.clickhouse.credentials="clickhouse://dlt:Dlt*12345789234567@localhost:9000/dlt?secure=1"
   ```

### 3. Add configuration options

You can set the following configuration options in the `.dlt/secrets.toml` file:

```toml
[destination.clickhouse]
dataset_table_separator = "___"                         # The default separator for dataset table names from the dataset.
table_engine_type = "merge_tree"                        # The default table engine to use.
dataset_sentinel_table_name = "dlt_sentinel_table"      # The default name for sentinel tables.
staging_use_https = true                                # Whether to connect to the staging bucket via https (defaults to True)
```

## Write disposition

All [write dispositions](../../general-usage/incremental-loading#choosing-a-write-disposition) are supported.

## Data loading

Data is loaded into ClickHouse using the most efficient method depending on the data source:

- For local files, the `clickhouse-connect` library is used to directly load files into ClickHouse tables using the `INSERT` command.
- For files in remote storage like S3, Google Cloud Storage, or Azure Blob Storage, ClickHouse table functions like `s3`, `gcs`, and `azureBlobStorage` are used to read the files and insert the data into tables.

## Datasets

ClickHouse does not support multiple datasets in one database; dlt relies on datasets to exist for multiple reasons.
To make ClickHouse work with `dlt`, tables generated by `dlt` in your ClickHouse database will have their names prefixed with the dataset name, separated by
the configurable `dataset_table_separator`.
Additionally, a special sentinel table that doesn't contain any data will be created, so dlt knows which virtual datasets already exist in a
clickhouse
destination.

:::tip
`dataset_name` is optional for ClickHouse. When skipped `dlt` will create all tables without prefix. Note that staging dataset
tables will still be prefixed with `_staging` (or other name that you configure).
:::

## Supported file formats

- [JSONL](../file-formats/jsonl.md) is the preferred format for both direct loading and staging.
- [Parquet](../file-formats/parquet.md) is supported for both direct loading and staging.

The `clickhouse` destination has a few specific deviations from the default SQL destinations:

1. ClickHouse has an experimental `object` datatype, but we've found it to be a bit unpredictable, so the dlt `clickhouse` destination will load the `json` datatype to a `text` column.
   If you need
   this feature, get in touch with our Slack community, and we will consider adding it.
2. ClickHouse does not support the `time` datatype. Time will be loaded to a `text` column.
3. ClickHouse does not support the `binary` datatype. Binary will be loaded to a `text` column. When loading from JSONL, this will be a base64 string; when loading from parquet, this will be
   the `binary` object converted to `text`.
4. ClickHouse accepts adding columns to a populated table that aren’t null.
5. ClickHouse can produce rounding errors under certain conditions when using the float/double datatype. Make sure to use decimal if you can’t afford to have rounding errors. Loading the value 12.7001 to a double column with the loader file format jsonl set will predictably produce a rounding error, for example.

## Supported column hints

ClickHouse supports the following [column hints](../../general-usage/schema#tables-and-columns):

- `primary_key` - marks the column as part of the primary key. Multiple columns can have this hint to create a composite primary key.

## Choosing a table engine

dlt defaults to the `MergeTree` table engine. You can specify an alternate table engine in two ways:

### Setting a default table engine in the configuration

You can set a default table engine for all resources and dlt tables by adding the `table_engine_type` parameter to your ClickHouse credentials in the `.dlt/secrets.toml` file:

```toml
[destination.clickhouse]
# ... (other configuration options)
table_engine_type = "merge_tree"                        # default table engine for all tables
```

Set `dlt_tables_table_engine_type` if you want to use a different default table engine for `dlt`-managed metadata tables (such as `_dlt_loads` and `_dlt_version`):

```toml
[destination.clickhouse]
table_engine_type = "merge_tree"                        # default table engine for data tables
dlt_tables_table_engine_type = "replicated_merge_tree"  # default table engine for dlt tables
```

`dlt_tables_table_engine_type` falls back to `table_engine_type` if not explicitly configured.

### Setting the table engine for specific resources

You can also set the table engine for specific resources using the clickhouse_adapter, which will override the default engine set in `.dlt/secrets.toml` for that resource:

```py
from dlt.destinations.adapters import clickhouse_adapter

@dlt.resource()
def my_resource():
    ...

clickhouse_adapter(my_resource, table_engine_type="merge_tree")
```

Supported values for `table_engine_type` are:

- `merge_tree` (default) - creates tables using the `MergeTree` engine, suitable for most use cases. [Learn more about MergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree).
- `shared_merge_tree` - creates tables using the `SharedMergeTree` engine, optimized for cloud-native environments with shared storage. This table is **only** available on ClickHouse Cloud, and it is the default selection if `merge_tree` is selected. [Learn more about SharedMergeTree](https://clickhouse.com/docs/en/cloud/reference/shared-merge-tree).
- `replicated_merge_tree` - creates tables using the `ReplicatedMergeTree` engine, which supports data replication across multiple nodes for high availability. [Learn more about ReplicatedMergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replication). This defaults to `shared_merge_tree` on ClickHouse Cloud.
- Experimental support for the `Log` engine family with `stripe_log` and `tiny_log`.

For local development and testing with ClickHouse running locally, the `MergeTree` engine is recommended.

## Sorting and partitioning
You can use the `clickhouse_adapter` to specify a [sorting](https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree#order_by) and/or [partition](https://clickhouse.com/docs/engines/table-engines/mergetree-family/custom-partitioning-key) key:

```py
from dlt.destinations.adapters import clickhouse_adapter

@dlt.resource
def my_resource():
    ...

clickhouse_adapter(
   my_resource,
   sort = sorting_key,
   partition = partition_key
)
```

`sort` and `partition` are used to generate the `ORDER BY` and `PARTITION BY` clauses of the table creation statement, and they accept either a **sequence of column names** or a **SQL expression**:
1. **sequence of column names:** recommended if column transformations are not required
2. **SQL expression:** use if column transformations are required

The two examples below show both approaches to adapt `my_resource`:

```py
@dlt.resource(
   columns={
      "timestamp": {"nullable": False},
      "town": {"nullable": False},
      "street": {"nullable": False}
   }
)
def my_resource():
    ...
```

### Example 1: sequence of column names

```py
clickhouse_adapter(
   my_resource,
   sort=["timestamp", "street"],
   partition=["town"]
)
```

Corresponding SQL:

```sql
CREATE TABLE ...
...
ORDER BY (timestamp, street)
PARTITION by town
```

### Example 2: SQL expression

```py
clickhouse_adapter(
   my_resource,
   sort="(upper(town), street)",
   partition="toYYYYMMDD(timestamp)"
)
```

Corresponding SQL:

```sql
CREATE TABLE ...
...
ORDER BY (upper(town), street)
PARTITION BY toYYYYMMDD(timestamp)
```

### Usage notes

SQL expressions are used **as is** to generate the SQL clauses. Hence, when providing a SQL expression, use normalized column names:

```py
@dlt.resource(columns={"TIMESTAMP": {"nullable": False}})  # non-normalized column name (upper case)
def my_resource():
    ...

clickhouse_adapter(my_resource, partition="toYYYYMMDD(timestamp)")  # RIGHT: normalized column name (lower case)

clickhouse_adapter(my_resource, partition="toYYYYMMDD(TIMESTAMP)")  # WRONG: non-normalized column name (lower case)
```

:::note
- The sorting/partitioning key can only be set when the table is first created. The value for `sort`/`partition` is ignored for existing tables.
- We explicitly mark the sorting/partition columns as **not nullable** in the examples above, because, by default, ClickHouse does not allow nullable columns in the sorting/partition key. Set `allow_nullable_key` to `True` in your [table settings](#mergetree-table-settings) if you insist on nullable key columns.
:::

### `sort` and `partition` column hints
`dlt` automatically creates `sort`/`partition` [column hints](../../general-usage/schema.md#tables-and-columns) for columns present in the `sort`/`partition` value provided to `clickhouse_adapter` (when this value is a SQL expression, we parse it to extract the column names).

Although it's possible to set `sort`/`partition` column hints directly, we recommend using `clickhouse_adapter` instead.

If you still choose to set `sort`/`partition` column hints yourself, know that:
- columns are added to the `ORDER BY`/`PARTITION BY` clause in order of appearance in the schema
- they may be overridden/removed if you also use `clickhouse_adapter`: the adapter takes precedence, and it will set column hints in accordance with the values provided to its `sort`/`partition` parameters

## MergeTree table settings
Use the `settings` parameter of the `clickhouse_adapter` to specify [MergeTree settings](https://clickhouse.com/docs/operations/settings/merge-tree-settings) for the table:

```py
from dlt.destinations.adapters import clickhouse_adapter

@dlt.resource
def my_resource():
    ...

clickhouse_adapter(
   my_resource,
   settings = {
      "allow_nullable_key": True,
      "max_suspicious_broken_parts": 500,
      "deduplicate_merge_projection_mode": "ignore"
   }
)
```

The `settings` parameter is used to generate the `SETTINGS` clause of the table creation statement:

```sql
CREATE TABLE ...
...
SETTINGS allow_nullable_key = true, max_suspicious_broken_parts = 500, deduplicate_merge_projection_mode = 'ignore'
```

## Column codecs
Use the `codecs` parameter of the `clickhouse_adapter` to specify [codecs](https://clickhouse.com/docs/sql-reference/statements/create/table#column_compression_codec) for the table's columns:

```py
from dlt.destinations.adapters import clickhouse_adapter

@dlt.resource
def my_resource():
    ...

clickhouse_adapter(
   my_resource,
   codecs = {
      "town": "ZSTD(3)",
      "number": "Delta, ZSTD(2)"
   }
)
```

The codecs are used in the table creation statement as follows:

```sql
CREATE TABLE my_resource
(
   `town` String CODEC(ZSTD(3)),
   `street` String, -- no codec specified
   `number` Int64 CODEC(Delta, ZSTD(2))
)
...
```

## Staging support

ClickHouse supports Amazon S3, Google Cloud Storage, and Azure Blob Storage as file staging destinations.

`dlt` will upload Parquet or JSONL files to the staging location and use ClickHouse table functions to load the data directly from the staged files.

Please refer to the filesystem documentation to learn how to configure credentials for the staging destinations:

- [Amazon S3](./filesystem.md#aws-s3)
- [Google Cloud Storage](./filesystem.md#google-storage)
- [Azure Blob Storage](./filesystem.md#azure-blob-storage)

To run a pipeline with staging enabled:

```py
pipeline = dlt.pipeline(
  pipeline_name='chess_pipeline',
  destination='clickhouse',
  staging='filesystem',  # add this to activate staging
  dataset_name='chess_data'
)
```

### Using Google Cloud or S3-compatible storage as a staging area

dlt supports using S3-compatible storage services, including Google Cloud Storage (GCS), as a staging area when loading data into ClickHouse.
This is handled automatically by
ClickHouse's [GCS table function](https://clickhouse.com/docs/en/sql-reference/table-functions/gcs), which dlt uses under the hood.

The ClickHouse GCS table function only supports authentication using Hash-based Message Authentication Code (HMAC) keys, which is compatible with the Amazon S3 API.
To enable this, GCS provides an S3
compatibility mode that emulates the S3 API, allowing ClickHouse to access GCS buckets via its S3 integration.

For detailed instructions on setting up S3-compatible storage with dlt, including AWS S3, MinIO, and Cloudflare R2, refer to
the [dlt documentation on filesystem destinations](../../dlt-ecosystem/destinations/filesystem#using-s3-compatible-storage).

To set up GCS staging with HMAC authentication in dlt:

1. Create HMAC keys for your GCS service account by following the [Google Cloud guide](https://cloud.google.com/storage/docs/authentication/managing-hmackeys#create).

2. Configure the HMAC keys (`aws_access_key_id` and `aws_secret_access_key`) as well as `endpoint_url` in your dlt project's ClickHouse destination settings in `config.toml`, similar to how you would configure AWS S3 credentials:

```toml
[destination.filesystem]
bucket_url = "s3://my_awesome_bucket"

[destination.filesystem.credentials]
aws_access_key_id = "JFJ$$*f2058024835jFffsadf"
aws_secret_access_key = "DFJdwslf2hf57)%$02jaflsedjfasoi"
project_id = "my-awesome-project"
endpoint_url = "https://storage.googleapis.com"
```

:::warning
When configuring the `bucket_url` for S3-compatible storage services like Google Cloud Storage (GCS) with ClickHouse in dlt, ensure that the URL is prepended with `s3://` instead of `gs://`. This is
because the ClickHouse GCS table function requires the use of HMAC credentials, which are compatible with the S3 API. Prepending with `s3://` allows the HMAC credentials to integrate properly with
dlt's staging mechanisms for ClickHouse.
:::

When using S3 for a staging area you can alternatively have ClickHouse authenticate using Role-based access with the
[supported](https://clickhouse.com/docs/sql-reference/table-functions/s3#using-s3-credentials-clickhouse-cloud) `extra_credentials` argument by setting this with the destination credentials:
```py
import dlt
from dlt.destinations import clickhouse

destination = clickhouse(
  credentials={
    # Other credentials you need
    "s3_extra_credentials": {
      'role_arn': 'arn:your:role' # The AWS Role assumed by ClickHouse
    }
  }
)

pipeline = dlt.pipeline(
  pipeline_name='chess_pipeline',
  destination=destination,
  staging='filesystem',  # add this to activate staging
  dataset_name='chess_data'
)
```

### dbt support

Integration with [dbt](../transformations/dbt/dbt.md) is generally supported via dbt-clickhouse but not tested by us. Note how
we support datasets by prefixing the table names. You should take it into account when writing models (or use empty dataset to avoid prefixing).

### Syncing of `dlt` state

This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

# ClickHouse Cluster
The `clickhouse_cluster` destination is specifically made to load data into a ClickHouse cluster. It adapts the SQL statements generated by `dlt` to work well in cluster setups, for example by using [distributed DDL](https://clickhouse.com/docs/sql-reference/distributed-ddl) (i.e. `ON CLUSTER` clauses).

:::note
If you have a single-node ClickHouse or ClickHouse Cloud database, then use the `clickhouse` destination described above. Do **not** use the `clickhouse_cluster` destination in those cases.
:::

## Cluster configuration
If your ClickHouse cluster has more than one replica, `dlt` expects it is configured with `internal_replication` set to `true`; having it set to `false` may lead to incorrect data.

:::note
As a reference, you can find the cluster configuration used to test `dlt` [here](https://github.com/dlt-hub/dlt/tree/master/tests/load/clickhouse_cluster/test_cluster).
:::

## Setup guide
A basic configuration looks like this:

```toml
[destination.clickhouse_cluster]
cluster = "cluster_2S_2R"                       # name of the cluster

[destination.clickhouse_cluster.credentials]
database = "dlt"
username = "dlt"
password = "Dlt*12345789234567"
host = "localhost"
port = 9001
http_port = 8444
secure = 1
```

Credentials setup is the same as it is for `clickhouse` (documentation [here](#3-add-credentials)), but for `clickhouse_cluster` we need to additionally specify the name of the cluster.

:::note
`clickhouse_cluster` supports all configuration [options](#3-add-configuration-options) that `clickhouse` supports.
:::

## Distributed tables
Set `create_distributed_tables` to `true` to create [distributed](https://clickhouse.com/docs/engines/table-engines/special/distributed) tables alongside the standard tables:

```toml
[destination.clickhouse_cluster]
create_distributed_tables = true
```

By default, this will create distributed tables with suffix `_dist` and sharding key `rand()` in the same database as the standard tables are in. Hence, if you have a pipeline that creates table `my_db`.`my_table`, `dlt` will create an additional distributed table called `my_db`.`my_table_dist` with engine:

```sql
Distributed(cluster_2S_2R, my_db, my_table, rand())
```

When running your pipeline to load data, `dlt` will insert into the distributed table, and records are allocated to shards according to the sharding key.

Use these options to customize behavior:

```toml
[destination.clickhouse_cluster]
distributed_tables_database = "db_dist"      # database to create distributed tables in
distributed_table_suffix = "_distributed"    # suffix to generate name of distributed tables
sharding_key = "rand() + 1"                  # sharding key for distributed tables
```

## Typical architectures
ClickHouse describes three typical architectures in their documentation:
1. [Replication](https://clickhouse.com/docs/architecture/replication)
2. [Scaling](https://clickhouse.com/docs/architecture/horizontal-scaling)
3. [Replication + Scaling](https://clickhouse.com/docs/architecture/cluster-deployment)

Configure `dlt` as follows to apply these architectures:

1. Replication
```toml
[destination.clickhouse_cluster]
table_engine_type = "replicated_merge_tree"
create_distributed_tables = false            # default, so you can leave this out
```

2. Scaling
```toml
[destination.clickhouse_cluster]
table_engine_type = "merge_tree"             # default, so you can leave this out
create_distributed_tables = false
```
3. Replication + Scaling
```toml
[destination.clickhouse_cluster]
table_engine_type = "replicated_merge_tree"
create_distributed_tables = true
```

## ClickHouse Cluster adapter
You can use `clickhouse_cluster_adapter` to overwrite global configuration options per resource:

```py
from dlt.destinations.adapters import clickhouse_cluster_adapter

@dlt.resource
def replicated_resource():
    ...

@dlt.resource
def scaled_resource():
    ...

clickhouse_cluster_adapter(
   replicated_resource,
   table_engine_type = "replicated_merge_tree" ,
   create_distributed_tables = False               # default, so you can leave this out
)

clickhouse_cluster_adapter(
   scaled_resource,
   table_engine_type = "merge_tree",               # default, so you can leave this out
   create_distributed_tables = True,
   distributed_table_suffix = "_query",
   sharding_key = "id % 4"
)
```

Adapter settings apply to the root table, as well as any nested tables. There is one exception: `sharding_key` is only applied to the root table; distributed tables associated with nested tables always use the default
sharding key `rand()`. This is to avoid breaking pipelines in case a custom sharding key refers to
a column that is not present in the nested table.

:::note
`clickhouse_cluster_adapter` supports all configuration options that `clickhouse_adapter` supports.
:::

## Alternative hosts
You can specify alternative hosts to fall back to in case the primary node cannot be connected to:

```toml
[destination.clickhouse_cluster.credentials]
host = "host1"                                  # primary host
port = 9440                                     # primary TCP port
http_port = 8443                                # primary HTTP port
# ...
alt_hosts = "host1:9441,host2:9440"             # do not include primary `host:port` here
alt_http_hosts = "host1:8444,host2:8443"        # do not include primary `host:http_port` here
```


<!--@@@DLT_TUBA clickhouse-->
