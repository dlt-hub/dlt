---
title: "Destination: Delta"
description: Delta destination
keywords: [delta, delta lake]
---

# Delta

import { DltHubFeatureAdmonition } from '@theme/DltHubFeatureAdmonition';

<DltHubFeatureAdmonition />

The Delta destination is based on the [filesystem destination](../../dlt-ecosystem/destinations/filesystem.md) in dlt. All configuration options from the filesystem destination can be configured as well.

:::warning
Under the hood, dltHub uses the [deltalake library](https://pypi.org/project/deltalake/) to write Delta tables. Beware that when loading a large amount of data for one table, the underlying Rust implementation will consume a lot of memory. This is a known issue, and the maintainers are actively working on a solution. You can track the progress [here](https://github.com/delta-io/delta-rs/pull/2289). Until the issue is resolved, you can mitigate the memory consumption by doing multiple smaller incremental pipeline runs.
:::

## Setup

Make sure you have installed the necessary dependencies:
```sh
pip install deltalake
pip install pyarrow>=2.0.18
```

Initialize a dltHub project in the current working directory with the following command:

```sh
# replace sql_database with the source of your choice
dlt project init sql_database delta
```

This will create a Delta destination in your `dlt.yml`, where you can configure the destination:

```yaml
destinations:
  delta_destination:
    type: delta
    bucket_url: "s3://your_bucket" # replace with bucket url
```

The credentials can be defined in the `secrets.toml`:

<Tabs
  groupId="filesystem-type"
  defaultValue="aws"
  values={[
    {"label": "AWS S3", "value": "aws"},
    {"label": "GCS/GDrive", "value": "gcp"},
    {"label": "Azure", "value": "azure"},
    {"label": "SFTP", "value": "sftp"},
]}>

<TabItem value="aws">

```toml
# secrets.toml
[destination.delta.credentials]
aws_access_key_id="Please set me up!"
aws_secret_access_key="Please set me up!"
```
</TabItem>

<TabItem value="azure">

```toml
# secrets.toml
[destination.delta.credentials]
azure_storage_account_name="Please set me up!"
azure_storage_account_key="Please set me up!"
```
</TabItem>

<TabItem value="gcp">

:::warning
Only [Service Account](../../dlt-ecosystem/destinations/bigquery#setup-guide) and [Application Default Credentials](../../dlt-ecosystem/destinations/bigquery#using-default-credentials) authentication methods are supported for Google Cloud Storage.
:::

```toml
# secrets.toml
[destination.delta.credentials]
client_email="Please set me up!"
private_key="Please set me up!"
project_id="Please set me up!"
```
</TabItem>

<TabItem value="sftp">

Learn how to set up SFTP credentials for each authentication method in the [SFTP section](../../dlt-ecosystem/destinations/filesystem#sftp).
For example, in the case of key-based authentication, you can configure the source the following way:

```toml
# secrets.toml
[destination.delta.credentials]
sftp_username = "foo"
sftp_key_filename = "/path/to/id_rsa"     # Replace with the path to your private key file
sftp_key_passphrase = "your_passphrase"   # Optional: passphrase for your private key
```
</TabItem>

</Tabs>


The Delta destination can also be defined in Python as follows:

```py
pipeline = dlt.pipeline("loads_delta", destination="delta")
```

## Write dispositions

The Delta destination handles the write dispositions as follows:
- `append` - files belonging to such tables are added to the dataset folder.
- `replace` - all files that belong to such tables are deleted from the dataset folder, and then the current set of files is added.
- `merge` - can be used only with the `upsert` [merge strategy](../../general-usage/merge-loading.md#upsert-strategy).

:::warning
The `upsert` merge strategy for the Delta destination is **experimental**.
:::

The `merge` write disposition can be configured as follows on the source/resource level:

```py
import dlt

@dlt.resource(
    primary_key="id",  # merge_key also works; primary_key and merge_key may be used together
    write_disposition={"disposition": "merge", "strategy": "upsert"},
)
def my_resource():
    yield [
        {"id": 1, "foo": "foo"},
        {"id": 2, "foo": "bar"}
    ]
...

pipeline = dlt.pipeline("loads_delta", destination="delta")
```

Or on the `pipeline.run` level: <!-- can this also be defined in the yaml??-->

```py
pipeline.run(write_disposition={"disposition": "merge", "strategy": "upsert"})
```

## Partitioning

Delta tables can be partitioned (using [Hive-style partitioning](https://delta.io/blog/pros-cons-hive-style-partionining/)) by specifying one or more partition column hints on the source/resource level:

```py
import dlt

@dlt.resource(
  columns={"_dlt_load_id": {"partition": True}}
)
def my_resource():
    ...

pipeline = dlt.pipeline("loads_delta", destination="delta")
```

:::warning
Partition evolution (changing partition columns after a table has been created) is currently not supported.
:::

## Table access helper functions
You can use the `get_delta_tables` helper functions to access the native [DeltaTable](https://delta-io.github.io/delta-rs/api/delta_table/) objects.

```py
from dlt.common.libs.deltalake import get_delta_tables

...

# get dictionary of DeltaTable objects
delta_tables = get_delta_tables(pipeline)

# execute operations on DeltaTable objects
delta_tables["my_delta_table"].optimize.compact()
delta_tables["another_delta_table"].optimize.z_order(["col_a", "col_b"])
# delta_tables["my_delta_table"].vacuum()
# etc.
```

## Table format
The Delta destination automatically assigns the `delta` table format to all resources that it will load. You can still fall back to storing files by setting `table_format` to native on the resource level:

  ```py
  @dlt.resource(
    table_format="native"
  )
  def my_resource():
      ...

  pipeline = dlt.pipeline("loads_delta", destination="delta")
  ```

## Storage options and configuration
You can pass storage options and configuration by configuring both `destination.filesystem.deltalake_storage_options` and
`destination.filesystem.deltalake_configuration`:

```toml
[destination.filesystem]
deltalake_configuration = '{"delta.enableChangeDataFeed": "true", "delta.minWriterVersion": "7"}'
deltalake_storage_options = '{"AWS_S3_LOCKING_PROVIDER": "dynamodb", "DELTA_DYNAMO_TABLE_NAME": "custom_table_name"}'
```

dlt passes these as arguments to the `write_deltalake` method in the `deltalake` library (`deltalake_configuration` maps to `configuration` and `deltalake_storage_options` maps to `storage_options`). Look at their [documentation](https://delta-io.github.io/delta-rs/api/delta_writer/#deltalake.write_deltalake) to see which options can be used.

You don't need to specify credentials here. dlt merges the required credentials with the options you provided before passing it as `storage_options`.

:::warning
When using `s3`, you need to specify storage options to [configure](https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider/) locking behavior.
:::
