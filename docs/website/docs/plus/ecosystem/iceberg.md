---
title: "Destination: Iceberg"
description: Iceberg destination
keywords: [Iceberg, pyiceberg]
---

# Iceberg

The Iceberg destination is based on the [filesystem destination](../../dlt-ecosystem/destinations/filesystem.md) in dlt. All configuration options from the filesystem destination can be configured as well.

Under the hood, dlt+ uses the [pyiceberg library](https://py.iceberg.apache.org/) to write Iceberg tables. One or multiple Parquet files are prepared during the extract and normalize steps. In the load step, these Parquet files are exposed as an Arrow data structure and fed into pyiceberg.

## Setup

Make sure you have installed the necessary dependencies:
```sh
pip install pyiceberg
pip install sqlalchemy>=2.0.18
```

Initialize a dlt+ project in the current working directory with the following command:

```sh
# replace sql_database with the source of your choice
dlt project init sql_database iceberg
```

This will create an Iceberg destination in your `dlt.yml`, where you can configure the destination:

```yaml
destinations:
  iceberg_destination:
    type: iceberg
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
[destination.iceberg.credentials]
aws_access_key_id="Please set me up!"
aws_secret_access_key="Please set me up!"
```
</TabItem>

<TabItem value="azure">

```toml
# secrets.toml
[destination.iceberg.credentials]
azure_storage_account_name="Please set me up!"
azure_storage_account_key="Please set me up!"
```
</TabItem>

<TabItem value="gcp">

Only OAuth 2.0 is currently supported for GCS.

```toml
# secrets.toml
[destination.iceberg.credentials]
project_id="project_id"  # please set me up!
client_id = "client_id"  # please set me up!
client_secret = "client_secret"  # please set me up!
refresh_token = "refresh_token"  # please set me up!
```
</TabItem>

<TabItem value="sftp">

Learn how to set up SFTP credentials for each authentication method in the [SFTP section](../../dlt-ecosystem/destinations/filesystem#sftp).
For example, in the case of key-based authentication, you can configure the source the following way:

```toml
# secrets.toml
[destination.iceberg.credentials]
sftp_username = "foo"
sftp_key_filename = "/path/to/id_rsa"     # Replace with the path to your private key file
sftp_key_passphrase = "your_passphrase"   # Optional: passphrase for your private key
```
</TabItem>

</Tabs>

The Iceberg destination can also be defined in Python as follows:

```py
pipeline = dlt.pipeline("loads_iceberg", destination="iceberg")
```

## Write dispositions

The Iceberg destination handles the write dispositions as follows:
- `append` - files belonging to such tables are added to the dataset folder.
- `replace` - all files that belong to such tables are deleted from the dataset folder, and then the current set of files is added.
- `merge` - can be used only with the `delete-insert` [merge strategy](../../general-usage/incremental-loading#delete-insert-strategy).

The `merge` write disposition can be configured as follows on the source/resource level:

<Tabs values={[{"label": "dlt.yml", "value": "yaml"}, {"label": "Python", "value": "python"}]}  groupId="language" defaultValue="yaml">
  <TabItem value="yaml">

```yaml
sources:
  my_source:
    type: sources.my_source
    with_args:
      write_disposition:
        disposition: merge
        strategy: delete-insert
```
  </TabItem>
  <TabItem value="python">

```py
@dlt.resource(
    primary_key="id",  # merge_key also works; primary_key and merge_key may be used together
    write_disposition={"disposition": "merge", "strategy": "delete-insert"},
)
def my_resource():
    yield [
        {"id": 1, "foo": "foo"},
        {"id": 2, "foo": "bar"}
    ]
...

pipeline = dlt.pipeline("loads_iceberg", destination="iceberg")

```
</TabItem>
</Tabs>

Or on the `pipeline.run` level: <!-- can this also be defined in the yaml??-->

```py
pipeline.run(write_disposition={"disposition": "merge", "strategy": "delete-insert"})
```

## Partitioning

Iceberg tables can be partitioned (using [hidden partitioning](https://iceberg.apache.org/docs/latest/partitioning/)) by specifying one or more partition column hints on the source/resource level:

<Tabs values={[{"label": "dlt.yml", "value": "yaml"}, {"label": "Python", "value": "python"}]}  groupId="language" defaultValue="yaml">
  <TabItem value="yaml">

  ```yaml
  sources:
    my_source:
      type: sources.my_source
      with_args:
        columns:
          foo:
            partition: True
  ```

  </TabItem>
  <TabItem value="python">

  ```py
  @dlt.resource(
    columns={"foo": {"partition": True}}
  )
  def my_resource():
      ...

  pipeline = dlt.pipeline("loads_iceberg", destination="iceberg")
  ```

  </TabItem>
</Tabs>

:::caution
Partition evolution (changing partition columns after a table has been created) is not supported.
:::

## Catalogs

dlt+ uses single-table, ephemeral, in-memory, sqlite-based Iceberg catalogs. These catalogs are created "on demand" when a pipeline is run, and do not persist afterwards. If a table already exists in the filesystem, it gets registered into the catalog using its latest metadata file. This allows for a serverless setup.

It is currently not possible to connect your own Iceberg catalog, but support for multi-vendor catalogs (such as Polaris & Unity Catalog) is coming soon.

:::caution
While ephemeral catalogs make it easy to get started with Iceberg, they come with limitations:
* Concurrent writes are not handled and may lead to a corrupt table state.
* We cannot guarantee that reads concurrent with writes are clean.
* The latest manifest file needs to be searched for using file listing—this can become slow with large tables, especially in cloud object stores.
:::

## Table access helper functions
You can use the `get_iceberg_tables` helper function to access native pyiceberg [Table](https://py.iceberg.apache.org/reference/pyiceberg/table/#pyiceberg.table.Table) objects.

```py
from dlt.common.libs.pyiceberg import get_iceberg_tables

...

# get dictionary of Table objects
delta_tables = get_iceberg_tables(pipeline)

# execute operations on Table objects
iceberg_tables["my_iceberg_table"].optimize.compact()
iceberg_tables["another_iceberg_table"].optimize.z_order(["col_a", "col_b"])
# iceberg_tables["my_iceberg_table"].vacuum()
```

## Table format
The Iceberg destination automatically assigns the `iceberg` table format to all resources that it will load. You can still fall back to storing files by setting `table_format` to native on the resource level:

  ```py
  @dlt.resource(
    table_format="native"
  )
  def my_resource():
      ...

  pipeline = dlt.pipeline("loads_iceberg", destination="iceberg")
  ```

## Known limitations
The Iceberg destination is still under active development and therefore has a few known limitations described below.

### GCS authentication methods

Only [OAuth 2.0](../../dlt-ecosystem/destinations/bigquery#oauth-20-authentication) is supported for Google Cloud Storage.

The [S3-compatible](../../dlt-ecosystem/destinations/filesystem#using-s3-compatible-storage) interface for Google Cloud Storage is not supported with the Iceberg destination.

### Azure Blob Storage URL

The `az` [scheme](../../dlt-ecosystem/destinations/filesystem#supported-schemes) for Azure paths specified in `bucket_url` does not work out of the box. To get it to work, you need to specify the environment variable `AZURE_STORAGE_ANON="false"`.

### Compound keys
Compound keys are not supported: use a single `primary_key` **and/or** a single `merge_key`.

As a workaround, you can [transform](../../general-usage/resource#filter-transform-and-pivot-data) your resource data with `add_map` to add a new column that contains a hash of the key columns, and use that column as `primary_key` or `merge_key`.

### Nested tables
Nested tables are currently not supported with the `merge` write disposition: avoid complex data types or [disable nesting](../../general-usage/source#reduce-the-nesting-level-of-generated-tables).

