---

title: Databricks
description: Databricks `dlt` destination
keywords: [Databricks, destination, data warehouse]

---

# Databricks
*Big thanks to Evan Phillips and [swishbi.com](https://swishbi.com/) for contributing code, time and test environment*

## Install dlt with Databricks
**To install the DLT library with Databricks dependencies:**
```
pip install dlt[databricks]
```

## Setup Guide

**1. Initialize a project with a pipeline that loads to Databricks by running**
```
dlt init chess databricks
```

**2. Install the necessary dependencies for Databricks by running**
```
pip install -r requirements.txt
```
This will install dlt with **databricks** extra which contains Databricks Python dbapi client.

**4. Enter your credentials into `.dlt/secrets.toml`.**

This should have your connection parameters and your personal access token.

It should now look like:

```toml
[destination.databricks.credentials]
server_hostname = "MY_DATABRICKS.azuredatabricks.net"
http_path = "/sql/1.0/warehouses/12345"
access_token "MY_ACCESS_TOKEN"
catalog = "my_catalog"
```

## Write disposition
All write dispositions are supported

## Data loading
Data is loaded using `INSERT VALUES` statements by default.

Efficient loading from a staging filesystem is also supported by configuring an Amazon S3 or Azure Blob Storage bucket as a staging destination. When staging is enabled `dlt` will upload data in `parquet` files to the bucket and then use `COPY INTO` statements to ingest the data into Databricks.
For more information on staging, see the [staging support](#staging-support) section below.

## Supported file formats
* [insert-values](../file-formats/insert-format.md) is used by default
* [jsonl](../file-formats/jsonl.md) supported when staging is enabled (see limitations below)
* [parquet](../file-formats/parquet.md) supported when staging is enabled

The `jsonl` format has some limitations when used with Databricks:

1. Compression must be disabled to load jsonl files in databricks. Set `data_writer.disable_compression` to `true` in dlt config when using this format.
2. The following data types are not supported when using `jsonl` format with `databricks`: `decimal`, `complex`, `date`, `binary`. Use `parquet` if your data contains these types.
3. `bigint` data type with precision is not supported with `jsonl` format


## Staging support

Databricks supports both Amazon S3 and Azure Blob Storage as staging locations. `dlt` will upload files in `parquet` format to the staging location and will instruct Databricks to load data from there.

### Databricks and Amazon S3

Please refer to the [S3 documentation](./filesystem.md#aws-s3) for details on connecting your s3 bucket with the bucket_url and credentials.

Example to set up Databricks with s3 as a staging destination:

```python
import dlt

# Create a dlt pipeline that will load
# chess player data to the Databricks destination
# via staging on s3
pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline',
    destination='databricks',
    staging=dlt.destinations.filesystem('s3://your-bucket-name'), # add this to activate the staging location
    dataset_name='player_data',
)
```

### Databricks and Azure Blob Storage

Refer to the [Azure Blob Storage filesystem documentation](./filesystem.md#azure-blob-storage) for details on connecting your Azure Blob Storage container with the bucket_url and credentials.

Example to set up Databricks with Azure as a staging destination:

```python
# Create a dlt pipeline that will load
# chess player data to the Databricks destination
# via staging on Azure Blob Storage
pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline',
    destination='databricks',
    staging=dlt.destinations.filesystem('az://your-container-name'), # add this to activate the staging location
    dataset_name='player_data'
)
```
### dbt support
This destination [integrates with dbt](../transformations/dbt/dbt.md) via [dbt-databricks](https://github.com/databricks/dbt-databricks)

### Syncing of `dlt` state
This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).
