---

title: Databricks
description: Databricks `dlt` destination
keywords: [Databricks, destination, data warehouse]

---

# Databricks

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
* [parquet](../file-formats/parquet.md) supported when staging is enabled

## Staging support

Databricks supports both Amazon S3 and Azure Blob Storage as staging locations. `dlt` will upload files in `parquet` format to the staging location and will instruct Databricks to load data from there.

### Databricks and Amazon S3

Please refer to the [S3 documentation](./filesystem.md#aws-s3) to learn how to set up your bucket with the bucket_url and credentials. For s3, the dlt Databricks loader will use the AWS credentials provided for s3 to access the s3 bucket if not specified otherwise (see config options below). You can specify your s3 bucket directly in your d

lt configuration:

To set up Databricks with s3 as a staging destination:

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

Refer to the [Azure Blob Storage filesystem documentation](./filesystem.md#azure-blob-storage) for setting up your container with the bucket_url and credentials. For Azure Blob Storage, Databricks can directly load data from the storage container specified in the configuration:

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
