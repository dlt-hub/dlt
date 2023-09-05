---
title: AWS Athena / Glue Catalog
description: AWS Athena `dlt` destination
keywords: [aws, athena, glue catalog]
---

# AWS Athena / Glue Catalog

The athena destination stores data as parquet files in s3 buckets and creates [external tables in aws athena](https://docs.aws.amazon.com/athena/latest/ug/creating-tables.html). You can then query those tables with athena sql commands which will then scan the whole folder of parquet files and return the results. This destination works very similar to other sql based destinations, with the exception of the merge write disposition not being supported at this time. DLT metadata will be stored in the same bucket as the parquet files, but as iceberg tables.

## Setup Guide
### 1. Initialize the dlt project

Let's start by initializing a new dlt project as follows:
   ```bash
   dlt init chess athena
   ```
   > 💡 This command will initialise your pipeline with chess as the source and aws athena as the destination using the filesystem staging destination


### 2. Setup bucket storage and athena credentials

First install dependencies by running:
```
pip install -r requirements.txt
```
or with `pip install dlt[athena]` which will install `s3fs`, `pyarrow`, `pyathena` and `botocore` packages.

:::caution

You may also install the dependencies independently
try
```sh
pip install dlt
pip install s3fs
pip install pyarrow
pip install pyathena
```
so pip does not fail on backtracking
:::

To edit the `dlt` credentials file with your secret info, open `.dlt/secrets.toml`. You will need to provide a `bucket_url` which holds the uploaded parquet files, a `query_result_bucket` which athena uses to write query results too, and credentials that have write and read access to these two buckets as well as the full athena access aws role.

The toml file looks like this:

```toml
[destination.filesystem]
bucket_url = "s3://[your_bucket_name]" # replace with your bucket name,

[destination.filesystem.credentials]
aws_access_key_id = "please set me up!" # copy the access key here
aws_secret_access_key = "please set me up!" # copy the secret access key here

[destination.athena]
query_result_bucket="s3://[results_bucket_name]" # replace with your query results bucket name

[destination.athena.credentials]
aws_access_key_id="please set me up!" # same as credentials for filesystem
aws_secret_access_key="please set me up!" # same as credentials for filesystem
region_name="please set me up!" # set your aws region, for example "eu-central-1" for frankfurt
```

if you have your credentials stored in `~/.aws/credentials` just remove the **[destination.filesystem.credentials]** and **[destination.athena.credentials]** section above and `dlt` will fall back to your **default** profile in local credentials. If you want to switch the  profile, pass the profile name as follows (here: `dlt-ci-user`):
```toml
[destination.filesystem.credentials]
aws_profile="dlt-ci-user"

[destination.athena.credentials]
aws_profile="dlt-ci-user"
```

## Additional Destination Configuration

You can provide an athena workgroup like so:
```toml
[destination.athena]
athena_work_group="my_workgroup"
```

## Write disposition

`athena` destination handles the write dispositions as follows:
- `append` - files belonging to such tables are added to dataset folder
- `replace` - all files that belong to such tables are deleted from dataset folder and then current set of files is added.
- `merge` - falls back to `append`

## Data loading

Data loading happens by storing parquet files in an s3 bucket and defining a schema on athena. If you query data via sql queries on athena, the returned data is read by
scanning your bucket and reading all relevant parquet files in there.

`dlt` internal tables are saved as Iceberg tables.

### Data types
Athena tables store timestamps with millisecond precision and with that precision we generate parquet files. Mind that Iceberg tables have microsecond precision. 

Athena does not support JSON fields so JSON is stored as string.

> ❗**Athena does not support TIME columns in parquet files**. `dlt` will fail such jobs permanently. Convert `datetime.time` objects to `str` or `datetime.datetime` to load them.

### Naming Convention
We follow our snake_case name convention. Mind the following:
* DDL use HIVE escaping with ``````
* Other queries use PRESTO and regular SQL escaping.

## Staging support

Using a staging destination is mandatory when using the athena destination. If you do not set staging to `filesystem`, dlt will automatically do this for you.

If you decide to change the [filename layout](./filesystem#data-loading) from the default value, keep the following in mind so that athena can reliable build your tables:
 - You need to provide the `{table_name}` placeholder and this placeholder needs to be followed by a forward slash
 - You need to provide the `{file_id}` placeholder and it needs to be somewhere after the `{table_name}` placeholder.
 - {table_name} must be a first placeholder in the layout.


## Additional destination options


### dbt support

Athena is supported via `dbt-athena-community`. Credentials are passed into `aws_access_key_id` and `aws_secret_access_key` of generated dbt profile.
Athena adapter requires that you setup **region_name** in Athena configuration below. You can also setup table catalog name to change the default: **awsdatacatalog**
```toml
[destination.athena]
aws_data_catalog="awsdatacatalog"
```

### Syncing of `dlt` state
- This destination fully supports [dlt state sync.](../../general-usage/state#syncing-state-with-destination). The state is saved in athena iceberg tables in your s3 bucket.


## Supported file formats
You can choose the following file formats:
* [parquet](../file-formats/parquet.md) is used by default

------
