---
title: Amazon Redshift
description: Amazon Redshift `dlt` destination
keywords: [redshift, destination, data warehouse]
---

# Amazon Redshift

**1. Initialize a project with a pipeline that loads to Redshift by running**
```
dlt init chess redshift
```

**2. Install the necessary dependencies for Redshift by running**
```
pip install -r requirements.txt
```
This will install dlt with **redshift** extra which contains `psycopg2` client.

**3. Edit the `dlt` credentials file with your info**
```
open .dlt/secrets.toml
```

## dbt support
This destination [integrates with dbt](../transformations/dbt.md) via [dbt-redshift](https://github.com/dbt-labs/dbt-redshift).

## Supported loader file formats

Supported loader file formats for Redshift are `sql` and `insert_values` (default). When using a staging location, Redshift supports `parquet` and `jsonl`.

## Redshift and staging on s3

Redshift supports s3 as a file staging destination. DLT will upload files in the parquet format to s3 and ask redshift to copy their data directly into the db. Please refere to the [S3 documentation](./filesystem.md#aws-s3) to learn how to set up your s3 bucket with the bucket_url and credentials. The dlt Redshift loader will use the aws credentials provided for s3 to access the s3 bucket if not specified otherwise (see config options below). Alternavitely to parquet files, you can also specify jsonl as the staging file format. For this set the `loader_file_format` argument of the `run` command of the pipeline to `jsonl`.


### Authentication via cluster 

If you would like to load from s3 without forwarding the aws staging credentials in every copy command to redshift, you can disable this by setting the config option `forward_staging_credentials` to false:

```toml
[destination.filesystem]
forward_staging_credentials=false
```

And create authentication via an S3 Cluster: TODO

### Redshift/S3 staging example code

```python
# Create a dlt pipeline that will load
# chess player data to the redshift destination
# via staging on s3
pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline',
    destination='redshift',
    staging='filesystem', # add this to activate the staging location
    dataset_name='player_data'
)
```
