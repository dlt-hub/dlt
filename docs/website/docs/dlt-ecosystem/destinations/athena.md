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

To edit the `dlt` credentials file with your secret info, open `.dlt/secrets.toml`. You will need to provide a `bucket_url` which holds the uploaded parquetfiles, a `query_result_bucket` which athena uses to write query results too, and credentials that have write and read access to these two buckets as well as the full athena access aws role.

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
region_name="please set me up!" # set your aws region, for exampe "eu-central-1" for frankfurt
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
workgroup="my_workgroup"
```

## Write disposition
`filesystem` destination handles the write dispositions as follows:
- `append` - files belonging to such tables are added to dataset folder
- `replace` - all files that belong to such tables are deleted from dataset folder and then current set of files is added.
- `merge` - fallbacks to `append`


## Supported file formats
You can choose the following file formats:
* [parquet](../file-formats/parquet.md) is used by default

------
