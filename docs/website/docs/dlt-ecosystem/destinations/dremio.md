---
title: 🧪 Dremio
description: Dremio `dlt` destination
keywords: [dremio, iceberg, aws, glue catalog]
---

# Dremio

## Install dlt with Dremio
**To install the dlt library with Dremio and s3 dependencies:**
```sh
pip install "dlt[dremio,s3]"
```

## Setup guide
### 1. Initialize the dlt project

Let's start by initializing a new dlt project as follows:
   ```sh
   dlt init chess dremio
   ```
   > 💡 This command will initialize your pipeline with chess as the source and aws dremio as the destination using the filesystem staging destination.


### 2. Setup bucket storage and Dremio credentials

First, install dependencies by running:
```sh
pip install -r requirements.txt
```
or with `pip install "dlt[dremio,s3]"` which will install `s3fs`, `pyarrow`, and `botocore` packages.

To edit the `dlt` credentials file with your secret info, open `.dlt/secrets.toml`. You will need to provide a `bucket_url` which holds the uploaded parquet files.

The TOML file looks like this:

```toml
[destination.filesystem]
bucket_url = "s3://[your_bucket_name]" # replace with your bucket name,

[destination.filesystem.credentials]
aws_access_key_id = "please set me up!" # copy the access key here
aws_secret_access_key = "please set me up!" # copy the secret access key here

[destination.dremio]
staging_data_source = "<staging-data-source>" # the name of the "Object Storage" data source in Dremio containing the s3 bucket

[destination.dremio.credentials]
username = "<username>"  # the Dremio username
password = "<password or pat token>"  # Dremio password or PAT token
database = "<database>" # the name of the "data source" set up in Dremio where you want to load your data
host = "localhost" # the Dremio hostname
port = 32010 # the Dremio Arrow Flight grpc port
drivername="grpc" # either 'grpc' or 'grpc+tls'
```

You can also pass a SqlAlchemy-like connection like below:
```toml
[destination.dremio]
staging_data_source="s3_staging"
credentials="grpc://<username>:<password>@<host>:<port>/<data_source>"
```

If you have your credentials stored in `~/.aws/credentials`, just remove the **[destination.filesystem.credentials]** and **[destination.dremio.credentials]** sections above and `dlt` will fall back to your **default** profile in local credentials. If you want to switch the profile, pass the profile name as follows (here: `dlt-ci-user`):
```toml
[destination.filesystem.credentials]
profile_name="dlt-ci-user"
```

## Write disposition

`dremio` destination handles the write dispositions as follows:
- `append`
- `replace`
- `merge`

> The `merge` write disposition uses the default DELETE/UPDATE/INSERT strategy to merge data into the destination. Be aware that Dremio does not support transactions, so a partial pipeline failure can result in the destination table being in an inconsistent state. The `merge` write disposition will eventually be implemented using [MERGE INTO](https://docs.dremio.com/current/reference/sql/commands/apache-iceberg-tables/apache-iceberg-merge/) to resolve this issue.

## Data loading

Data loading happens by copying staged parquet files from an object storage bucket to the destination table in Dremio using [COPY INTO](https://docs.dremio.com/cloud/reference/sql/commands/copy-into-table/) statements. The destination table format is specified by the storage format for the data source in Dremio. Typically, this will be Apache Iceberg.

> ❗ **Dremio cannot load `fixed_len_byte_array` columns from `parquet` files**.

## Dataset creation

Dremio does not support `CREATE SCHEMA` DDL statements.

Therefore, "Metastore" data sources, such as Hive or Glue, require that the dataset schema exists prior to running the dlt pipeline. `dev_mode=True` is unsupported for these data sources.

"Object Storage" data sources do not have this limitation.

## Staging support

Using a staging destination is mandatory when using the Dremio destination. If you do not set staging to `filesystem`, dlt will automatically do this for you.

## Table partitioning and local sort
Apache Iceberg table partitions and local sort properties can be configured as shown below:
```py
import dlt
from dlt.common.schema import TColumnSchema

@dlt.resource(
    table_name="my_table",
    columns=dict(
        foo=TColumnSchema(partition=True),
        bar=TColumnSchema(partition=True),
        baz=TColumnSchema(sort=True),
    ),
)
def my_table_resource():
  ...
```
This will result in `PARTITION BY ("foo","bar")` and `LOCALSORT BY ("baz")` clauses being added to the `CREATE TABLE` DML statement.

> ***Note:*** Table partition migration is not implemented. The table will need to be dropped and recreated to alter partitions or localsort.

### Syncing of `dlt` state
- This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

<!--@@@DLT_TUBA dremio-->

