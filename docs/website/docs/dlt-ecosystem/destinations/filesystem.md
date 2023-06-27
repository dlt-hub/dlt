# Filesystem & buckets

Filesystem destination stores data in remote file systems and bucket storages like **S3**, **google
storage** or **azure blob storage**. Underneath it uses
[fsspec](https://github.com/fsspec/filesystem_spec) to abstract file operations. Its primary role is
to be used as a staging for other destinations but you can also quickly build a
[data lake](../../getting-started/build-a-data-platform/build-structured-data-lakehouse.md) with it.

The default file format is **jsonl**. **parquet** is supported.

> 💡 Please read the notes on the layout of the data files. Currently we are getting feedback on it.
> Please join our slack (icon at the top of the page) and help us to find the optimal layout.

## Setup Guide

### 1. Initialize the dlt project

Let's start by initializing a new dlt project as follows:

```bash
dlt init chess filesystem
```

> 💡 This command will initialise your pipeline with chess as the source and the AWS S3 filesystem as
> the destination.

### 2. Setup bucket storage and credentials

#### AWS S3

The command above creates sample `secrets.toml` and requirements file for AWS S3 bucket. You can
install those dependencies by running:

```
pip install -r requirements.txt
```

or with `pip install dlt[s3]` which will install `s3fs` and `boto3` packages.

To edit the `dlt` credentials file with your secret info, open `.dlt/secrets.toml`, which looks like
this:

```toml
[destination.filesystem]
bucket_url = "s3://[your_bucket_name]" # replace with your bucket name,

[destination.filesystem.credentials]
aws_access_key_id = "please set me up!" # copy the access key here
aws_secret_access_key = "please set me up!" # copy the secret access key here
```

if you have your credentials stored in `~/.aws/credentials` just remove the
**\[destination.filesystem.credentials\]** section above and `dlt` will fall back to your
**default** profile in local credentials. If you want to switch the profile, pass the profile name
as follows (here: `dlt-ci-user`):

```toml
[destination.filesystem.credentials]
aws_profile="dlt-ci-user"
```

You need to create a S3 bucket and a user who can access that bucket. `dlt` is not creating buckets
automatically.

1. You can create the S3 bucket in AWS console by clicking on "Create Bucket" in S3 and assigning
   appropriate name and permissions to the bucket.

1. Once the bucket is created you'll have the bucket URL. For example, If the bucket name is
   `dlt-ci-test-bucket`, then the bucket URL will be:

   ```
   s3://dlt-ci-test-bucket
   ```

1. To grant permissions to the user being used to access the S3 bucket, go to the IAM > Users, and
   click on “Add Permissions”.

1. Below you can find a sample policy that gives a minimum permissions required by `dlt` to a bucket
   we created above. The policy contains permissions to list files in a bucket, get, put and delete
   objects. **Remember to place your bucket name in Resource section of the policy!**

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "DltBucketAccess",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject",
                "s3:GetObjectAttributes",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::dlt-ci-test-bucket/*",
                "arn:aws:s3:::dlt-ci-test-bucket"
            ]
        }
    ]
}
```

5. To grab the access and secret key for the user. Go to IAM > Users and in the “Security
   Credentials”, click on “Create Access Key”, and preferably select “Command Line Interface” and
   create the access key.
1. Grab the “Access Key” and “Secret Access Key” created that are to be used in "secrets.toml".

#### Google Storage

Run `pip install dlt[gs]` which will install `gcfs` package.

To edit the `dlt` credentials file with your secret info, open `.dlt/secrets.toml`. You'll see AWS
credentials by default. Use google cloud credentials that you may know from
[BigQuery destination](bigquery.md)

```toml
[destination.filesystem]
bucket_url = "gs://[your_bucket_name]" # replace with your bucket name,

[destination.filesystem.credentials]
project_id = "project_id" # please set me up!
private_key = "private_key" # please set me up!
client_email = "client_email" # please set me up!
```

> 💡 Note that you can share the same credentials with BigQuery, just replace the
> **\[destination.filesystem.credentials\]** section with less specific one:
> **\[destination.credentials\]** which applies to both destinations

if you have default google cloud credentials in your environment (ie. on cloud function) just remove
the credentials sections above and `dlt` will fallback to them.

Use **Cloud Storage** admin to create a new bucket. Then assign the **Storage Object Admin** role to
your service account.

#### Azure Blob Storage

Let us know on our Slack that you need it.

#### Local file system

If for any reason you want to have those files in local folder, setup the `bucket_url` as follows
(you are free to use `config.toml` for that as there are no secrets required)

```toml
[destination.filesystem]
bucket_url = "file:///absolute/path"  # three / for absolute path
# bucket_url = "file://relative/path"  # two / for relative path
```

## Data loading

All the files are stored in a single folder with the name of the dataset that you passed to the
`run` or `load` methods of `pipeline`. In our example chess pipeline it is
**chess_players_games_data**.

> 💡 Note that bucket storages are in fact key-blob storage so folder structure is emulated by
> splitting file names into components by `/`.

The name of each file contains essential metadata on the content:

`<schema_name>.<table_name>.<load_id>.<file_id>.<file_format>` where:

- **schema_name** and **table_name** identify the [schema](../../general-usage/schema.md) and table
  that define the file structure (column names, data types etc.)
- **load_id** is the
  [id of the load package](https://dlthub.com/docs/dlt-ecosystem/visualizations/understanding-the-tables#load-ids)
  form which the file comes from.
- **file_id** is there are many files with data for a single table, they are copied with different
  file id.
- **file_format** a format of the file ie. `jsonl` or `parquet`

Please note:

- `dlt` will not dump the current schema content to the bucket
- `dlt` will mark complete loads by creating an empty file that corresponds to `_dlt_loads` table.
  For example if `chess._dlt_loads.1685299832` file is present in dataset folders, you can be sure
  that all files for the load package `1685299832` are completely loaded

## Write disposition

`filesystem` destination handles the write dispositions as follows:

- `append` - files belonging to such tables are added to dataset folder
- `replace` - all files that belong to such tables are deleted from dataset folder and then current
  set of files is added.
- `merge` - fallbacks to `append` or to `replace` when no merge nor primary key is defined

## Syncing of `dlt` state

This destination does not support restoring the `dlt` state. You can change that by contributing to
the core library 😄 You can however easily backup and restore the pipeline working folder - reusing
the bucket and credentials used to store files.
