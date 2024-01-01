# Filesystem & buckets
Filesystem destination stores data in remote file systems and bucket storages like **S3**, **google storage** or **azure blob storage**.
Underneath, it uses [fsspec](https://github.com/fsspec/filesystem_spec) to abstract file operations.
Its primary role is to be used as a staging for other destinations, but you can also quickly build a data lake with it.

> 💡 Please read the notes on the layout of the data files. Currently, we are getting feedback on it. Please join our Slack (icon at the top of the page) and help us find the optimal layout.

## Install dlt with filesystem
**To install the DLT library with filesystem dependencies:**
```
pip install dlt[filesystem]
```

This installs `s3fs` and `botocore` packages.

:::caution

You may also install the dependencies independently.
Try:
```sh
pip install dlt
pip install s3fs
```
so pip does not fail on backtracking.
:::

## Setup Guide

### 1. Initialise the dlt project

Let's start by initialising a new dlt project as follows:
   ```bash
   dlt init chess filesystem
   ```
   > 💡 This command will initialise your pipeline with chess as the source and the AWS S3 filesystem as the destination.

### 2. Set up bucket storage and credentials

#### AWS S3
The command above creates sample `secrets.toml` and requirements file for AWS S3 bucket. You can install those dependencies by running:
```
pip install -r requirements.txt
```

To edit the `dlt` credentials file with your secret info, open `.dlt/secrets.toml`, which looks like this:
```toml
[destination.filesystem]
bucket_url = "s3://[your_bucket_name]" # replace with your bucket name,

[destination.filesystem.credentials]
aws_access_key_id = "please set me up!" # copy the access key here
aws_secret_access_key = "please set me up!" # copy the secret access key here
```

If you have your credentials stored in `~/.aws/credentials` just remove the **[destination.filesystem.credentials]** section above
and `dlt` will fall back to your **default** profile in local credentials.
If you want to switch the profile, pass the profile name as follows (here: `dlt-ci-user`):
```toml
[destination.filesystem.credentials]
profile_name="dlt-ci-user"
```

You can also pass an AWS region:
```toml
[destination.filesystem.credentials]
region_name="eu-central-1"
```

You need to create a S3 bucket and a user who can access that bucket. `dlt` is not creating buckets automatically.

1. You can create the S3 bucket in the AWS console by clicking on "Create Bucket" in S3 and assigning the appropriate name and permissions to the bucket.
2. Once the bucket is created, you'll have the bucket URL. For example, If the bucket name is `dlt-ci-test-bucket`, then the bucket URL will be:

   ```
   s3://dlt-ci-test-bucket
   ```

3. To grant permissions to the user being used to access the S3 bucket, go to the IAM > Users, and click on “Add Permissions”.
4. Below you can find a sample policy that gives a minimum permission required by `dlt` to a bucket we created above. The policy contains permissions to list files in a bucket, get, put and delete objects. **Remember to place your bucket name in Resource section of the policy!**

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
5. To grab the access and secret key for the user. Go to IAM > Users and in the “Security Credentials”, click on “Create Access Key”, and preferably select “Command Line Interface” and create the access key.
6. Grab the “Access Key” and “Secret Access Key” created that are to be used in "secrets.toml".

##### Using S3 compatible storage

To use an S3 compatible storage other than AWS S3 like [MinIO](https://min.io/) or [Cloudflare R2](https://www.cloudflare.com/en-ca/developer-platform/r2/) you may supply an `endpoint_url` in the config. This should be set along with aws credentials:

```toml
[destination.filesystem]
bucket_url = "s3://[your_bucket_name]" # replace with your bucket name,

[destination.filesystem.credentials]
aws_access_key_id = "please set me up!" # copy the access key here
aws_secret_access_key = "please set me up!" # copy the secret access key here
endpoint_url = "https://<account_id>.r2.cloudflarestorage.com" # copy your endpoint URL here
```

##### Adding Additional Configuration

To pass any additional arguments to `fsspec`, you may supply `kwargs` and `client_kwargs` in the config as a **stringified dictionary**:

```toml
[destination.filesystem]
kwargs = '{"use_ssl": true}'
client_kwargs = '{"verify": "public.crt"}'
```

#### Google Storage
Run `pip install dlt[gs]` which will install `gcfs` package.

To edit the `dlt` credentials file with your secret info, open `.dlt/secrets.toml`.
You'll see AWS credentials by default.
Use Google cloud credentials that you may know from [BigQuery destination](bigquery.md)
```toml
[destination.filesystem]
bucket_url = "gs://[your_bucket_name]" # replace with your bucket name,

[destination.filesystem.credentials]
project_id = "project_id" # please set me up!
private_key = "private_key" # please set me up!
client_email = "client_email" # please set me up!
```

> 💡 Note that you can share the same credentials with BigQuery, replace the **[destination.filesystem.credentials]** section with less specific one: **[destination.credentials]** which applies to both destinations

if you have default google cloud credentials in your environment (i.e. on cloud function) remove the credentials sections above and `dlt` will fall back to the available default.

Use **Cloud Storage** admin to create a new bucket. Then assign the **Storage Object Admin** role to your service account.

#### Azure Blob Storage
Run `pip install dlt[az]` which will install the `adlfs` package to interface with Azure Blob Storage.

Edit the credentials in `.dlt/secrets.toml`, you'll see AWS credentials by default replace them with your Azure credentials:

```toml
[destination.filesystem]
bucket_url = "az://[your_container name]" # replace with your container name

[destination.filesystem.credentials]
# The storage account name is always required
azure_storage_account_name = "account_name" # please set me up!
# You can set either account_key or sas_token, only one is needed
azure_storage_account_key = "account_key" # please set me up!
azure_storage_sas_token = "sas_token" # please set me up!
```

If you have the correct Azure credentials set up on your machine (e.g. via azure cli),
you can omit both `azure_storage_account_key` and `azure_storage_sas_token` and `dlt` will fall back to the available default.
Note that `azure_storage_account_name` is still required as it can't be inferred from the environment.

#### Local file system
If for any reason you want to have those files in local folder, set up the `bucket_url` as follows (you are free to use `config.toml` for that as there are no secrets required)

```toml
[destination.filesystem]
bucket_url = "file:///absolute/path"  # three / for absolute path
# bucket_url = "file://relative/path" # two / for a relative path
```

## Write disposition
`filesystem` destination handles the write dispositions as follows:
- `append` - files belonging to such tables are added to dataset folder
- `replace` - all files that belong to such tables are deleted from dataset folder, and then the current set of files is added.
- `merge` - falls back to `append`

## Data loading
All the files are stored in a single folder with the name of the dataset that you passed to the `run` or `load` methods of `pipeline`. In our example chess pipeline it is **chess_players_games_data**.

> 💡 Note that bucket storages are in fact key-blob storage so folder structure is emulated by splitting file names into components by `/`.

### Files layout

The name of each file contains essential metadata on the content:

- **schema_name** and **table_name** identify the [schema](../../general-usage/schema.md) and table that define the file structure (column names, data types, etc.)
- **load_id** is the [id of the load package](../../general-usage/destination-tables.md#load-packages-and-load-ids) form which the file comes from.
- **file_id** is there are many files with data for a single table, they are copied with different file id.
- **ext** a format of the file i.e. `jsonl` or `parquet`

Current default layout: **{table_name}/{load_id}.{file_id}.{ext}`**

> 💡 Note that the default layout format has changed from `{schema_name}.{table_name}.{load_id}.{file_id}.{ext}` to `{table_name}/{load_id}.{file_id}.{ext}` in dlt 0.3.12. You can revert to the old layout by setting the old value in your toml file.


You can change the file name format by providing the layout setting for the filesystem destination like so:
```toml
[destination.filesystem]
layout="{table_name}/{load_id}.{file_id}.{ext}" # current preconfigured naming scheme
# layout="{schema_name}.{table_name}.{load_id}.{file_id}.{ext}" # naming scheme in dlt 0.3.11 and earlier
```

A few things to know when specifying your filename layout:
- If you want a different base path that is common to all filenames, you can suffix your `bucket_url` rather than prefix your `layout` setting.
- If you do not provide the `{ext}` placeholder, it will automatically be added to your layout at the end with a dot as separator.
- It is the best practice to have a separator between each placeholder. Separators can be any character allowed as a filename character, but dots, dashes and forward slashes are most common.
- When you are using the `replace` disposition, `dlt`` will have to be able to figure out the correct files to delete before loading the new data. For this
to work, you have to
  - include the `{table_name}` placeholder in your layout
  - not have any other placeholders except for the `{schema_name}` placeholder before the table_name placeholder and
  - have a separator after the table_name placeholder

Please note:
- `dlt` will not dump the current schema content to the bucket
- `dlt` will mark complete loads by creating an empty file that corresponds to `_dlt_loads` table. For example, if `chess._dlt_loads.1685299832` file is present in dataset folders, you can be sure that all files for the load package `1685299832` are completely loaded

## Supported file formats
You can choose the following file formats:
* [jsonl](../file-formats/jsonl.md) is used by default
* [parquet](../file-formats/parquet.md) is supported


## Syncing of `dlt` state
This destination does not support restoring the `dlt` state. You can change that by contributing to the core library 😄
You can, however, easily [backup and restore the pipeline working folder](https://gist.github.com/rudolfix/ee6e16d8671f26ac4b9ffc915ad24b6e) - reusing the bucket and credentials used to store files.
