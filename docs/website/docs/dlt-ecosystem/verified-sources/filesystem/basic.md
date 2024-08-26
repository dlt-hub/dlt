---
title: Filesystem source
description: Learn how to set up and configure
keywords: [readers source and filesystem, files, filesystem, readers source, cloud storage]
---
import Header from '../_source-info-header.md';
<Header/>

Filesystem source is a generic source that allows loading files from remote locations (AWS S3, Google Cloud Storage, Google Drive, Azure) or the local filesystem seamlessly. Filesystem source natively supports `csv`, `parquet`, and `jsonl` files and allows customization for loading any type of structured files.

To load unstructured data (`.pdf`, `.txt`, e-mail), please refer to the [unstructured data source](https://github.com/dlt-hub/verified-sources/tree/master/sources/unstructured_data).

## Quick example

```py
import dlt
from dlt.filesystem import filesystem, read_parquet

filesystem_resource = filesystem(
  bucket_url="file://Users/admin/Documents/parquet_files",
  file_glob="**/*.parquet"
)
filesystem_pipe = filesystem_resource | read_parquet()
filesystem_pipe.apply_hints(incremental=dlt.sources.incremental("modification_date"))

# We load the data into the table_name table
pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
load_info = pipeline.run(filesystem_pipe.with_name("table_name"))
print(load_info)
print(pipeline.last_trace.last_normalize_info)
```

## Setup

### Prerequisites

Please make sure the `dlt` library is installed. Refer to the [installation guide](../../../getting-started).

### Initialize the filesystem source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init filesystem duckdb
   ```

   [dlt init command](../../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/filesystem_pipeline.py)
   with the filesystem as the source and [duckdb](../../destinations/duckdb.md) as the destination.

2. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../../destinations).

3. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

## Configuration

### Get credentials

<Tabs
  groupId="filesystem-type"
  defaultValue="aws"
  values={[
    {"label": "AWS S3", "value": "aws"},
    {"label": "GCS/GDrive", "value": "gcp"},
    {"label": "Azure", "value": "azure"},
    {"label": "Local filesystem", "value": "local"},
]}>

<TabItem value="aws">

To get AWS keys for S3 access:

1. Access IAM in the AWS Console.
2. Select "Users", choose a user, and open "Security credentials".
3. Click "Create access key" for AWS ID and Secret Key.

For more info, see
[AWS official documentation.](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html)

</TabItem>

<TabItem value="gcp">

To get GCS/GDrive access:

1. Log in to [console.cloud.google.com](http://console.cloud.google.com/).
2. Create a [service account](https://cloud.google.com/iam/docs/service-accounts-create#creating).
3. Enable "Cloud Storage API" / "Google Drive API"; see
   [Google's guide](https://support.google.com/googleapi/answer/6158841?hl=en).
4. In IAM & Admin > Service Accounts, find your account, click the three-dot menu > "Manage Keys" >
   "ADD KEY" > "CREATE" to get a JSON credential file.
5. Grant the service account appropriate permissions for cloud storage access.

For more info, see how to
[create a service account](https://support.google.com/a/answer/7378726?hl=en).

</TabItem>

<TabItem value="azure">

To obtain Azure blob storage access:

1. Go to the Azure Portal (portal.azure.com).
2. Select "Storage accounts" > your storage.
3. Click "Settings" > "Access keys".
4. View the account name and two keys (primary/secondary). Keep keys confidential.

For more info, see
[Azure official documentation](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal).

</TabItem>

<TabItem value="local">
You don't need any credentials for the local filesystem.
</TabItem>

</Tabs>

### Add credentials to dlt pipeline

To provide credentials to the filesystem source, you can use any method available in `dlt`.
One of the easiest ways is to use configuration files. The `.dlt` folder in your working directory
contains two files: `config.toml` and  `secrets.toml`. Sensitive information, like passwords and
access tokens, should only be put into `secrets.toml`, while any other configuration, like the path to
a bucket, can be specified in `config.toml`.

<Tabs
  groupId="filesystem-type"
  defaultValue="aws"
  values={[
    {"label": "AWS S3", "value": "aws"},
    {"label": "GCS/GDrive", "value": "gcp"},
    {"label": "Azure", "value": "azure"},
    {"label": "Local filesystem", "value": "local"},
]}>

<TabItem value="aws">

```toml
# secrets.toml
[sources.filesystem.credentials]
aws_access_key_id="Please set me up!"
aws_secret_access_key="Please set me up!"

# config.toml
[sources.filesystem]
bucket_url="s3://<bucket_name>/<path_to_files>/"
```
</TabItem>

<TabItem value="azure">

```toml
# secrets.toml
[sources.filesystem.credentials]
azure_storage_account_name="Please set me up!"
azure_storage_account_key="Please set me up!"

# config.toml
[sources.filesystem] # use [sources.readers.credentials] for the "readers" source
bucket_url="az://<container_name>/<path_to_files>/"
```
</TabItem>

<TabItem value="gcp">

```toml
# secrets.toml
[sources.filesystem.credentials]
client_email="Please set me up!"
private_key="Please set me up!"
project_id="Please set me up!"

# config.toml
# gdrive
[gdrive_pipeline_name.sources.filesystem]
bucket_url="gdrive://<folder_name>/<subfolder_or_file_path>/"

# config.toml
# Google storage
[gstorage_pipeline_name.sources.filesystem]
bucket_url="gs://<bucket_name>/<path_to_files>/"
```
</TabItem>

<TabItem value="local">

You can use both native local filesystem paths and `file://` URI. Absolute, relative, and UNC Windows paths are supported.

You could provide an absolute filepath:

```toml
# config.toml
[sources.filesystem]
bucket_url='file://Users/admin/Documents/csv_files'
```

Or skip the schema and provide the local path in a format native for your operating system. For example, for Windows:

```toml
[sources.filesystem]
bucket_url='~\Documents\csv_files\'
```

</TabItem>

</Tabs>

You can also specify the credentials using Environment variables. The name of the corresponding environment
variable should be slightly different than the corresponding name in the `toml` file. Simply replace dots `.` with double
underscores `__`:

```sh
export SOURCES__FILESYSTEM__AWS_ACCESS_KEY_ID = "Please set me up!"
export SOURCES__FILESYSTEM__AWS_SECRET_ACCESS_KEY = "Please set me up!"
```

:::tip
`dlt` supports more ways of authorizing with the cloud storage, including identity-based
and default credentials. To learn more about adding credentials to your pipeline, please refer to the
[Configuration and secrets section](../../../general-usage/credentials/complex_types#gcp-credentials).
:::

## Usage

The filesystem source is quite unique since it provides you with building blocks for loading data from files.
First, it iterates over files in the storage and then process each file to yield the records.
Usually, you need two resources:

1. The filesystem resource enumerates files in a selected bucket using a glob pattern, returning details as `FileInfo` in customizable page sizes.
2. One of the available transformer resources to process each file in a specific transforming function and yield the records.

### 1. Initialize a `filesystem` resource

All parameters of the resource can be specified directly in code:
```py
from dlt.filesystem import filesystem

filesystem_source = filesystem(
  bucket_url="file://Users/admin/Documents/csv_files",
  file_glob="*.csv"
)
```
or taken from the config:
```py
from dlt.filesystem import filesystem

filesystem_source = filesystem()
```

Full list of `filesystem` resource parameters:

* `bucket_url` - full URL of the bucket (could be a relative path in the case of the local filesystem).
* `credentials` - cloud storage credentials of `AbstractFilesystem` instance (should be empty for the local filesystem). We recommend not to specify this parameter in the code, but put it in secrets file instead.
* `file_glob` -  file filter in glob format. Defaults to listing all non-recursive files in the bucket URL.
* `files_per_page` - number of files processed at once. The default value is `100`.
* `extract_content` - if true, the content of the file will be read and returned in the resource. The default value is `False`.

### 2. Choose the right transformer resource

The current implementation of the filesystem source natively supports three file types: `csv`, `parquet`, and `jsonl`.
You can apply any of the above or create your own [transformer](advanced#create-your-own-transformer). To apply the selected transformer
resource, use pipe notation `|`:

```py
from dlt.filesystem import filesystem, read_csv

filesystem_pipe = filesystem(
  bucket_url="file://Users/admin/Documents/csv_files",
  file_glob="*.csv"
) | read_csv()
```

#### Available transformers

- `read_csv()`
- `read_jsonl()`
- `read_parquet()`
- `read_csv_duckdb()`

:::tip
We advise that you give each resource a
[specific name](../../../general-usage/resource#duplicate-and-rename-resources)
before loading with `pipeline.run`. This will make sure that data goes to a table with the name you
want and that each pipeline uses a
[separate state for incremental loading.](../../../general-usage/state#read-and-write-pipeline-state-in-a-resource)
:::

### 3. Create and run a pipeline

```py
import dlt
from dlt.filesystem import filesystem, read_csv

filesystem_pipe = filesystem(bucket_url="file://Users/admin/Documents/csv_files", file_glob="*.csv") | read_csv()
pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
info = pipeline.run(filesystem_pipe)
print(info)
```

For more information on how to create and run the pipeline, read the [Walkthrough: Run a pipeline](../../../walkthroughs/run-a-pipeline).

### 4. Apply hints

```py
import dlt
from dlt.filesystem import filesystem, read_csv

filesystem_pipe = filesystem(bucket_url="file://Users/admin/Documents/csv_files", file_glob="*.csv") | read_csv()
# tell dlt to merge on date
filesystem_pipe.apply_hints(write_disposition="merge", merge_key="date")

# We load the data into the table_name table
pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
load_info = pipeline.run(filesystem_pipe.with_name("table_name"))
print(load_info)
print(pipeline.last_trace.last_normalize_info)
```

### 5. Incremental loading

To load only new CSV files with [incremental loading](../../../general-usage/incremental-loading):

 ```py
 # This configuration will only consider new csv files
 new_files = filesystem(bucket_url=BUCKET_URL, file_glob="directory/*.csv")
 # add incremental on modification time
 new_files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
 load_info = pipeline.run((new_files | read_csv()).with_name("csv_files"))
 print(load_info)
 print(pipeline.last_trace.last_normalize_info)
 ```

## Troubleshooting

### Access extremely long file paths

Windows supports paths up to 255 characters. When you access a path longer than 255 characters, you'll see a `FileNotFound` exception.

 To go over this limit, you can use [extended paths](https://learn.microsoft.com/en-us/windows/win32/fileio/maximum-file-path-limitation?tabs=registry).
 **Note that Python glob does not work with extended UNC paths**, so you will not be able to use them

```toml
[sources.filesystem]
bucket_url = '\\?\C:\a\b\c'
```

### If you get an empty list of files

If you are running a `dlt` pipeline with the filesystem source and get zero records, we recommend you check
the configuration of `bucket_url` and `file_glob` parameters.

For example, with Azure Blob storage, people sometimes mistake the account name for the container name. Make sure
you've set up a URL as `"az://<container name>/"`.

Also, please reference the [glob](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.spec.AbstractFileSystem.glob)
function to configure the resource correctly. Use `**` to include recursive files. Note that the local
filesystem supports full Python [glob](https://docs.python.org/3/library/glob.html#glob.glob) functionality,
while cloud storage supports a restricted `fsspec` [version](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.spec.AbstractFileSystem.glob).

<!--@@@DLT_TUBA filesystem-->