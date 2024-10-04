---
title: Filesystem source
description: Learn how to set up and configure
keywords: [readers source and filesystem, files, filesystem, readers source, cloud storage, object storage, local file system]
---
import Header from '../_source-info-header.md';
<Header/>

Filesystem source allows loading files from remote locations (AWS S3, Google Cloud Storage, Google Drive, Azure Blob Storage, SFTP server) or the local filesystem seamlessly. Filesystem source natively supports [CSV](../../file-formats/csv.md), [Parquet](../../file-formats/parquet.md), and [JSONL](../../file-formats/jsonl.md) files and allows customization for loading any type of structured files.

To load unstructured data (PDF, plain text, e-mail), please refer to the [unstructured data source](https://github.com/dlt-hub/verified-sources/tree/master/sources/unstructured_data).

## How filesystem source works

The Filesystem source doesn't just give you an easy way to load data from both remote and local files — it also comes with a powerful set of tools that let you customize the loading process to fit your specific needs.

Filesystem source loads data in two steps:
1. It [accesses the files](#1-initialize-a-filesystem-resource) in your remote or local file storage without actually reading the content yet. At this point, you can [filter files by metadata or name](#6-filter-files). You can also set up [incremental loading](#5-incremental-loading) to load only new files.
2. [The transformer](#2-choose-the-right-transformer-resource) reads the files' content and yields the records. At this step, you can filter out the actual data, enrich records with metadata from files, or [perform incremental loading](#load-new-records-based-on-a-specific-column) based on the file content.

## Quick example

```py
import dlt
from dlt.sources.filesystem import filesystem, read_parquet

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

Please make sure the `dlt` library is installed. Refer to the [installation guide](../../../intro).

### Initialize the filesystem source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```sh
   dlt init filesystem duckdb
   ```

   The [dlt init command](../../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/filesystem_pipeline.py)
   with the filesystem as the source and [duckdb](../../destinations/duckdb.md) as the destination.

2. If you would like to use a different destination, simply replace `duckdb` with the name of your
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
    {"label": "SFTP", "value": "sftp"},
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

<TabItem value="sftp">

dlt supports several authentication methods:

1. Key-based authentication
2. SSH Agent-based authentication
3. Username/Password authentication
4. GSS-API authentication

Learn more about SFTP authentication options in the [SFTP section](../../destinations/filesystem#sftp). To obtain credentials, contact your server administrator.
</TabItem>

<TabItem value="local">
You don't need any credentials for the local filesystem.
</TabItem>

</Tabs>

### Add credentials to dlt pipeline

To provide credentials to the filesystem source, you can use [any method available](../../../general-usage/credentials/setup#available-config-providers) in dlt.
One of the easiest ways is to use configuration files. The `.dlt` folder in your working directory contains two files: `config.toml` and `secrets.toml`. Sensitive information, like passwords and access tokens, should only be put into `secrets.toml`, while any other configuration, like the path to a bucket, can be specified in `config.toml`.

<Tabs
  groupId="filesystem-type"
  defaultValue="aws"
  values={[
    {"label": "AWS S3", "value": "aws"},
    {"label": "GCS/GDrive", "value": "gcp"},
    {"label": "Azure", "value": "azure"},
    {"label": "SFTP", "value": "sftp"},
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

<TabItem value="sftp">

Learn how to set up SFTP credentials for each authentication method in the [SFTP section](../../destinations/filesystem#sftp).
For example, in the case of key-based authentication, you can configure the source the following way:

```toml
# secrets.toml
[sources.filesystem.credentials]
sftp_username = "foo"
sftp_key_filename = "/path/to/id_rsa"     # Replace with the path to your private key file
sftp_key_passphrase = "your_passphrase"   # Optional: passphrase for your private key

# config.toml
[sources.filesystem] # use [sources.readers.credentials] for the "readers" source
bucket_url = "sftp://[hostname]/[path]"
```
</TabItem>

<TabItem value="local">

You can use both native local filesystem paths and the `file://` URI. Absolute, relative, and UNC Windows paths are supported.

You could provide an absolute filepath:

```toml
# config.toml
[sources.filesystem]
bucket_url='file://Users/admin/Documents/csv_files'
```

Or skip the schema and provide the local path in a format native to your operating system. For example, for Windows:

```toml
[sources.filesystem]
bucket_url='~\Documents\csv_files\'
```

</TabItem>

</Tabs>

You can also specify the credentials using environment variables. The name of the corresponding environment variable should be slightly different from the corresponding name in the TOML file. Simply replace dots `.` with double underscores `__`:

```sh
export SOURCES__FILESYSTEM__AWS_ACCESS_KEY_ID = "Please set me up!"
export SOURCES__FILESYSTEM__AWS_SECRET_ACCESS_KEY = "Please set me up!"
```

:::tip
dlt supports more ways of authorizing with cloud storage, including identity-based and default credentials. To learn more about adding credentials to your pipeline, please refer to the [Configuration and secrets section](../../../general-usage/credentials/complex_types#gcp-credentials).
:::

## Usage

The filesystem source is quite unique since it provides you with building blocks for loading data from files. First, it iterates over files in the storage and then processes each file to yield the records. Usually, you need two resources:

1. The `filesystem` resource enumerates files in a selected bucket using a glob pattern, returning details as `FileItem` in customizable page sizes.
2. One of the available transformer resources to process each file in a specific transforming function and yield the records.

### 1. Initialize a `filesystem` resource

:::note
If you use just the `filesystem` resource, it will only list files in the storage based on glob parameters and yield the files [metadata](advanced#fileitem-fields). The `filesystem` resource itself does not read or copy files.
:::

All parameters of the resource can be specified directly in code:
```py
from dlt.sources.filesystem import filesystem

filesystem_source = filesystem(
  bucket_url="file://Users/admin/Documents/csv_files",
  file_glob="*.csv"
)
```
or taken from the config:

* python code:

  ```py
  from dlt.sources.filesystem import filesystem

  filesystem_source = filesystem()
  ```

* configuration file:
  ```toml
  [sources.filesystem]
  bucket_url="file://Users/admin/Documents/csv_files"
  file_glob="*.csv"
  ```

Full list of `filesystem` resource parameters:

* `bucket_url` - full URL of the bucket (could be a relative path in the case of the local filesystem).
* `credentials` - cloud storage credentials of `AbstractFilesystem` instance (should be empty for the local filesystem). We recommend not specifying this parameter in the code, but putting it in a secrets file instead.
* `file_glob` -  file filter in glob format. Defaults to listing all non-recursive files in the bucket URL.
* `files_per_page` - number of files processed at once. The default value is `100`.
* `extract_content` - if true, the content of the file will be read and returned in the resource. The default value is `False`.

### 2. Choose the right transformer resource

The current implementation of the filesystem source natively supports three file types: CSV, Parquet, and JSONL.
You can apply any of the above or [create your own transformer](advanced#create-your-own-transformer). To apply the selected transformer resource, use pipe notation `|`:

```py
from dlt.sources.filesystem import filesystem, read_csv

filesystem_pipe = filesystem(
  bucket_url="file://Users/admin/Documents/csv_files",
  file_glob="*.csv"
) | read_csv()
```

#### Available transformers

- `read_csv()` - processes CSV files using [Pandas](https://pandas.pydata.org/)
- `read_jsonl()` - processes JSONL files chunk by chunk
- `read_parquet()` - processes Parquet files using [PyArrow](https://arrow.apache.org/docs/python/)
- `read_csv_duckdb()` - this transformer processes CSV files using DuckDB, which usually shows better performance than pandas.

:::tip
We advise that you give each resource a [specific name](../../../general-usage/resource#duplicate-and-rename-resources) before loading with `pipeline.run`. This will ensure that data goes to a table with the name you want and that each pipeline uses a [separate state for incremental loading.](../../../general-usage/state#read-and-write-pipeline-state-in-a-resource)
:::

### 3. Create and run a pipeline

```py
import dlt
from dlt.sources.filesystem import filesystem, read_csv

filesystem_pipe = filesystem(bucket_url="file://Users/admin/Documents/csv_files", file_glob="*.csv") | read_csv()
pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
info = pipeline.run(filesystem_pipe)
print(info)
```

For more information on how to create and run the pipeline, read the [Walkthrough: Run a pipeline](../../../walkthroughs/run-a-pipeline).

### 4. Apply hints

```py
import dlt
from dlt.sources.filesystem import filesystem, read_csv

filesystem_pipe = filesystem(bucket_url="file://Users/admin/Documents/csv_files", file_glob="*.csv") | read_csv()
# Tell dlt to merge on date
filesystem_pipe.apply_hints(write_disposition="merge", merge_key="date")

# We load the data into the table_name table
pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
load_info = pipeline.run(filesystem_pipe.with_name("table_name"))
print(load_info)
```

### 5. Incremental loading

Here are a few simple ways to load your data incrementally:

1. [Load files based on modification date](#load-files-based-on-modification-date). Only load files that have been updated since the last time `dlt` processed them. `dlt` checks the files' metadata (like the modification date) and skips those that haven't changed.
2. [Load new records based on a specific column](#load-new-records-based-on-a-specific-column). You can load only the new or updated records by looking at a specific column, like `updated_at`. Unlike the first method, this approach would read all files every time and then filter the records which were updated.
3. [Combine loading only updated files and records](#combine-loading-only-updated-files-and-records). Finally, you can combine both methods. It could be useful if new records could be added to existing files, so you not only want to filter the modified files, but also the modified records.

#### Load files based on modification date
For example, to load only new CSV files with [incremental loading](../../../general-usage/incremental-loading), you can use the `apply_hints` method.

```py
import dlt
from dlt.sources.filesystem import filesystem, read_csv

# This configuration will only consider new CSV files
new_files = filesystem(bucket_url="s3://bucket_name", file_glob="directory/*.csv")
# Add incremental on modification time
new_files.apply_hints(incremental=dlt.sources.incremental("modification_date"))

pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
load_info = pipeline.run((new_files | read_csv()).with_name("csv_files"))
print(load_info)
```

#### Load new records based on a specific column

In this example, we load only new records based on the field called `updated_at`. This method may be useful if you are not able to
filter files by modification date because, for example, all files are modified each time a new record appears.

```py
import dlt
from dlt.sources.filesystem import filesystem, read_csv

# We consider all CSV files
all_files = filesystem(bucket_url="s3://bucket_name", file_glob="directory/*.csv")

# But filter out only updated records
filesystem_pipe = (all_files | read_csv())
filesystem_pipe.apply_hints(incremental=dlt.sources.incremental("updated_at"))
pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
load_info = pipeline.run(filesystem_pipe)
print(load_info)
```

#### Combine loading only updated files and records

```py
import dlt
from dlt.sources.filesystem import filesystem, read_csv

# This configuration will only consider modified CSV files
new_files = filesystem(bucket_url="s3://bucket_name", file_glob="directory/*.csv")
new_files.apply_hints(incremental=dlt.sources.incremental("modification_date"))

# And in each modified file, we filter out only updated records
filesystem_pipe = (new_files | read_csv())
filesystem_pipe.apply_hints(incremental=dlt.sources.incremental("updated_at"))
pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
load_info = pipeline.run(filesystem_pipe)
print(load_info)
```

### 6. Filter files

If you need to filter out files based on their metadata, you can easily do this using the `add_filter` method.
Within your filtering function, you'll have access to [any field](advanced#fileitem-fields) of the `FileItem` representation.

#### Filter by name
To filter only files that have `London` and `Berlin` in their names, you can do the following:
```py
import dlt
from dlt.sources.filesystem import filesystem, read_csv

# Filter files accessing file_name field
filtered_files = filesystem(bucket_url="s3://bucket_name", file_glob="directory/*.csv")
filtered_files.add_filter(lambda item: ("London" in item["file_name"]) or ("Berlin" in item["file_name"]))

filesystem_pipe = (filtered_files | read_csv())
pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
load_info = pipeline.run(filesystem_pipe)
print(load_info)
```

:::tip
You could also use `file_glob` to filter files by names. It works very well in simple cases, for example, filtering by extension:

```py
from dlt.sources.filesystem import filesystem

filtered_files = filesystem(bucket_url="s3://bucket_name", file_glob="**/*.json")
```
:::

#### Filter by size

If for some reason you only want to load small files, you can also do that:

```py
import dlt
from dlt.sources.filesystem import filesystem, read_csv

MAX_SIZE_IN_BYTES = 10

# Filter files accessing size_in_bytes field
filtered_files = filesystem(bucket_url="s3://bucket_name", file_glob="directory/*.csv")
filtered_files.add_filter(lambda item: item["size_in_bytes"] < MAX_SIZE_IN_BYTES)

filesystem_pipe = (filtered_files | read_csv())
pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
load_info = pipeline.run(filesystem_pipe)
print(load_info)
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

If you are running a dlt pipeline with the filesystem source and get zero records, we recommend you check
the configuration of `bucket_url` and `file_glob` parameters.

For example, with Azure Blob Storage, people sometimes mistake the account name for the container name. Make sure you've set up a URL as `"az://<container name>/"`.

Also, please reference the [glob](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.spec.AbstractFileSystem.glob) function to configure the resource correctly. Use `**` to include recursive files. Note that the local filesystem supports full Python [glob](https://docs.python.org/3/library/glob.html#glob.glob) functionality, while cloud storage supports a restricted `fsspec` [version](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.spec.AbstractFileSystem.glob).

<!--@@@DLT_TUBA filesystem-->

