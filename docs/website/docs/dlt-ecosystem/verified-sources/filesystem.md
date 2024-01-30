---
title: Filesystem
description: dlt verified source for Readers Source and Filesystem
keywords: [readers source and filesystem, filesystem, readers source]
---
# Readers Source and Filesystem

:::info Need help deploying these sources, or figuring out how to run them in your data stack?

[Join our Slack community](https://dlthub.com/community)
or [book a call](https://calendar.app.google/kiLhuMsWKpZUpfho6) with our support engineer Adrian.
:::

This verified source easily streams files from AWS s3, GCS, Azure, or local filesystem using the reader
source.

Sources and resources that can be used with this verified source are:

| Name         | Type                 | Description                                                               |
|--------------|----------------------|---------------------------------------------------------------------------|
| readers      | Source               | Lists and reads files with resource `filesystem` and readers transformers |
| filesystem   | Resource             | Lists files in `bucket_url` using `file_glob` pattern                     |
| read_csv     | Resource-transformer | Reads csv file with **Pandas** chunk by chunk                             |
| read_jsonl   | Resource-transformer | Reads jsonl file content and extract the data                             |
| read_parquet | Resource-transformer | Reads parquet file content and extract the data with **Pyarrow**          |

## Setup Guide

### Grab credentials

This source can access various bucket types, including:

- AWS S3.
- Google Cloud Storage.
- Azure Blob Storage.
- Local Storage

To access these, you'll need secret credentials:

#### AWS S3 credentials

To get AWS keys for S3 access:

1. Access IAM in AWS Console.
1. Select "Users", choose a user, and open "Security credentials".
1. Click "Create access key" for AWS ID and Secret Key.

For more info, see
[AWS official documentation.](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html)

#### Google Cloud Storage credentials

To get GCS access:

1. Log in to [console.cloud.google.com](http://console.cloud.google.com/).
1. Create a [service account](https://cloud.google.com/iam/docs/service-accounts-create#creating).
1. Enable "Google Analytics API"; see
   [Google's guide](https://support.google.com/googleapi/answer/6158841?hl=en).
1. In IAM & Admin > Service Accounts, find your account, click the three-dot menu > "Manage Keys" >
   "ADD KEY" > "CREATE" to get a JSON credential file.
1. Grant the service account appropriate permissions for cloud storage access.

For more info, see how to
[create service account](https://support.google.com/a/answer/7378726?hl=en).

#### Azure Blob Storage credentials

To obtain Azure blob storage access:

1. Go to Azure Portal (portal.azure.com).
1. Select "Storage accounts" > your storage.
1. Click "Settings" > "Access keys".
1. View account name and two keys (primary/secondary). Keep keys confidential.

For more info, see
[Azure official documentation](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal).

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:

   ```bash
   dlt init filesystem duckdb
   ```

   [This command](../../reference/command-line-interface) will initialize
   [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/filesystem_pipeline.py)
   with filesystem as the [source](../../general-usage/source) and
   [duckdb](../destinations/duckdb.md) as the [destination](../destinations).

1. If you'd like to use a different destination, simply replace `duckdb` with the name of your
   preferred [destination](../destinations).

1. After running this command, a new directory will be created with the necessary files and
   configuration settings to get started.

For more information, read the
[Walkthrough: Add a verified source.](../../walkthroughs/add-a-verified-source)

### Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive
   information securely, like access tokens. Keep this file safe. Here's its format for service
   account authentication:

   ```toml
   [sources.filesystem.credentials] # use [sources.readers.credentials] for the "readers" source
   # For AWS S3 access:
   aws_access_key_id="Please set me up!"
   aws_secret_access_key="Please set me up!"

   # For GCS storage bucket access:
   client_email="Please set me up!"
   private_key="Please set me up!"
   project_id="Please set me up!"

   # For Azure blob storage access:
   azure_storage_account_name="Please set me up!"
   azure_storage_account_key="Please set me up!"
   ```

1. Finally, enter credentials for your chosen destination as per the [docs](../destinations/).

1. You can pass the bucket URL and glob pattern or use `config.toml`. For local filesystems, use
   `file://` or skip the schema.

   ```toml
   [sources.filesystem] # use [sources.readers.credentials] for the "readers" source
   bucket_url="~/Documents/csv_files/"
   file_glob="*"
   ```

   For remote file systems you need to add the schema, it will be used to get the protocol being
   used, for example:

   ```toml
   [sources.filesystem] # use [sources.readers.credentials] for the "readers" source
   bucket_url="s3://my-bucket/csv_files/"
   ```
   :::caution
   For Azure, use adlfs>=2023.9.0. Older versions mishandle globs.
   :::
## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by
   running the command:

   ```bash
   pip install -r requirements.txt
   ```

1. Install optional modules:

   - For AWS S3:
     ```bash
     pip install s3fs
     ```
   - For Azure blob:
     ```bash
     pip install adlfs>=2023.9.0
     ```
   - GCS storage: No separate module needed.

1. You're now ready to run the pipeline! To get started, run the following command:

   ```bash
   python filesystem_pipeline.py
   ```

1. Once the pipeline has finished running, you can verify that everything loaded correctly by using
   the following command:

   ```bash
   dlt pipeline <pipeline_name> show
   ```

   For example, the `pipeline_name` for the above pipeline example is `standard_filesystem`, you may
   also use any custom name instead.

For more information, read the [Walkthrough: Run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and
[resources](../../general-usage/resource).

### Source `readers`

This source offers chunked file readers as resources, which can be optionally customized. Provided resources include:

- `read_csv()`
- `read_jsonl()`
- `read_parquet()`

```python
@dlt.source(_impl_cls=ReadersSource, spec=FilesystemConfigurationResource)
def readers(
    bucket_url: str = dlt.secrets.value,
    credentials: Union[FileSystemCredentials, AbstractFileSystem] = dlt.secrets.value,
    file_glob: Optional[str] = "*",
) -> Tuple[DltResource, ...]:
```

- `bucket_url`: The url to the bucket.
- `credentials`: The credentials to the filesystem of fsspec `AbstractFilesystem` instance.
- `file_glob`: Glob filter for files. Defaults to non-recursive listing in the bucket.

:::tip
We advise that you give each resource a
[specific name](../../general-usage/resource#duplicate-and-rename-resources)
before loading with `pipeline.run`. This will make sure that data goes to a table with the name you
want and that each pipeline uses a
[separate state for incremental loading.](../../general-usage/state#read-and-write-pipeline-state-in-a-resource)
:::


### Resource `filesystem`

This resource lists files in `bucket_url` based on the `file_glob` pattern, returning them as
[FileItem](https://github.com/dlt-hub/dlt/blob/devel/dlt/common/storages/fsspec_filesystem.py#L22)
with data access methods. These can be paired with transformers for enhanced processing.

```python
@dlt.resource(
    primary_key="file_url", spec=FilesystemConfigurationResource, standalone=True
)
def filesystem(
    bucket_url: str = dlt.secrets.value,
    credentials: Union[FileSystemCredentials, AbstractFileSystem] = dlt.secrets.value,
    file_glob: Optional[str] = "*",
    files_per_page: int = DEFAULT_CHUNK_SIZE,
    extract_content: bool = False,
) -> Iterator[List[FileItem]]:
```

- `bucket_url`: URL of the bucket.
- `credentials`: Filesystem credentials of `AbstractFilesystem` instance.
- `file_glob`: File filter in glob format. Defaults to listing all non-recursive files
in bucket URL.
- `files_per_page`: Number of files processed at once. Default: 100.
- `extract_content`: If true, the content of the file will be read and returned in the resource. Default: False.


## Filesystem Integration and Data Extraction Guide

### Filesystem Usage

- The filesystem tool enumerates files in a selected bucket using a glob pattern, returning details as FileInfo in customizable page sizes.

- This resource integrates with transform functions and transformers for customized extraction pipelines.

To load data into a specific table (instead of the default filesystem table), see the snippet below:

```python
@dlt.transformer(standalone=True)
def read_csv(items, chunksize: int = 15) ->:
    """Reads csv file with Pandas chunk by chunk."""
    ...

# list only the *.csv in specific folder and pass the file items to read_csv()
met_files = (
    filesystem(bucket_url="s3://my_bucket/data", file_glob="csv_folder/*.csv")
    | read_csv()
)
# load to met_csv table using with_name()
pipeline.run(met_files.with_name("csv_data"))
```

Use the
[standalone filesystem](../../general-usage/resource#declare-a-standalone-resource)
resource to list files in s3, GCS, and Azure buckets. This allows you to customize file readers or
manage files using [fsspec](https://filesystem-spec.readthedocs.io/en/latest/index.html).
```python
files = filesystem(bucket_url="s3://my_bucket/data", file_glob="csv_folder/*.csv")
pipeline.run(files)
```
The filesystem ensures consistent file representation across bucket types and offers methods to access and read
data. You can quickly build pipelines to:

- Extract text from PDFs.
- Stream large file content directly from buckets.
- Copy files locally.

### `FileItem` Representation

- All dlt sources/resources that yield files follow the [FileItem](https://github.com/dlt-hub/dlt/blob/devel/dlt/common/storages/fsspec_filesystem.py#L22) contract.
- File content is typically not loaded; instead, full file info and methods to access content are
  available.
- Users can request an authenticated [fsspec AbstractFileSystem](https://filesystem-spec.readthedocs.io/en/latest/_modules/fsspec/spec.html#AbstractFileSystem) instance.

#### `FileItem` Fields:

- `file_url` - Complete URL of the file; also the primary key (e.g. `file://`).
- `file_name` - Name or relative path of the file from the bucket URL.
- `mime_type` - File's mime type; sourced from the bucket provider or inferred from its extension.
- `modification_date` - File's last modification time (format: `pendulum.DateTime`).
- `size_in_bytes` - File size.
- `file_content` - Content, provided upon request.

:::info
When using a nested or recursive glob pattern, `file_name` will include the file's path. For
instance, using the resource:
`filesystem("az://dlt-ci-test-bucket/standard_source/samples", file_glob="met_csv/A801/*.csv")`
will produce file names relative to the `/standard_source/samples` path, such as
`met_csv/A801/A881_20230920.csv`.
:::

### File Manipulation

[FileItem](https://github.com/dlt-hub/dlt/blob/devel/dlt/common/storages/fsspec_filesystem.py#L22), backed by a dictionary implementation, offers these helper methods:

- `read_bytes()`: Returns the file content as bytes.
- `open()`: Provides a file object when opened.
- `filesystem`: Gives access to an authorized `AbstractFilesystem` with standard fsspec methods.

## Customization

### Create your own pipeline

If you wish to create your own pipelines, you can leverage source and resource methods from this
verified source.

1. Configure the pipeline by specifying the pipeline name, destination, and dataset as follows:

   ```python
   pipeline = dlt.pipeline(
        pipeline_name="standard_filesystem",  # Use a custom name if desired
        destination="duckdb",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
        dataset_name="filesystem_data_csv"  # Use a custom name if desired
   )
   ```

1. To read and load CSV files:

   ```python
   BUCKET_URL = "YOUR_BUCKET_PATH_HERE"   # path of the bucket url or local destination
   met_files = readers(
        bucket_url=BUCKET_URL, file_glob="directory/*.csv"
    ).read_csv()
    # tell dlt to merge on date
    met_files.apply_hints(write_disposition="merge", merge_key="date")
    # We load the data into the met_csv table
    load_info = pipeline.run(met_files.with_name("table_name"))
    print(load_info)
    print(pipeline.last_trace.last_normalize_info)
   ```

    - The `file_glob` parameter targets all CSVs in the "met_csv/A801" directory.
    - The `print(pipeline.last_trace.last_normalize_info)` line displays the data normalization details from the pipeline's last trace.

    :::info
    If you have a default bucket URL set in `.dlt/config.toml`, you can omit the `bucket_url` parameter.
    :::
1. To load only new CSV files with [incremental loading](../../general-usage/incremental-loading):

   ```python
   # This configuration will only consider new csv files
   new_files = filesystem(bucket_url=BUCKET_URL, file_glob="directory/*.csv")
   # add incremental on modification time
   new_files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
   load_info = pipeline.run((new_files | read_csv()).with_name("csv_files"))
   print(load_info)
   print(pipeline.last_trace.last_normalize_info)
   ```

1. To read and load Parquet and JSONL from a bucket:
   ```python
   jsonl_reader = readers(BUCKET_URL, file_glob="**/*.jsonl").read_jsonl(
        chunksize=10000
    )
   # PARQUET reading
   parquet_reader = readers(BUCKET_URL, file_glob="**/*.parquet").read_parquet()
   # load both folders together to specified tables
   load_info = pipeline.run(
        [
            jsonl_reader.with_name("jsonl_data"),
            parquet_reader.with_name("parquet_data"),
       ]
   )
   print(load_info)
   print(pipeline.last_trace.last_normalize_info)
   ```
    - The `file_glob`: Specifies file pattern; reads all JSONL and Parquet files across directories.
    - The `chunksize`: Set to 10,000; data read in chunks of 10,000 records each.
    - `print(pipeline.last_trace.last_normalize_info)`: Displays the data normalization details from the pipeline's last trace.

1. To set up a pipeline that reads from an Excel file using a standalone transformer:

   ```python
   # Define a standalone transformer to read data from an Excel file.
   @dlt.transformer(standalone=True)
   def read_excel(
       items: Iterator[FileItemDict], sheet_name: str
   ) -> Iterator[TDataItems]:
       # Import the required pandas library.
       import pandas as pd

       # Iterate through each file item.
       for file_obj in items:
           # Open the file object.
           with file_obj.open() as file:
               # Read from the Excel file and yield its content as dictionary records.
               yield pd.read_excel(file, sheet_name).to_dict(orient="records")

   # Set up the pipeline to fetch a specific Excel file from a filesystem (bucket).
   example_xls = filesystem(
       bucket_url=BUCKET_URL, file_glob="../directory/example.xlsx"
   ) | read_excel("example_table")   # Pass the data through the transformer to read the "example_table" sheet.

   # Execute the pipeline and load the extracted data into the "duckdb" destination.
   load_info = dlt.run(
       example_xls.with_name("example_xls_data"),
       destination="duckdb",
       dataset_name="example_xls_data",
   )

   # Print the loading information.
   print(load_info)
   ```

    The code loads data from `example.xlsx` into the `duckdb` destination.

1. To copy files locally, add a step in the filesystem resource and then load the listing to the database:

   ```python
    def _copy(item: FileItemDict) -> FileItemDict:
         # instantiate fsspec and copy file
         dest_file = os.path.join(local_folder, item["file_name"])
         # create dest folder
         os.makedirs(os.path.dirname(dest_file), exist_ok=True)
         # download file
         item.fsspec.download(item["file_url"], dest_file)
         # return file item unchanged
         return item

     # use recursive glob pattern and add file copy step
    downloader = filesystem(BUCKET_URL, file_glob="**").add_map(_copy)

    # NOTE: you do not need to load any data to execute extract, below we obtain
    # a list of files in a bucket and also copy them locally
    listing = list(downloader)
    print(listing)
    # download to table "listing"
    load_info = pipeline.run(
        downloader.with_name("listing"), write_disposition="replace"
    )
    # pretty print the information on data that was loaded
    print(load_info)
    print(listing)(pipeline.last_trace.last_normalize_info)
   ```

1. Cleanup after loading:

   You can get a fsspec client from filesystem resource after it was extracted i.e. in order to delete processed files etc.
   The filesystem module contains a convenient method `fsspec_from_resource` that can be used as follows:

      ```python
      from filesystem import filesystem, fsspec_from_resource
      # get filesystem source
      gs_resource = filesystem("gs://ci-test-bucket/")
      # extract files
      pipeline.run(gs_resource | read_csv)
      # get fs client
      fs_client = fsspec_from_resource(gs_resource)
      # do any operation
      fs_client.ls("ci-test-bucket/standard_source/samples")
      ```

