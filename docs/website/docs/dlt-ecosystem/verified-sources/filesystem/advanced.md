---
title: Advanced filesystem usage
description: Use filesystem source as a building block
keywords: [readers source and filesystem, files, filesystem, readers source, cloud storage]
---

The filesystem source provides the building blocks to load data from files. This section explains how you can customize the filesystem source for your use case.

## Standalone filesystem resource

You can use the [standalone filesystem](../../../general-usage/resource#declare-a-standalone-resource) resource to list files in cloud storage or a local filesystem. This allows you to customize file readers or manage files using [fsspec](https://filesystem-spec.readthedocs.io/en/latest/index.html).

```py
from dlt.sources.filesystem import filesystem

pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
files = filesystem(bucket_url="s3://my_bucket/data", file_glob="csv_folder/*.csv")
pipeline.run(files)
```

The filesystem ensures consistent file representation across bucket types and offers methods to access and read data. You can quickly build pipelines to:

- Extract text from PDFs ([unstructured data source](https://github.com/dlt-hub/verified-sources/tree/master/sources/unstructured_data)).
- Stream large file content directly from buckets.
- Copy files locally ([copy files](#copy-files-locally))

### `FileItem` representation

- All dlt sources/resources that yield files follow the [FileItem](https://github.com/dlt-hub/dlt/blob/devel/dlt/common/storages/fsspec_filesystem.py#L40) contract.
- File content is typically not loaded (you can control it with the `extract_content` parameter of the filesystem resource). Instead, full file info and methods to access content are available.
- Users can request an authenticated [fsspec AbstractFileSystem](https://filesystem-spec.readthedocs.io/en/latest/_modules/fsspec/spec.html#AbstractFileSystem) instance.

#### `FileItem` fields

- `file_url` - complete URL of the file (e.g., `s3://bucket-name/path/file`). This field serves as a primary key.
- `file_name` - name of the file from the bucket URL.
- `relative_path` - set when doing `glob`, is a relative path to a `bucket_url` argument.
- `mime_type` - file's MIME type. It is sourced from the bucket provider or inferred from its extension.
- `modification_date` - file's last modification time (format: `pendulum.DateTime`).
- `size_in_bytes` - file size.
- `file_content` - content, provided upon request.

:::info
When using a nested or recursive glob pattern, `relative_path` will include the file's path relative to `bucket_url`. For instance, using the resource: `filesystem("az://dlt-ci-test-bucket/standard_source/samples", file_glob="met_csv/A801/*.csv")` will produce file names relative to the `/standard_source/samples` path, such as `met_csv/A801/A881_20230920.csv`. For local filesystems, POSIX paths (using "/" as separator) are returned.
:::

### File manipulation

[FileItem](https://github.com/dlt-hub/dlt/blob/devel/dlt/common/storages/fsspec_filesystem.py#L40), backed by a dictionary implementation, offers these helpers:

- `read_bytes()` - method, which returns the file content as bytes.
- `open()` - method which provides a file object when opened.
- `filesystem` - field, which gives access to authorized `AbstractFilesystem` with standard fsspec methods.

## Create your own transformer

Although the `filesystem` resource yields the files from cloud storage or a local filesystem, you need to apply a transformer resource to retrieve the records from files. dlt natively supports three file types: [CSV](../../file-formats/csv.md), [Parquet](../../file-formats/parquet.md), and [JSONL](../../file-formats/jsonl.md) (more details in [filesystem transformer resource](../filesystem/basic#2-choose-the-right-transformer-resource)).

But you can easily create your own. In order to do this, you just need a function that takes as input a `FileItemDict` iterator and yields a list of records (recommended for performance) or individual records.

### Example: read data from Excel files

The code below sets up a pipeline that reads from an Excel file using a standalone transformer:

```py
import dlt
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem

BUCKET_URL = "s3://my_bucket/data"

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

pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb", dataset_name="example_xls_data")
# Execute the pipeline and load the extracted data into the "duckdb" destination.
load_info = pipeline.run(example_xls.with_name("example_xls_data"))
# Print the loading information.
print(load_info)
```

### Example: read data from XML files

You can use any third-party library to parse an `xml` file (e.g., [BeautifulSoup](https://pypi.org/project/beautifulsoup4/), [pandas](https://pandas.pydata.org/docs/reference/api/pandas.read_xml.html)). In the following example, we will be using the [xmltodict](https://pypi.org/project/xmltodict/) Python library.

```py
import dlt
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem

BUCKET_URL = "s3://my_bucket/data"

# Define a standalone transformer to read data from an XML file.
@dlt.transformer(standalone=True)
def read_xml(items: Iterator[FileItemDict]) -> Iterator[TDataItems]:
    # Import the required xmltodict library.
    import xmltodict

    # Iterate through each file item.
    for file_obj in items:
        # Open the file object.
        with file_obj.open() as file:
            # Parse the file to dict records.
            yield xmltodict.parse(file.read())

# Set up the pipeline to fetch a specific XML file from a filesystem (bucket).
example_xml = filesystem(
    bucket_url=BUCKET_URL, file_glob="../directory/example.xml"
) | read_xml()   # Pass the data through the transformer

pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb", dataset_name="example_xml_data")
# Execute the pipeline and load the extracted data into the "duckdb" destination.
load_info = pipeline.run(example_xml.with_name("example_xml_data"))

# Print the loading information.
print(load_info)
```

## Clean files after loading

You can get an fsspec client from the filesystem resource after it was extracted, i.e., in order to delete processed files, etc. The filesystem module contains a convenient method `fsspec_from_resource` that can be used as follows:

```py
from dlt.sources.filesystem import filesystem, read_csv
from dlt.sources.filesystem.helpers import fsspec_from_resource

# Get filesystem source.
gs_resource = filesystem("gs://ci-test-bucket/")
# Extract files.
pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
pipeline.run(gs_resource | read_csv())
# Get fs client.
fs_client = fsspec_from_resource(gs_resource)
# Do any operation.
fs_client.ls("ci-test-bucket/standard_source/samples")
```

## Copy files locally

To copy files locally, add a step in the filesystem resource and then load the listing to the database:

```py
import os

import dlt
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.sources.filesystem import filesystem

def _copy(item: FileItemDict) -> FileItemDict:
    # Instantiate fsspec and copy file
    dest_file = os.path.join(local_folder, item["file_name"])
    # Create destination folder
    os.makedirs(os.path.dirname(dest_file), exist_ok=True)
    # Download file
    item.fsspec.download(item["file_url"], dest_file)
    # Return file item unchanged
    return item

BUCKET_URL = "gs://ci-test-bucket/"

# Use recursive glob pattern and add file copy step
downloader = filesystem(BUCKET_URL, file_glob="**").add_map(_copy)

# NOTE: You do not need to load any data to execute extract; below, we obtain
# a list of files in a bucket and also copy them locally
listing = list(downloader)
print(listing)
# Download to table "listing"
pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
load_info = pipeline.run(
    downloader.with_name("listing"), write_disposition="replace"
)
# Pretty print the information on data that was loaded
print(load_info)
print(listing)
print(pipeline.last_trace.last_normalize_info)
```

