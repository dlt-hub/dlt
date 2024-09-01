---
title: Load data from Cloud Storage or a local filesystem
description: Load data from
keywords: [tutorial, filesystem, cloud storage, dlt, python, data pipeline, incremental loading]
---

This tutorial is for you if you need to load data files like `jsonl`, `csv`, `parquet` from
either Cloud Storage (ex. AWS S3, Google Cloud Storage, Google Drive, Azure) or a local filesystem.

## What you will learn

- How to set up a filesystem or cloud storage as a data source
- Configuration basics for filesystems and cloud storage
- Loading methods
- Incremental loading of data from filesystems or cloud storage
- How to load data of any type

## 0. Prerequisites

- Python 3.9 or higher installed
- Virtual environment set up
- `dlt` installed. Follow the instructions in the [installation guide](../reference/installation) to create a new virtual environment and install dlt.

## 1. Setting up a new project

To help you get started quickly, `dlt` provides some handy CLI commands. One of these commands will help you set up a new `dlt` project:

```
dlt init filesystem duckdb
```

This command creates a project that loads data from a filesystem into a DuckDB database. You can easily switch out duckdb for any other [supported destinations](../dlt-ecosystem/destinations).
After running this command, your project will have the following structure:

```txt
filesystem_pipeline.py
requirements.txt
.dlt/
    config.toml
    secrets.toml
```

Here’s what each file does:

- `filesystem_pipeline.py`: This is the main script where you'll define your data pipeline. It contains several different examples of loading data from filesystem source.
- `requirements.txt`: This file lists all the Python dependencies required for your project.
- `.dlt/`: This directory contains the [configuration files](../general-usage/credentials/) for your project:
    - `secrets.toml`: This file stores your API keys, tokens, and other sensitive information.
    - `config.toml`: This file contains the configuration settings for your dlt project.

:::note
When deploying your pipeline in a production environment, managing all configurations with files
might not be convenient. In this case, you can use environment variables or other [configuration providers](../general-usage/credentials/setup#available-config-providers) available in
`dlt` to store secrets and configs instead.
:::

## 2. Creating the pipeline

Filesystem source provides users with building blocks for loading data from any type of files. You can break down the data extraction into two steps:

1. Accessing files in the bucket / directory.
2. Reading the files and yielding records.

`dlt`'s filesystem source includes several resources that you can use together or individually:

- `filesystem` resource accesses files in the directory or bucket
- several readers resources (`read_csv`, `read_parquet`, `read_jsonl`) read files and yield the records

Let's initialize a source and create a pipeline for loading parquet files from an AWS S3 to DuckDB:

```
import dlt
from dlt.sources.filesystem import filesystem, read_parquet

parquet_resource = filesystem(file_glob="**/*.parquet") | read_parquet()
pipeline = dlt.pipeline(pipeline_name="s3_to_duckdb", dataset_name="parquet_data", destination="duckdb")
info = pipeline.run(parquet_resource)
print(info)
```

You can replace the example code from `filesystem_pipeline.py` with the code above. What's happening in this snippet?

1. We initialize the filesystem resource and pass the `file_glob` parameter. Based in this parameter, `dlt` will filter
all files in the bucket.
2. We pipe the transformer `read_parquet` to read the files yielded by filesystem resource and iterate over records
from the file.

  ```note
  A **transformer** in `dlt` is a special type of resource that processes each record from another resource. This lets you
  chain multiple resources together. To learn more, check out the
  ["@dlt.transformer section"](../general-usage/resource#process-resources-with-dlttransformer).
  ```

3. We create the `dlt` pipeline with the name `s3_to_duckdb` and DuckDB destination and run this pipeline.

## 3. Configuring filesystem source

Next, we need to configure the connection. Specifically, we’ll set the bucket URL and AWS credentials.
This example focuses on AWS S3. For other Cloud Storage services, see the [Filesystem configuration section](../dlt-ecosystem/verified-sources/filesystem/basic#configuration).

Let's specify the bucket url and AWS credentials:

<Tabs
  groupId="config-provider-type"
  defaultValue="toml"
  values={[
    {"label": "Toml config provider", "value": "toml"},
    {"label": "ENV variables", "value": "env"},
    {"label": "In the code", "value": "code"},
]}>

  <TabItem value="toml">
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

<TabItem value="env">
```sh
export SOURCES__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID="Please set me up!"
export SOURCES__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY="Please set me up!"
export SOURCES__FILESYSTEM__BUCKET_URL="s3://<bucket_name>/<path_to_files>/"
```
  </TabItem>

<TabItem value="code">
```py
import os

from dlt.common.configuration.specs.aws_credentials import AwsCredentials
from dlt.sources.filesystem import filesystem

parquet_resource = filesystem(
    bucket_url="s3://<bucket_name>/<path_to_files>/",
    # please, do not specify secrets values directly in the code,
    # instead, you can use env variables to get the credentials
    credentials=AwsCredentials(
        aws_access_key_id=os.environ["AWS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET"],
    )
    file_glob="**/*.parquet") | read_parquet()
```
</TabItem>
</Tabs>

As you can see all parameters of `filesystem` can be specified in the code or taken from the configuration.

:::tip
`dlt` supports more ways of authorizing with the cloud storage, including identity-based
and default credentials. To learn more about adding credentials to your pipeline, please refer to the
[Configuration and secrets section](../../../general-usage/credentials/complex_types#aws-credentials).
:::

## 4. Running the pipeline

Let's verify that the pipeline is working as expected. Run the following command to execute the pipeline:

```sh
python filesystem_pipeline.py
```

You should see the output of the pipeline execution in the terminal. The output will also display the location of the DuckDB database file where the data is stored:

```sh
Pipeline s3_to_duckdb load step completed in 1.08 seconds
1 load package(s) were loaded to destination duckdb and into dataset parquet_data
The duckdb destination used duckdb:////home/user-name/quick_start/s3_to_duckdb.duckdb location to store data
Load package 1692364844.3523829 is LOADED and contains no failed jobs
```

## 5. Exploring the data

Now that the pipeline has run successfully, let's explore the data loaded into DuckDB. `dlt` comes with a built-in browser application that allows you to interact with the data. To enable it, run the following command:

```sh
pip install streamlit
```

Next, run the following command to start the data browser:

```sh
dlt pipeline s3_to_duckdb show
```

The command opens a new browser window with the data browser application. `s3_to_duckdb` is the name of the pipeline defined in the `filesystem_pipeline.py` file.
You can explore the loaded data, run queries and see some pipeline execution details.

## 6. Appending, replacing, and merging loaded data

Try running the pipeline again with `python filesystem_pipeline.py`. You will notice that
all the tables have data duplicated. This happens because by default, `dlt` appends the data to the destination table. It is very useful, for example, when you have daily data updates and you want to ingest them. In `dlt` you can control how the data is loaded into the destination table by setting the `write_disposition` parameter in the resource configuration. The possible values are:
- `append`: Appends the data to the destination table. This is the default.
- `replace`: Replaces the data in the destination table with the new data.
- `merge`: Merges the new data with the existing data in the destination table based on the primary key.

To specify the `write_disposition` you can set it in `pipeline.run` command:

```py
import dlt
from dlt.sources.filesystem import filesystem, read_parquet

parquet_resource = filesystem(file_glob="**/*.parquet") | read_parquet()
pipeline = dlt.pipeline(pipeline_name="s3_to_duckdb", dataset_name="parquet_data", destination="duckdb")
info = pipeline.run(parquet_resource, write_disposition="replace")
print(info)
```

You can learn more about write_disposition in the [Write dispositions section](../general-usage/incremental-loading#the-3-write-dispositions).

## 7. Loading data incrementally

When loading data from files, you often only want to load files that have been modified. `dlt` makes this easy with [incremental loading](../general-usage/incremental-loading). To load only modified files you can use `apply_hint`
function:

```py
import dlt
from dlt.sources.filesystem import filesystem, read_parquet

files_resource = filesystem(file_glob="**/*.parquet")
files_resource.apply_hints(incremental=dlt.sources.incremental("modification_date"))
parquet_resource = files_resource | read_parquet()
pipeline = dlt.pipeline(pipeline_name="s3_to_duckdb", dataset_name="parquet_data", destination="duckdb")
info = pipeline.run(parquet_resource, write_disposition="replace")
print(info)
```

As you can see the incremental hint is applied to the `filesystem` resource before `read_parquet` transformer, because
we want to apply the incremental loading to filter the loaded files.

## 8. Load any other type of files

`dlt` natively supports three file types: `csv`, `parquet`, and `jsonl` (more details in [filesystem transformer resource](../dlt-ecosystem/filesystem/basic#2-choose-the-right-transformer-resource)).
But you can easily create your own. In order to do this, you just need a function that takes as input a `FileItemDict` iterator and yields a list of records (recommended for performance) or individual records.

Let's create and apply a transformer, which reads `json` files instead of `parquet` (the implementation for `json`
is a little bit different than for `jsonl`).

```
import dlt
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem

# Define a standalone transformer to read data from an json file.
@dlt.transformer(standalone=True)
def read_json(items: Iterator[FileItemDict]) -> Iterator[TDataItems]:
    for file_obj in items:
        with file_obj.open() as f:
            yield json.load(f)

files_resource = filesystem(file_glob="**/*.json")
files_resource.apply_hints(incremental=dlt.sources.incremental("modification_date"))
json_resource = files_resource | read_json()
pipeline = dlt.pipeline(pipeline_name="s3_to_duckdb", dataset_name="json_data", destination="duckdb")
info = pipeline.run(json_resource, write_disposition="replace")
print(info)
```

Check out [other examples](../dlt-ecosystem/verified-sources/filesystem/advanced#create-your-own-transformer) showing how to read data from `excel` and `xml` files.

## What's next?

Congratulations on completing the tutorial! You've learned how to set up a Filesystem source in `dlt` and run a data pipeline to load the data into DuckDB.

Interested in learning more about `dlt`? Here are some suggestions:

- Learn more about the Filesystem source configuration in [Filesystem source](../dlt-ecosystem/verified-sources/filesystem)
- Learn more about different credential types in [Built-in credentials](../general-usage/credentials/complex_types#built-in-credentials)
- Learn how to [create a custom source](./load-data-from-an-api.md) in the advanced tutorial
