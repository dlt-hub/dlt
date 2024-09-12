---
title: Load data from Cloud Storage or a local filesystem
description: Load data from
keywords: [tutorial, filesystem, cloud storage, dlt, python, data pipeline, incremental loading]
---

This tutorial is for you if you need to load data files like `jsonl`, `csv`, `parquet` from either Cloud Storage (ex. AWS S3, Google Cloud Storage, Google Drive, Azure) or a local filesystem.

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
When deploying your pipeline in a production environment, managing all configurations with files might not be convenient. In this case, we recommend you to use the environment variables to store secrets and configs instead. Read more about [configuration providers](../general-usage/credentials/setup#available-config-providers) available in `dlt`.
:::

## 2. Creating the pipeline

Filesystem source provides users with building blocks for loading data from any type of files. You can break down the data extraction into two steps:

1. Accessing files in the bucket / directory.
2. Reading the files and yielding records.

`dlt`'s filesystem source includes several resources that you can use together or individually:

- `filesystem` resource accesses files in the directory or bucket
- several readers resources (`read_csv`, `read_parquet`, `read_jsonl`) read files and yield the records

Let's initialize a source and create a pipeline for loading `csv` files from Google Cloud Storage to DuckDB. You can replace code from `filesystem_pipeline.py` with the following:

```py
import dlt
from dlt.sources.filesystem import filesystem, read_csv

files = filesystem(bucket_url="generic_hospital_data", file_glob="encounters*.csv")
reader = (files | read_csv()).with_name(encounters)
pipeline = dlt.pipeline(pipeline_name="hospital_data_pipeline", dataset_name="hospital_data", destination="duckdb")
info = pipeline.run(reader)
print(info)
```

What's happening in this snippet?

1. We initialize the filesystem resource and pass the `file_glob` parameter. Based on this parameter, `dlt` will filter all files in the bucket.
2. We pipe the transformer `read_csv` to read the files yielded by the filesystem resource and iterate over records from the file.

:::note
A **transformer** in `dlt` is a special type of resource that processes each record from another resource. This lets you chain multiple resources together. To learn more, check out the [transformer section](../general-usage/resource#process-resources-with-dlttransformer).
:::

3. We create the `dlt` pipeline with the name `hospital_data_pipeline` and DuckDB destination and run this pipeline.

## 3. Configuring filesystem source

Next, we need to configure the connection. Specifically, we’ll set the bucket URL and credentials. This example focuses on Google Cloud Storage. For other Cloud Storage services, see the [Filesystem configuration section](../dlt-ecosystem/verified-sources/filesystem/basic#configuration).

Let's specify the bucket URL and credentials:

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
client_email = "vio-test@dlthub-sandbox.iam.gserviceaccount.com"
project_id = "dlthub-sandbox"
private_key = "-----BEGIN PRIVATE"

# config.toml
[sources.filesystem]
bucket_url="gc://generic_hospital_data"
```
  </TabItem>

<TabItem value="env">

```sh
export SOURCES__FILESYSTEM__CREDENTIALS__CLIENT_EMAIL="vio-test@dlthub-sandbox.iam.gserviceaccount.com"
export SOURCES__FILESYSTEM__CREDENTIALS__PROJECT_ID="dlthub-sandbox"
export SOURCES__FILESYSTEM__CREDENTIALS__PRIVATE_KEY="-----BEGIN PRIVATE"
export SOURCES__FILESYSTEM__BUCKET_URL="gc://generic_hospital_data"
```
  </TabItem>

<TabItem value="code">

```py
import os

from dlt.common.configuration.specs import GcpClientCredentials
from dlt.sources.filesystem import filesystem, read_csv

files = filesystem(
    bucket_url="gc://generic_hospital_data",
    # please, do not specify sensitive information directly in the code,
    # instead, you can use env variables to get the credentials
    credentials=GcpClientCredentials(
        client_email="vio-test@dlthub-sandbox.iam.gserviceaccount.com",
        project_id="dlthub-sandbox",
        private_key=os.environ["GCP_PRIVATE_KEY"]
    ),
    file_glob="encounters*.csv") | read_csv()
```
</TabItem>
</Tabs>

As you can see, all parameters of `filesystem` can be specified directly in the code or taken from the configuration.

:::tip
`dlt` supports more ways of authorizing with the cloud storages, including identity-based and default credentials. To learn more about adding credentials to your pipeline, please refer to the [Configuration and secrets section](../general-usage/credentials/complex_types#aws-credentials).
:::

## 4. Running the pipeline

Let's verify that the pipeline is working as expected. Run the following command to execute the pipeline:

```sh
python filesystem_pipeline.py
```

You should see the output of the pipeline execution in the terminal. The output will also display the location of the DuckDB database file where the data is stored:

```sh
Pipeline hospital_data_pipeline load step completed in 4.11 seconds
1 load package(s) were loaded to destination duckdb and into dataset hospital_data
The duckdb destination used duckdb:////Users/vmishechk/PycharmProjects/dlt/hospital_data_pipeline.duckdb location to store data
Load package 1726074108.8017762 is LOADED and contains no failed jobs
```

## 5. Exploring the data

Now that the pipeline has run successfully, let's explore the data loaded into DuckDB. `dlt` comes with a built-in browser application that allows you to interact with the data. To enable it, run the following command:

```sh
pip install streamlit
```

Next, run the following command to start the data browser:

```sh
dlt pipeline hospital_data_pipeline show
```

The command opens a new browser window with the data browser application. `hospital_data_pipeline` is the name of the pipeline defined in the `filesystem_pipeline.py` file.

![Streamlit Explore data](/img/filesystem-tutorial/streamlit-data.png)

You can explore the loaded data, run queries, and see some pipeline execution details.

## 6. Appending, replacing, and merging loaded data

If you try running the pipeline again with `python filesystem_pipeline.py`, you will notice that all the tables have duplicated data. This happens because by default, `dlt` appends the data to the destination table. It is very useful, for example, when you have daily data updates and you want to ingest them. With `dlt`, you can control how the data is loaded into the destination table by setting the `write_disposition` parameter in the resource configuration. The possible values are:
- `append`: Appends the data to the destination table. This is the default.
- `replace`: Replaces the data in the destination table with the new data.
- `merge`: Merges the new data with the existing data in the destination table based on the primary key.

To specify the `write_disposition`, you can set it in the `pipeline.run` command. Let's change the write disposition to `merge`. In this case, `dlt` will deduplicate the data before loading them into the destination.

To enable data deduplication, we also should specify a `primary_key` or `merge_key`, which will be used by `dlt` to define if two records are different. Both keys could consist of several columns. `dlt` will try to use `merge_key` and fallback to `primary_key` if it's not specified. To specify any hints about the data, including column types, primary keys, you can use the [`apply_hints` function](../general-usage/resource#set-table-name-and-adjust-schema).

```py
import dlt
from dlt.sources.filesystem import filesystem, read_csv

files = filesystem(bucket_url="generic_hospital_data", file_glob="encounters*.csv")
reader = (files | read_csv()).with_name("encounters")
reader.apply_hints(primary_key="id")
pipeline = dlt.pipeline(pipeline_name="hospital_data_pipeline", dataset_name="hospital_data", destination="duckdb")
info = pipeline.run(reader, write_disposition="merge")
print(info)
```
:::tip
You may need to drop the previously loaded data if you loaded data several times with `append` write disposition to make sure the primary key column has unique values.
:::

You can learn more about `write_disposition` in the [Write dispositions section](../general-usage/incremental-loading#the-3-write-dispositions).

## 7. Loading data incrementally

When loading data from files, you often only want to load files that have been modified. `dlt` makes this easy with [incremental loading](../general-usage/incremental-loading). To load only modified files, you can use the `apply_hint` function:

```py
import dlt
from dlt.sources.filesystem import filesystem, read_csv

files = filesystem(bucket_url="generic_hospital_data", file_glob="encounters*.csv")
files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
reader = (files | read_csv()).with_name("encounters")
reader.apply_hints(primary_key="id")
pipeline = dlt.pipeline(pipeline_name="hospital_data_pipeline", dataset_name="hospital_data", destination="duckdb")
info = pipeline.run(reader, write_disposition="merge")
print(info)
```

Notice that we used `apply_hints` on the `files` resource, not on `reader`. Why did we do that? As mentioned before, the `filesystem` resource lists all files in the storage based on the `file_glob` parameter. So at this point, we can also specify additional conditions to filter out files. In this case, we only want to load files that have been modified since the last load. `dlt` will automatically keep the state of incremental load and manage the correct filtering.

But what if we not only want to process modified files, but we also want to load only new records? In the `encounters` table, we can see the column named `STOP` indicating the timestamp of the end of the encounter. Let's modify our code to load only those records whose `STOP` timestamp was updated since our last load.

```py
import dlt
from dlt.sources.filesystem import filesystem, read_csv

files = filesystem(bucket_url="generic_hospital_data", file_glob="encounters*.csv")
files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
reader = (files | read_csv()).with_name("encounters")
reader.apply_hints(primary_key="id", incremental=dlt.sources.incremental("STOP"))
pipeline = dlt.pipeline(pipeline_name="hospital_data_pipeline", dataset_name="hospital_data", destination="duckdb")
info = pipeline.run(reader, write_disposition="merge")
print(info)
```

Notice that we applied incremental loading both for `files` and for `reader`. Therefore, `dlt` will first filter out only modified files and then filter out new records based on the `STOP` column.

If you run `dlt pipeline hospital_data_pipeline show`, you can see the pipeline now has new information in the state about the incremental variable:

![Streamlit Explore data](/img/filesystem-tutorial/streamlit-incremental-state.png)

To learn more about incremental loading, check out the [filesystem incremental loading section](../dlt-ecosystem/verified-sources/filesystem/basic#5-incremental-loading).

## 8. Enrich records with the files metadata

Now let's add the file names to the actual records. This could be useful to connect the files' origins to the actual records.

Since the `filesystem` source yields information about files, we can modify the transformer to add any available metadata. Let's create a custom transformer function. We can just copy-paste the `read_csv` function from `dlt` code and add one column `file_name` to the dataframe:

```py
from typing import Any, Iterator

import dlt
from dlt.sources import TDataItems
from dlt.sources.filesystem import FileItemDict
from dlt.sources.filesystem import filesystem


@dlt.transformer()
def read_csv_custom(items: Iterator[FileItemDict], chunksize: int = 10000, **pandas_kwargs: Any) -> Iterator[TDataItems]:
    import pandas as pd

    # apply defaults to pandas kwargs
    kwargs = {**{"header": "infer", "chunksize": chunksize}, **pandas_kwargs}

    for file_obj in items:
        with file_obj.open() as file:
            for df in pd.read_csv(file, **kwargs):
                df["file_name"] = file_obj["file_name"]
                yield df.to_dict(orient="records")

files = filesystem(bucket_url="generic_hospital_data", file_glob="encounters*.csv")
files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
reader = (files | read_csv_custom()).with_name("encounters")
reader.apply_hints(primary_key="id", incremental=dlt.sources.incremental("STOP"))
pipeline = dlt.pipeline(pipeline_name="hospital_data_pipeline", dataset_name="hospital_data", destination="duckdb")
info = pipeline.run(reader, write_disposition="merge")
print(info)
```

After executing this code, you'll see a new column in the `encounters` table:

![Streamlit Explore data](/img/filesystem-tutorial/streamlit-new-col.png)

## 9. Load any other type of files

`dlt` natively supports three file types: `csv`, `parquet`, and `jsonl` (more details in [filesystem transformer resource](../dlt-ecosystem/filesystem/basic#2-choose-the-right-transformer-resource)). But you can easily create your own. In order to do this, you just need a function that takes as input a `FileItemDict` iterator and yields a list of records (recommended for performance) or individual records.

Let's create and apply a transformer that reads `json` files instead of `csv` (the implementation for `json` is a little bit different from `jsonl`).

```py
from typing import Iterator

import dlt
import json
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem

# Define a standalone transformer to read data from a json file.
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