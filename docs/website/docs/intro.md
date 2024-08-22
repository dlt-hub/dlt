---
title: Introduction
description: Introduction to dlt
keywords: [introduction, who, what, how]
---

import snippets from '!!raw-loader!./intro-snippets.py';

# Introduction

![dlt pacman](/img/dlt-pacman.gif)

## What is dlt?

dlt is a Python library that simplifies how you move data between various sources and destinations. It offers a lightweight interface for extracting data from [REST APIs](./tutorial/rest-api), [SQL databases](./tutorial/sql-database), [cloud storages](./tutorial/filesystem), [Python data structures](getting-started), and more.

dlt is designed to be easy to use, flexible, and scalable:

- dlt infers [schemas](./general-usage/schema) and [data types](./general-usage/schema/#data-types), [normalizes the data](./general-usage/schema/#data-normalizer), and handles nested data structures.
- dlt supports variety of [popular destinations](./dlt-ecosystem/destinations/) and has an interface to add [custom destinations](./dlt-ecosystem/destinations/destination) to create reverse ETL pipelines.
- Use dlt locally or [in the cloud](./walkthroughs/deploy-a-pipeline) to build data pipelines, data lakes, and data warehouses.

To get started with dlt, install the library using pip:

```sh
pip install dlt
```
:::tip
We recommend using a clean virtual environment for your experiments! Here are [detailed instructions](/reference/installation) on how to set up one.
:::

## Load data with dlt from …

<Tabs
  groupId="source-type"
  defaultValue="rest-api"
  values={[
    {"label": "REST APIs", "value": "rest-api"},
    {"label": "SQL databases", "value": "sql-database"},
    {"label": "Cloud storages or files", "value": "filesystem"},
    {"label": "Python data structures", "value": "python-data"},
]}>
  <TabItem value="rest-api">

Use dlt's [REST API source](tutorial/rest-api) to extract data from any REST API. Define API endpoints you’d like to fetch data from, pagination method and authentication and dlt will handle the rest:

```py
# from dlt.sources import rest_api

source = rest_api({
    "client": {
        "base_url": "https://api.example.com/",
        "auth": {
            "token": dlt.secrets["your_api_token"],
        },
        "paginator": {
            "type": "json_response",
            "next_url_path": "paging.next",
        },
    },
    "resources": [
	      "posts",
	      "comments"
    ]
})

pipeline = dlt.pipeline(
    pipeline_name="rest_api_example",
    destination="duckdb",
    dataset_name="rest_api_data",
)

load_info = pipeline.run(source)
```

Follow the [REST API source tutorial](tutorial/rest-api) to learn more about the source configuration and pagination methods.
  </TabItem>
  <TabItem value="sql-database">

Use the [SQL source](tutorial/sql-database) to extract data from the database like PostgreSQL, MySQL, SQLite, Oracle and more.

```py
# from dlt.sources.sql import sql_database

source = sql_database(
    "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
)

pipeline = dlt.pipeline(
    pipeline_name="sql_database_example",
    destination="duckdb",
    dataset_name="sql_data",
)

load_info = pipeline.run(source)
```

Follow the [SQL source tutorial](tutorial/sql-database) to learn more about the source configuration and supported databases.

  </TabItem>
  <TabItem value="filesystem">

[Filesystem](./tutorial/filesystem) source extracts data from AWS S3, Google Cloud Storage, Google Drive, Azure, or a local file system.

```py
# from dlt.sources.filesystem import filesystem

source = filesystem(
    bucket_url="s3://example-bucket",
    file_glob="*.csv"
)

pipeline = dlt.pipeline(
    pipeline_name="filesystem_example",
    destination="duckdb",
    dataset_name="filesystem_data",
)

load_info = pipeline.run(source)
```

Follow the [filesystem source tutorial](./tutorial/filesystem) to learn more about the source configuration and supported storage services.

  </TabItem>
  <TabItem value="python-data">

dlt is able to load data from Python generators or directly from Python data structures:

```py
import dlt

@dlt.resource
def foo():
    for i in range(10):
        yield {"id": i, "name": f"This is item {i}"}

pipeline = dlt.pipeline(
    pipeline_name="python_data_example",
    destination="duckdb",
)

load_info = pipeline.run(foo)
```

Check out the [getting started guide](getting-started) to learn more about working with Python data.

  </TabItem>

</Tabs>


## Why use dlt?

- Automated maintenance - with schema inference and evolution and alerts, and with short declarative
code, maintenance becomes simple.
- Run it where Python runs - on Airflow, serverless functions, notebooks. No
external APIs, backends, or containers, scales on micro and large infra alike.
- User-friendly, declarative interface that removes knowledge obstacles for beginners
while empowering senior professionals.

## Getting started with dlt
1. Dive into our [Getting started guide](getting-started.md) for a quick intro to the essentials of dlt.
2. Play with the
[Google Colab demo](https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing).
This is the simplest way to see dlt in action.
3. Read the [Tutorial](tutorial/intro) to learn how to build a pipeline that loads data from an API.
4. Check out the [How-to guides](walkthroughs/) for recipes on common use cases for creating, running, and deploying pipelines.
5. Ask us on
[Slack](https://dlthub.com/community)
if you have any questions about use cases or the library.

## Join the dlt community

1. Give the library a ⭐ and check out the code on [GitHub](https://github.com/dlt-hub/dlt).
1. Ask questions and share how you use the library on
[Slack](https://dlthub.com/community).
1. Report problems and make feature requests [here](https://github.com/dlt-hub/dlt/issues/new/choose).