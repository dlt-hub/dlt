---
title: Introduction
description: Introduction to dlt
keywords: [introduction, who, what, how]
---

import snippets from '!!raw-loader!./intro-snippets.py';

# Introduction

![dlt pacman](/img/dlt-pacman.gif)

## What is `dlt`?

`dlt` is an open-source library that you can add to your Python scripts to load data
from various and often messy data sources into well-structured, live datasets. To get started, install it with:
```sh
pip install dlt
```
:::tip
We recommend using a clean virtual environment for your experiments! Here are [detailed instructions](/reference/installation).
:::

Unlike other solutions, with dlt, there's no need to use any backends or containers. Simply import `dlt` in a Python file or a Jupyter Notebook cell, and create a pipeline to load data into any of the [supported destinations](dlt-ecosystem/destinations/). You can load data from any source that produces Python data structures, including APIs, files, databases, and more. `dlt` also supports building a [custom destination](dlt-ecosystem/destinations/destination.md), which you can use as reverse ETL.

The library will create or update tables, infer data types, and handle nested data automatically. Here are a few example pipelines:

<Tabs
  groupId="source-type"
  defaultValue="api"
  values={[
    {"label": "Data from an API", "value": "api"},
    {"label": "Data from a dlt Source", "value": "source"},
    {"label": "Data from CSV/XLS/Pandas", "value": "csv"},
    {"label": "Data from a Database", "value":"database"}
]}>
  <TabItem value="api">

:::tip
Looking to use a REST API as a source? Explore our new [REST API generic source](dlt-ecosystem/verified-sources/rest_api) for a declarative way to load data.
:::

<!--@@@DLT_SNIPPET api-->


Copy this example to a file or a Jupyter Notebook and run it. To make it work with the DuckDB destination, you'll need to install the **duckdb** dependency (the default `dlt` installation is really minimal):
```sh
pip install "dlt[duckdb]"
```
Now **run** your Python file or Notebook cell.

How it works? The library extracts data from a [source](general-usage/glossary.md#source) (here: **chess.com REST API**), inspects its structure to create a
[schema](general-usage/glossary.md#schema), structures, normalizes, and verifies the data, and then
loads it into a [destination](general-usage/glossary.md#destination) (here: **duckdb**, into a database schema **player_data** and table name **player**).


  </TabItem>

  <TabItem value="source">

Initialize the [Slack source](dlt-ecosystem/verified-sources/slack) with `dlt init` command:

```sh
dlt init slack duckdb
```

Create and run a pipeline:

```py
import dlt

from slack import slack_source

pipeline = dlt.pipeline(
    pipeline_name="slack",
    destination="duckdb",
    dataset_name="slack_data"
)

source = slack_source(
    start_date=datetime(2023, 9, 1),
    end_date=datetime(2023, 9, 8),
    page_size=100,
)

load_info = pipeline.run(source)
print(load_info)
```

  </TabItem>
  <TabItem value="csv">

  Pass anything that you can load with Pandas to `dlt`

<!--@@@DLT_SNIPPET csv-->


  </TabItem>
  <TabItem value="database">

:::tip
Use our verified [SQL database source](dlt-ecosystem/verified-sources/sql_database)
to sync your databases with warehouses, data lakes, or vector stores.
:::

<!--@@@DLT_SNIPPET db-->


Install **pymysql** driver:
```sh
pip install sqlalchemy pymysql
```

  </TabItem>
</Tabs>


## Why use `dlt`?

- Automated maintenance - with schema inference and evolution and alerts, and with short declarative
code, maintenance becomes simple.
- Run it where Python runs - on Airflow, serverless functions, notebooks. No
external APIs, backends, or containers, scales on micro and large infra alike.
- User-friendly, declarative interface that removes knowledge obstacles for beginners
while empowering senior professionals.

## Getting started with `dlt`
1. Dive into our [Getting started guide](getting-started.md) for a quick intro to the essentials of `dlt`.
2. Play with the
[Google Colab demo](https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing).
This is the simplest way to see `dlt` in action.
3. Read the [Tutorial](tutorial/intro) to learn how to build a pipeline that loads data from an API.
4. Check out the [How-to guides](walkthroughs/) for recipes on common use cases for creating, running, and deploying pipelines.
5. Ask us on
[Slack](https://dlthub.com/community)
if you have any questions about use cases or the library.

## Join the `dlt` community

1. Give the library a ⭐ and check out the code on [GitHub](https://github.com/dlt-hub/dlt).
1. Ask questions and share how you use the library on
[Slack](https://dlthub.com/community).
1. Report problems and make feature requests [here](https://github.com/dlt-hub/dlt/issues/new/choose).