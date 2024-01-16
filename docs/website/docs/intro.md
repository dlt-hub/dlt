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
Unlike other solutions, with dlt, there's no need to use any backends or containers. Simply import `dlt` in a Python file or a Jupyter Notebook cell, and create a pipeline to load data into any of the [supported destinations](dlt-ecosystem/destinations/). You can load data from any source that produces Python data structures, including APIs, files, databases, and more.

The library will create or update tables, infer data types and handle nested data automatically. Here are a few example pipelines:

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

<!--@@@DLT_SNIPPET_START api-->
```py
import dlt
from dlt.sources.helpers import requests

# Create a dlt pipeline that will load
# chess player data to the DuckDB destination
pipeline = dlt.pipeline(
    pipeline_name="chess_pipeline", destination="duckdb", dataset_name="player_data"
)
# Grab some player data from Chess.com API
data = []
for player in ["magnuscarlsen", "rpragchess"]:
    response = requests.get(f"https://api.chess.com/pub/player/{player}")
    response.raise_for_status()
    data.append(response.json())
# Extract, normalize, and load the data
load_info = pipeline.run(data, table_name='player')
```
<!--@@@DLT_SNIPPET_END api-->

Copy this example to a file or a Jupyter Notebook and run it. To make it work with the DuckDB destination, you'll need to install the **duckdb** dependency (the default `dlt` installation is really minimal):
```sh
pip install "dlt[duckdb]"
```
Now **run** your Python file or Notebook cell.

How it works? The library extracts data from a [source](general-usage/glossary.md#source) (here: **chess.com REST API**), inspects its structure to create a
[schema](general-usage/glossary.md#schema), structures, normalizes and verifies the data, and then
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

<!--@@@DLT_SNIPPET_START csv-->
```py
import dlt
import pandas as pd

owid_disasters_csv = (
    "https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/"
    "Natural%20disasters%20from%201900%20to%202019%20-%20EMDAT%20(2020)/"
    "Natural%20disasters%20from%201900%20to%202019%20-%20EMDAT%20(2020).csv"
)
df = pd.read_csv(owid_disasters_csv)
data = df.to_dict(orient="records")

pipeline = dlt.pipeline(
    pipeline_name="from_csv",
    destination="duckdb",
    dataset_name="mydata",
)
load_info = pipeline.run(data, table_name="natural_disasters")

print(load_info)
```
<!--@@@DLT_SNIPPET_END csv-->

  </TabItem>
  <TabItem value="database">

:::tip
Use our verified [SQL database source](dlt-ecosystem/verified-sources/sql_database)
to sync your databases with warehouses, data lakes, or vector stores.
:::

<!--@@@DLT_SNIPPET_START db-->
```py
import dlt
from sqlalchemy import create_engine

# Use any SQL database supported by SQLAlchemy, below we use a public
# MySQL instance to get data.
# NOTE: you'll need to install pymysql with `pip install pymysql`
# NOTE: loading data from public mysql instance may take several seconds
engine = create_engine("mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam")

with engine.connect() as conn:
    # Select genome table, stream data in batches of 100 elements
    query = "SELECT * FROM genome LIMIT 1000"
    rows = conn.execution_options(yield_per=100).exec_driver_sql(query)

    pipeline = dlt.pipeline(
        pipeline_name="from_database",
        destination="duckdb",
        dataset_name="genome_data",
    )

    # Convert the rows into dictionaries on the fly with a map function
    load_info = pipeline.run(
        map(lambda row: dict(row._mapping), rows),
        table_name="genome"
    )

print(load_info)
```
<!--@@@DLT_SNIPPET_END db-->

Install **pymysql** driver:
```sh
pip install pymysql
```

  </TabItem>
</Tabs>


## Why use `dlt`?

- Automated maintenance - with schema inference and evolution and alerts, and with short declarative
code, maintenance becomes simple.
- Run it where Python runs - on Airflow, serverless functions, notebooks. No
external APIs, backends or containers, scales on micro and large infra alike.
- User-friendly, declarative interface that removes knowledge obstacles for beginners
while empowering senior professionals.

## Getting started with `dlt`
1. Dive into our [Getting started guide](getting-started.md) for a quick intro to the essentials of `dlt`.
2. Play with the
[Google Colab demo](https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing).
This is the simplest way to see `dlt` in action.
3. Read the [Tutorial](tutorial/intro) to learn how to build a pipeline that loads data from an API.
4. Check out the [How-to guides](walkthroughs/) for recepies on common use cases for creating, running and deploying pipelines.
5. Ask us on
[Slack](https://join.slack.com/t/dlthub-community/shared_invite/zt-1n5193dbq-rCBmJ6p~ckpSFK4hCF2dYA)
if you have any questions about use cases or the library.

## Join the `dlt` community

1. Give the library a ⭐ and check out the code on [GitHub](https://github.com/dlt-hub/dlt).
1. Ask questions and share how you use the library on
[Slack](https://join.slack.com/t/dlthub-community/shared_invite/zt-1n5193dbq-rCBmJ6p~ckpSFK4hCF2dYA).
1. Report problems and make feature requests [here](https://github.com/dlt-hub/dlt/issues/new/choose).
