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
from various and often messy data sources into well-structured, live datasets. Install it with:
```sh
pip install dlt
```
There's no need to start any backends or containers. Import `dlt` in your Python script and write a simple pipeline like the one below:

<!--AUTO-GENERATED-CONTENT:END-->

<!--@@@DLT_SNIPPET_START index-->
```py
import dlt
from dlt.sources.helpers import requests
# Create a dlt pipeline that will load
# chess player data to the DuckDB destination
pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline',
    destination='duckdb',
    dataset_name='player_data'
)
# Grab some player data from Chess.com API
data = []
for player in ['magnuscarlsen', 'rpragchess']:
    response = requests.get(f'https://api.chess.com/pub/player/{player}')
    response.raise_for_status()
    data.append(response.json())
# Extract, normalize, and load the data
load_info = pipeline.run(data, table_name='player')
```
<!--@@@DLT_SNIPPET_END index-->

Now copy this snippet to a file or a Notebook cell and run it. If you do not have it yet, install **duckdb** dependency (default `dlt` installation is really minimal):
```sh
pip install "dlt[duckdb]"
```

How the script works?: It extracts data from a
[source](general-usage/glossary.md#source) (here: **chess.com REST API**), inspects its structure to create a
[schema](general-usage/glossary.md#schema), structures, normalizes and verifies the data, and then
loads it into a [destination](general-usage/glossary.md#destination) (here: **duckdb** into a database schema **player_data** and table name **player**).

## Why use `dlt`?

- Automated maintenance - with schema inference and evolution and alerts, and with short declarative
code, maintenance becomes simple.
- Run it where Python runs - on Airflow, serverless functions, notebooks. No
external APIs, backends or containers, scales on micro and large infra alike.
- User-friendly, declarative interface that removes knowledge obstacles for beginners
while empowering senior professionals.

## Getting started with `dlt`
1. Play with the
[Google Colab demo](https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing).
This is the simplest way to see `dlt` in action.
2. Run [Getting Started snippets](getting-started.md) and load data from python objects, files, data frames, databases, APIs or PDFs into any [destination](dlt-ecosystem/destinations/).
3. Read [Pipeline Tutorial](build-a-pipeline-tutorial.md) to start building E(t)LT pipelines from ready components.
4. We have many interesting [walkthroughs](walkthroughs/) where you create, run, customize and deploy pipelines.
5. Ask us on
[Slack](https://join.slack.com/t/dlthub-community/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g)
if you have any questions about use cases or the library.

## Become part of the `dlt` community

1. Give the library a ⭐ and check out the code on [GitHub](https://github.com/dlt-hub/dlt).
1. Ask questions and share how you use the library on
[Slack](https://join.slack.com/t/dlthub-community/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g).
1. Report problems and make feature requests [here](https://github.com/dlt-hub/dlt/issues/new/choose).
