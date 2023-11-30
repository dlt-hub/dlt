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
There's no need to start any backends or containers. Simply import `dlt` in a Python file or Jupyter Notebook cell, and write a simple pipeline like this:

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

If you don't have `duckdb`, you can install it as an extra:
```sh
pip install "dlt[duckdb]"
```
Now **run** your Python file or Notebook cell.

What that code does:

1. Extracts data from a
[source](general-usage/glossary.md#source) (here: **chess.com REST API**)
2. `dlt` inspects the data's structure to create a
[schema](general-usage/glossary.md#schema)
3. `dlt` structures, normalizes and verifies the data.
4. `dlt` loads data into a [destination](general-usage/glossary.md#destination) (here: a **duckdb** database schema **player_data** and table name **player**).

See below for other easy ways to try `dlt`.

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
[Slack](https://join.slack.com/t/dlthub-community/shared_invite/zt-1n5193dbq-rCBmJ6p~ckpSFK4hCF2dYA)
if you have any questions about use cases or the library.

## Become part of the `dlt` community

1. Give the library a ⭐ and check out the code on [GitHub](https://github.com/dlt-hub/dlt).
1. Ask questions and share how you use the library on
[Slack](https://join.slack.com/t/dlthub-community/shared_invite/zt-1n5193dbq-rCBmJ6p~ckpSFK4hCF2dYA).
1. Report problems and make feature requests [here](https://github.com/dlt-hub/dlt/issues/new/choose).
