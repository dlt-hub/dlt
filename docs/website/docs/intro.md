---
title: Introduction
description: Introduction to dlt
keywords: [introduction, who, what, how]
---

# Introduction

![dlt pacman](/img/dlt-pacman.gif)

## What is `dlt`?

`dlt` is an open-source library that enables you to create a data [pipeline](./general-usage/glossary.md#pipeline) in a Python script. To use it, `pip install dlt` and then `import dlt`. Once set up, it will automatically load any [source](./general-usage/glossary.md#source) (e.g. an API) into a live dataset stored in the [destination](./general-usage/glossary.md#destination) of your choice.

To try it out, install `dlt` with DuckDB using `pip install dlt[duckdb]` and run the following example:
```python
import dlt
import requests

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
pipeline.run(data, table_name='player')
```

[How it works](./how-dlt-works.md): `dlt` extracts data from a [source](general-usage/glossary.md#source), inspects its structure to create a [schema](general-usage/glossary.md#schema), normalizes and verifies the data,
and then loads the data into a [destination](general-usage/glossary.md#destination).

## Why use `dlt`?

The `dlt` library can be dropped into a variety of tools (e.g. a Colab notebook, AWS Lambda, an Airflow DAG, your local laptop, a GPT-4 assisted development playground, etc). That is, it was designed to fit into what you are already doing, rather than forcing your solution to fit into `dlt`.

`dlt` pipelines are highly scalable, easy to maintain, and straightforward to deploy in production. People add it to their code for everything from exploring data from a new API with DuckDB in under 15 minutes to building an advanced loading infrastructure for their entire organization.

## Community & Support

### Getting started with `dlt`
1. Play with the [Google Colab demo](https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing)
2. [Install](installation.mdx) and try the [Getting Started](getting-started.md) tutorial
3. Add a [verified source](walkthroughs/add-a-verified-source.md) or start [creating your own pipeline](./walkthroughs/create-a-pipeline.md)

### Become part of the `dlt` community
1. Give the library a ⭐ and check out the code on [GitHub](https://github.com/dlt-hub/dlt)
2. Ask questions and share how you use the library on [Slack](https://join.slack.com/t/dlthub-community/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g)
3. Report problems and make feature requests [here](https://github.com/dlt-hub/dlt/issues/new)
