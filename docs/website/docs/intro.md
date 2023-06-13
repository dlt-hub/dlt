
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

[How it works](./how-dlt-works.md): `dlt` extracts data from a [source](general-usage/glossary.md#source), inspects its structure to create a [schema](general-usage/glossary.md#schema), structures, normalizes and verifies the data,
and then loads the data into a [destination](general-usage/glossary.md#destination).

## Who is dlt for?


### Why does the data team love dlt?
* Automated maintenance - with schema inference and evolution and alerts, and with short declarative code, maintenance becomes simple.
* Run anywhere - Run on Airflow, serverless functions, anywhere that python goes data flows. No special requirements, scales on micro and large infra alike.
* dlt offers a user-friendly, declarative interface that removes knowledge obstacles for beginners while empowering senior professionals.

### By automating a large core of data engineering work, dlt solves different problems for different people.
- For organisations: dlt unleashes a tidal wave of transformation around usage and curation of datasets.
- For tech leads: dlt is a game changer that enables all your team quickly build "masterpiece-grade" pipelines faster and more robust than they would build the usual throwaway code.
- For engineers, dlt is a groundbreaking force that streamlines their work, banishes errors, notifies contract changes, leaving a trail of awe and amazement in its wake.
- For Data scientists and analysts, dlt is the light on the path to data discovery, structuring and usage.
- For data producers, dlt serves as an all-in-one "data contract and pipeline solution," that enables delivering governed data.

Read more about use cases by persona here: [User guides](../docs/user-guides.md)

## Community & Support

### Getting started with `dlt`

1. Play with the [Google Colab demo](https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing). This is the simplest way you could use dlt. You can review
2. Read the docs on [building a pipeline, warehouse or lake with dlt](../docs/build-with-dlt.md) or [using the existing pipelines]. Ask us on slack if you are unsure if your use case is supported.
3. Send the [user guides](../docs/user-guides.md) to the rest of your team and discuss use cases.

### Become part of the `dlt` community
1. Give the library a ⭐ and check out the code on [GitHub](https://github.com/dlt-hub/dlt)
2. Ask questions and share how you use the library on [Slack](https://join.slack.com/t/dlthub-community/shared_invite/zt-1slox199h-HAE7EQoXmstkP_bTqal65g)
3. Report problems and make feature requests [here](https://github.com/dlt-hub/dlt/issues/new)
