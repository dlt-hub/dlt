---
title: Introduction
description: Introduction to dlt
keywords: [introduction, who, what, how]
---

# Introduction

![dlt pacman](/img/dlt-pacman.gif)

## Summary

**Automatically turn the JSON returned by any API into a live dataset stored wherever you want it**

`dlt` is an open source library that you include in a Python script to create a highly 
scalable, easy to maintain, straightforward to deploy data [pipeline](general-usage/glossary.md#pipeline).
Once you set it up, it will then automatically turn JSON returned by any 
[source](general-usage/glossary.md#source) (e.g. an API) into a live dataset stored in the 
[destination](general-usage/glossary.md#destination) of your choice (e.g. Google BigQuery).


**[Colab Demo](https://colab.research.google.com/drive/1BXvma_9R9MX8p_iSvHE4ebg90sUroty2)**

```python
import dlt
from chess import chess # a utility function that grabs data from the chess.com API

# create a dlt pipeline that will load chess game data to the DuckDB destination
pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline', 
    destination='duckdb', 
    dataset_name='games_data'
)

# use chess.com API to grab data about a few players
data = chess(['magnuscarlsen', 'rpragchess'], start_month='2022/11', end_month='2022/12')

# extract, normalize, and load the data
pipeline.run(data)
```

## What does `dlt` do?

**`pip install python-dlt` and then include `import dlt` to use it in your loading script**

`dlt` is used to automate fetching data for others, copying production data to somewhere else, putting data from an API into a database, getting data for dashboards that automatically refresh with new data, and more.

## Who should use `dlt`?

Anyone who solves problems in their job using Python (e.g. data scientists, data analysts, data engineers, etc). should use `dlt`, which is licensed under the Apache License 2.0 and can be used for free forever. 

## How does `dlt` work?

`dlt` extracts data from a [source](general-usage/glossary.md#source), inspects its structure to create a [schema](general-usage/glossary.md#schema), normalizes and verifies the data,
and then loads the data into a [destination](general-usage/glossary.md#destination). 
You can read more about how it works [here](./how-dlt-works.md).
