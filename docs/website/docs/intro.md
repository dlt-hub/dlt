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
scalable, easy to maintain, straightforward to deploy data [pipeline](./glossary.md#pipeline).
Once you set it up, it will then automatically turn JSON returned by any 
[source](./glossary.md#source) (e.g. an API) into a live dataset stored in the 
[destination](./glossary.md#destination) of your choice (e.g. Google BigQuery).

## What does `dlt` do?

**`pip install python-dlt` and then include `import dlt` to use it in your loading script**

`dlt` is used to automate fetching data for others, copying production data to somewhere else, putting data from an API into a database, getting data for dashboards that automatically refresh with new data, and more.

## Who should use `dlt`?

Anyone who solves problems in their job using Python (e.g. data scientists, data analysts, data engineers, etc). should use `dlt`, which is licensed under the Apache License 2.0 and can be used for free forever. 

## How does `dlt` work?

`dlt` extracts data from a [source](./glossary.md#source), inspects its structure to create a [schema](./glossary.md#schema), normalizes and verifies the data,
and then loads the data into a [destination](./glossary.md#destination). 
You can read more about how it works [here](./architecture.md).