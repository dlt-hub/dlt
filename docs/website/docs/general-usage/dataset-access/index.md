---
title: Accessing loaded data
description: How to access your loaded datasets
keywords: [datasets, data, access]
---
import DocCardList from '@theme/DocCardList';

# Accessing loaded data

After one or more successful runs of your pipeline, you can inspect or access the loaded data in various ways:

* We have a simple [`streamlit` app](./streamlit.md) that you can use to view your data locally in your webapp.
* We have a [python interface](./dataset.md) that allows you to access your data in python as python tuples, `arrow` tables or `pandas` dataframes with a simple dataset object or an sql interface. You can even run sql commands on the filesystem destination via `DuckDB` or forward data from any table into another pipeline.
* We have an [`ibis` interface](./ibis-backend.md) that allows you to use hand over your loaded data to the powerful [ibis-framework](https://ibis-project.org/) library.
* Lastly we have some advice for [monitoring and ensuring the quality of your data](./data-quality-dashboard.md).

# Learn more
<DocCardList />

