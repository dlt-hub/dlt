---
title: Pokemon details in parallel using transformers
description: Learn how to use dlt transformers and how to speed up your loads with parallelism
keywords: [transformers, parallelism, example]
---

import Header from '../_examples-header.md';

<Header
    intro="In this example, you will learn how to load a list of Pokemon from the PokeAPI and with the help of dlt transformers
    automatically query additional data per retrieved Pokemon. You will also learn how to harness parallelism with a thread pool."
    slug="transformers"
    run_file="pokemon"
    destination="duckdb"/>


## Using transformers with the Pokemon API

For this example, we will be loading Pokemon data from the [PokeAPI](https://pokeapi.co/) with the help of transformers to load
Pokemon details in parallel.

We'll learn how to:
- create 2 [transformers](../../general-usage/resource.md#feeding-data-from-one-resource-into-another) and connect them to a resource with the pipe operator `|`;
- [load these transformers in parallel](../../reference/performance.md#parallelism) using the `@dlt.defer` decorator;
- [configure parallelism](../../reference/performance.md#parallel-pipeline-config-example) in the `config.toml` file;
- deselect the main resource, so it will not be loaded into the database;
- importing and using a pre-configured `requests` library with automatic retries (`from dlt.sources.helpers import requests`).

### Loading code

<!--@@@DLT_SNIPPET ./code/pokemon-snippets.py::example-->



### config.toml with examples how to configure parallelism
<!--@@@DLT_SNIPPET ./code/.dlt/config.toml::example-->

