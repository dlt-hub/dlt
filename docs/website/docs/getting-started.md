---
title: Getting Started
description: quick start with dlt
keywords: [getting started, quick start, basic examples]
---
import snippets from '!!raw-loader!./getting-started-snippets.py';

# Getting Started

## Overview

`dlt` is an open-source library that you can add to your Python scripts to load data
from various and often messy data sources into well-structured, live datasets.
Below we give you a preview how you can get data from APIs, files, Python objects or
pandas dataframes and move it into a local or remote database, data lake or a vector data store.

Let's get started!

## Installation

Install dlt using `pip`:

```bash
pip install -U dlt
```

Command above installs (or upgrades) library core, in example below we use DuckDB as a destination so let's add a `duckdb` dependency:

```bash
pip install "dlt[duckdb]"
```

:::tip
Use clean virtual environment for your experiments! Here are [detailed instructions](reference/installation).

Make sure that your `dlt` version is **0.3.15** or above. Check it in the terminal with `dlt --version`.
:::

## Quick start

For starters, let's load a list of Python dictionaries into DuckDB and inspect the created dataset.

<!--@@@DLT_SNIPPET_START ./getting-started-snippets.py::start-->
```py
import dlt

data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

pipeline = dlt.pipeline(
    pipeline_name="quick_start", destination="duckdb", dataset_name="mydata"
)
load_info = pipeline.run(data, table_name="users")

print(load_info)
```
<!--@@@DLT_SNIPPET_END ./getting-started-snippets.py::start-->

When you look at the code above, you can see that we:
1. Import the `dlt` library.
2. Define our data to load.
3. Create a pipeline that loads data into DuckDB. Here we also specify the `pipeline_name` and `dataset_name`. We'll use both in a moment.
4. Run the pipeline.

Save this Python script with the name `quick_start_pipeline.py` and run the following command:

```bash
python quick_start_pipeline.py
```

The output should look like:

```bash
Pipeline quick_start completed in 0.59 seconds
1 load package(s) were loaded to destination duckdb and into dataset mydata
The duckdb destination used duckdb:////home/user-name/quick_start/quick_start.duckdb location to store data
Load package 1692364844.460054 is LOADED and contains no failed jobs
```

`dlt` just created a database schema called **mydata** (the `dataset_name`) with a table **users** in it.

### Explore the data

To allow sneak peek and basic discovery you can take advantage of [built-in integration with Strealmit](reference/command-line-interface#show-tables-and-data-in-the-destination):

```bash
dlt pipeline quick_start show
```

**quick_start** is the name of the pipeline from the script above. If you do not have Streamlit installed yet do:

```bash
pip install streamlit
```

Now you should see the **users** table:

![Streamlit Explore data](/img/streamlit1.png)
Streamlit Explore data. Schema and data for a test pipeline “quick_start”.

:::tip
`dlt` works in Jupyter Notebook and Google Colab! See our [Quickstart Colab Demo.](https://colab.research.google.com/drive/1NfSB1DpwbbHX9_t5vlalBTf13utwpMGx?usp=sharing)

Looking for source code of all the snippets? You can find and run them [from this repository](https://github.com/dlt-hub/dlt/blob/devel/docs/website/docs/getting-started-snippets.py).
:::

Learn more:
- What is a data pipeline?
  [General usage: Pipeline.](general-usage/pipeline)
- [Walkthrough: Create a pipeline](walkthroughs/create-a-pipeline).
- [Walkthrough: Run a pipeline.](walkthroughs/run-a-pipeline)
- How to configure DuckDB?
  [Destinations: DuckDB.](dlt-ecosystem/destinations/duckdb)
- [The full list of available destinations.](dlt-ecosystem/destinations/)
- [Exploring the data](dlt-ecosystem/visualizations/exploring-the-data).
- What happens after loading?
  [Destination tables](general-usage/destination-tables).
