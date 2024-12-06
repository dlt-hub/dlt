---
title: Viewing your data with Streamlit
description: Viewing your data with streamlit
keywords: [data, dataset, streamlit]
---

# Viewing your data with Streamlit

Once you have run a pipeline locally, you can launch a web app that displays the loaded data. For this to work, you will need to have the `streamlit` package installed.

:::tip
The Streamlit app does not work with all destinations supported by `dlt`. Only destinations that provide a SQL client will work. The filesystem destination has support via the [Filesystem SQL client](./sql-client#the-filesystem-sql-client) and will work in most cases. Vector databases generally are unsupported.
:::

## Prerequisites

To install Streamlit, run the following command:

```sh
pip install streamlit
```

## Launching the Streamlit app

You can use the `show` [CLI command](../../reference/command-line-interface.md#show-tables-and-data-in-the-destination)
with your pipeline name:

```sh
dlt pipeline {pipeline_name} show
```

Use the pipeline name you defined in your Python code with the `pipeline_name` argument. If you are unsure, you can use the `dlt pipeline --list` command to list all pipelines.

## Credentials

`dlt` will look for `secrets.toml` and `config.toml` in the `.dlt` folder.

If `secrets.toml` are not found, it will use
`secrets.toml` from `.streamlit` folder.

If you run locally, maintain your usual `.dlt` folder.

When running on streamlit cloud, paste the content of `dlt`
`secrets.toml` into the `streamlit` secrets.

## Inspecting your data

You can now inspect the schema and your data. Use the left sidebar to switch between:

* Exploring your data (default);
* Information about your loads.


## Further reading

If you are running `dlt` in Python interactively or in a notebook, read the [Accessing loaded data in Python](./dataset.md) guide.

