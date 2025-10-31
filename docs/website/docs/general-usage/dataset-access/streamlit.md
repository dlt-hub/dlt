---
title: The legacy streamlit app
description: Viewing your data with streamlit
keywords: [data, dataset, streamlit]
---

# View data with Streamlit

Once you have run a pipeline locally, you can launch a web app that displays the loaded data. For this to work, you will need to have the `streamlit` package installed.

:::tip
The Streamlit app does not work with all destinations supported by `dlt`. Only destinations that provide a SQL client will work. The filesystem destination has support via the [Filesystem SQL client](./sql-client#the-filesystem-sql-client) and will work in most cases. Vector databases generally are unsupported.
:::

:::warning
The Streamlit app is not under active development anymore and may soon be deprecated. We encourage all users to use the [workspace dashboard](../dashboard.md)
:::

## Prerequisites

To install Streamlit, run the following command:

```sh
pip install streamlit
```

*Note*: Your python version must be installed with C library support. You can verify by running
```sh
`python -c "import _ctypes"`
```
If this fails, you need to install the [libffi](https://sourceware.org/libffi/)-package
(`libffi-dev` for debian/ubuntu/Windows and `libffi` for macOs) package.
Afterwards you might need to also reinstall your python version.


## Launching the Streamlit app

You can use the `show` [CLI command](../../reference/command-line-interface.md#dlt-pipeline-show)
with your pipeline name:

```sh
dlt pipeline {pipeline_name} show --streamlit
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

