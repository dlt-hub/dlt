---
title: dlt CLI in IPython Notebooks
description: The dlt (Data Loading Tool) Command Line Interface (CLI) can be seamlessly integrated into IPython notebooks, enabling a more interactive and visual approach to managing and deploying data pipelines.
keywords: [magics, notebook]
---



# dlt CLI in IPython Notebooks

The `dlt` (Data Loading Tool) Command Line Interface (CLI) can be seamlessly integrated into IPython notebooks,
enabling a more interactive and visual approach to managing and deploying data pipelines.

:::info
Please note that every magic command run by default has `--non-interactive` flag set to True.
This means that the pipeline will run without any prompts.
If you want to run the pipeline with prompts, you can use command line arguments.
:::

## Installation

You can install `dlt` and its notebook dependencies using `pip`:

```bash
pip install dlt[notebook]
```

## `%pipeline`

In an IPython notebook, you can use the `%pipeline` magic command to execute the `dlt pipeline` command.
This command allows managing of pipelines.

```ipython
%pipeline --operation <command>
```

Possible operations are:
  1. `info`
  2. `list-pipelines`
  3. `show`
  4. `info`
  5. `trace`
  6. `failed-jobs`
  7. `drop-pending-packages`
  8. `load-package`
  9. `drop`
  10. `schema`

Example:

```ipython
%pipeline --operation info
```

For more information on these commands, check `dlt` [CLI documentation](command-line-interface).


## `%init`

In an IPython notebook, you can use the `%init` magic command to execute the `dlt init` command.
This command sets up a new `dlt` pipeline script that transfers data from a `source` to a `destination`.


```ipython
%init --source <source>  --destination <destination>
```

Example:

```ipython
%init --source pokemon  --destination duckdb
```

## `%dlt_version`

Check the `dlt` version.

```ipython
%dlt_version
```

