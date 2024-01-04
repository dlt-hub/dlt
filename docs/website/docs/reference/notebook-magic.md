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

## `%pipeline`

In an IPython notebook, you can use the `%pipeline` magic command to execute the `dlt pipeline` command.
This command allows managing of pipelines.

```ipython
%pipeline --operation <command> --pipeline_name <pipeline_name>
```

Possible operations are:
1. `list-pipelines`
2. `info`
3. `sync`
4. `trace`
5. `failed-jobs`
6. `drop-pending-packages`
7. `schema`

Examples:

```ipython
%pipeline --operation list-pipelines
```

```ipython
%pipeline --operation info --pipeline_name chess
```

For more information on these commands, check `dlt` [CLI documentation](command-line-interface).


## `%init`

In an IPython notebook, you can use the `%init` magic command to execute the `dlt init` command.
This command sets up a new `dlt` pipeline script that transfers data from a `source` to a `destination`.


```ipython
%init --source_name <source>  --destination_name <destination>
```

Example:

```ipython
%init --source_name pokemon  --destination_name duckdb
```

## `%settings`

Enable or disable telemetry globally.

```ipython
%settings --enable-telemetry
```
or
```ipython
%settings --disable-telemetry
```

## `%schema`
This command shows, converts and upgrades schemas.

```ipython
%schema --file_path <schema_file_path> --format json
```
Options:
- `--format`: Display schema in yaml or json format. Defaults to `yaml`.
- `--remove-defaults`: Does not show default hint values.

## `%dlt_version`

Check the `dlt` version.

```ipython
%dlt_version
```

