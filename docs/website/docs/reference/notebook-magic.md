# Command Line Interface for DLT in IPython Notebooks

The DLT (Data Loading Tool) Command Line Interface (CLI) can be seamlessly integrated into IPython notebooks, enabling a more interactive and visual approach to managing and deploying data pipelines.

## `%init`

In an IPython notebook, you can use the `%init` magic command to execute the `dlt init` command. This command sets up a new DLT pipeline script that transfers data from a `source` to a `destination`.

```ipython
%init --source <source>  --destionation <destination>

```

## `%pipeline`

In an IPython notebook, you can use the `%init` magic command to execute the `dlt init` command. This command sets up a new DLT pipeline script that transfers data from a `source` to a `destination`.

```ipython
%init --source <source>  --destionation <destination>

```

## `%deploy`

Deploy a DLT pipeline to a destination.

```ipython

%deploy --pipeline_script_path <path> --deployment_method <git-actions> --repo_location <repo location> --branch <branch> --schedule <5 minutes, 15 minutes, 30 minutes, 45 minutes or hour> --run_on_push <> --run_on_schedule <True/False> --run_on_manual <True/False>

```

## `%pipeline`

In an IPython notebook, you can use the `%init` magic command to execute the `dlt init` command. This command sets up a new DLT pipeline script that transfers data from a `source` to a `destination`.

```ipython
%pipeline --operation <operation>  -- <destination>

```

## `%dlt_version`

Check the DLT version.

```ipython
%dlt_version
```

