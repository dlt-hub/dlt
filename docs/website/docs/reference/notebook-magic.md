# Command Line Interface for DLT in IPython Notebooks

The DLT (Data Loading Tool) Command Line Interface (CLI) can be seamlessly integrated into IPython notebooks, enabling a more interactive and visual approach to managing and deploying data pipelines.

Please note that every magic command run by default has --non-interactive flag set to True. This means that the pipeline will run without any prompts. If you want to run the pipeline with prompts, you can use command line arguments



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


## `%dlt_version`

Check the DLT version.

```ipython
%dlt_version
```

To check an example, please see [notebooks/dlt_version.ipynb](notebooks/dlt_version.ipynb).
