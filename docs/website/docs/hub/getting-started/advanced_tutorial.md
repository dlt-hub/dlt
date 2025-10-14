---
title: Packaging a dltHub Project tutorial
description: Using the dltHub cli commands to package a dltHub Project and enable secure access to data
keywords: [command line interface, cli, dlt init, dltHub, project]
---

Packaging a dltHub Project simplifies distribution across teams or stakeholders, such as data analysts or data science teams, without requiring direct access to the project’s internal code. Once installed, the package can be used to run pipelines and access production data through a standardized Python interface.

In this tutorial, you will learn how to package your dltHub project for reuse and distribution and make it pip-installable.


## Prerequisites

Before you begin, ensure the following requirements are met:

- dltHub is installed and set up according to the [installation guide](./installation.md)
- You are familiar with the [core concepts of dlt](../../reference/explainers/how-dlt-works.md)
- You have completed the [basic project tutorial](./tutorial.md)

Additionally, install the required Python packages:

```sh
pip install pandas numpy pyarrow marimo dlt[duckdb] uv
```


## Packaging a project

`dlthub` provides tools to help you package a project for distribution. This makes your project installable via `pip` and easier to share across your organization.

To create the project structure required for a package, add the `--package` option when initializing:

```sh
dlt project init arrow duckdb --package my_dlt_project
```

This creates the same basic project as in the [basic tutorial](./tutorial.md), but places it inside a module named `my_dlt_project`, and includes a basic `pyproject.toml` file following PEP standards.
You’ll also get a default `__init__.py` file to make the package usable after installation:

```sh
.
├── my_dlt_project/       # Your project module
│   ├── __init__.py       # Package entry point
│   ├── dlt.yml           # dltHub project manifest
│   └── ...               # Other project files
├── .gitignore
└── pyproject.toml        # the main project manifest
```

Your `dlt.yml` works exactly the same as in non-packaged projects.
The key difference is the module structure and the presence of the `pyproject.toml` file.
The file includes a special entry point setting to let dltHub discover your project:

```toml
[project.entry-points.dlt_package]
dlt-project = "my_project"
```

You can still run the pipeline as usual with the CLI commands from the root folder:

```sh
dlt pipeline my_pipeline run
```

If you open the `__init__.py` file inside your project module, you'll see the full interface that users of your package will interact with.
This interface is very similar to the [`current`](../features/project/python-api.md) interface used in flat (non-packaged) projects. The main difference is that it automatically uses the `access` profile by default.
You can customize the `__init__.py` file to your project's needs.

### Using the packaged project

To demonstrate how your packaged project can be used, let's simulate a real-world scenario where a data scientist installs and runs your project in a separate Python environment.
In this example, we'll use the [**uv** package manager](https://github.com/astral-sh/uv), but the same steps apply when using **poetry** or **pip**. You can find installation instructions [here](https://github.com/astral-sh/uv?tab=readme-ov-file#installation).
Assume your packaged dltHub project is located at: `/Volumes/my_drive/my_folder/pyproject.toml`.
Navigate to a new directory and initialize your project:

```sh
uv init
```

Install your packaged project directly from the local path:

```sh
uv pip install /Volumes/my_drive/my_folder
```

Your dltHub project is now available for use in this environment.

As an example, create a new Python file named `test_project.py`, use your packaged project, and define the environment variables it needs:

```py
# import the packaged project
import my_dlt_project
import os
import pandas as pd

os.environ["MY_PIPELINE__SOURCES__ARROW__ARROW__ROW_COUNT"] = "0"
os.environ["MY_PIPELINE__SOURCES__ARROW__ARROW__SOME_SECRET"] = "0"

if __name__ == "__main__":
    # should print "access" as defined in your dlt package
    print(my_dlt_project.config().current_profile)
    # Run the pipeline from the packaged project
    my_dlt_project.runner().run_pipeline("my_pipeline")
    # should list the defined destinations
    print(my_dlt_project.config().destinations)
    # get a dataset from the catalog
    dataset = my_dlt_project.catalog().dataset("my_pipeline_dataset")
    # Write a DataFrame to the "my_table" table in the dataset
    dataset.save(pd.DataFrame({"name": ["John", "Jane", "Jim"], "age": [30, 25, 35]}), table_name="my_table")
    # get the row counts of all tables in the dataset as a dataframe
    print(dataset.row_counts().df())
```

Run the script inside the uv virtual environment:

```sh
uv run python test_project.py
```
Once your pipeline has run, you can explore and share the loaded data using various access methods provided by dltHub. [Learn more about it in the Secure data access and sharing.](../features/data-access#data-access-and-sharing)

:::info
In a real-world setup, a data scientist wouldn't install the package from a local path.
Instead, it would typically come from a private PyPI repository or a Git URL.
:::


## Next steps

