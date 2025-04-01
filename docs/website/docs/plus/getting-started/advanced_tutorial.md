---
title: Advanced Project tutorial
description: Using the dlt+ cli commands to create and manage dlt+ Project
keywords: [command line interface, cli, dlt init, dlt+, project]
---

import Link from '../../_plus_admonition.md';

<Link/>

In this tutorial, you will explore advanced features of a dlt+ project. By the end, you will be able to:
* Understand how dlt+ automatically creates implicit entities when not explicitly defined—and how to prevent this behavior
* Use dlt+ datasets to control access to specific datasets and destinations 
* Interact with the dlt+ project context and reference entities directly from your code
* Package a dlt+ project for distribution to other teams or stakeholders within your organization

## Prerequisites

Before you begin, ensure the following requirements are met:

- dlt+ is installed and set up according to the [installation guide](./installation.md)
- You are familiar with the [core concepts of dlt](../../reference/explainers/how-dlt-works.md)
- You have completed the [basic project tutorial](./tutorial.md)

Additionally, install the required Python packages:

```sh
pip install pandas numpy pyarrow streamlit dlt[duckdb] uv

```

## Implicit entities

By default, dlt+ will automatically create entities such as datasets or destinations when they are requested by the user or the executed code..
To see how this works, initialize a new project with: 
```sh
dlt project init arrow duckdb --package my_dlt_project
```

```yaml
# your sources are the data sources you want to load from
sources:
  arrow:
    type: sources.arrow.source

# your destinations are the databases where your data will be saved
destinations:
  duckdb:
    type: duckdb

# your pipelines orchestrate data loading actions
pipelines:
  my_pipeline:
    source: arrow
    destination: duckdb
    dataset_name: my_pipeline_dataset
```

When you run the pipeline using the following command:
```sh
dlt pipeline my_pipeline run
```

dlt+ will use the source and destination defined in the pipeline configuration. Since datasets are not explicitly defined in this example, dlt+ will automatically create an implicit dataset named after the pipeline `my_pipeline_dataset`. 
Similarly, even if the DuckDB destination is not declared, dlt+ will implicitly create it for you.

If you want to prevent dlt+ from creating implicit entities, you can set the `allow_undefined_entities` option to `false` in your `dlt.yml` file:

```yaml
project:
  allow_undefined_entities: false
```

If you now try to run the pipeline my_pipeline, you’ll encounter an error indicating that the dataset `my_pipeline_dataset` does not exist.
To fix this, you need to define the dataset explicitly in the `dlt.yml` file:

```yaml
datasets:
  my_pipeline_dataset:
    destination:
        - duckdb
```

Note: The destination field is an array, allowing you to specify one or more destinations where the dataset can be materialized. 
We'll explore this in more detail in the next section.


## Managing Datasets and Destinations

When you add explicit dataset entries to the `dlt.yml` file, as shown earlier, you must define the destinations where each dataset is allowed to be materialized.
This constraint applies even if `allow_undefined_entities` is set to `true`.

Update your `dlt.yml` file as follows:


```yaml
# your sources are the data sources you want to load from
sources:
  arrow:
    type: sources.arrow.source

# your destinations are the databases where your data will be saved
destinations:
  duckdb:
    type: duckdb


datasets:
  my_pipeline_dataset:
    destination:
        - bigquery

# your pipelines orchestrate data loading actions
pipelines:
  my_pipeline:
    source: arrow
    destination: duckdb
    dataset_name: my_pipeline_dataset
```

If you try to run the `my_pipeline` pipeline now, you will receive an error stating that the dataset `my_pipeline_dataset` does not exist on the `duckdb` destination.
That’s because when a dataset is explicitly defined, dlt+ will not automatically infer or allow additional destinations.
You must include all intended destinations manually.
To fix this, simply add `duckdb` to the list of allowed destinations for `my_pipeline_dataset` in your `dlt.yml` file:

```yaml
my_pipeline_dataset:
  destination:
      - duckdb
      - bigquery
```

:::tip
If you run dataset CLI commands without specifying a destination, dlt+ will use the first destination listed for that dataset by default.
If implicit entities are allowed, dlt+ can also discover datasets defined only within pipelines and will use the pipeline's destination when creating the dataset.
The same rule applies when retrieving datasets from the catalog in Python, more on that below.
:::


## Interacting with the project context in code

You can also interact with your dlt+ project directly in code, instead of using CLI commands. 
Start by initializing a project as before:

```bash
dlt project init arrow duckdb
```
Next, create a new Python file in the root of your project called `do_something.py` and add the following code:

```py
from dlt_plus import current

if __name__ == "__main__":
    # get the currently active project and print its name
    print(current.project().name)
```

Run the script with:
```bash
python do_something.py`
```

The current module provides access to various parts of your active dlt+ project:

- `current.project()` - Retrieves the project configuration
- `current.entities()` - Returns a factory with all instantiated entities
- `current.catalog()` - Provides access to all defined datasets in the catalog
- `current.runner()` - Allows you to run pipelines programmatically


### Accessing project settings

Here are a few examples of what you can access from the project object:
```py
# show the currently active profile
print(current.project().current_profile)
# show the main project dir
print(current.project().project_dir)
# show the project config
print(current.project().config)
# list explicitely defined datasets (also works with destinations, sources, pipelines etc.)
print(current.project().datasets)
```

### Accessing entities

Accessing entities in code works the same way as when referencing them in the `dlt.yml` file. 
If allowed, implicit entities will be created and returned automatically. If not, an error will be raised.
```py
# get a pipeline instance
pipeline = current.entities().create_pipeline("my_pipeline")
# get a destination instance
destination = current.entities().create_destination("duckdb")
```

### Running pipelines with the predefined source with the runner

`dlt+` includes a pipeline runner, which is the same one used when you run pipelines from the CLI.  
You can also use it directly in your code through the project context:

```py
# get the runner
runner = current.runner()
# run the "my_pipeline" pipeline from the currently active project
runner.run_pipeline("my_pipeline")
```

### Accessing the catalog

The catalog allows you to access all explicitly defined datasets:

```py
# get a dataset instance pointing to the default destination (first in dataset destinations list) and access data inside of it
# Note: The dataset must already exist physically for this to work
dataset = current.catalog().dataset("my_pipeline_dataset")
# get the row counts of all tables in the dataset as a dataframe
print(dataset.row_counts().df())
```

:::tip
Read the docs on how to access data from python in dlt datasets 
[learn more here](../../../general-usage/dataset-access/dataset)
You’ll discover how to browse and filter tables, and retrieve data in various formats.
:::

### Writing data back to the catalog

You can also write data to datasets in the dlt+ catalog. Each dataset has a `.save()` method that lets you write data back to it.  
In the future, you'll be able to control which datasets are writable using contracts.
Under the hood, `dlt+` runs an ad-hoc pipeline to handle the write operation.

:::warning
Writing data to the catalog is an **experimental feature**.  
Use it with caution until it's fully stable.:::

```py
# get a dataset from the catalog (it must already exist and be defined in dlt.yml)
dataset = current.catalog().dataset("my_pipeline_dataset")
# Write a DataFrame to the "my_table" table in the dataset
dataset.save(pd.DataFrame({"name": ["John", "Jane", "Jim"], "age": [30, 25, 35]}), table_name="my_table")
```

You can also read from an existing table and write the data to a new table, either in the same or another dataset:

```py
# get dataset from the catalog
dataset = current.catalog().dataset("my_pipeline_dataset")

# This function reads data in chunks from an existing table and yields each chunk
def transform_frames():
    # Read the 'items' table in chunks of 1000 rows
    for df in dataset.items.iter_df(chunk_size=1000):
        # You can process the data here if needed
        yield df

# Write the data to a new table called "my_new_table"
dataset.save(transform_frames, table_name="my_new_table")
```

### Switching profiles in code

By default, when you access the project in code, it uses the default or pinned profile.  
You can switch to a different profile using the `switch_profile` function.

Here’s an example:

```py   
from dlt_plus import current
from dlt_plus.project.run_context import switch_profile

if __name__ == "__main__":
    # Shows the current active profile
    print(current.project().current_profile)
    # Switch to the tests profile
    switch_profile("tests")
    # Now "tests" is the active profile, merged with the project config
    print(current.project().current_profile)
```

## Packaging a project

`dlt+` provides tools to help you package a project for distribution.  
This makes your project installable via `pip` and easier to share across your organization.

To create a packaged project, add the `--package` option when initializing:

```sh
dlt project init arrow duckdb --package my_dlt_project
```

This creates the same basic project as in the tutorial, but places it inside a module named `my_dlt_project`, and includes a basic `pyproject.toml` file following PEP standards. 
You’ll also get a default `__init__.py` file to make the package usable after installation:

```sh
.
├── my_dlt_project/       # Your project module
│   ├── __init__.py       # Package entry point
│   ├── dlt.yml           # dlt+ project manifest
│   └── ...               # Other project files
├── .gitignore
└── pyproject.toml        # the main project manifest
```

Your `dlt.yml` works exactly the same as in non-packaged projects. 
The key difference is the module structure and the presence of the `pyproject.toml` file. 
The file includes a special entry point setting to let dlt+ discover your project:

```toml
[project.entry-points.dlt_package]
dlt-project = "my_project"
```

You can still run the pipeline as usual with the cli commands from the root folder:

```sh
dlt pipeline my_pipeline run
```

If you open the `__init__.py` file inside your project module, you'll see the full interface that users of your package will interact with.
This interface is very similar to the `current` interface used in flat (non-packaged) projects. The main difference is that it automatically uses the `access` profile by default.
You can customize the `__init__.py` file to your project's needs.

### Using the packaged project

To demonstrate how your packaged project can be used, let's simulate a real-world scenario where a data scientist installs and runs your project in a separate Python environment.
In this example, we'll use the **uv** package manager, but the same steps apply when using **poetry** or **pip**.
Assume your packaged `dlt+` project is located at: `/Volumes/my_drive/my_folder/pyproject.toml`. 
Navigate to a new directory and initialize your project:

```sh
uv init
```

Install your packaged project directly from the local path:

```sh
uv pip install /Volumes/my_drive/my_folder
```

Your dlt+ project is now available for use in this environment.
Now, create a new Python file named `test_project.py` and use your packaged project like this:

```py
# import the packaged project
from my_dlt_project import current

if __name__ == "__main__":
    # should print "access" as defined in your dlt package
    print(my_dlt_project.config().current_profile)
    # Run the pipeline from the packaged project
    my_dlt_project.runner().run_pipeline("my_pipeline")
```

Run the script inside the uv virtual environment:

```sh
uv run python test_project.py
```


:::info
In a real-world setup, a data scientist wouldn't install the package from a local path.
Instead, it would typically come from a private PyPI repository or a Git URL.
:::


## Next steps