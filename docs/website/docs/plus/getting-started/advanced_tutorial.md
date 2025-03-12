---
title: Advanced Project tutorial
description: Using the dlt+ cli commands to create and manage dlt+ Project
keywords: [command line interface, cli, dlt init, dlt+, project]
---

import Link from '../../_plus_admonition.md';

<Link/>

This tutorial will introduce you to advanced dlt project features. You will learn how to:

* Understand how dlt+ creates implicit dlt entites if they are not defined and how to prevent this
* Learn how to use dlt+ datasets to restrict access to certain datasets and destinations
* Learn how to interact with the dlt+ project context and entities from code
* Create a packaged dlt+ project which you can distribute to other stakeholders in your company

## Prerequisites

To follow this tutorial, make sure:

- dlt+ is set up according to the [installation guide](./installation.md)
- you're familiar with the [core concepts of dlt](../../reference/explainers/how-dlt-works.md)
- you have completed the [basic project tutorial](./tutorial.md)

## Implicit entities

By default, dlt+ will create implicit entities when they are requested by the user or the executed code. Let's create an example project again with `dlt project init arrow duckdb --package my_dlt_project`.

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

If you run the pipeline with `dlt pipeline my_pipeline run`, dlt+ will use the source and the destination referenced by the pipeline configuration. Datasets are not defined in this example, so dlt+ will create an implicit dataset with the name as defined in the pipeline configuration. For this example, you do not even need the duckdb destination defined, as this would also be implicitely created for you. 

If you want to prevent dlt+ from creating implicit entities, you can set the `allow_undefined_entities` option to `false` in the project configuration by adding the following to your `dlt.yml` file:

```yaml
project:
  allow_undefined_entities: false
```

If you try to run the pipeline "my_pipeline" now, you will receive an error that the dataset `my_pipeline_dataset` does not exist. To fix this, you can create the dataset explicitly in the `dlt.yml` file:

```yaml
datasets:
  my_pipeline_dataset:
    destination:
        - duckdb
```

Note that the destination property is an array of destinations on which this dataset may be materialized. More on that in the next section.


## Managing Datasets and Destinations

If you add explicit dataset entries to the `dlt.yml` file, as we did above, you need to defined on which destinations the dataset may be materialized. This constraint will also be enforced when `allow_undefined_entities` is set to `true`. Change your dlt.yml file to the following:


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

If you try to run the pipeline "my_pipeline" now, you will receive an error that the dataset `my_pipeline_dataset` does not exist on the bigquery destination, you may allow this by adding `bigquery` to the allowed destinations of `my_pipeline_dataset`.

```yaml
my_pipeline_dataset:
  destination:
      - duckdb
      - bigquery
```

:::tip
If  you run dataset cli commands without providing a destination name, dlt+ will always select the first destination in the list by default. If you allow implicit entities, dlt+ can also discover datasets only defined on pipelines and will use the destination of that pipeline for instantiating the dataset. The same is true when retrieving datasets from the catalog in python, ore on that below.
:::


## Interacting with the project context in code

You can also interact with your dlt+ project directly in code as opposed to running cli commands. Let's use the same starting point as above by running `dlt project init arrow duckdb`. Now add a new python file in the root of your project called `do_something.py` and add the following code:

```py
from dlt_plus import current

if __name__ == "__main__":
    # this will get the currently active project and print its name
    print(current.project().name)
```

Run the above with `python do_something.py` and see the output.

The current module will allow you to interact with the currently active project. You can retrieve the project configuration with `current.project()`, get an instantiatend version of all defined entities from the entities factory at `current.entities()`, access all defined datasets in the catalog: `current.catalog()`, and run pipelines with the runner at `current.runner()`.

### Accessing project settings

A few examples of what you have access too on the project object:

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

Accessing entities works in the same way as when accessing entities in the `dlt.yml`. If possible and allowed, implicit entities are created and returned back to you, if you, an error will be raised.

```py
# get a pipeline instance
pipeline = current.entities().create_pipeline("my_pipeline")
# get a destination instance
destination = current.entities().create_destination("duckdb")
```

### Running pipelines with the predefined source with the runner

`dlt+` provides a pipeline runner that is used when you run pipelines from the cli as described in the basic tutorial. The project context also provides a runner instance that you can use to run your pipelines in code. 

```py
# get the runner
runner = current.runner()
# run the pipeline "my_pipeline" from the currently active project
runner.run_pipeline("my_pipeline")
```

### Accessing the catalog

The catalog provides access to all explicitely defined datasets

```py
# get a dataset instance pointing to the default destination (first in dataset destinations list) and access data inside of it
# for this to work this dataset must already exist physically
dataset = current.catalog().dataset("my_pipeline_dataset")
# get the row counts of all tables in the dataset as a dataframe
print(dataset.row_counts().df())
```

:::tip
Read the docs on how to access data in python in dlt datasets [here](../../../general-usage/dataset-access/dataset) to learn more about the available data access methods. You can browse and filter tables and get the data in various formats.
:::

### Writing data back to the catalog

The datasets on the dlt+ catalogs may also be written to. The datasets provide `write` method that can be used to write data back to the catalog. You will be able to control which datasets may be written to with contracts in the future. Under the hood, `dlt+` will use an ad-hoc dlt pipeline to run this operation.

:::warning
Writing data back to the catalog is an experimental feature at this time and should be used with caution until it is fully stable.
:::

```py
# get a dataset from the catalog (this dataset must already exist physically and be explicitely defined in the dlt.yml)
dataset = current.catalog().dataset("my_pipeline_dataset")
# write a dataframe to the dataset into the table "my_table"
dataset.write(pd.DataFrame({"name": ["John", "Jane", "Jim"], "age": [30, 25, 35]}), table_name="my_table")
```

You can also iterate over dataframes or arrow tables in an existing table and write them to a new table in the same or another dataset:

```py
# get dataset from the catalog
dataset = current.catalog().dataset("my_pipeline_dataset")

# here we iterate over the existing items table and write each dataframe to the dataset into a new table
def transform_frames():
    # iterate of table `items` in the dataset in chunks of 1000 rows
    for df in dataset.items.iter_df(chunk_size=1000):
        # do something with the dataframe here
        yield df

# write the dataframe to the dataset into the table "my_new_table"
dataset.write(transform_frames, table_name="my_new_table")
```

### Switching profiles in code

By default if you access the project, you will be using the default or pinned profile. You can switch the profile with the `switch_profile` function. Consider the following example:

```py   
from dlt_plus import current
from dlt_plus.project.run_context import switch_profile

if __name__ == "__main__":
    # will show that the default profile is active
    print(current.project().current_profile)
    # switch to the tests profile
    switch_profile("tests")
    # now the tests profile is active and is merged with the project config
    print(current.project().current_profile)
```

## Packaging a project

`dlt+` also has tools to help you package a dlt+ project for distribution. This will make your dlt+ project pip installable and make it easier for you to distribute accross your organization. To start with a dlt+ packaged project, you can supply the package argument to the project init command:

```sh
dlt project init arrow duckdb --package my_dlt_project
```

This will provide you with the same project as used in the basic tutorial but places in a module directory named `my_dlt_project` and provided with a simple PEP compatible `pyproject.toml` file. You will also receive a default package `__init__.py` file to make the module accssible from the outside when installed:

```sh
.
├── my_dlt_project/       # your project module
│   ├── __init__.py       # your package entry point
│   ├── dlt.yml           # the dlt+ project manifest
│   └── ...               # more project files
├── .gitignore
└── pyproject.toml        # the main project manifest
```

Your `dlt.yml` will look and work exactly the same as in the non-packaged version. The only difference is the module structure and the presence of the `pyproject.toml` file. The pyproject.toml has a special entry point setting that makes the dlt+ project discoverable by dlt+:

```toml
[project.entry-points.dlt_package]
dlt-project = "my_project"
```

You can still run the pipeline the same way as before with the cli commands from the root folder:

```sh
dlt pipeline my_pipeline run
```

If you look at the module `__init__.py` file that was created for you, you can see the full interface of your packaged project that the user of your package will have access to. It is very similar to the interface of the `current` interface of the flat project, there is an additional feature that selects the `access` profile by default. It is up to you to adapt this `__init__.py` file to your needs.

### Using the packaged project

Using this project in its default state, let's simulate installing it into a new python project like a data-scientist in your organization might do, to demonstrate how this could be used in practice. For this example we use the uv package manager, but this is also possible with poetry or pip. Let's assume you have been developing your dlt+ packaged project at `/Volumes/my_drive/my_folder/pyproject.toml`. Navigate to a new directory and initialize your project:

```sh
uv init
```

Now we can install our package directly from the directory into the uv virtual environment:

```sh
uv pip install /Volumes/my_drive/my_folder
```

Now your packaged dlt project should be available for you to use. Let's create a new python file names `test_project.py` and use the packaged project:

```py
# import the packaged project
from my_dlt_project import current

if __name__ == "__main__":
    # should print "access" as defined in your dlt package
    print(my_dlt_project.config().current_profile)
    # now lets run the pipeline in the packaged project
    my_dlt_project.runner().run_pipeline("my_pipeline")
```

Now you can run the script within the uv virtual environment:

```sh
uv run python test_project.py
```


:::info
In a real-life scenario, your data-scientist will probably not install this package from another directory, but from a pypi repository or a git url. 
:::


## Next steps