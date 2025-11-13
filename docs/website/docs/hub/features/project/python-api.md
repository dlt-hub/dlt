---
title: Python API
description:  Python interface of dltHub Project
---

# Python API to interact with dltHub Project

import { DltHubFeatureAdmonition } from '@theme/DltHubFeatureAdmonition';

<DltHubFeatureAdmonition />

You can access any [dltHub Project](../../core-concepts/project.md) entity or function via the Python interface.
The current module provides access to various parts of your active dltHub Project.

Import statement:
```py
import dlt
```

Available methods:
- `dlt.hub.current.project.config()` - Retrieves the project configuration
- `dlt.hub.current.project.entities()` - Returns a factory with all instantiated entities
- `dlt.hub.current.project.catalog()` - Provides access to all defined datasets in the catalog
- `dlt.hub.current.project.runner()` - Allows you to run pipelines programmatically

:::info
If you packaged your dltHub Project into a pip-installable package, you can access all methods above directly from the package. For example:
```py
import my_dlt_package

my_dlt_package.catalog()
```
[Learn more](../../getting-started/advanced_tutorial.md) about how to package your project.
:::


## Accessing project settings

Here are a few examples of what you can access from the project object:
```py
import dlt

# show the currently active profile
# TODO: remove ignore when dlthub plugin releases
print(dlt.hub.current.project.config().current_profile)  # type: ignore
# show the main project dir
print(dlt.hub.current.project.config().project_dir)  # type: ignore
# show the project config dict
print(dlt.hub.current.project.project().config)  # type: ignore
# list explicitly defined datasets (also works with destinations, sources, pipelines, etc.)
print(dlt.hub.current.project.project().datasets)  # type: ignore
```
## Accessing entities

Accessing entities in code works the same way as when referencing them in the `dlt.yml` file.
If allowed, implicit entities will be created and returned automatically. If not, an error will be raised.
```py
import dlt

entities = dlt.hub.current.project.entities()  # type: ignore
pipeline = entities.get_pipeline("my_pipeline")
destination = entities.get_destination("duckdb")
transformation = entities.get_transformation("stressed_transformation")

```
Here, we access the entities manager, which allows you to create sources, destinations, pipelines, and other objects.

## Running pipelines with the runner

dltHub includes a pipeline runner, which is the same one used when you run pipelines from the CLI.
You can also use it directly in your code through the project context:

```py
import dlt

# get the runner
runner = dlt.hub.current.project.runner()  # type: ignore
# run the "my_pipeline" pipeline from the currently active project
runner.run_pipeline("my_pipeline")
```

## Accessing the catalog

The catalog allows you to access all explicitly defined datasets:

```py
import dlt

# Get a dataset instance pointing to the default destination (first in dataset destinations list) and access data inside of it
# Note: The dataset must already exist physically for this to work
dataset = dlt.hub.current.project.catalog().dataset("my_pipeline_dataset")  # type: ignore
# Get the row counts of all tables in the dataset as a dataframe
print(dataset.row_counts().df())
```

:::tip
Learn more about the available data access methods in dlt datasets by reading the [Python loaded data access guide](../../../general-usage/dataset-access/dataset).
It covers how to browse, filter tables, and retrieve data in various formats.
:::

## Writing data back to the catalog

You can also write data to datasets in the dltHub catalog. Each dataset has a `.save()` method that lets you write data back to it.
In the future, you'll be able to control which datasets are writable using contracts.
Under the hood, dltHub runs an ad-hoc pipeline to handle the write operation.

:::warning
Writing data to the catalog is an **experimental feature**.
Use it with caution until it's fully stable.
:::

```py
import pandas as pd
import dlt

# Get a dataset from the catalog (it must already exist and be defined in dlt.yml)
dataset = dlt.hub.current.project.catalog().dataset("my_pipeline_dataset")  # type: ignore
# Write a DataFrame to the "my_table" table in the dataset
dataset.save(pd.DataFrame({"name": ["John", "Jane", "Jim"], "age": [30, 25, 35]}), table_name="my_table")
```

You can also read from an existing table and write the data to a new table, either in the same or another dataset:

```py
import dlt

# Get dataset from the catalog
dataset = dlt.hub.current.project.catalog().dataset("my_pipeline_dataset")  # type: ignore

# This function reads data in chunks from an existing table and yields each chunk
def transform_frames():
    # Read the 'items' table in chunks of 1000 rows
    for df in dataset.table("items").iter_df(chunk_size=1000):
        # You can process the data here if needed
        yield df

# Write the data to a new table called "my_new_table"
dataset.save(transform_frames, table_name="my_new_table")
```

## Switching profiles in code

By default, when you access the project in code, it uses the default or pinned profile.
You can switch to a different profile using the `switch_profile` function.

Here’s an example:

```py
from dlt.hub.current import project  # type: ignore


if __name__ == "__main__":
    # Shows the current active profile
    print(project.config().current_profile)
    # Switch to the tests profile
    project.context().switch_profile("tests")
    # Now "tests" is the active profile, merged with the project config
    print(project.config().current_profile)
```