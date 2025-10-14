---
title: "Secure data access and sharing"
description: Provide secure data access to your organization
keywords: ["data access", "security", "contracts", "data sharing"]
---

# Secure data access and sharing

dltHub makes it easy for end-users like data scientists or analysts to access high-quality production data in a secure and Python-friendly way. A [dltHub Project](../core-concepts/project.md) exposes a standard Python API which connects to the production data using an "access" [profile](../core-concepts/profiles.md). This profile can be configured to specify how users are allowed to interact with the data, e.g., by applying restrictions on datasets that are not allowed to be modified.

## Project packaging

dltHub Projects can be distributed as Python packages, with which data end-users can easily interact within their own Pythonic workflows.
To package an existing dltHub Project, you need to:

1. Add an `__init__.py` file to the root of your project.

<details>

<summary>__init__.py example</summary>

```py
"""
A demonstration package that sends GitHub events to Delta Lake, aggregates, and shares via Snowflake

>>> import dlt_package_template
>>>
>>> print(dlt_package_template.catalog())  # list datasets
>>> print(dlt_package_template.catalog().dataset_name) # lists tables in dataset
>>> df_ = dlt_package_template.catalog().dataset_name.table_name.df()  # reads table
"""

import os
import dlt as dlt
from dlthub.project import Catalog, EntityFactory, ProjectRunContext, Project, PipelineManager

def access_profile() -> str:
    """Implement this function to select profile assigned to users that import this Python package
    into their own scripts or other modules.
    """
    return "access"


def context() -> ProjectRunContext:
    """Returns the context of this package, including run directory,
    data directory and project config
    """
    from dlthub.project.run_context import ensure_project
    return ensure_project(run_dir=os.path.dirname(__file__), profile=access_profile())


def config() -> Project:
    """Returns project configuration and getters of entities like sources, destinations
    and pipelines"""
    return context().project


def entities() -> EntityFactory:
    """Returns methods to create entities in this package likes sources, pipelines etc."""
    return EntityFactory(config())


def runner() -> PipelineManager:
    return PipelineManager(config())


def catalog() -> Catalog:
    """Returns a catalogue with available datasets, which can be read and written to"""
    return Catalog(context())
```
</details>

2. Package the project using any of the Python package managers (e.g., [uv](https://docs.astral.sh/uv/), [poetry](https://python-poetry.org/), or setuptools).

<details>

<summary>pyproject.toml example</summary>

```toml
[project]
name = "dlt_example_project"
version = "0.0.1"
description = "Description"
requires-python = ">=3.9,<3.13"

dependencies = [
    "dlt>=1.18.0",
    "dlthub"
]

[project.entry-points.dlt_package]
dlt-project = "dlt_example_project"
```
</details>

:::info
cli support for packaging dltHub Projects is currently in development and will be available in future releases.
:::

## Data access and sharing

Once you've created a Python package, you can distribute it via PyPI (private or public) or a git repository. The resulting Python package will allow users to access the data in their Pythonic workflows. An example of such a workflow:

1. Pip install the Python package in your local Python environment (for example, a notebook).

    ```sh
    pip install -U --index-url https://pypi.dlthub.com dlt_example_project
    ```

2. Import the project like any Python module.

    ```py
    import dlt_example_project as dlt_project
    ```

3. Explore the data.

    The datasets declared in the project create a data catalog that can be used to explore which datasets and tables are available and even discover their schema without having to actually load any data into the local machine.

    ```py
    my_catalog = dlt_project.catalog() # Access the data catalog created by dlt
    print(my_catalog) # Inspect datasets and available tables
    print(my_catalog.github_events_dataset) # Access the dataset github_events_dataset from the catalog
    ```

    In this example, `print(my_catalog)` shows two available datasets in the catalog: `github_events_dataset` in an s3 bucket and `reports_dataset` in a Snowflake warehouse.

    ```sh
    Datasets in project dlt_example_project for profile access:
    github_events_dataset@delta_lake[s3://dlt-ci-test-bucket/dlthub_demo/lake_1/]
    reports_dataset@warehouse[snowflake://loader:***@kgiotue-wn98412/dlt_data]
    ```

    And `print(my_catalog.github_events_dataset)` shows the available tables in the dataset `github_events_dataset`.

    ```sh
    Dataset github_events_dataset tables in logical schema events@v2
    pull_request_event
    issues_event
    watch_event
    push_event
    public_event
    pull_request_review_event
    create_event
    issue_comment_event
    delete_event
    fork_event
    release_event
    pull_request_review_comment_event
    gollum_event
    commit_comment_event
    member_event
    gollum_event__payload__pages
    push_event__payload__commits
    ```

4. Access the data.

    Choose the tables from the catalog you want to work with (for example: `issues_event`) and only load those into the local environment:

    ```py
    df = my_catalog.github_events_dataset.issues_event.df()
    ```

    These can be loaded into:
    * Pandas dataframes with `.df()`
    * Arrow tables with `.arrow()`
    * SQL with `.sql()`


5. Do work on the data.

    Once loaded into your environment, you can work on the data just as you would in your regular Python workflow. In the example below, a custom Python function `aggregate_issues()` performs some aggregations on the data.

    ```py
    reports_df = aggregate_issues(df)
    ```

6. Share back the results.

    Using the `.save()` method, you can write back data directly to the destination. For example, the code below writes `reports_df` from Step 5 as a new table `aggregated_issues` in the dataset `reports_dataset`:

    ```py
    print(my_catalog.reports_dataset.save(reports_df, table_name="aggregated_issues"))
    ```

    Running these lines gives the following output:
    ```sh
    Pipeline save_aggregated_issues_pipeline load step completed in 7.85 seconds
    1 load package(s) were loaded to destination warehouse and into dataset reports_dataset
    The warehouse destination used snowflake://loader:***@kgiotue-wn98412/dlt_data location to store data
    Load package 1730314457.4512188 is LOADED and contains no failed jobs
    ```

## Security and contracts

When end-users interact with data using the Python API, they do so through a profile called "access". As a data engineer, you can manage this access by setting configurations and credentials for this profile in `dlt.yml` or in the toml files. Read more about setting secrets and configurations for different profiles [here](../core-concepts/profiles.md).

It's possible to set granular limits on how users can write data through schema and data contracts. These can be set individually per profile per dataset.

```yaml
profiles:
    access:
        datasets:
            github_events_dataset:
                # no new tables, no column changes
                contract: freeze

            reports_dataset:
                # allow new tables but no column changes
                contract:
                    tables: evolve
                    columns: freeze
                    data_type: freeze
```

In this example, users with the profile "access" are restricted from writing any tables or modifying the schema of existing tables in the dataset `github_events_dataset`. So if the end-user from the [previous example](#data-access-and-sharing) tried to write back their tables to this dataset instead of the `reports_dataset`:

```py
print(my_catalog.github_events_dataset.save(reports_df, table_name="aggregated_issues"))
```

then they would get the following error:

```sh
PipelineStepFailed: Pipeline execution failed at stage extract when processing package 1730314603.1941314 with exception:

<class 'dlt.common.schema.exceptions.DataValidationError'>
In schema: events: In Schema: events Table: aggregated_issues. Contract on tables with mode freeze is violated. Trying to add table aggregated_issues but new tables are frozen.
```

There are also contracts set on the `reports_dataset` that allow users to write tables but restrict them from modifying the existing schema. So, if the same user tried to add a new column `id` to the existing table `aggregated_issues` inside the `reports_dataset`:

```py
# Access the aggregated_issues table from the reports_dataset in the catalog
reports_df = my_catalog.reports_dataset.aggregated_issues.df()

# Create a new column "id"
reports_df["id"] = 1

# Push back the modified table
print(my_catalog.reports_dataset.save(reports_df, table_name="aggregated_issues"))
```

then they would get the following error:

```sh
PipelineStepFailed: Pipeline execution failed at stage extract when processing package 1730314610.4309433 with exception:

<class 'dlt.common.schema.exceptions.DataValidationError'>
In schema: out_source: In Schema: out_source Table: aggregated_issues Column: id. Contract on columns with mode freeze is violated. Trying to add column id to table aggregated_issues but columns are frozen.
```

