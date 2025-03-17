---
title: Test utils
description: dlt+ Test utils
keywords: ["dlt+", "data tests", "test"]
---

## Introduction

dlt+ provides a `pytest` plugin with a set of powerful fixtures and utilities that simplify testing for dlt+ projects. These testing utilities are packaged separately in `dlt-plus-tests`, making it easy to install them as a development dependency. Check the [installation guide](#installation) for instructions on how to install the package.

The `dlt-plus-tests` package includes:

- [predefined fixtures and utils](#predefined-fixtures-and-utils)
- [additional `pytest.ini` options](#pytestini-options)
- [`pytest_config` setup](#pytest-config-setup) to ensure tests run in the correct context and switch to the `tests` profile.


## Installation

Currently, `dlt-plus-tests` is available on the dltHub PyPI registry. However, it will soon be moved to `pypi.org`.

```sh
pip install --index-url https://pypi.dlthub.com --no-deps  dlt-plus-tests
```

## Predefined fixtures and utils

The dlt-plus-tests package provides a set of predefined fixtures and utility functions:

* Fixtures are available under `dlt_plus_tests.fixtures`
* Utility functions can be found in `dlt_plus_tests.utils`

### Fixtures

To enable essential fixtures, add the following imports to your `conftest.py`:

```py
from dlt_plus_tests.fixtures import (
    auto_preserve_environ as auto_preserve_environ,
    drop_pipeline as drop_pipeline,
    autouse_test_storage as autouse_test_storage,
)
```

These fixtures must be explicitly imported to be activated. Please find the short description for the fixtures below:

| Fixture Name                     | Description                                                                   | Fixture Settings                 |
| -------------------------------- | ----------------------------------------------------------------------------- | -------------------------------- |
| `auto_preserve_environ`          | Preserves environment variables before the test and restores them afterward.  | `autouse=True`                   |
| `auto_drop_pipeline`             | Drops active pipeline data after test execution unless marked with 'no_load'. | `autouse=True`                   |
| `autouse_test_storage`           | Cleans and provides test storage for the project context.                     | `autouse=True`                   |
| `auto_unload_modules`            | Unloads all modules inspected in these tests.                                 | `autouse=True`                   |
| `auto_preserve_run_context`      | Restores the initial run context when the test completes.                     | `autouse=True`                   |
| `auto_preserve_sources_registry` | Preserves and restores the source registry for tests.                         | `autouse=True, scope="function"` |
| `auto_cwd_to_local_dir`          | Changes the working directory to a temporary directory for test execution.    | `autouse=True`                   |
| `auto_test_access_profile`       | Mocks the access profile by prefixing 'tests-' to the returned profile name.  | `autouse=True`                   |
|                                  |                                                                               |                                  |

### Tests execution

Config setup will activate the run context with `dlt.yml` of the Project being tested. The `tests` profile will be activated (and must be present).

In the test project run context:
- `run_dir` points to the project being tested
- `data_dir` points to `_data/tests/`

:::note
`autouse_test_storage` fixture:
* cleans up `data_dir` (typically `_data/tests`) folder (in relation to project root dir)
* cleans up `local_dir` (typically `_data/tests/local`) folder (in relation to project root dir)
:::

### Utils

Additional cool 😎 utilities for verifying loads, checking table counts, and inspecting metrics can be imported from `utils.py`:

| Name                        | Type     | Description                                                                                                                                                                               |
| --------------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `IsInstanceMockMatch`       | Class    | A helper for mocking comparisons: its `__eq__` method returns `True` if the compared object is an instance of a specified class.                                                          |
| `get_test_project_context`  | Function | Retrieves the current `ProjectRunContext` from `dlt_plus`.                                                                                                                                |
| `get_local_dir`             | Function | Fetches the path to the local directory from the current project's configuration.                                                                                                          |
| `clean_test_storage`        | Function | Removes any existing data directory, recreates it, and sets up a `FileStorage` in the project's temporary directory. Optionally copies configuration files from `tests/.dlt`.             |
| `delete_test_storage`       | Function | Deletes the folder used by the test storage if it exists.                                                                                                                                 |
| `drop_active_pipeline_data` | Function | Drops all datasets for the currently active pipeline, attempts to remove its working folder, and then deactivates the pipeline context.                                                   |
| `assert_load_info`          | Function | Ensures that the specified number of load packages have been loaded successfully, with no failed jobs. Raises an error if any failed jobs are present.                                    |
| `load_table_counts`         | Function | Returns a dictionary of row counts for the given table names by querying the pipeline's SQL client.                                                                                       |
| `load_tables_to_dicts`      | Function | Retrieves the contents of specified tables from the pipeline as lists of dictionaries, optionally excluding system columns (`_dlt*`) and allowing the result to be sorted by a given key. |
| `assert_records_as_set`     | Function | Compares two lists of dictionaries by converting each to a set of key-value pairs, ensuring they match regardless of order.                                                               |


## pytest.ini options

The plugin introduces two additional `pytest.ini` options, which are automatically set and usually do not need modifications:

```toml
[tool.pytest.ini_options]
dlt_tests_project_path="..."
dlt_tests_project_profile="..."
```

## Pytest config setup

Below is an example pyproject.toml configuration for uv:

```toml
[project]
name = "dlt-portable-data-lake-demo"

dependencies = [
    "dlt[duckdb,parquet,deltalake,filesystem,snowflake]>=1.4.1a0",
    "dlt-plus>=0.2.6",
    "enlighten",
    "duckdb<=1.1.2"
]

[tool.uv]
dev-dependencies = [
    "dlt-plus-tests>=0.1.2",
]

[[tool.uv.index]]
name = "dlt-hub"
url = "https://pypi.dlthub.com"
explicit=true

[tool.uv.sources]
dlt-plus = { index = "dlt-hub" }
dlt-plus-tests = { index = "dlt-hub" }
```

## Writing tests

When writing tests, you can use the dlt project API to request project entities and run them. For example:

```py
from dlt_plus.project import Project
from dlt_plus.project.entity_factory import EntityFactory
from dlt_plus.project.pipeline_manager import PipelineManager
from dlt_plus_tests.fixtures import auto_test_access_profile as auto_test_access_profile
from dlt_plus_tests.utils import assert_load_info, load_table_counts

def test_events_to_data_lake(dpt_project_config: Project) -> None:
    """Make sure we dispatch the events to tables properly"""
    factory = EntityFactory(dpt_project_config)
    github_events = factory.get_source("events")
    events_to_lake = factory.get_pipeline("events_to_lake")
    info = events_to_lake.run(github_events())
    assert_load_info(info)

    # Did I load my test data?
    assert load_table_counts(
        events_to_lake, *events_to_lake.default_schema.data_table_names()
    ) == {
        "issues_event": 604,
    }

def test_t_layer(dpt_project_config: Project) -> None:
    """Make sure that our generated dbt package creates expected reports in the data warehouse"""
    pipeline_manager = PipelineManager(dpt_project_config)
    info = pipeline_manager.run_pipeline("events_to_lake")
    assert_load_info(info)
```

Here, we get the current project via the `dpt_project_config` fixture and use `EntityFactory` and `PipelineManager` to
get instances of the entities and run them. The test plugin ensures that each test starts with a clean
state, with the `test` profile active, and that any datasets created by pipelines (also on remote destinations)
are dropped.

