---
title: Test utils
description: dlt+ Test utils
keywords: ["dlt+", "data tests", "test"]
---

## Introduction

dlt+ provides a `pytest` plugin with many useful fixtures and utilities that make writing tests for dlt+ Projects easy.

## Setup

The pytest plugin includes:
* A set of predefined fixtures (see plugin.py).
* Additional pytest.ini options (detailed below).
* A `pytest_config` setup that ensures the test project runs in the correct context and switches to the `tests` profile.

The plugin introduces two `pytest.ini` options, which are automatically set and usually do not need modifications:
```toml
[tool.pytest.ini_options]
dlt_tests_project_path="..."
dlt_tests_project_profile="..."
```

### Suggested `conftest.py`

To enable essential fixtures, include the following imports in the `conftest.py`:

```py
from dlt_plus_tests.fixtures import (
    auto_preserve_environ as auto_preserve_environ,
    drop_pipeline as drop_pipeline,
    autouse_test_storage as autouse_test_storage,
    auto_cwd_to_tmp_dir as auto_cwd_to_tmp_dir,
)
```

By default, no autouse fixtures are enabled, so explicitly importing them is recommended.

Additional cool 😎 utilities for verifying loads, checking table counts, and inspecting metrics can be imported from `utils.py`.

### Tests execution

Config setup will activate the run context with `dlt.yml` of the Project being tested. The `tests` profile will be activated (and must be present).

In the test project run context:
- `run_dir` points to the package being tested (in the case of `dlt_plus`, the package is part of `tests`, otherwise it is a "real" package)
- `data_dir` points to {$HOME}/.dlt/<package_name>/tests/

:::note
`autouse_test_storage` fixture:
* cleans up `tmp_dir` (typically `_storage`) folder (in relation to `cwd()`)
* copies `tests/.dlt` into the temporary storage directory
:::

## Example: dlt+ Tests setup in Python project

The dlt+ Tests package is a development dependency available from the dltHub PyPI index. Below is an example pyproject.toml configuration for uv:

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

If you want to view or hack the source code:
```sh
pip download --index-url https://pypi.dlthub.com --no-deps  dlt-plus-tests
```
then you need to unpack the downloaded wheel.

