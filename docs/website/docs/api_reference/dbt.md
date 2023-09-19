---
sidebar_label: dbt
title: dbt
---

#### get\_venv

```python
def get_venv(pipeline: Pipeline,
             venv_path: str = "dbt",
             dbt_version: str = _DEFAULT_DBT_VERSION) -> Venv
```

Creates or restores a virtual environment in which the `dbt` packages are executed.

The recommended way to execute dbt package is to use a separate virtual environment where only the dbt-core
and required destination dependencies are installed. This avoid dependency clashes with the user-installed libraries.
This method will create such environment at the location specified in `venv_path` and automatically install required dependencies
as required by `pipeline`.

**Arguments**:

- `pipeline` _Pipeline_ - A pipeline for which the required dbt dependencies are inferred
- `venv_path` _str, optional_ - A path where virtual environment is created or restored from.
  If relative path is provided, the environment will be created within pipeline's working directory. Defaults to "dbt".
- `dbt_version` _str, optional_ - Version of dbt to be used. Exact version (ie. "1.2.4") or pip requirements string (ie. ">=1.1<1.5" may be provided).
  

**Returns**:

- `Venv` - A Virtual Environment with dbt dependencies installed

#### package

```python
def package(pipeline: Pipeline,
            package_location: str,
            package_repository_branch: str = None,
            package_repository_ssh_key: TSecretValue = TSecretValue(""),
            auto_full_refresh_when_out_of_sync: bool = None,
            venv: Venv = None) -> DBTPackageRunner
```

Creates a Python wrapper over `dbt` package present at specified location, that allows to control it (ie. run and test) from Python code.

The created wrapper minimizes the required effort to run `dbt` packages on datasets created with `dlt`. It clones the package repo and keeps it up to data,
shares the `dlt` destination credentials with `dbt` and allows the isolated execution with `venv` parameter.
The wrapper creates a `dbt` profile from `dlt` pipeline configuration. Specifically:
1. destination is used to infer correct dbt profile
2. destinations credentials are passed to dbt via environment variables
3. dataset_name is used to configure the dbt database schema

**Arguments**:

- `pipeline` _Pipeline_ - A pipeline containing destination, credentials and dataset_name used to configure the dbt package.
- `package_location` _str_ - A git repository url to be cloned or a local path where dbt package is present
- `package_repository_branch` _str, optional_ - A branch name, tag name or commit-id to check out. Defaults to None.
- `package_repository_ssh_key` _TSecretValue, optional_ - SSH key to be used to clone private repositories. Defaults to TSecretValue("").
- `auto_full_refresh_when_out_of_sync` _bool, optional_ - If set to True (default), the wrapper will automatically fall back to full-refresh mode when schema is out of sync
- `See` - https://docs.getdbt.com/docs/build/incremental-models#what-if-the-columns-of-my-incremental-model-change
- `venv` _Venv, optional_ - A virtual environment with required dbt dependencies. Defaults to None which will execute dbt package in current environment.
  

**Returns**:

- `DBTPackageRunner` - A configured and authenticated Python `dbt` wrapper

