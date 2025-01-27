---
sidebar_label: runner
title: helpers.dbt.runner
---

## DBTPackageRunner Objects

```python
class DBTPackageRunner()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/helpers/dbt/runner.py#L31)

A Python wrapper over a dbt package

The created wrapper minimizes the required effort to run `dbt` packages on datasets created with `dlt`. It clones the package repo and keeps it up to data,
shares the `dlt` destination credentials with `dbt` and allows the isolated execution with `venv` parameter.
The wrapper creates a `dbt` profile from a passed `dlt` credentials and executes the transformations in `source_dataset_name` schema. Additional configuration is
passed via DBTRunnerConfiguration instance

### ensure\_newest\_package

```python
def ensure_newest_package() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/helpers/dbt/runner.py#L98)

Clones or brings the dbt package at `package_location` up to date.

### run

```python
def run(cmd_params: Sequence[str] = ("--fail-fast", ),
        additional_vars: StrAny = None,
        destination_dataset_name: str = None) -> Sequence[DBTNodeResult]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/helpers/dbt/runner.py#L168)

Runs `dbt` package

Executes `dbt run` on previously cloned package.

**Arguments**:

- `run_params` _Sequence[str], optional_ - Additional parameters to `run` command ie. `full-refresh`. Defaults to ("--fail-fast", ).
- `additional_vars` _StrAny, optional_ - Additional jinja variables to be passed to the package. Defaults to None.
- `destination_dataset_name` _str, optional_ - Overwrites the dbt schema where transformed models will be created. Useful for testing or creating several copies of transformed data . Defaults to None.
  

**Returns**:

- `Sequence[DBTNodeResult]` - A list of processed model with names, statuses, execution messages and execution times
  
  Exceptions:
- `DBTProcessingError` - `run` command failed. Contains a list of models with their execution statuses and error messages

### test

```python
def test(cmd_params: Sequence[str] = None,
         additional_vars: StrAny = None,
         destination_dataset_name: str = None) -> Sequence[DBTNodeResult]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/helpers/dbt/runner.py#L193)

Tests `dbt` package

Executes `dbt test` on previously cloned package.

**Arguments**:

- `run_params` _Sequence[str], optional_ - Additional parameters to `test` command ie. test selectors`.
- `additional_vars` _StrAny, optional_ - Additional jinja variables to be passed to the package. Defaults to None.
- `destination_dataset_name` _str, optional_ - Overwrites the dbt schema where transformed models will be created. Useful for testing or creating several copies of transformed data . Defaults to None.
  

**Returns**:

- `Sequence[DBTNodeResult]` - A list of executed tests with names, statuses, execution messages and execution times
  
  Exceptions:
- `DBTProcessingError` - `test` command failed. Contains a list of models with their execution statuses and error messages

### run\_all

```python
def run_all(run_params: Sequence[str] = ("--fail-fast", ),
            additional_vars: StrAny = None,
            source_tests_selector: str = None,
            destination_dataset_name: str = None) -> Sequence[DBTNodeResult]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/helpers/dbt/runner.py#L251)

Prepares and runs a dbt package.

This method executes typical `dbt` workflow with following steps:
1. First it clones the package or brings it up to date with the origin. If package location is a local path, it stays intact
2. It installs the dependencies (`dbt deps`)
3. It runs seed (`dbt seed`)
4. It runs optional tests on the sources
5. It runs the package (`dbt run`)
6. If the `dbt` fails with "incremental model out of sync", it will retry with full-refresh on (only when `auto_full_refresh_when_out_of_sync` is set).
See https://docs.getdbt.com/docs/build/incremental-models#what-if-the-columns-of-my-incremental-model-change

**Arguments**:

- `run_params` _Sequence[str], optional_ - Additional parameters to `run` command ie. `full-refresh`. Defaults to ("--fail-fast", ).
- `additional_vars` _StrAny, optional_ - Additional jinja variables to be passed to the package. Defaults to None.
- `source_tests_selector` _str, optional_ - A source tests selector ie. will execute all tests from `sources` model. Defaults to None.
- `destination_dataset_name` _str, optional_ - Overwrites the dbt schema where transformed models will be created. Useful for testing or creating several copies of transformed data . Defaults to None.
  

**Returns**:

- `Sequence[DBTNodeResult]` - A list of processed model with names, statuses, execution messages and execution times
  
  Exceptions:
- `DBTProcessingError` - any of the dbt commands failed. Contains a list of models with their execution statuses and error messages
- `PrerequisitesException` - the source tests failed
- `IncrementalSchemaOutOfSyncError` - `run` failed due to schema being out of sync. the DBTProcessingError with failed model is in `args[0]`

## create\_runner

```python
@with_telemetry("helper", "dbt_create_runner", False, "package_profile_name")
@with_config(spec=DBTRunnerConfiguration,
             sections=(known_sections.DBT_PACKAGE_RUNNER, ))
def create_runner(
        venv: Venv,
        credentials: DestinationClientDwhConfiguration,
        working_dir: str,
        package_location: str = dlt.config.value,
        package_repository_branch: Optional[str] = None,
        package_repository_ssh_key: Optional[TSecretValue] = TSecretValue(""),
        package_profiles_dir: Optional[str] = None,
        package_profile_name: Optional[str] = None,
        auto_full_refresh_when_out_of_sync: bool = True,
        config: DBTRunnerConfiguration = None) -> DBTPackageRunner
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/helpers/dbt/runner.py#L303)

Creates a Python wrapper over `dbt` package present at specified location, that allows to control it (ie. run and test) from Python code.

The created wrapper minimizes the required effort to run `dbt` packages. It clones the package repo and keeps it up to data,
optionally shares the `dlt` destination credentials with `dbt` and allows the isolated execution with `venv` parameter.

Note that you can pass config and secrets in DBTRunnerConfiguration as configuration in section "dbt_package_runner"

**Arguments**:

- `venv` _Venv_ - A virtual environment with required dbt dependencies. Pass None to use current environment.
- `credentials` _DestinationClientDwhConfiguration_ - Any configuration deriving from DestinationClientDwhConfiguration ie. ConnectionStringCredentials
- `working_dir` _str_ - A working dir to which the package will be cloned
- `package_location` _str_ - A git repository url to be cloned or a local path where dbt package is present
- `package_repository_branch` _str, optional_ - A branch name, tag name or commit-id to check out. Defaults to None.
- `package_repository_ssh_key` _TSecretValue, optional_ - SSH key to be used to clone private repositories. Defaults to TSecretValue("").
- `package_profiles_dir` _str, optional_ - Path to the folder where "profiles.yml" resides
- `package_profile_name` _str, optional_ - Name of the profile in "profiles.yml"
- `auto_full_refresh_when_out_of_sync` _bool, optional_ - If set to True (default), the wrapper will automatically fall back to full-refresh mode when schema is out of sync
- `See` - https://docs.getdbt.com/docs/build/incremental-models#what-if-the-columns-of-my-incremental-model-change_description_. Defaults to None.
- `config` _DBTRunnerConfiguration, optional_ - Explicit additional configuration for the runner.
  

**Returns**:

- `DBTPackageRunner` - A Python `dbt` wrapper

