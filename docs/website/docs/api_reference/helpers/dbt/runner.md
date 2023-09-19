---
sidebar_label: runner
title: helpers.dbt.runner
---

## DBTPackageRunner Objects

```python
class DBTPackageRunner()
```

A Python wrapper over a dbt package

The created wrapper minimizes the required effort to run `dbt` packages on datasets created with `dlt`. It clones the package repo and keeps it up to data,
shares the `dlt` destination credentials with `dbt` and allows the isolated execution with `venv` parameter.
The wrapper creates a `dbt` profile from a passed `dlt` credentials and executes the transformations in `source_dataset_name` schema. Additional configuration is
passed via DBTRunnerConfiguration instance

#### ensure\_newest\_package

```python
def ensure_newest_package() -> None
```

Clones or brings the dbt package at `package_location` up to date.

#### run

```python
def run(cmd_params: Sequence[str] = ("--fail-fast", ),
        additional_vars: StrAny = None,
        destination_dataset_name: str = None) -> Sequence[DBTNodeResult]
```

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

#### test

```python
def test(cmd_params: Sequence[str] = None,
         additional_vars: StrAny = None,
         destination_dataset_name: str = None) -> Sequence[DBTNodeResult]
```

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

#### run\_all

```python
def run_all(run_params: Sequence[str] = ("--fail-fast", ),
            additional_vars: StrAny = None,
            source_tests_selector: str = None,
            destination_dataset_name: str = None) -> Sequence[DBTNodeResult]
```

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

