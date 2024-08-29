---
sidebar_label: load_package
title: common.storages.load_package
---

## TLoadPackageState Objects

```python
class TLoadPackageState(TVersionedState)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/load_package.py#L53)

### created\_at

Timestamp when the loadpackage was created

### destination\_state

private space for destinations to store state relevant only to the load package

## TLoadPackage Objects

```python
class TLoadPackage(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/load_package.py#L62)

### load\_id

Load id

### state

State of the load package

## ParsedLoadJobFileName Objects

```python
class ParsedLoadJobFileName(NamedTuple)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/load_package.py#L110)

Represents a file name of a job in load package. The file name contains name of a table, number of times the job was retired, extension
and a 5 bytes random string to make job file name unique.
The job id does not contain retry count and is immutable during loading of the data

### job\_id

```python
def job_id() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/load_package.py#L121)

Unique identifier of the job

### file\_name

```python
def file_name() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/load_package.py#L125)

A name of the file with the data to be loaded

### with\_retry

```python
def with_retry() -> "ParsedLoadJobFileName"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/load_package.py#L129)

Returns a job with increased retry count

## PackageStorage Objects

```python
class PackageStorage()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/load_package.py#L259)

### APPLIED\_SCHEMA\_UPDATES\_FILE\_NAME

updates applied to the destination

### \_\_init\_\_

```python
def __init__(storage: FileStorage, initial_state: TLoadPackageStatus) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/load_package.py#L279)

Creates storage that manages load packages with root at `storage` and initial package state `initial_state`

### list\_packages

```python
def list_packages() -> Sequence[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/load_package.py#L297)

Lists all load ids in storage, earliest first

NOTE: Load ids are sorted alphabetically. This class does not store package creation time separately.

### list\_failed\_jobs\_infos

```python
def list_failed_jobs_infos(load_id: str) -> Sequence[LoadJobInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/load_package.py#L329)

List all failed jobs and associated error messages for a load package with `load_id`

### import\_job

```python
def import_job(load_id: str,
               job_file_path: str,
               job_state: TJobState = "new_jobs") -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/load_package.py#L351)

Adds new job by moving the `job_file_path` into `new_jobs` of package `load_id`

### complete\_loading\_package

```python
def complete_loading_package(load_id: str,
                             load_state: TLoadPackageStatus) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/load_package.py#L417)

Completes loading the package by writing marker file with`package_state. Returns path to the completed package

### remove\_completed\_jobs

```python
def remove_completed_jobs(load_id: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/load_package.py#L426)

Deletes completed jobs. If package has failed jobs, nothing gets deleted.

### schema\_name

```python
def schema_name(load_id: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/load_package.py#L447)

Gets schema name associated with the package

### get\_load\_package\_info

```python
def get_load_package_info(load_id: str) -> LoadPackageInfo
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/load_package.py#L497)

Gets information on normalized/completed package with given load_id, all jobs and their statuses.

### is\_package\_partially\_loaded

```python
@staticmethod
def is_package_partially_loaded(package_info: LoadPackageInfo) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/load_package.py#L602)

Checks if package is partially loaded - has jobs that are not new.

## load\_package

```python
def load_package() -> TLoadPackage
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/load_package.py#L648)

Get full load package state present in current context. Across all threads this will be the same in memory dict.

## commit\_load\_package\_state

```python
def commit_load_package_state() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/load_package.py#L660)

Commit load package state present in current context. This is thread safe.

## destination\_state

```python
def destination_state() -> DictStrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/load_package.py#L670)

Get segment of load package state that is specific to the current destination.

## clear\_destination\_state

```python
def clear_destination_state(commit: bool = True) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/storages/load_package.py#L676)

Clear segment of load package state that is specific to the current destination. Optionally commit to load package.

