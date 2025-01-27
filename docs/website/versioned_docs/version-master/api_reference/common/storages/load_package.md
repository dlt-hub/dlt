---
sidebar_label: load_package
title: common.storages.load_package
---

## TJobFileFormat

Loader file formats with internal job types

## TPipelineStateDoc Objects

```python
class TPipelineStateDoc(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L63)

Corresponds to the StateInfo Tuple

## TLoadPackageDropTablesState Objects

```python
class TLoadPackageDropTablesState(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L75)

### dropped\_tables

List of tables that are to be dropped from the schema and destination (i.e. when `refresh` mode is used)

### truncated\_tables

List of tables that are to be truncated in the destination (i.e. when `refresh='drop_data'` mode is used)

## TLoadPackageState Objects

```python
class TLoadPackageState(TVersionedState, TLoadPackageDropTablesState)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L82)

### created\_at

Timestamp when the load package was created

### pipeline\_state

Pipeline state, added at the end of the extraction phase

### destination\_state

private space for destinations to store state relevant only to the load package

## TLoadPackage Objects

```python
class TLoadPackage(TypedDict)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L93)

### load\_id

Load id

### state

State of the load package

## create\_load\_id

```python
def create_load_id() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L135)

Creates new package load id which is the current unix timestamp converted to string.
Load ids must have the following properties:
- They must maintain increase order over time for a particular dlt schema loaded to particular destination and dataset
`dlt` executes packages in order of load ids
`dlt` considers a state with the highest load id to be the most up to date when restoring state from destination

## ParsedLoadJobFileName Objects

```python
class ParsedLoadJobFileName(NamedTuple)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L151)

Represents a file name of a job in load package. The file name contains name of a table, number of times the job was retried, extension
and a 5 bytes random string to make job file name unique.
The job id does not contain retry count and is immutable during loading of the data

### job\_id

```python
def job_id() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L162)

Unique identifier of the job

### file\_name

```python
def file_name() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L166)

A name of the file with the data to be loaded

### with\_retry

```python
def with_retry() -> "ParsedLoadJobFileName"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L170)

Returns a job with increased retry count

## PackageStorage Objects

```python
class PackageStorage()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L301)

### APPLIED\_SCHEMA\_UPDATES\_FILE\_NAME

updates applied to the destination

### \_\_init\_\_

```python
def __init__(storage: FileStorage, initial_state: TLoadPackageStatus) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L321)

Creates storage that manages load packages with root at `storage` and initial package state `initial_state`

### get\_package\_path

```python
def get_package_path(load_id: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L330)

Gets path of the package relative to storage root

### get\_job\_state\_folder\_path

```python
def get_job_state_folder_path(load_id: str, state: TPackageJobState) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L334)

Gets path to the jobs in `state` in package `load_id`, relative to the storage root

### get\_job\_file\_path

```python
def get_job_file_path(load_id: str, state: TPackageJobState,
                      file_name: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L338)

Get path to job with `file_name` in `state` in package `load_id`, relative to the storage root

### list\_packages

```python
def list_packages() -> Sequence[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L342)

Lists all load ids in storage, earliest first

NOTE: Load ids are sorted alphabetically. This class does not store package creation time separately.

### list\_failed\_jobs\_infos

```python
def list_failed_jobs_infos(load_id: str) -> Sequence[LoadJobInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L385)

List all failed jobs and associated error messages for a load package with `load_id`

### import\_job

```python
def import_job(load_id: str,
               job_file_path: str,
               job_state: TPackageJobState = "new_jobs") -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L416)

Adds new job by moving the `job_file_path` into `new_jobs` of package `load_id`

### complete\_loading\_package

```python
def complete_loading_package(load_id: str,
                             load_state: TLoadPackageStatus) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L489)

Completes loading the package by writing marker file with`package_state. Returns path to the completed package

### remove\_completed\_jobs

```python
def remove_completed_jobs(load_id: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L501)

Deletes completed jobs. If package has failed jobs, nothing gets deleted.

### schema\_name

```python
def schema_name(load_id: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L522)

Gets schema name associated with the package

### get\_load\_package\_jobs

```python
def get_load_package_jobs(
        load_id: str) -> Dict[TPackageJobState, List[ParsedLoadJobFileName]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L572)

Gets all jobs in a package and returns them as lists assigned to a particular state.

### get\_load\_package\_info

```python
def get_load_package_info(load_id: str) -> LoadPackageInfo
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L592)

Gets information on normalized/completed package with given load_id, all jobs and their statuses.

Will reach to the file system to get additional stats, mtime, also collects exceptions for failed jobs.
NOTE: do not call this function often. it should be used only to generate metrics

### get\_job\_failed\_message

```python
def get_job_failed_message(load_id: str, job: ParsedLoadJobFileName) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L638)

Get exception message of a failed job.

### job\_to\_job\_info

```python
def job_to_job_info(load_id: str, state: TPackageJobState,
                    job: ParsedLoadJobFileName) -> LoadJobInfo
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L648)

Creates partial job info by converting job object. size, mtime and failed message will not be populated

### is\_package\_partially\_loaded

```python
@staticmethod
def is_package_partially_loaded(package_info: LoadPackageInfo) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L732)

Checks if package is partially loaded - has jobs that are completed and jobs that are not.

## load\_package

```python
def load_package() -> TLoadPackage
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L767)

Get full load package state present in current context. Across all threads this will be the same in memory dict.

## commit\_load\_package\_state

```python
def commit_load_package_state() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L779)

Commit load package state present in current context. This is thread safe.

## destination\_state

```python
def destination_state() -> DictStrAny
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L789)

Get segment of load package state that is specific to the current destination.

## clear\_destination\_state

```python
def clear_destination_state(commit: bool = True) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/common/storages/load_package.py#L795)

Clear segment of load package state that is specific to the current destination. Optionally commit to load package.

