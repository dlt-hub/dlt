---
sidebar_label: load_storage
title: common.storages.load_storage
---

## LoadItemStorage Objects

```python
class LoadItemStorage(DataItemStorage)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/storages/load_storage.py#L28)

### \_\_init\_\_

```python
def __init__(package_storage: PackageStorage,
             writer_spec: FileWriterSpec) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/storages/load_storage.py#L29)

Data item storage using `storage` to manage load packages

## LoadStorage Objects

```python
class LoadStorage(VersionedStorage)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/storages/load_storage.py#L43)

### NORMALIZED\_FOLDER

folder within the volume where load packages are stored

### LOADED\_FOLDER

folder to keep the loads that were completely processed

### NEW\_PACKAGES\_FOLDER

folder where new packages are created

### list\_new\_jobs

```python
def list_new_jobs(load_id: str) -> Sequence[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/storages/load_storage.py#L96)

Lists all jobs in new jobs folder of normalized package storage and checks if file formats are supported

### list\_normalized\_packages

```python
def list_normalized_packages() -> Sequence[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/storages/load_storage.py#L117)

Lists all packages that are normalized and will be loaded or are currently loaded

### list\_loaded\_packages

```python
def list_loaded_packages() -> Sequence[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/storages/load_storage.py#L121)

List packages that are completely loaded

### list\_failed\_jobs\_in\_loaded\_package

```python
def list_failed_jobs_in_loaded_package(load_id: str) -> Sequence[LoadJobInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/storages/load_storage.py#L125)

List all failed jobs and associated error messages for a completed load package with `load_id`

### commit\_schema\_update

```python
def commit_schema_update(load_id: str, applied_update: TSchemaTables) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/storages/load_storage.py#L140)

Marks schema update as processed and stores the update that was applied at the destination

### import\_new\_job

```python
def import_new_job(load_id: str,
                   job_file_path: str,
                   job_state: TPackageJobState = "new_jobs") -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/storages/load_storage.py#L152)

Adds new job by moving the `job_file_path` into `new_jobs` of package `load_id`

### maybe\_remove\_completed\_jobs

```python
def maybe_remove_completed_jobs(load_id: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/storages/load_storage.py#L176)

Deletes completed jobs if delete_completed_jobs config flag is set. If package has failed jobs, nothing gets deleted.

### get\_load\_package\_info

```python
def get_load_package_info(load_id: str) -> LoadPackageInfo
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/storages/load_storage.py#L198)

Gets information on normalized OR loaded package with given load_id, all jobs and their statuses.

### get\_load\_package\_state

```python
def get_load_package_state(load_id: str) -> TLoadPackageState
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/storages/load_storage.py#L205)

Gets state of normlized or loaded package with given load_id, all jobs and their statuses.

