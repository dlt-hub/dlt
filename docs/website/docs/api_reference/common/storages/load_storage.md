---
sidebar_label: load_storage
title: common.storages.load_storage
---

## LoadStorage Objects

```python
class LoadStorage(DataItemStorage, VersionedStorage)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/load_storage.py#L125)

#### NORMALIZED\_FOLDER

folder within the volume where load packages are stored

#### LOADED\_FOLDER

folder to keep the loads that were completely processed

#### SCHEMA\_UPDATES\_FILE\_NAME

updates to the tables in schema created by normalizer

#### APPLIED\_SCHEMA\_UPDATES\_FILE\_NAME

updates applied to the destination

#### SCHEMA\_FILE\_NAME

package schema

#### PACKAGE\_COMPLETED\_FILE\_NAME

completed package marker file, currently only to store data with os.stat

#### list\_failed\_jobs\_in\_completed\_package

```python
def list_failed_jobs_in_completed_package(
        load_id: str) -> Sequence[LoadJobInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/load_storage.py#L246)

List all failed jobs and associated error messages for a completed load package with `load_id`

#### get\_load\_package\_info

```python
def get_load_package_info(load_id: str) -> LoadPackageInfo
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/load_storage.py#L258)

Gets information on normalized/completed package with given load_id, all jobs and their statuses.

#### commit\_schema\_update

```python
def commit_schema_update(load_id: str, applied_update: TSchemaTables) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/load_storage.py#L300)

Marks schema update as processed and stores the update that was applied at the destination

#### add\_new\_job

```python
def add_new_job(load_id: str,
                job_file_path: str,
                job_state: TJobState = "new_jobs") -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/storages/load_storage.py#L310)

Adds new job by moving the `job_file_path` into `new_jobs` of package `load_id`

