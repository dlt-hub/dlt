---
sidebar_label: filesystem
title: destinations.impl.filesystem.filesystem
---

## FilesystemLoadJob Objects

```python
class FilesystemLoadJob(RunnableLoadJob)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/filesystem/filesystem.py#L72)

### make\_remote\_path

```python
def make_remote_path() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/filesystem/filesystem.py#L93)

Returns path on the remote filesystem to which copy the file, without scheme. For local filesystem a native path is used

### make\_remote\_url

```python
def make_remote_url() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/filesystem/filesystem.py#L113)

Returns path on a remote filesystem as a full url including scheme.

## FilesystemClient Objects

```python
class FilesystemClient(FSClientBase, WithSqlClient, JobClientBase,
                       WithStagingDataset, WithStateSync)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/filesystem/filesystem.py#L229)

filesystem client storing jobs in memory

### dataset\_path

```python
@property
def dataset_path() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/filesystem/filesystem.py#L281)

A path within a bucket to tables in a dataset
NOTE: dataset_name changes if with_staging_dataset is active

### get\_storage\_tables

```python
def get_storage_tables(
        table_names: Iterable[str]
) -> Iterable[Tuple[str, TTableSchemaColumns]]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/filesystem/filesystem.py#L318)

Yields tables that have files in storage, does not return column schemas

### truncate\_tables

```python
def truncate_tables(table_names: List[str]) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/filesystem/filesystem.py#L329)

Truncate a set of regular tables with given `table_names`

### get\_table\_dirs

```python
def get_table_dirs(table_names: Iterable[str],
                   remote: bool = False) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/filesystem/filesystem.py#L421)

Gets directories where table data is stored.

### list\_table\_files

```python
def list_table_files(table_name: str) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/filesystem/filesystem.py#L425)

gets list of files associated with one table

### list\_files\_with\_prefixes

```python
def list_files_with_prefixes(table_dir: str, prefixes: List[str]) -> List[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/filesystem/filesystem.py#L432)

returns all files in a directory that match given prefixes

### make\_remote\_url

```python
def make_remote_url(remote_path: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/filesystem/filesystem.py#L477)

Returns uri to the remote filesystem to which copy the file

### get\_stored\_schema

```python
def get_stored_schema(schema_name: str = None) -> Optional[StorageSchemaInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/destinations/impl/filesystem/filesystem.py#L705)

Retrieves newest schema from destination storage

