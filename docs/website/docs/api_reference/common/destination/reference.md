---
sidebar_label: reference
title: common.destination.reference
---

## DestinationClientConfiguration Objects

```python
@configspec
class DestinationClientConfiguration(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L43)

#### destination\_name

which destination to load data to

#### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L47)

Returns a destination fingerprint which is a hash of selected configuration fields. ie. host in case of connection string

#### \_\_str\_\_

```python
def __str__() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L51)

Return displayable destination location

## DestinationClientDwhConfiguration Objects

```python
@configspec
class DestinationClientDwhConfiguration(DestinationClientConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L62)

Configuration of a destination that supports datasets/schemas

#### dataset\_name

dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix

#### default\_schema\_name

name of default schema to be used to name effective dataset to load data to

#### replace\_strategy

How to handle replace disposition for this destination, can be classic or staging

#### normalize\_dataset\_name

```python
def normalize_dataset_name(schema: Schema) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L72)

Builds full db dataset (schema) name out of configured dataset name and schema name: {dataset_name}_{schema.name}. The resulting name is normalized.

If default schema name is None or equals schema.name, the schema suffix is skipped.

## DestinationClientStagingConfiguration Objects

```python
@configspec
class DestinationClientStagingConfiguration(DestinationClientDwhConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L98)

Configuration of a staging destination, able to store files with desired `layout` at `bucket_url`.

Also supports datasets and can act as standalone destination.

## DestinationClientDwhWithStagingConfiguration Objects

```python
@configspec
class DestinationClientDwhWithStagingConfiguration(
        DestinationClientDwhConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L122)

Configuration of a destination that can take data from staging destination

#### staging\_config

configuration of the staging, if present, injected at runtime

## LoadJob Objects

```python
class LoadJob()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L141)

Represents a job that loads a single file

Each job starts in "running" state and ends in one of terminal states: "retry", "failed" or "completed".
Each job is uniquely identified by a file name. The file is guaranteed to exist in "running" state. In terminal state, the file may not be present.
In "running" state, the loader component periodically gets the state via `status()` method. When terminal state is reached, load job is discarded and not called again.
`exception` method is called to get error information in "failed" and "retry" states.

The `__init__` method is responsible to put the Job in "running" state. It may raise `LoadClientTerminalException` and `LoadClientTransientException` to
immediately transition job into "failed" or "retry" state respectively.

#### \_\_init\_\_

```python
def __init__(file_name: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L152)

File name is also a job id (or job id is deterministically derived) so it must be globally unique

#### state

```python
@abstractmethod
def state() -> TLoadJobState
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L162)

Returns current state. Should poll external resource if necessary.

#### file\_name

```python
def file_name() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L166)

A name of the job file

#### job\_id

```python
def job_id() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L170)

The job id that is derived from the file name

#### exception

```python
@abstractmethod
def exception() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L178)

The exception associated with failed or retry states

## NewLoadJob Objects

```python
class NewLoadJob(LoadJob)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L183)

Adds a trait that allows to save new job file

#### new\_file\_path

```python
@abstractmethod
def new_file_path() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L187)

Path to a newly created temporary job file. If empty, no followup job should be created

## FollowupJob Objects

```python
class FollowupJob()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L192)

Adds a trait that allows to create a followup job

## JobClientBase Objects

```python
class JobClientBase(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L198)

#### initialize\_storage

```python
@abstractmethod
def initialize_storage(truncate_tables: Iterable[str] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L207)

Prepares storage to be used ie. creates database schema or file system folder. Truncates requested tables.

#### is\_storage\_initialized

```python
@abstractmethod
def is_storage_initialized() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L213)

Returns if storage is ready to be read/written.

#### drop\_storage

```python
@abstractmethod
def drop_storage() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L218)

Brings storage back into not initialized state. Typically data in storage is destroyed.

#### update\_stored\_schema

```python
def update_stored_schema(
        only_tables: Iterable[str] = None,
        expected_update: TSchemaTables = None) -> Optional[TSchemaTables]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L222)

Updates storage to the current schema.

Implementations should not assume that `expected_update` is the exact difference between destination state and the self.schema. This is only the case if
destination has single writer and no other processes modify the schema.

**Arguments**:

- `only_tables` _Sequence[str], optional_ - Updates only listed tables. Defaults to None.
- `expected_update` _TSchemaTables, optional_ - Update that is expected to be applied to the destination

**Returns**:

- `Optional[TSchemaTables]` - Returns an update that was applied at the destination.

#### start\_file\_load

```python
@abstractmethod
def start_file_load(table: TTableSchema, file_path: str,
                    load_id: str) -> LoadJob
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L238)

Creates and starts a load job for a particular `table` with content in `file_path`

#### restore\_file\_load

```python
@abstractmethod
def restore_file_load(file_path: str) -> LoadJob
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L243)

Finds and restores already started loading job identified by `file_path` if destination supports it.

#### create\_table\_chain\_completed\_followup\_jobs

```python
def create_table_chain_completed_followup_jobs(
        table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L251)

Creates a list of followup jobs that should be executed after a table chain is completed

#### complete\_load

```python
@abstractmethod
def complete_load(load_id: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L256)

Marks the load package with `load_id` as completed in the destination. Before such commit is done, the data with `load_id` is invalid.

## WithStateSync Objects

```python
class WithStateSync(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L291)

#### get\_stored\_schema

```python
@abstractmethod
def get_stored_schema() -> Optional[StorageSchemaInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L294)

Retrieves newest schema from destination storage

#### get\_stored\_state

```python
@abstractmethod
def get_stored_state(pipeline_name: str) -> Optional[StateInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L303)

Loads compressed state from destination storage

## WithStagingDataset Objects

```python
class WithStagingDataset(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L308)

Adds capability to use staging dataset and request it from the loader

#### get\_stage\_dispositions

```python
@abstractmethod
def get_stage_dispositions() -> List[TWriteDisposition]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L312)

Returns a list of write dispositions that require staging dataset

#### with\_staging\_dataset

```python
@abstractmethod
def with_staging_dataset() -> ContextManager["JobClientBase"]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L317)

Executes job client methods on staging dataset

## DestinationReference Objects

```python
class DestinationReference(Protocol)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L325)

#### capabilities

```python
def capabilities() -> DestinationCapabilitiesContext
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L329)

Destination capabilities ie. supported loader file formats, identifier name lengths, naming conventions, escape function etc.

#### client

```python
def client(
    schema: Schema,
    initial_config: DestinationClientConfiguration = config.value
) -> "JobClientBase"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L332)

A job client responsible for starting and resuming load jobs

#### spec

```python
def spec() -> Type[DestinationClientConfiguration]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/30d0f64fb2cdbacc2e88fdb304371650f417e1f0/dlt/common/destination/reference.py#L335)

A spec of destination configuration that also contains destination credentials

