---
sidebar_label: reference
title: common.destination.reference
---

## DestinationClientConfiguration Objects

```python
@configspec
class DestinationClientConfiguration(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L77)

### destination\_type

which destination to load data to

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L85)

Returns a destination fingerprint which is a hash of selected configuration fields. ie. host in case of connection string

### \_\_str\_\_

```python
def __str__() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L89)

Return displayable destination location

## DestinationClientDwhConfiguration Objects

```python
@configspec
class DestinationClientDwhConfiguration(DestinationClientConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L108)

Configuration of a destination that supports datasets/schemas

### dataset\_name

dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix

### default\_schema\_name

name of default schema to be used to name effective dataset to load data to

### replace\_strategy

How to handle replace disposition for this destination, can be classic or staging

### normalize\_dataset\_name

```python
def normalize_dataset_name(schema: Schema) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L118)

Builds full db dataset (schema) name out of configured dataset name and schema name: {dataset_name}_{schema.name}. The resulting name is normalized.

If default schema name is None or equals schema.name, the schema suffix is skipped.

## DestinationClientStagingConfiguration Objects

```python
@configspec
class DestinationClientStagingConfiguration(DestinationClientDwhConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L153)

Configuration of a staging destination, able to store files with desired `layout` at `bucket_url`.

Also supports datasets and can act as standalone destination.

## DestinationClientDwhWithStagingConfiguration Objects

```python
@configspec
class DestinationClientDwhWithStagingConfiguration(
        DestinationClientDwhConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L181)

Configuration of a destination that can take data from staging destination

### staging\_config

configuration of the staging, if present, injected at runtime

## LoadJob Objects

```python
class LoadJob()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L203)

Represents a job that loads a single file

Each job starts in "running" state and ends in one of terminal states: "retry", "failed" or "completed".
Each job is uniquely identified by a file name. The file is guaranteed to exist in "running" state. In terminal state, the file may not be present.
In "running" state, the loader component periodically gets the state via `status()` method. When terminal state is reached, load job is discarded and not called again.
`exception` method is called to get error information in "failed" and "retry" states.

The `__init__` method is responsible to put the Job in "running" state. It may raise `LoadClientTerminalException` and `LoadClientTransientException` to
immediately transition job into "failed" or "retry" state respectively.

### \_\_init\_\_

```python
def __init__(file_name: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L215)

File name is also a job id (or job id is deterministically derived) so it must be globally unique

### state

```python
@abstractmethod
def state() -> TLoadJobState
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L225)

Returns current state. Should poll external resource if necessary.

### file\_name

```python
def file_name() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L229)

A name of the job file

### job\_id

```python
def job_id() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L233)

The job id that is derived from the file name and does not changes during job lifecycle

### exception

```python
@abstractmethod
def exception() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L241)

The exception associated with failed or retry states

## NewLoadJob Objects

```python
class NewLoadJob(LoadJob)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L246)

Adds a trait that allows to save new job file

### new\_file\_path

```python
@abstractmethod
def new_file_path() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L250)

Path to a newly created temporary job file. If empty, no followup job should be created

## FollowupJob Objects

```python
class FollowupJob()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L255)

Adds a trait that allows to create a followup job

### create\_followup\_jobs

```python
def create_followup_jobs(final_state: TLoadJobState) -> List[NewLoadJob]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L258)

Return list of new jobs. `final_state` is state to which this job transits

## DoNothingJob Objects

```python
class DoNothingJob(LoadJob)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L263)

The most lazy class of dlt

## DoNothingFollowupJob Objects

```python
class DoNothingFollowupJob(DoNothingJob, FollowupJob)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L278)

The second most lazy class of dlt

## JobClientBase Objects

```python
class JobClientBase(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L284)

### initialize\_storage

```python
@abstractmethod
def initialize_storage(truncate_tables: Iterable[str] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L292)

Prepares storage to be used ie. creates database schema or file system folder. Truncates requested tables.

### is\_storage\_initialized

```python
@abstractmethod
def is_storage_initialized() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L297)

Returns if storage is ready to be read/written.

### drop\_storage

```python
@abstractmethod
def drop_storage() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L302)

Brings storage back into not initialized state. Typically data in storage is destroyed.

### update\_stored\_schema

```python
def update_stored_schema(
        only_tables: Iterable[str] = None,
        expected_update: TSchemaTables = None) -> Optional[TSchemaTables]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L306)

Updates storage to the current schema.

Implementations should not assume that `expected_update` is the exact difference between destination state and the self.schema. This is only the case if
destination has single writer and no other processes modify the schema.

**Arguments**:

- `only_tables` _Sequence[str], optional_ - Updates only listed tables. Defaults to None.
- `expected_update` _TSchemaTables, optional_ - Update that is expected to be applied to the destination

**Returns**:

- `Optional[TSchemaTables]` - Returns an update that was applied at the destination.

### start\_file\_load

```python
@abstractmethod
def start_file_load(table: TTableSchema, file_path: str,
                    load_id: str) -> LoadJob
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L324)

Creates and starts a load job for a particular `table` with content in `file_path`

### restore\_file\_load

```python
@abstractmethod
def restore_file_load(file_path: str) -> LoadJob
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L329)

Finds and restores already started loading job identified by `file_path` if destination supports it.

### create\_table\_chain\_completed\_followup\_jobs

```python
def create_table_chain_completed_followup_jobs(
        table_chain: Sequence[TTableSchema]) -> List[NewLoadJob]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L336)

Creates a list of followup jobs that should be executed after a table chain is completed

### complete\_load

```python
@abstractmethod
def complete_load(load_id: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L343)

Marks the load package with `load_id` as completed in the destination. Before such commit is done, the data with `load_id` is invalid.

## WithStateSync Objects

```python
class WithStateSync(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L448)

### get\_stored\_schema

```python
@abstractmethod
def get_stored_schema() -> Optional[StorageSchemaInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L450)

Retrieves newest schema from destination storage

### get\_stored\_state

```python
@abstractmethod
def get_stored_state(pipeline_name: str) -> Optional[StateInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L459)

Loads compressed state from destination storage

## WithStagingDataset Objects

```python
class WithStagingDataset(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L464)

Adds capability to use staging dataset and request it from the loader

### with\_staging\_dataset

```python
@abstractmethod
def with_staging_dataset() -> ContextManager["JobClientBase"]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L472)

Executes job client methods on staging dataset

## SupportsStagingDestination Objects

```python
class SupportsStagingDestination()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L477)

Adds capability to support a staging destination for the load

## Destination Objects

```python
class Destination(ABC, Generic[TDestinationConfig, TDestinationClient])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L493)

A destination factory that can be partially pre-configured
with credentials and other config params.

### spec

```python
@property
@abstractmethod
def spec() -> Type[TDestinationConfig]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L512)

A spec of destination configuration that also contains destination credentials

### capabilities

```python
@abstractmethod
def capabilities() -> DestinationCapabilitiesContext
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L517)

Destination capabilities ie. supported loader file formats, identifier name lengths, naming conventions, escape function etc.

### destination\_name

```python
@property
def destination_name() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L522)

The destination name will either be explicitly set while creating the destination or will be taken from the type

### client\_class

```python
@property
@abstractmethod
def client_class() -> Type[TDestinationClient]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L537)

A job client class responsible for starting and resuming load jobs

### configuration

```python
def configuration(initial_config: TDestinationConfig) -> TDestinationConfig
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L541)

Get a fully resolved destination config from the initial config

### normalize\_type

```python
@staticmethod
def normalize_type(destination_type: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L562)

Normalizes destination type string into a canonical form. Assumes that type names without dots correspond to build in destinations.

### from\_reference

```python
@staticmethod
def from_reference(
    ref: TDestinationReferenceArg,
    credentials: Optional[CredentialsConfiguration] = None,
    destination_name: Optional[str] = None,
    environment: Optional[str] = None,
    **kwargs: Any
) -> Optional["Destination[DestinationClientConfiguration, JobClientBase]"]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L574)

Instantiate destination from str reference.
The ref can be a destination name or import path pointing to a destination class (e.g. `dlt.destinations.postgres`)

### client

```python
def client(
        schema: Schema,
        initial_config: TDestinationConfig = config.value
) -> TDestinationClient
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/3739c9ac839aafef713f6d5ebbc6a81b2a39a1b0/dlt/common/destination/reference.py#L626)

Returns a configured instance of the destination's job client

