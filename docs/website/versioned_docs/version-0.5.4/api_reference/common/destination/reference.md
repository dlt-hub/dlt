---
sidebar_label: reference
title: common.destination.reference
---

## StorageSchemaInfo Objects

```python
class StorageSchemaInfo(NamedTuple)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L63)

### from\_normalized\_mapping

```python
@classmethod
def from_normalized_mapping(
        cls, normalized_doc: Dict[str, Any],
        naming_convention: NamingConvention) -> "StorageSchemaInfo"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L72)

Instantiate this class from mapping where keys are normalized according to given naming convention

**Arguments**:

- `normalized_doc` - Mapping with normalized keys (e.g. {Version: ..., SchemaName: ...})
- `naming_convention` - Naming convention that was used to normalize keys
  

**Returns**:

- `StorageSchemaInfo` - Instance of this class

## StateInfo Objects

```python
@dataclasses.dataclass
class StateInfo()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L95)

### from\_normalized\_mapping

```python
@classmethod
def from_normalized_mapping(
        cls, normalized_doc: Dict[str, Any],
        naming_convention: NamingConvention) -> "StateInfo"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L113)

Instantiate this class from mapping where keys are normalized according to given naming convention

**Arguments**:

- `normalized_doc` - Mapping with normalized keys (e.g. {Version: ..., PipelineName: ...})
- `naming_convention` - Naming convention that was used to normalize keys
  

**Returns**:

- `StateInfo` - Instance of this class

## DestinationClientConfiguration Objects

```python
@configspec
class DestinationClientConfiguration(BaseConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L137)

### destination\_type

which destination to load data to

### fingerprint

```python
def fingerprint() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L147)

Returns a destination fingerprint which is a hash of selected configuration fields. ie. host in case of connection string

### \_\_str\_\_

```python
def __str__() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L151)

Return displayable destination location

### credentials\_type

```python
@classmethod
def credentials_type(
    cls,
    config: "DestinationClientConfiguration" = None
) -> Type[CredentialsConfiguration]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L159)

Figure out credentials type, using hint resolvers for dynamic types

For correct type resolution of filesystem, config should have bucket_url populated

## DestinationClientDwhConfiguration Objects

```python
@configspec
class DestinationClientDwhConfiguration(DestinationClientConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L179)

Configuration of a destination that supports datasets/schemas

### dataset\_name

dataset name in the destination to load data to, for schemas that are not default schema, it is used as dataset prefix

### default\_schema\_name

name of default schema to be used to name effective dataset to load data to

### replace\_strategy

How to handle replace disposition for this destination, can be classic or staging

### staging\_dataset\_name\_layout

Layout for staging dataset, where %s is replaced with dataset name. placeholder is optional

### enable\_dataset\_name\_normalization

Whether to normalize the dataset name. Affects staging dataset as well.

### normalize\_dataset\_name

```python
def normalize_dataset_name(schema: Schema) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L208)

Builds full db dataset (schema) name out of configured dataset name and schema name: {dataset_name}_{schema.name}. The resulting name is normalized.

If default schema name is None or equals schema.name, the schema suffix is skipped.

### normalize\_staging\_dataset\_name

```python
def normalize_staging_dataset_name(schema: Schema) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L223)

Builds staging dataset name out of dataset_name and staging_dataset_name_layout.

## DestinationClientStagingConfiguration Objects

```python
@configspec
class DestinationClientStagingConfiguration(DestinationClientDwhConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L254)

Configuration of a staging destination, able to store files with desired `layout` at `bucket_url`.

Also supports datasets and can act as standalone destination.

## DestinationClientDwhWithStagingConfiguration Objects

```python
@configspec
class DestinationClientDwhWithStagingConfiguration(
        DestinationClientDwhConfiguration)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L267)

Configuration of a destination that can take data from staging destination

### staging\_config

configuration of the staging, if present, injected at runtime

### truncate\_tables\_on\_staging\_destination\_before\_load

If dlt should truncate the tables on staging destination before loading data.

## LoadJob Objects

```python
class LoadJob(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L279)

A stateful load job, represents one job file

### job\_id

```python
def job_id() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L293)

The job id that is derived from the file name and does not changes during job lifecycle

### file\_name

```python
def file_name() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L297)

A name of the job file

### state

```python
@abstractmethod
def state() -> TLoadJobState
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L305)

Returns current state. Should poll external resource if necessary.

### exception

```python
@abstractmethod
def exception() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L310)

The exception associated with failed or retry states

### metrics

```python
def metrics() -> Optional[LoadJobMetrics]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L314)

Returns job execution metrics

## RunnableLoadJob Objects

```python
class RunnableLoadJob(LoadJob, ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L327)

Represents a runnable job that loads a single file

Each job starts in "running" state and ends in one of terminal states: "retry", "failed" or "completed".
Each job is uniquely identified by a file name. The file is guaranteed to exist in "running" state. In terminal state, the file may not be present.
In "running" state, the loader component periodically gets the state via `status()` method. When terminal state is reached, load job is discarded and not called again.
`exception` method is called to get error information in "failed" and "retry" states.

The `__init__` method is responsible to put the Job in "running" state. It may raise `LoadClientTerminalException` and `LoadClientTransientException` to
immediately transition job into "failed" or "retry" state respectively.

### \_\_init\_\_

```python
def __init__(file_path: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L339)

File name is also a job id (or job id is deterministically derived) so it must be globally unique

### set\_run\_vars

```python
def set_run_vars(load_id: str, schema: Schema,
                 load_table: TTableSchema) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L354)

called by the loader right before the job is run

### run\_managed

```python
def run_managed(job_client: "JobClientBase") -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L366)

wrapper around the user implemented run method

### run

```python
@abstractmethod
def run() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L401)

run the actual job, this will be executed on a thread and should be implemented by the user
exception will be handled outside of this function

### state

```python
def state() -> TLoadJobState
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L408)

Returns current state. Should poll external resource if necessary.

### exception

```python
def exception() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L412)

The exception associated with failed or retry states

## FollowupJobRequest Objects

```python
class FollowupJobRequest()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L417)

Base class for follow up jobs that should be created

### new\_file\_path

```python
@abstractmethod
def new_file_path() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L421)

Path to a newly created temporary job file. If empty, no followup job should be created

## HasFollowupJobs Objects

```python
class HasFollowupJobs()
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L426)

Adds a trait that allows to create single or table chain followup jobs

### create\_followup\_jobs

```python
def create_followup_jobs(
        final_state: TLoadJobState) -> List[FollowupJobRequest]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L429)

Return list of jobs requests for jobs that should be created. `final_state` is state to which this job transits

## JobClientBase Objects

```python
class JobClientBase(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L434)

### initialize\_storage

```python
@abstractmethod
def initialize_storage(truncate_tables: Iterable[str] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L446)

Prepares storage to be used ie. creates database schema or file system folder. Truncates requested tables.

### is\_storage\_initialized

```python
@abstractmethod
def is_storage_initialized() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L451)

Returns if storage is ready to be read/written.

### drop\_storage

```python
@abstractmethod
def drop_storage() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L456)

Brings storage back into not initialized state. Typically data in storage is destroyed.

### update\_stored\_schema

```python
def update_stored_schema(
        only_tables: Iterable[str] = None,
        expected_update: TSchemaTables = None) -> Optional[TSchemaTables]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L460)

Updates storage to the current schema.

Implementations should not assume that `expected_update` is the exact difference between destination state and the self.schema. This is only the case if
destination has single writer and no other processes modify the schema.

**Arguments**:

- `only_tables` _Sequence[str], optional_ - Updates only listed tables. Defaults to None.
- `expected_update` _TSchemaTables, optional_ - Update that is expected to be applied to the destination

**Returns**:

- `Optional[TSchemaTables]` - Returns an update that was applied at the destination.

### create\_load\_job

```python
@abstractmethod
def create_load_job(table: TTableSchema,
                    file_path: str,
                    load_id: str,
                    restore: bool = False) -> LoadJob
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L486)

Creates a load job for a particular `table` with content in `file_path`

### prepare\_load\_job\_execution

```python
def prepare_load_job_execution(job: RunnableLoadJob) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L492)

Prepare the connected job client for the execution of a load job (used for query tags in sql clients)

### create\_table\_chain\_completed\_followup\_jobs

```python
def create_table_chain_completed_followup_jobs(
    table_chain: Sequence[TTableSchema],
    completed_table_chain_jobs: Optional[Sequence[LoadJobInfo]] = None
) -> List[FollowupJobRequest]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L501)

Creates a list of followup jobs that should be executed after a table chain is completed

### complete\_load

```python
@abstractmethod
def complete_load(load_id: str) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L510)

Marks the load package with `load_id` as completed in the destination. Before such commit is done, the data with `load_id` is invalid.

## WithStateSync Objects

```python
class WithStateSync(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L553)

### get\_stored\_schema

```python
@abstractmethod
def get_stored_schema() -> Optional[StorageSchemaInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L555)

Retrieves newest schema from destination storage

### get\_stored\_schema\_by\_hash

```python
@abstractmethod
def get_stored_schema_by_hash(version_hash: str) -> StorageSchemaInfo
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L560)

retrieves the stored schema by hash

### get\_stored\_state

```python
@abstractmethod
def get_stored_state(pipeline_name: str) -> Optional[StateInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L565)

Loads compressed state from destination storage

## WithStagingDataset Objects

```python
class WithStagingDataset(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L570)

Adds capability to use staging dataset and request it from the loader

### with\_staging\_dataset

```python
@abstractmethod
def with_staging_dataset() -> ContextManager["JobClientBase"]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L578)

Executes job client methods on staging dataset

## SupportsStagingDestination Objects

```python
class SupportsStagingDestination(ABC)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L583)

Adds capability to support a staging destination for the load

### should\_load\_data\_to\_staging\_dataset\_on\_staging\_destination

```python
def should_load_data_to_staging_dataset_on_staging_destination(
        table: TTableSchema) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L586)

If set to True, and staging destination is configured, the data will be loaded to staging dataset on staging destination
instead of a regular dataset on staging destination. Currently it is used by Athena Iceberg which uses staging dataset
on staging destination to copy data to iceberg tables stored on regular dataset on staging destination.
The default is to load data to regular dataset on staging destination from where warehouses like Snowflake (that have their
own storage) will copy data.

### should\_truncate\_table\_before\_load\_on\_staging\_destination

```python
@abstractmethod
def should_truncate_table_before_load_on_staging_destination(
        table: TTableSchema) -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L598)

If set to True, data in `table` will be truncated on staging destination (regular dataset). This is the default behavior which
can be changed with a config flag.
For Athena + Iceberg this setting is always False - Athena uses regular dataset to store Iceberg tables and we avoid touching it.
For Athena we truncate those tables only on "replace" write disposition.

## Destination Objects

```python
class Destination(ABC, Generic[TDestinationConfig, TDestinationClient])
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L613)

A destination factory that can be partially pre-configured
with credentials and other config params.

### config\_params

Explicit config params, overriding any injected or default values.

### caps\_params

Explicit capabilities params, overriding any default values for this destination

### spec

```python
@property
@abstractmethod
def spec() -> Type[TDestinationConfig]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L653)

A spec of destination configuration that also contains destination credentials

### capabilities

```python
def capabilities(
    config: Optional[TDestinationConfig] = None,
    naming: Optional[NamingConvention] = None
) -> DestinationCapabilitiesContext
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L657)

Destination capabilities ie. supported loader file formats, identifier name lengths, naming conventions, escape function etc.
Explicit caps arguments passed to the factory init and stored in `caps_params` are applied.

If `config` is provided, it is used to adjust the capabilities, otherwise the explicit config composed just of `config_params` passed
  to factory init is applied
If `naming` is provided, the case sensitivity and case folding are adjusted.

### destination\_name

```python
@property
def destination_name() -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L691)

The destination name will either be explicitly set while creating the destination or will be taken from the type

### client\_class

```python
@property
@abstractmethod
def client_class() -> Type[TDestinationClient]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L706)

A job client class responsible for starting and resuming load jobs

### configuration

```python
def configuration(initial_config: TDestinationConfig,
                  accept_partial: bool = False) -> TDestinationConfig
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L710)

Get a fully resolved destination config from the initial config

### client

```python
def client(schema: Schema,
           initial_config: TDestinationConfig = None) -> TDestinationClient
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L724)

Returns a configured instance of the destination's job client

### adjust\_capabilities

```python
@classmethod
def adjust_capabilities(
        cls, caps: DestinationCapabilitiesContext, config: TDestinationConfig,
        naming: Optional[NamingConvention]) -> DestinationCapabilitiesContext
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L732)

Adjust the capabilities to match the case sensitivity as requested by naming convention.

### normalize\_type

```python
@staticmethod
def normalize_type(destination_type: str) -> str
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L776)

Normalizes destination type string into a canonical form. Assumes that type names without dots correspond to built in destinations.

### from\_reference

```python
@staticmethod
def from_reference(
    ref: TDestinationReferenceArg,
    credentials: Optional[Any] = None,
    destination_name: Optional[str] = None,
    environment: Optional[str] = None,
    **kwargs: Any
) -> Optional["Destination[DestinationClientConfiguration, JobClientBase]"]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/common/destination/reference.py#L788)

Instantiate destination from str reference.
The ref can be a destination name or import path pointing to a destination class (e.g. `dlt.destinations.postgres`)

