---
sidebar_label: pipeline
title: pipeline.pipeline
---

## Pipeline Objects

```python
class Pipeline(SupportsPipeline)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L274)

### pipeline\_name

Name of the pipeline

### first\_run

Indicates a first run of the pipeline, where run ends with successful loading of the data

### pipelines\_dir

A directory where the pipelines' working directories are created

### working\_dir

A working directory of the pipeline

### staging

The destination reference which is the Destination Class. `destination.destination_name` returns the name string

### dataset\_name

Name of the dataset to which pipeline will be loaded to

### is\_active

Tells if instance is currently active and available via dlt.pipeline()

### \_\_init\_\_

```python
def __init__(pipeline_name: str,
             pipelines_dir: str,
             pipeline_salt: TSecretValue,
             destination: TDestination,
             staging: TDestination,
             dataset_name: str,
             import_schema_path: str,
             export_schema_path: str,
             dev_mode: bool,
             progress: _Collector,
             must_attach_to_local_pipeline: bool,
             config: PipelineConfiguration,
             runtime: RunConfiguration,
             refresh: Optional[TRefreshMode] = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L314)

Initializes the Pipeline class which implements `dlt` pipeline. Please use `pipeline` function in `dlt` module to create a new Pipeline instance.

### drop

```python
def drop(pipeline_name: str = None) -> "Pipeline"
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L363)

Deletes local pipeline state, schemas and any working files.

**Arguments**:

- `pipeline_name` _str_ - Optional. New pipeline name.

### extract

```python
@with_runtime_trace()
@with_schemas_sync
@with_state_sync(may_extract_state=True)
@with_config_section((known_sections.EXTRACT, ))
def extract(data: Any,
            *,
            table_name: str = None,
            parent_table_name: str = None,
            write_disposition: TWriteDispositionConfig = None,
            columns: TAnySchemaColumns = None,
            primary_key: TColumnNames = None,
            schema: Schema = None,
            max_parallel_items: int = ConfigValue,
            workers: int = ConfigValue,
            schema_contract: TSchemaContract = None,
            refresh: Optional[TRefreshMode] = None) -> ExtractInfo
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L392)

Extracts the `data` and prepare it for the normalization. Does not require destination or credentials to be configured. See `run` method for the arguments' description.

### normalize

```python
@with_runtime_trace()
@with_schemas_sync
@with_config_section((known_sections.NORMALIZE, ))
def normalize(workers: int = 1,
              loader_file_format: TLoaderFileFormat = None) -> NormalizeInfo
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L495)

Normalizes the data prepared with `extract` method, infers the schema and creates load packages for the `load` method. Requires `destination` to be known.

### load

```python
@with_runtime_trace(send_state=True)
@with_state_sync()
@with_config_section((known_sections.LOAD, ))
def load(destination: TDestinationReferenceArg = None,
         dataset_name: str = None,
         credentials: Any = None,
         *,
         workers: int = 20,
         raise_on_failed_jobs: bool = False) -> LoadInfo
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L546)

Loads the packages prepared by `normalize` method into the `dataset_name` at `destination`, optionally using provided `credentials`

### run

```python
@with_runtime_trace()
@with_config_section(("run", ))
def run(data: Any = None,
        *,
        destination: TDestinationReferenceArg = None,
        staging: TDestinationReferenceArg = None,
        dataset_name: str = None,
        credentials: Any = None,
        table_name: str = None,
        write_disposition: TWriteDispositionConfig = None,
        columns: TAnySchemaColumns = None,
        primary_key: TColumnNames = None,
        schema: Schema = None,
        loader_file_format: TLoaderFileFormat = None,
        schema_contract: TSchemaContract = None,
        refresh: Optional[TRefreshMode] = None) -> LoadInfo
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L599)

Loads the data from `data` argument into the destination specified in `destination` and dataset specified in `dataset_name`.

**Notes**:

  This method will `extract` the data from the `data` argument, infer the schema, `normalize` the data into a load package (ie. jsonl or PARQUET files representing tables) and then `load` such packages into the `destination`.
  
  The data may be supplied in several forms:
  * a `list` or `Iterable` of any JSON-serializable objects ie. `dlt.run([1, 2, 3], table_name="numbers")`
  * any `Iterator` or a function that yield (`Generator`) ie. `dlt.run(range(1, 10), table_name="range")`
  * a function or a list of functions decorated with @dlt.resource ie. `dlt.run([chess_players(title="GM"), chess_games()])`
  * a function or a list of functions decorated with @dlt.source.
  
  Please note that `dlt` deals with `bytes`, `datetime`, `decimal` and `uuid` objects so you are free to load documents containing ie. binary data or dates.
  
  Execution:
  The `run` method will first use `sync_destination` method to synchronize pipeline state and schemas with the destination. You can disable this behavior with `restore_from_destination` configuration option.
  Next it will make sure that data from the previous is fully processed. If not, `run` method normalizes, loads pending data items and **exits**
  If there was no pending data, new data from `data` argument is extracted, normalized and loaded.
  

**Arguments**:

- `data` _Any_ - Data to be loaded to destination
  
- `destination` _str | DestinationReference, optional_ - A name of the destination to which dlt will load the data, or a destination module imported from `dlt.destination`.
  If not provided, the value passed to `dlt.pipeline` will be used.
  
  dataset_name (str, optional):A name of the dataset to which the data will be loaded. A dataset is a logical group of tables ie. `schema` in relational databases or folder grouping many files.
  If not provided, the value passed to `dlt.pipeline` will be used. If not provided at all then defaults to the `pipeline_name`
  
- `credentials` _Any, optional_ - Credentials for the `destination` ie. database connection string or a dictionary with google cloud credentials.
  In most cases should be set to None, which lets `dlt` to use `secrets.toml` or environment variables to infer right credentials values.
  
- `table_name` _str, optional_ - The name of the table to which the data should be loaded within the `dataset`. This argument is required for a `data` that is a list/Iterable or Iterator without `__name__` attribute.
  The behavior of this argument depends on the type of the `data`:
  * generator functions: the function name is used as table name, `table_name` overrides this default
  * `@dlt.resource`: resource contains the full table schema and that includes the table name. `table_name` will override this property. Use with care!
  * `@dlt.source`: source contains several resources each with a table schema. `table_name` will override all table names within the source and load the data into single table.
  
- `write_disposition` _TWriteDispositionConfig, optional_ - Controls how to write data to a table. Accepts a shorthand string literal or configuration dictionary.
  Allowed shorthand string literals: `append` will always add new data at the end of the table. `replace` will replace existing data with new data. `skip` will prevent data from loading. "merge" will deduplicate and merge data based on "primary_key" and "merge_key" hints. Defaults to "append".
  Write behaviour can be further customized through a configuration dictionary. For example, to obtain an SCD2 table provide `write_disposition={"disposition": "merge", "strategy": "scd2"}`.
  Please note that in case of `dlt.resource` the table schema value will be overwritten and in case of `dlt.source`, the values in all resources will be overwritten.
  
- `columns` _Sequence[TColumnSchema], optional_ - A list of column schemas. Typed dictionary describing column names, data types, write disposition and performance hints that gives you full control over the created table schema.
  
- `primary_key` _str | Sequence[str]_ - A column name or a list of column names that comprise a private key. Typically used with "merge" write disposition to deduplicate loaded data.
  
- `schema` _Schema, optional_ - An explicit `Schema` object in which all table schemas will be grouped. By default `dlt` takes the schema from the source (if passed in `data` argument) or creates a default one itself.
  
  loader_file_format (Literal["jsonl", "insert_values", "parquet"], optional). The file format the loader will use to create the load package. Not all file_formats are compatible with all destinations. Defaults to the preferred file format of the selected destination.
  
- `schema_contract` _TSchemaContract, optional_ - On override for the schema contract settings, this will replace the schema contract settings for all tables in the schema. Defaults to None.
  
- `refresh` _str | TRefreshMode_ - Fully or partially reset sources before loading new data in this run. The following refresh modes are supported:
  * `drop_sources`: Drop tables and source and resource state for all sources currently being processed in `run` or `extract` methods of the pipeline. (Note: schema history is erased)
  * `drop_resources`: Drop tables and resource state for all resources being processed. Source level state is not modified. (Note: schema history is erased)
  * `drop_data`: Wipe all data and resource state for all resources being processed. Schema is not modified.
  

**Raises**:

  PipelineStepFailed when a problem happened during `extract`, `normalize` or `load` steps.

**Returns**:

- `LoadInfo` - Information on loaded data including the list of package ids and failed job statuses. Please not that `dlt` will not raise if a single job terminally fails. Such information is provided via LoadInfo.

### sync\_destination

```python
@with_schemas_sync
def sync_destination(destination: TDestinationReferenceArg = None,
                     staging: TDestinationReferenceArg = None,
                     dataset_name: str = None) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L726)

Synchronizes pipeline state with the `destination`'s state kept in `dataset_name`

**Notes**:

  Attempts to restore pipeline state and schemas from the destination. Requires the state that is present at the destination to have a higher version number that state kept locally in working directory.
  In such a situation the local state, schemas and intermediate files with the data will be deleted and replaced with the state and schema present in the destination.
  
  A special case where the pipeline state exists locally but the dataset does not exist at the destination will wipe out the local state.
  
- `Note` - this method is executed by the `run` method before any operation on data. Use `restore_from_destination` configuration option to disable that behavior.

### activate

```python
def activate() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L823)

Activates the pipeline

The active pipeline is used as a context for several `dlt` components. It provides state to sources and resources evaluated outside of
`pipeline.run` and `pipeline.extract` method. For example, if the source you use is accessing state in `dlt.source` decorated function, the state is provided
by active pipeline.

The name of active pipeline is used when resolving secrets and config values as the optional most outer section during value lookup. For example if pipeline
with name `chess_pipeline` is active and `dlt` looks for `BigQuery` configuration, it will look in `chess_pipeline.destination.bigquery.credentials` first and then in
`destination.bigquery.credentials`.

Active pipeline also provides the current DestinationCapabilitiesContext to other components ie. Schema instances. Among others, it sets the naming convention
and maximum identifier length.

Only one pipeline is active at a given time.

Pipeline created or attached with `dlt.pipeline`/'dlt.attach` is automatically activated. `run`, `load` and `extract` methods also activate pipeline.

### deactivate

```python
def deactivate() -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L843)

Deactivates the pipeline

Pipeline must be active in order to use this method. Please refer to `activate()` method for the explanation of active pipeline concept.

### has\_data

```python
@property
def has_data() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L853)

Tells if the pipeline contains any data: schemas, extracted files, load packages or loaded packages in the destination

### has\_pending\_data

```python
@property
def has_pending_data() -> bool
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L863)

Tells if the pipeline contains any extracted files or pending load packages

### state

```python
@property
def state() -> TPipelineState
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L879)

Returns a dictionary with the pipeline state

### naming

```python
@property
def naming() -> NamingConvention
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L884)

Returns naming convention of the default schema

### last\_trace

```python
@property
def last_trace() -> PipelineTrace
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L889)

Returns or loads last trace generated by pipeline. The trace is loaded from standard location.

### list\_extracted\_resources

```python
@deprecated(
    "Please use list_extracted_load_packages instead. Flat extracted storage format got dropped"
    " in dlt 0.4.0",
    category=Dlt04DeprecationWarning,
)
def list_extracted_resources() -> Sequence[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L900)

Returns a list of all the files with extracted resources that will be normalized.

### list\_extracted\_load\_packages

```python
def list_extracted_load_packages() -> Sequence[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L904)

Returns a list of all load packages ids that are or will be normalized.

### list\_normalized\_load\_packages

```python
def list_normalized_load_packages() -> Sequence[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L908)

Returns a list of all load packages ids that are or will be loaded.

### list\_completed\_load\_packages

```python
def list_completed_load_packages() -> Sequence[str]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L912)

Returns a list of all load package ids that are completely loaded

### get\_load\_package\_info

```python
def get_load_package_info(load_id: str) -> LoadPackageInfo
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L916)

Returns information on extracted/normalized/completed package with given load_id, all jobs and their statuses.

### get\_load\_package\_state

```python
def get_load_package_state(load_id: str) -> TLoadPackageState
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L923)

Returns information on extracted/normalized/completed package with given load_id, all jobs and their statuses.

### list\_failed\_jobs\_in\_package

```python
def list_failed_jobs_in_package(load_id: str) -> Sequence[LoadJobInfo]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L927)

List all failed jobs and associated error messages for a specified `load_id`

### drop\_pending\_packages

```python
def drop_pending_packages(with_partial_loads: bool = True) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L931)

Deletes all extracted and normalized packages, including those that are partially loaded by default

### sync\_schema

```python
@with_schemas_sync
def sync_schema(schema_name: str = None) -> TSchemaTables
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L946)

Synchronizes the schema `schema_name` with the destination. If no name is provided, the default schema will be synchronized.

### set\_local\_state\_val

```python
def set_local_state_val(key: str, value: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L962)

Sets value in local state. Local state is not synchronized with destination.

### get\_local\_state\_val

```python
def get_local_state_val(key: str) -> Any
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L973)

Gets value from local state. Local state is not synchronized with destination.

### sql\_client

```python
def sql_client(schema_name: str = None) -> SqlClientBase[Any]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L982)

Returns a sql client configured to query/change the destination and dataset that were used to load the data.
Use the client with `with` statement to manage opening and closing connection to the destination:
```py
with pipeline.sql_client() as client:
    with client.execute_query(
        "SELECT id, name, email FROM customers WHERE id = %s", 10
    ) as cursor:
        print(cursor.fetchall())
```

The client is authenticated and defaults all queries to dataset_name used by the pipeline. You can provide alternative
`schema_name` which will be used to normalize dataset name.

### destination\_client

```python
def destination_client(schema_name: str = None) -> JobClientBase
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L1021)

Get the destination job client for the configured destination
Use the client with `with` statement to manage opening and closing connection to the destination:
```py
with pipeline.destination_client() as client:
    client.drop_storage()  # removes storage which typically wipes all data in it
```

The client is authenticated. You can provide alternative `schema_name` which will be used to normalize dataset name.
If no schema name is provided and no default schema is present in the pipeline, and ad hoc schema will be created and discarded after use.

### managed\_state

```python
@contextmanager
def managed_state(*, extract_state: bool = False) -> Iterator[TPipelineState]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/9857029af018a582dd24da4070562f58bb7e9fc5/dlt/pipeline/pipeline.py#L1551)

Puts pipeline state in managed mode, where yielded state changes will be persisted or fully roll-backed on exception.

Makes the state to be available via StateInjectableContext

