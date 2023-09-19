---
sidebar_label: pipeline
title: pipeline
---

## Pipeline Objects

```python
class Pipeline(SupportsPipeline)
```

#### pipeline\_name

Name of the pipeline

#### first\_run

Indicates a first run of the pipeline, where run ends with successful loading of the data

#### pipelines\_dir

A directory where the pipelines&#x27; working directories are created

#### working\_dir

A working directory of the pipeline

#### staging

The destination reference which is ModuleType. `destination.__name__` returns the name string

#### dataset\_name

Name of the dataset to which pipeline will be loaded to

#### is\_active

Tells if instance is currently active and available via dlt.pipeline()

#### \_\_init\_\_

```python
def __init__(pipeline_name: str, pipelines_dir: str,
             pipeline_salt: TSecretValue, destination: DestinationReference,
             staging: DestinationReference, dataset_name: str,
             credentials: Any, import_schema_path: str,
             export_schema_path: str, full_refresh: bool, progress: _Collector,
             must_attach_to_local_pipeline: bool,
             config: PipelineConfiguration, runtime: RunConfiguration) -> None
```

Initializes the Pipeline class which implements `dlt` pipeline. Please use `pipeline` function in `dlt` module to create a new Pipeline instance.

#### drop

```python
def drop() -> "Pipeline"
```

Deletes local pipeline state, schemas and any working files

#### extract

```python
@with_runtime_trace
@with_schemas_sync
@with_state_sync(may_extract_state=True)
@with_config_section((known_sections.EXTRACT, ))
def extract(data: Any,
            *,
            table_name: str = None,
            parent_table_name: str = None,
            write_disposition: TWriteDisposition = None,
            columns: TAnySchemaColumns = None,
            primary_key: TColumnNames = None,
            schema: Schema = None,
            max_parallel_items: int = None,
            workers: int = None) -> ExtractInfo
```

Extracts the `data` and prepare it for the normalization. Does not require destination or credentials to be configured. See `run` method for the arguments&#x27; description.

#### normalize

```python
@with_runtime_trace
@with_schemas_sync
@with_config_section((known_sections.NORMALIZE, ))
def normalize(workers: int = 1,
              loader_file_format: TLoaderFileFormat = None) -> NormalizeInfo
```

Normalizes the data prepared with `extract` method, infers the schema and creates load packages for the `load` method. Requires `destination` to be known.

#### load

```python
@with_runtime_trace
@with_schemas_sync
@with_state_sync()
@with_config_section((known_sections.LOAD, ))
def load(destination: TDestinationReferenceArg = None,
         dataset_name: str = None,
         credentials: Any = None,
         *,
         workers: int = 20,
         raise_on_failed_jobs: bool = False) -> LoadInfo
```

Loads the packages prepared by `normalize` method into the `dataset_name` at `destination`, using provided `credentials`

#### run

```python
@with_runtime_trace
@with_config_section(("run", ))
def run(data: Any = None,
        *,
        destination: TDestinationReferenceArg = None,
        staging: TDestinationReferenceArg = None,
        dataset_name: str = None,
        credentials: Any = None,
        table_name: str = None,
        write_disposition: TWriteDisposition = None,
        columns: TAnySchemaColumns = None,
        primary_key: TColumnNames = None,
        schema: Schema = None,
        loader_file_format: TLoaderFileFormat = None) -> LoadInfo
```

Loads the data from `data` argument into the destination specified in `destination` and dataset specified in `dataset_name`.

### Summary
This method will `extract` the data from the `data` argument, infer the schema, `normalize` the data into a load package (ie. jsonl or PARQUET files representing tables) and then `load` such packages into the `destination`.

The data may be supplied in several forms:
* a `list` or `Iterable` of any JSON-serializable objects ie. `destination`0
* any `destination`1 or a function that yield (`destination`2) ie. `destination`3
* a function or a list of functions decorated with @dlt.resource ie. `destination`4
* a function or a list of functions decorated with @dlt.source.

Please note that `destination`5 deals with `destination`6, `destination`7, `destination`8 and `destination`9 objects so you are free to load documents containing ie. binary data or dates.

### Execution
The `dataset_name`0 method will first use `dataset_name`1 method to synchronize pipeline state and schemas with the destination. You can disable this behavior with `dataset_name`2 configuration option.
Next it will make sure that data from the previous is fully processed. If not, `dataset_name`0 method normalizes, loads pending data items and **exits**
If there was no pending data, new data from `data` argument is extracted, normalized and loaded.

### Args:
data (Any): Data to be loaded to destination

destination (str | DestinationReference, optional): A name of the destination to which dlt will load the data, or a destination module imported from `dataset_name`5.
If not provided, the value passed to `dataset_name`6 will be used.

dataset_name (str, optional):A name of the dataset to which the data will be loaded. A dataset is a logical group of tables ie. `dataset_name`7 in relational databases or folder grouping many files.
If not provided, the value passed to `dataset_name`6 will be used. If not provided at all then defaults to the `dataset_name`9


credentials (Any, optional): Credentials for the `destination` ie. database connection string or a dictionary with google cloud credentials.
In most cases should be set to None, which lets `destination`5 to use `extract`2 or environment variables to infer right credentials values.

table_name (str, optional): The name of the table to which the data should be loaded within the `extract`3. This argument is required for a `data` that is a list/Iterable or Iterator without `extract`5 attribute.
The behavior of this argument depends on the type of the `data`:
* generator functions: the function name is used as table name, `extract`7 overrides this default
* `extract`8: resource contains the full table schema and that includes the table name. `extract`7 will override this property. Use with care!
* `data`0: source contains several resources each with a table schema. `extract`7 will override all table names within the source and load the data into single table.

write_disposition (Literal[&quot;skip&quot;, &quot;append&quot;, &quot;replace&quot;, &quot;merge&quot;], optional): Controls how to write data to a table. `data`2 will always add new data at the end of the table. `data`3 will replace existing data with new data. `data`4 will prevent data from loading. &quot;merge&quot; will deduplicate and merge data based on &quot;primary_key&quot; and &quot;merge_key&quot; hints. Defaults to &quot;append&quot;.
Please note that in case of `data`5 the table schema value will be overwritten and in case of `data`6, the values in all resources will be overwritten.

columns (Sequence[TColumnSchema], optional): A list of column schemas. Typed dictionary describing column names, data types, write disposition and performance hints that gives you full control over the created table schema.

primary_key (str | Sequence[str]): A column name or a list of column names that comprise a private key. Typically used with &quot;merge&quot; write disposition to deduplicate loaded data.

schema (Schema, optional): An explicit `data`7 object in which all table schemas will be grouped. By default `destination`5 takes the schema from the source (if passed in `data` argument) or creates a default one itself.

loader_file_format (Literal[&quot;jsonl&quot;, &quot;insert_values&quot;, &quot;parquet&quot;], optional). The file format the loader will use to create the load package. Not all file_formats are compatible with all destinations. Defaults to the preferred file format of the selected destination.

### Raises:
PipelineStepFailed when a problem happened during `extract`, `normalize` or `load` steps.
### Returns:
LoadInfo: Information on loaded data including the list of package ids and failed job statuses. Please not that `destination`5 will not raise if a single job terminally fails. Such information is provided via LoadInfo.

#### sync\_destination

```python
@with_schemas_sync
def sync_destination(destination: TDestinationReferenceArg = None,
                     staging: TDestinationReferenceArg = None,
                     dataset_name: str = None) -> None
```

Synchronizes pipeline state with the `destination`&#x27;s state kept in `dataset_name`

### Summary
Attempts to restore pipeline state and schemas from the destination. Requires the state that is present at the destination to have a higher version number that state kept locally in working directory.
In such a situation the local state, schemas and intermediate files with the data will be deleted and replaced with the state and schema present in the destination.

A special case where the pipeline state exists locally but the dataset does not exist at the destination will wipe out the local state.

Note: this method is executed by the `run` method before any operation on data. Use `restore_from_destination` configuration option to disable that behavior.

#### activate

```python
def activate() -> None
```

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

Pipeline created or attached with `dlt.pipeline`/&#x27;dlt.attach`pipeline.run`0run`pipeline.run`1load`pipeline.run`2extract` methods also activate pipeline.

#### deactivate

```python
def deactivate() -> None
```

Deactivates the pipeline

Pipeline must be active in order to use this method. Please refer to `activate()` method for the explanation of active pipeline concept.

#### has\_data

```python
@property
def has_data() -> bool
```

Tells if the pipeline contains any data: schemas, extracted files, load packages or loaded packages in the destination

#### has\_pending\_data

```python
@property
def has_pending_data() -> bool
```

Tells if the pipeline contains any extracted files or pending load packages

#### state

```python
@property
def state() -> TPipelineState
```

Returns a dictionary with the pipeline state

#### last\_trace

```python
@property
def last_trace() -> PipelineTrace
```

Returns or loads last trace generated by pipeline. The trace is loaded from standard location.

#### list\_extracted\_resources

```python
def list_extracted_resources() -> Sequence[str]
```

Returns a list of all the files with extracted resources that will be normalized.

#### list\_normalized\_load\_packages

```python
def list_normalized_load_packages() -> Sequence[str]
```

Returns a list of all load packages ids that are or will be loaded.

#### list\_completed\_load\_packages

```python
def list_completed_load_packages() -> Sequence[str]
```

Returns a list of all load package ids that are completely loaded

#### get\_load\_package\_info

```python
def get_load_package_info(load_id: str) -> LoadPackageInfo
```

Returns information on normalized/completed package with given load_id, all jobs and their statuses.

#### list\_failed\_jobs\_in\_package

```python
def list_failed_jobs_in_package(load_id: str) -> Sequence[LoadJobInfo]
```

List all failed jobs and associated error messages for a specified `load_id`

#### sync\_schema

```python
@with_schemas_sync
def sync_schema(schema_name: str = None,
                credentials: Any = None) -> TSchemaTables
```

Synchronizes the schema `schema_name` with the destination. If no name is provided, the default schema will be synchronized.

#### set\_local\_state\_val

```python
def set_local_state_val(key: str, value: Any) -> None
```

Sets value in local state. Local state is not synchronized with destination.

#### get\_local\_state\_val

```python
def get_local_state_val(key: str) -> Any
```

Gets value from local state. Local state is not synchronized with destination.

#### sql\_client

```python
def sql_client(schema_name: str = None,
               credentials: Any = None) -> SqlClientBase[Any]
```

Returns a sql client configured to query/change the destination and dataset that were used to load the data.
Use the client with `with` statement to manage opening and closing connection to the destination:
&gt;&gt;&gt; with pipeline.sql_client() as client:
&gt;&gt;&gt;     with client.execute_query(
&gt;&gt;&gt;         &quot;SELECT id, name, email FROM customers WHERE id = %s&quot;, 10
&gt;&gt;&gt;     ) as cursor:
&gt;&gt;&gt;         print(cursor.fetchall())

The client is authenticated and defaults all queries to dataset_name used by the pipeline. You can provide alternative
`schema_name` which will be used to normalize dataset name and alternative `credentials`.

#### destination\_client

```python
def destination_client(schema_name: str = None,
                       credentials: Any = None) -> JobClientBase
```

Get the destination job client for the configured destination
Use the client with `with` statement to manage opening and closing connection to the destination:
&gt;&gt;&gt; with pipeline.destination_client() as client:
&gt;&gt;&gt;     client.drop_storage()  # removes storage which typically wipes all data in it

The client is authenticated. You can provide alternative `schema_name` which will be used to normalize dataset name and alternative `credentials`.
If no schema name is provided and no default schema is present in the pipeline, and ad hoc schema will be created and discarded after use.

