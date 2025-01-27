---
sidebar_label: pipeline
title: pipeline
---

## pipeline

```python
@overload
def pipeline(pipeline_name: str = None,
             pipelines_dir: str = None,
             pipeline_salt: TSecretStrValue = None,
             destination: TDestinationReferenceArg = None,
             staging: TDestinationReferenceArg = None,
             dataset_name: str = None,
             import_schema_path: str = None,
             export_schema_path: str = None,
             full_refresh: Optional[bool] = None,
             dev_mode: bool = False,
             refresh: Optional[TRefreshMode] = None,
             progress: TCollectorArg = _NULL_COLLECTOR,
             _impl_cls: Type[TPipeline] = Pipeline) -> TPipeline
```

Creates a new instance of `dlt` pipeline, which moves the data from the source ie. a REST API to a destination ie. database or a data lake.

**Notes**:

  The `pipeline` functions allows you to pass the destination name to which the data should be loaded, the name of the dataset and several other options that govern loading of the data.
  The created `Pipeline` object lets you load the data from any source with `run` method or to have more granular control over the loading process with `extract`, `normalize` and `load` methods.
  
  Please refer to the following doc pages
  - Write your first pipeline walkthrough: https://dlthub.com/docs/walkthroughs/create-a-pipeline
  - Pipeline architecture and data loading steps: https://dlthub.com/docs/reference
  - List of supported destinations: https://dlthub.com/docs/dlt-ecosystem/destinations
  

**Arguments**:

- `pipeline_name` _str, optional_ - A name of the pipeline that will be used to identify it in monitoring events and to restore its state and data schemas on subsequent runs.
  Defaults to the file name of pipeline script with `dlt_` prefix added.
  
- `pipelines_dir` _str, optional_ - A working directory in which pipeline state and temporary files will be stored. Defaults to user home directory: `~/dlt/pipelines/`.
  
- `pipeline_salt` _TSecretStrValue, optional_ - A random value used for deterministic hashing during data anonymization. Defaults to a value derived from the pipeline name.
  Default value should not be used for any cryptographic purposes.
  
- `destination` _str | DestinationReference, optional_ - A name of the destination to which dlt will load the data, or a destination module imported from `dlt.destination`.
  May also be provided to `run` method of the `pipeline`.
  
- `staging` _str | DestinationReference, optional_ - A name of the destination where dlt will stage the data before final loading, or a destination module imported from `dlt.destination`.
  May also be provided to `run` method of the `pipeline`.
  
- `dataset_name` _str, optional_ - A name of the dataset to which the data will be loaded. A dataset is a logical group of tables ie. `schema` in relational databases or folder grouping many files.
  May also be provided later to the `run` or `load` methods of the `Pipeline`. If not provided at all then defaults to the `pipeline_name`
  
- `import_schema_path` _str, optional_ - A path from which the schema `yaml` file will be imported on each pipeline run. Defaults to None which disables importing.
  
- `export_schema_path` _str, optional_ - A path where the schema `yaml` file will be exported after every schema change. Defaults to None which disables exporting.
  
- `dev_mode` _bool, optional_ - When set to True, each instance of the pipeline with the `pipeline_name` starts from scratch when run and loads the data to a separate dataset.
  The datasets are identified by `dataset_name_` + datetime suffix. Use this setting whenever you experiment with your data to be sure you start fresh on each run. Defaults to False.
  
- `refresh` _str | TRefreshMode_ - Fully or partially reset sources during pipeline run. When set here the refresh is applied on each run of the pipeline.
  To apply refresh only once you can pass it to `pipeline.run` or `extract` instead. The following refresh modes are supported:
  * `drop_sources`: Drop tables and source and resource state for all sources currently being processed in `run` or `extract` methods of the pipeline. (Note: schema history is erased)
  * `drop_resources`: Drop tables and resource state for all resources being processed. Source level state is not modified. (Note: schema history is erased)
  * `drop_data`: Wipe all data and resource state for all resources being processed. Schema is not modified.
  
  progress(str, Collector): A progress monitor that shows progress bars, console or log messages with current information on sources, resources, data items etc. processed in
  `extract`, `normalize` and `load` stage. Pass a string with a collector name or configure your own by choosing from `dlt.progress` module.
  We support most of the progress libraries: try passing `tqdm`, `enlighten` or `alive_progress` or `log` to write to console/log.
  

**Returns**:

- `Pipeline` - An instance of `Pipeline` class with. Please check the documentation of `run` method for information on what to do with it.

## pipeline

```python
@overload
def pipeline() -> Pipeline
```

When called without any arguments, returns the recently created `Pipeline` instance.
If not found, it creates a new instance with all the pipeline options set to defaults.

## attach

```python
@with_config(spec=PipelineConfiguration, auto_pipeline_section=True)
def attach(pipeline_name: str = None,
           pipelines_dir: str = None,
           pipeline_salt: TSecretStrValue = None,
           destination: TDestinationReferenceArg = None,
           staging: TDestinationReferenceArg = None,
           progress: TCollectorArg = _NULL_COLLECTOR,
           **injection_kwargs: Any) -> Pipeline
```

Attaches to the working folder of `pipeline_name` in `pipelines_dir` or in default directory. Requires that valid pipeline state exists in working folder.
Pre-configured `destination` and `staging` factories may be provided. If not present, default factories are created from pipeline state.

## run

```python
def run(data: Any,
        *,
        destination: TDestinationReferenceArg = None,
        staging: TDestinationReferenceArg = None,
        dataset_name: str = None,
        table_name: str = None,
        write_disposition: TWriteDispositionConfig = None,
        columns: Sequence[TColumnSchema] = None,
        schema: Schema = None,
        loader_file_format: TLoaderFileFormat = None,
        table_format: TTableFormat = None,
        schema_contract: TSchemaContract = None,
        refresh: Optional[TRefreshMode] = None) -> LoadInfo
```

Loads the data in `data` argument into the destination specified in `destination` and dataset specified in `dataset_name`.

**Notes**:

  This method will `extract` the data from the `data` argument, infer the schema, `normalize` the data into a load package (ie. jsonl or PARQUET files representing tables) and then `load` such packages into the `destination`.
  
  The data may be supplied in several forms:
  * a `list` or `Iterable` of any JSON-serializable objects ie. `dlt.run([1, 2, 3], table_name="numbers")`
  * any `Iterator` or a function that yield (`Generator`) ie. `dlt.run(range(1, 10), table_name="range")`
  * a function or a list of functions decorated with @dlt.resource ie. `dlt.run([chess_players(title="GM"), chess_games()])`
  * a function or a list of functions decorated with @dlt.source.
  
  Please note that `dlt` deals with `bytes`, `datetime`, `decimal` and `uuid` objects so you are free to load binary data or documents containing dates.
  
  Execution:
  The `run` method will first use `sync_destination` method to synchronize pipeline state and schemas with the destination. You can disable this behavior with `restore_from_destination` configuration option.
  Next it will make sure that data from the previous is fully processed. If not, `run` method normalizes and loads pending data items.
  Only then the new data from `data` argument is extracted, normalized and loaded.
  

**Arguments**:

- `data` _Any_ - Data to be loaded to destination
  
- `destination` _str | DestinationReference, optional_ - A name of the destination to which dlt will load the data, or a destination module imported from `dlt.destination`.
  If not provided, the value passed to `dlt.pipeline` will be used.
  
- `dataset_name` _str, optional_ - A name of the dataset to which the data will be loaded. A dataset is a logical group of tables ie. `schema` in relational databases or folder grouping many files.
  If not provided, the value passed to `dlt.pipeline` will be used. If not provided at all then defaults to the `pipeline_name`
  
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
  
- `schema` _Schema, optional_ - An explicit `Schema` object in which all table schemas will be grouped. By default `dlt` takes the schema from the source (if passed in `data` argument) or creates a default one itself.
  
- `loader_file_format` _Literal["jsonl", "insert_values", "parquet"], optional_ - The file format the loader will use to create the load package. Not all file_formats are compatible with all destinations. Defaults to the preferred file format of the selected destination.
  
- `table_format` _Literal["delta", "iceberg"], optional_ - The table format used by the destination to store tables. Currently you can select table format on filesystem and Athena destinations.
  
- `schema_contract` _TSchemaContract, optional_ - On override for the schema contract settings, this will replace the schema contract settings for all tables in the schema. Defaults to None.
  
- `refresh` _str | TRefreshMode_ - Fully or partially reset sources before loading new data in this run. The following refresh modes are supported:
  * `drop_sources`: Drop tables and source and resource state for all sources currently being processed in `run` or `extract` methods of the pipeline. (Note: schema history is erased)
  * `drop_resources`: Drop tables and resource state for all resources being processed. Source level state is not modified. (Note: schema history is erased)
  * `drop_data`: Wipe all data and resource state for all resources being processed. Schema is not modified.
  

**Raises**:

- `PipelineStepFailed` - when a problem happened during `extract`, `normalize` or `load` steps.

**Returns**:

- `LoadInfo` - Information on loaded data including the list of package ids and failed job statuses. Please not that `dlt` will not raise if a single job terminally fails. Such information is provided via LoadInfo.

