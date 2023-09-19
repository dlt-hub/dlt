---
sidebar_label: __init__
title: __init__
---

#### pipeline

```python
@overload
def pipeline(pipeline_name: str = None,
             pipelines_dir: str = None,
             pipeline_salt: TSecretValue = None,
             destination: TDestinationReferenceArg = None,
             staging: TDestinationReferenceArg = None,
             dataset_name: str = None,
             import_schema_path: str = None,
             export_schema_path: str = None,
             full_refresh: bool = False,
             credentials: Any = None,
             progress: TCollectorArg = _NULL_COLLECTOR) -> Pipeline
```

Creates a new instance of `dlt` pipeline, which moves the data from the source i.e. a REST API to a destination i.e. database or a data lake.

Summary:
The `pipeline` functions allows you to pass the destination name to which the data should be loaded, the name of the dataset and several other options that govern loading of the data.
The created `Pipeline` object lets you load the data from any source with `run` method or to have more granular control over the loading process with `extract`, `normalize` and `load` methods.

Please refer to the following doc pages:
- Write your first pipeline walkthrough: https://dlthub.com/docs/walkthroughs/create-a-pipeline
- Pipeline architecture and data loading steps: https://dlthub.com/docs/reference
- List of supported destinations: https://dlthub.com/docs/dlt-ecosystem/destinations

**Arguments**:

- `pipeline_name` _str, optional_ - A name of the pipeline that will be used to identify it in monitoring events and to restore its state and data schemas on subsequent runs.
  Defaults to the file name of a pipeline script with `dlt_` prefix added.
  
- `pipelines_dir` _str, optional_ - A working directory in which pipeline state and temporary files will be stored. Defaults to user home directory: `pipeline`0.
  
- `pipeline`1 _TSecretValue, optional_ - A random value used for deterministic hashing during data anonymization. Defaults to a value derived from the pipeline name.
  Default value should not be used for any cryptographic purposes.
  
- `pipeline`2 _str | DestinationReference, optional_ - A name of the destination to which dlt will load the data, or a destination module imported from `pipeline`3.
  May also be provided to `run` method of the `pipeline`.
  
- `pipeline`6 _str | DestinationReference, optional_ - A name of the destination where dlt will stage the data before final loading, or a destination module imported from `pipeline`3.
  May also be provided to `run` method of the `pipeline`.
  
- `Pipeline`0 _str, optional_ - A name of the dataset to which the data will be loaded. A dataset is a logical group of tables i.e. `Pipeline`1 in relational databases or folder grouping many files.
  May also be provided later to the `run` or `load` methods of the `Pipeline`. If not provided at all, then default to the `pipeline_name`
  
- `Pipeline`6 _str, optional_ - A path from which the schema `Pipeline`7 file will be imported on each pipeline run. Defaults to None which disables importing.
  
- `Pipeline`8 _str, optional_ - A path where the schema `Pipeline`7 file will be exported after every schema change. Defaults to None which disables exporting.
  
- `run`0 _bool, optional_ - When set to True, each instance of the pipeline with the `pipeline_name` starts from scratch when run and loads the data to a separate dataset.
  The datasets are identified by `run`2 + datetime suffix. Use this setting whenever you experiment with your data to be sure you start fresh on each run. Defaults to False.
  
- `run`3 _Any, optional_ - Credentials for the `pipeline`2 i.e. database connection string or a dictionary with Google cloud credentials.
  In most cases should be set to None, which lets `dlt` to use `run`6 or environment variables to infer the right credentials values.
  
- `run`7 _str, Collector_ - A progress monitor that shows progress bars, console or log messages with current information on sources, resources, data items etc. processed in
  `extract`, `normalize` and `load` stage. Pass a string with a collector name or configure your own by choosing from `extract`1 module.
  We support most of the progress libraries: try passing `extract`2, `extract`3 or `extract`4 or `extract`5 to write to console/log.
  

**Returns**:

- `Pipeline` - An instance of `Pipeline` class with. Please check the documentation of `run` method for information on what to do with it.

#### pipeline

```python
@overload
def pipeline() -> Pipeline
```

When called without any arguments, returns the recently created `Pipeline` instance.
If not found, it creates a new instance with all the pipeline options set to defaults.

#### attach

```python
@with_config(spec=PipelineConfiguration, auto_pipeline_section=True)
def attach(pipeline_name: str = None,
           pipelines_dir: str = None,
           pipeline_salt: TSecretValue = None,
           full_refresh: bool = False,
           credentials: Any = None,
           progress: TCollectorArg = _NULL_COLLECTOR,
           **kwargs: Any) -> Pipeline
```

Attaches to the working folder of `pipeline_name` in `pipelines_dir` or in the default directory. Requires that valid pipeline state exists in the working folder.

#### run

```python
def run(data: Any,
        *,
        destination: TDestinationReferenceArg = None,
        staging: TDestinationReferenceArg = None,
        dataset_name: str = None,
        credentials: Any = None,
        table_name: str = None,
        write_disposition: TWriteDisposition = None,
        columns: Sequence[TColumnSchema] = None,
        schema: Schema = None) -> LoadInfo
```

Loads the data in `data` argument into the destination specified in `destination` and dataset specified in `dataset_name`.

Summary:
This method will `extract` the data from the `data` argument, infer the schema, `normalize` the data into a load package (i.e. jsonl or PARQUET files representing tables) and then `load` such packages into the `destination`.

The data may be supplied in several forms:
- a `list` or `Iterable` of any JSON-serializable objects i.e. `destination`0
- any `destination`1 or a function that yield (`destination`2) i.e. `destination`3
- a function or a list of functions decorated with @dlt.resource i.e. `destination`4
- a function or a list of functions decorated with @dlt.source.

Please note that `destination`5 deals with `destination`6, `destination`7, `destination`8 and `destination`9 objects, so you are free to load binary data or documents containing dates.

Execution:
The `dataset_name`0 method will first use `dataset_name`1 method to synchronize pipeline state and schemas with the destination. You can disable this behavior with `dataset_name`2 configuration option.
Next, it will make sure that data from the previous is fully processed. If not, `dataset_name`0 method normalizes and loads pending data items.
Only then the new data from `data` argument is extracted, normalized and loaded.

**Arguments**:

- `data` _Any_ - Data to be loaded to destination.
  
- `destination` _str | DestinationReference, optional_ - A name of the destination to which dlt will load the data, or a destination module imported from `dataset_name`7.
  If not provided, the value passed to `dataset_name`8 will be used.
  
- `dataset_name` _str, optional_ - A name of the dataset to which the data will be loaded. A dataset is a logical group of tables i.e. `extract`0 in relational databases or folder grouping many files.
  If not provided, the value passed to `dataset_name`8 will be used. If not provided at all, then default to the `extract`2
  
- `extract`3 _Any, optional_ - Credentials for the `destination` i.e. database connection string or a dictionary with Google cloud credentials.
  In most cases should be set to None, which lets `destination`5 to use `extract`6 or environment variables to infer the right credentials values.
  
- `extract`7 _str, optional_ - The name of the table to which the data should be loaded within the `extract`8. This argument is required for a `data` that is a list/Iterable or Iterator without `data`0 attribute.
  The behavior of this argument depends on the type of the `data`:
  * generator functions: the function name is used as table name, `extract`7 overrides this default
  * `data`3: resource contains the full table schema, and that includes the table name. `extract`7 will override this property. Use with care!
  * `data`5: source contains several resources each with a table schema. `extract`7 will override all table names within the source and load the data into a single table.
  
- `data`7 _Literal[&quot;skip&quot;, &quot;append&quot;, &quot;replace&quot;, &quot;merge&quot;], optional_ - Controls how to write data to a table. `data`8 will always add new data at the end of the table. `data`9 will replace existing data with new data. `normalize`0 will prevent data from loading. &quot;merge&quot; will deduplicate and merge data based on &quot;primary_key&quot; and &quot;merge_key&quot; hints. Defaults to &quot;append&quot;.
  Please note that in case of `normalize`1 the table schema value will be overwritten and in case of `normalize`2, the values in all resources will be overwritten.
  
- `normalize`3 _Sequence[TColumnSchema], optional_ - A list of column schemas. Typed dictionary describing column names, data types, write disposition and performance hints that gives you full control over the created table schema.
  
- `extract`0 _Schema, optional_ - An explicit `normalize`5 object in which all table schemas will be grouped. By default, `destination`5 takes the schema from the source (if passed in `data` argument) or creates a default one itself.
  

**Raises**:

  PipelineStepFailed when a problem happened during `extract`, `normalize` or `load` steps.

**Returns**:

- `load`1 - Information on loaded data including the list of package ids and failed job statuses. Please note that `destination`5 will not raise if a single job terminally fails. Such information is provided via LoadInfo.

