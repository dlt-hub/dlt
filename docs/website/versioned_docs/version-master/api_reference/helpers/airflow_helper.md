---
sidebar_label: airflow_helper
title: helpers.airflow_helper
---

## PipelineTasksGroup Objects

```python
class PipelineTasksGroup(TaskGroup)
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/helpers/airflow_helper.py#L54)

Represents a dlt Airflow pipeline task group.

### \_\_init\_\_

```python
def __init__(pipeline_name: str,
             use_data_folder: bool = False,
             local_data_folder: str = None,
             use_task_logger: bool = True,
             log_progress_period: float = 30.0,
             buffer_max_items: int = 1000,
             retry_policy: Retrying = DEFAULT_RETRY_NO_RETRY,
             retry_pipeline_steps: Sequence[TPipelineStep] = ("load", ),
             abort_task_if_any_job_failed: bool = True,
             wipe_local_data: bool = True,
             save_load_info: bool = False,
             save_trace_info: bool = False,
             **kwargs: Any) -> None
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/helpers/airflow_helper.py#L59)

Creates a task group to which you can add pipeline runs

The run environment is prepared as follows
- the .dlt folder (the project folder) is searched under `dags` as configured by Airflow
- the data folder where pipelines are stored is always unique

The `data_folder` is available in certain Airflow deployments. In case of Composer, it is a location on the gcs bucket. `use_data_folder` is disabled and should be
enabled only when needed. The operations on bucket are non-atomic and way slower than on local storage and should be avoided.

`abort_task_if_any_job_failed` will abort the other dlt loading jobs and fail the Airflow task in any of the jobs failed. See https://dlthub.com/docs/running-in-production/running#handle-exceptions-failed-jobs-and-retry-the-pipeline.

The load info and trace info can be optionally saved to the destination. See https://dlthub.com/docs/running-in-production/running#inspect-and-save-the-load-info-and-trace

**Arguments**:

- `pipeline_name` _str_ - Name of the task group
- `use_data_folder` _bool, optional_ - If well defined 'data' folder is present it will be used. Currently only data folder on Composer is supported. Defaults to False.
- `local_data_folder` _str, optional_ - Path to a local folder on worker machine to where to store data. Used if local_data_folder is False or there's not well defined data folder. Defaults to gettempdir.
- `use_task_logger` _bool, optional_ - Will redirect dlt logger into task logger. Defaults to True.
- `log_progress_period` _float, optional_ - If progress is not configured for a pipeline, the `log` progress is used with a given period. Set 0 to disable. Defaults to 30.0.
- `buffer_max_items` _int, optional_ - Maximum number of buffered items. Use 0 to keep dlt built-in limit. Defaults to 1000.
- `retry_policy` __type_, optional_ - Tenacity retry policy. Defaults to no retry.
- `retry_pipeline_steps` _Sequence[TPipelineStep], optional_ - Which pipeline steps are eligible for retry. Defaults to ("load", ).
- `wipe_local_data` _bool, optional_ - Will wipe all the data created by pipeline, also in case of exception. Defaults to False.
- `save_load_info` _bool, optional_ - Will save extensive load info to the destination. Defaults to False.
- `save_trace_info` _bool, optional_ - Will save trace info to the destination. Defaults to False.

### run

```python
def run(pipeline: Pipeline,
        data: Any,
        table_name: str = None,
        write_disposition: TWriteDispositionConfig = None,
        loader_file_format: TLoaderFileFormat = None,
        schema_contract: TSchemaContract = None,
        pipeline_name: str = None,
        on_before_run: Callable[[], None] = None,
        **kwargs: Any) -> PythonOperator
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/helpers/airflow_helper.py#L159)

Create a task to run the given pipeline with the
given data in Airflow.

**Arguments**:

- `pipeline` _Pipeline_ - The pipeline to run
  data (Any):
  The data to run the pipeline with. If a non-resource
  callable given, it's evaluated during the DAG execution,
  right before the actual pipeline run.
- `NOTE` - If `on_before_run` is provided, first `on_before_run`
  is evaluated, and then callable `data`.
- `table_name` _str, optional_ - The name of the table to
  which the data should be loaded within the `dataset`.
- `write_disposition` _TWriteDispositionConfig, optional_ - Same as
  in `run` command.
  loader_file_format (TLoaderFileFormat, optional):
  The file format the loader will use to create the
  load package.
- `schema_contract` _TSchemaContract, optional_ - On override
  for the schema contract settings, this will replace
  the schema contract settings for all tables in the schema.
- `pipeline_name` _str, optional_ - The name of the derived pipeline.
- `on_before_run` _Callable, optional_ - A callable to be
  executed right before the actual pipeline run.
  

**Returns**:

- `PythonOperator` - Airflow task instance.

### add\_run

```python
@with_telemetry("helper", "airflow_add_run", False, "decompose")
def add_run(pipeline: Pipeline,
            data: Any,
            *,
            decompose: Literal["none", "serialize", "parallel",
                               "parallel-isolated"] = "none",
            table_name: str = None,
            write_disposition: TWriteDispositionConfig = None,
            loader_file_format: TLoaderFileFormat = None,
            schema_contract: TSchemaContract = None,
            on_before_run: Callable[[], None] = None,
            **kwargs: Any) -> List[PythonOperator]
```

[[view_source]](https://github.com/dlt-hub/dlt/blob/f0690715274590fc4cacf1165e3661aaa7af1c15/dlt/helpers/airflow_helper.py#L334)

Creates a task or a group of tasks to run `data` with `pipeline`

Creates an Airflow task that extracts, normalizes and loads `data` with the passed pipeline instance `pipeline`. If `data` is a source
and `decompose` is `serialize` it will decompose the source into disjoint connected components (isolated group of resources) and execute them
one after another as separate Airflow tasks. The decomposition makes sure that each resource or transformer is extracted only once. It preserves
the order of resources declared in the source when creating graph of tasks.

The `kwargs` are passed as arguments to all Airflow task instances created.

**Arguments**:

- `pipeline` _Pipeline_ - An instance of pipeline used to run the source
  data (Any):
  Any data supported by `run` method of the pipeline.
  If a non-resource callable given, it's called before
  the load to get the data.
  decompose (Literal["none", "serialize", "parallel"], optional):
  A source decomposition strategy into Airflow tasks:
  none - no decomposition, default value.
  serialize - decompose the source into a sequence of Airflow tasks.
  parallel - decompose the source into a parallel Airflow task group,
  except the first resource must be completed first.
  All tasks that are run in parallel share the same pipeline state.
  If two of them modify the state, part of state may be lost
  parallel-isolated - decompose the source into a parallel Airflow task group.
  with the same exception as above. All task have separate pipeline
  state (via separate pipeline name) but share the same dataset,
  schemas and tables.
- `NOTE` - The first component of the source in both parallel models is done first,
  after that the rest are executed in parallel to each other.
- `NOTE` - In case the SequentialExecutor is used by Airflow, the tasks
  will remain sequential despite 'parallel' or 'parallel-isolated' mode.
  Use another executor (e.g. CeleryExecutor) to make tasks parallel!
  
  Parallel tasks are executed in different pipelines, all derived from the original
  one, but with the state isolated from each other.
- `table_name` - (str): The name of the table to which the data should be loaded within the `dataset`
- `write_disposition` _TWriteDispositionConfig, optional_ - Same as in `run` command. Defaults to None.
- `loader_file_format` _Literal["jsonl", "insert_values", "parquet"], optional_ - The file format the loader will use to create the load package.
  Not all file_formats are compatible with all destinations. Defaults to the preferred file format of the selected destination.
- `schema_contract` _TSchemaContract, optional_ - On override for the schema contract settings,
  this will replace the schema contract settings for all tables in the schema. Defaults to None.
  on_before_run (Callable, optional):
  A callable to be executed right before the actual pipeline run.
  

**Returns**:

- `Any` - Airflow tasks created in order of creation.

