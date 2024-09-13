---
title: Pipeline
description: Explanation of what a dlt pipeline is
keywords: [pipeline, source, full refresh, dev mode]
---

# Pipeline

A [pipeline](glossary.md#pipeline) is a connection that moves the data from your Python code to a
[destination](glossary.md#destination). The pipeline accepts `dlt` [sources](source.md) or
[resources](resource.md)  as well as generators, async generators, lists and any iterables.
Once the pipeline runs, all resources get evaluated and the data is loaded at destination.

Example:

This pipeline will load a list of objects into `duckdb` table with a name "three":

```py
import dlt

pipeline = dlt.pipeline(destination="duckdb", dataset_name="sequence")

info = pipeline.run([{'id':1}, {'id':2}, {'id':3}], table_name="three")

print(info)
```

You instantiate a pipeline by calling `dlt.pipeline` function with following arguments:

- `pipeline_name` a name of the pipeline that will be used to identify it in trace and monitoring
  events and to restore its state and data schemas on subsequent runs. If not provided, `dlt` will
  create pipeline name from the file name of currently executing Python module.
- `destination` a name of the [destination](../dlt-ecosystem/destinations) to which dlt
  will load the data. May also be provided to `run` method of the `pipeline`.
- `dataset_name` a name of the dataset to which the data will be loaded. A dataset is a logical
  group of tables i.e. `schema` in relational databases or folder grouping many files. May also be
  provided later to the `run` or `load` methods of the pipeline. If not provided at all then
  defaults to the `pipeline_name`.

To load the data you call the `run` method and pass your data in `data` argument.

Arguments:

- `data` (the first argument) may be a dlt source, resource, generator function, or any Iterator /
  Iterable (i.e. a list or the result of `map` function).
- `write_disposition` controls how to write data to a table. Defaults to "append".
  - `append` will always add new data at the end of the table.
  - `replace` will replace existing data with new data.
  - `skip` will prevent data from loading.
  - `merge` will deduplicate and merge data based on `primary_key` and `merge_key` hints.
- `table_name` - specified in case when table name cannot be inferred i.e. from the resources or name
  of the generator function.

Example: This pipeline will load the data the generator `generate_rows(10)` produces:

```py
import dlt

def generate_rows(nr):
    for i in range(nr):
        yield {'id':1}

pipeline = dlt.pipeline(destination='bigquery', dataset_name='sql_database_data')

info = pipeline.run(generate_rows(10))

print(info)
```

## Pipeline working directory

Each pipeline that you create with `dlt` stores extracted files, load packages, inferred schemas,
execution traces and the [pipeline state](state.md) in a folder in the local filesystem. The default
location for such folders is in user home directory: `~/.dlt/pipelines/<pipeline_name>`.

You can inspect stored artifacts using the command
[dlt pipeline info](../reference/command-line-interface.md#dlt-pipeline) and
[programmatically](../walkthroughs/run-a-pipeline.md#4-inspect-a-load-process).

> 💡 A pipeline with given name looks for its working directory in location above - so if you have two
> pipeline scripts that create a pipeline with the same name, they will see the same working folder
> and share all the possible state. You may override the default location using `pipelines_dir`
> argument when creating the pipeline.

> 💡 You can attach `Pipeline` instance to an existing working folder, without creating a new
> pipeline with `dlt.attach`.

### Separate working environments with `pipelines_dir`.
You can run several pipelines with the same name but with different configuration ie. to target development / staging / production environments.
Set the `pipelines_dir` argument to store all the working folders in specific place. For example:
```py
import dlt
from dlt.common.pipeline import get_dlt_pipelines_dir

dev_pipelines_dir = os.path.join(get_dlt_pipelines_dir(), "dev")
pipeline = dlt.pipeline(destination="duckdb", dataset_name="sequence", pipelines_dir=dev_pipelines_dir)
```
stores pipeline working folder in `~/.dlt/pipelines/dev/<pipeline_name>`. Mind that you need to pass this `~/.dlt/pipelines/dev/`
in to all cli commands to get info/trace for that pipeline.

## Do experiments with dev mode

If you [create a new pipeline script](../walkthroughs/create-a-pipeline.md) you will be
experimenting a lot. If you want that each time the pipeline resets its state and loads data to a
new dataset, set the `dev_mode` argument of the `dlt.pipeline` method to True. Each time the
pipeline is created, `dlt` adds datetime-based suffix to the dataset name.

## Refresh pipeline data and state

You can reset parts or all of your sources by using the `refresh` argument to `dlt.pipeline` or the pipeline's `run` or `extract` method.
That means when you run the pipeline the sources/resources being processed will have their state reset and their tables either dropped or truncated
depending on which refresh mode is used.

`refresh` option works with all relational/sql destinations and file buckets (`filesystem`). it does not work with vector databases (we are working on that) and
with custom destinations.

The `refresh` argument should have one of the following string values to decide the refresh mode:

### Drop tables and pipeline state for a source with `drop_sources`
All sources being processed in `pipeline.run` or `pipeline.extract` are refreshed.
That means all tables listed in their schemas are dropped and state belonging to those sources and all their resources is completely wiped.
The tables are deleted both from pipeline's schema and from the destination database.

If you only have one source or run with all your sources together, then this is practically like running the pipeline again for the first time

:::caution
This erases schema history for the selected sources and only the latest version is stored
:::

```py
import dlt

pipeline = dlt.pipeline("airtable_demo", destination="duckdb")
pipeline.run(airtable_emojis(), refresh="drop_sources")
```
In example above we instruct `dlt` to wipe pipeline state belonging to `airtable_emojis` source and drop all the database tables in `duckdb` to
which data was loaded. The `airtable_emojis` source had two resources named "📆 Schedule" and "💰 Budget" loading to tables "_schedule" and "_budget". Here's
what `dlt` does step by step:
1. collects a list of tables to drop by looking for all the tables in the schema that are created in the destination.
2. removes existing pipeline state associated with `airtable_emojis` source
3. resets the schema associated with `airtable_emojis` source
4. executes `extract` and `normalize` steps. those will create fresh pipeline state and a schema
5. before it executes `load` step, the collected tables are dropped from staging and regular dataset
6. schema `airtable_emojis` (associated with the source) is be removed from `_dlt_version` table
7. executes `load` step as usual so tables are re-created and fresh schema and pipeline state are stored.

### Selectively drop tables and resource state with `drop_resources`
Limits the refresh to the resources being processed in `pipeline.run` or `pipeline.extract` (.e.g by using `source.with_resources(...)`).
Tables belonging to those resources are dropped and their resource state is wiped (that includes incremental state).
The tables are deleted both from pipeline's schema and from the destination database.

Source level state keys are not deleted in this mode (i.e. `dlt.state()[<'my_key>'] = '<my_value>'`)

:::caution
This erases schema history for all affected sources and only the latest schema version is stored.
:::

```py
import dlt

pipeline = dlt.pipeline("airtable_demo", destination="duckdb")
pipeline.run(airtable_emojis().with_resources("📆 Schedule"), refresh="drop_resources")
```
Above we request that the state associated with "📆 Schedule" resource is reset and the table generated by it ("_schedule") is dropped. Other resources,
tables and state are not affected. Please check `drop_sources` for step by step description of what `dlt` does internally.

### Selectively truncate tables and reset resource state with `drop_data`
Same as `drop_resources` but instead of dropping tables from schema only the data is deleted from them (i.e. by `TRUNCATE <table_name>` in sql destinations). Resource state for selected resources is also wiped. In case of [incremental resources](incremental-loading.md#incremental-loading-with-a-cursor-field) this will
reset the cursor state and fully reload the data from the `initial_value`.

The schema remains unmodified in this case.
```py
import dlt

pipeline = dlt.pipeline("airtable_demo", destination="duckdb")
pipeline.run(airtable_emojis().with_resources("📆 Schedule"), refresh="drop_data")
```
Above the incremental state of the "📆 Schedule" is reset before `extract` step so data is fully reacquired. Just before `load` step starts,
 the "_schedule" is truncated and new (full) table data will be inserted/copied.

## Display the loading progress

You can add a progress monitor to the pipeline. Typically, its role is to visually assure user that
pipeline run is progressing. `dlt` supports 4 progress monitors out of the box:

- [enlighten](https://github.com/Rockhopper-Technologies/enlighten) - a status bar with progress
  bars that also allows for logging.
- [tqdm](https://github.com/tqdm/tqdm) - most popular Python progress bar lib, proven to work in
  Notebooks.
- [alive_progress](https://github.com/rsalmei/alive-progress) - with the most fancy animations.
- **log** - dumps the progress information to log, console or text stream. **the most useful on
  production** optionally adds memory and cpu usage stats.

> 💡 You must install the required progress bar library yourself.

You pass the progress monitor in `progress` argument of the pipeline. You can use a name from the
list above as in the following example:

```py
# create a pipeline loading chess data that dumps
# progress to stdout each 10 seconds (the default)
pipeline = dlt.pipeline(
    pipeline_name="chess_pipeline",
    destination='duckdb',
    dataset_name="chess_players_games_data",
    progress="log"
)
```

You can fully configure the progress monitor. See two examples below:

```py
# log each minute to Airflow task logger
ti = get_current_context()["ti"]
pipeline = dlt.pipeline(
    pipeline_name="chess_pipeline",
    destination='duckdb',
    dataset_name="chess_players_games_data",
    progress=dlt.progress.log(60, ti.log)
)
```

```py
# set tqdm bar color to yellow
pipeline = dlt.pipeline(
    pipeline_name="chess_pipeline",
    destination='duckdb',
    dataset_name="chess_players_games_data",
    progress=dlt.progress.tqdm(colour="yellow")
)
```

Note that the value of the `progress` argument is
[configurable](../walkthroughs/run-a-pipeline.md#2-see-the-progress-during-loading).
