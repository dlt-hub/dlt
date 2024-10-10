---
title: Custom destination
description: Custom `dlt` destination function for reverse ETL
keywords: [reverse etl, sink, function, decorator, destination, custom destination]
---

# Custom destination: Reverse ETL

The `dlt` destination decorator allows you to receive all data passing through your pipeline in a simple function. This can be extremely useful for reverse ETL, where you are pushing data back to an API.

You can also use this for sending data to a queue or a simple database destination that is not yet supported by `dlt`, although be aware that you will have to manually handle your own migrations in this case.

It will also allow you to simply get a path to the files of your normalized data. So, if you need direct access to parquet or jsonl files to copy them somewhere or push them to a database, you can do this here too.

## Install `dlt` for reverse ETL

To install `dlt` without additional dependencies:
```sh
pip install dlt
```

## Set up a destination function for your pipeline

The custom destination decorator differs from other destinations in that you do not need to provide connection credentials, but rather you provide a function that gets called for all items loaded during a pipeline run or load operation. With the `@dlt.destination`, you can convert any function that takes two arguments into a `dlt` destination.

A very simple dlt pipeline that pushes a list of items into a destination function might look like this:

```py
import dlt
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema

@dlt.destination(batch_size=10)
def my_destination(items: TDataItems, table: TTableSchema) -> None:
    print(table["name"])
    print(items)

pipeline = dlt.pipeline("custom_destination_pipeline", destination=my_destination)
pipeline.run([1, 2, 3], table_name="items")
```

:::tip
1. You can also remove the typing information (`TDataItems` and `TTableSchema`) from this example. Typing is generally useful to know the shape of the incoming objects, though.
2. There are a few other ways to declare custom destination functions for your pipeline described below.
:::

### `@dlt.destination`, custom destination function, and signature

The full signature of the destination decorator plus its function is the following:

```py
@dlt.destination(
    batch_size=10,
    loader_file_format="jsonl",
    name="my_custom_destination",
    naming_convention="direct",
    max_table_nesting=0,
    skip_dlt_columns_and_tables=True,
    max_parallel_load_jobs=5,
    loader_parallelism_strategy="table-sequential",
)
def my_destination(items: TDataItems, table: TTableSchema) -> None:
    ...
```

### Decorator arguments
* The `batch_size` parameter on the destination decorator defines how many items per function call are batched together and sent as an array. If you set a batch size of `0`, instead of passing in actual data items, you will receive one call per load job with the path of the file as the items argument. You can then open and process that file in any way you like.
* The `loader_file_format` parameter on the destination decorator defines the format in which files are stored in the load package before being sent to the destination function. This can be `jsonl` or `parquet`.
* The `name` parameter on the destination decorator defines the name of the destination that gets created by the destination decorator.
* The `naming_convention` parameter on the destination decorator defines the name of the destination that gets created by the destination decorator. This controls how table and column names are normalized. The default is `direct`, which will keep all names the same.
* The `max_nesting_level` parameter on the destination decorator defines how deep the normalizer will go to normalize nested fields in your data to create subtables. This overwrites any settings on your `source` and is set to zero to not create any nested tables by default.
* The `skip_dlt_columns_and_tables` parameter on the destination decorator defines whether internal tables and columns will be fed into the custom destination function. This is set to `True` by default.
* The `max_parallel_load_jobs` parameter will define how many load jobs will run in parallel in threads. If you have a destination that only allows five connections at a time, you can set this value to 5, for example.
* The `loader_parallelism_strategy` parameter will control how load jobs are parallelized. Set to `parallel`, the default, jobs will be parallelized no matter which table is being loaded to. `table-sequential` will parallelize loading but only ever have one load job per table at a time, `sequential` will run all load jobs sequentially on the main thread.

:::note
Settings above ensure that the shape of the data you receive in the destination function is as close as possible to what you see in the data source.

* The custom destination sets the `max_nesting_level` to 0 by default, which means no sub-tables will be generated during the normalization phase.
* The custom destination also skips all internal tables and columns by default. If you need these, set `skip_dlt_columns_and_tables` to False.
:::

### Custom destination function
* The `items` parameter on the custom destination function contains the items being sent into the destination function.
* The `table` parameter contains the schema table the current call belongs to, including all table hints and columns. For example, the table name can be accessed with `table["name"]`.
* You can also add config values and secrets to the function arguments, see below!

## Add configuration, credentials, and other secrets to the destination function
The destination decorator supports settings and secrets variables. If you, for example, plan to connect to a service that requires an API secret or a login, you can do the following:

```py
@dlt.destination(batch_size=10, loader_file_format="jsonl", name="my_destination")
def my_destination(items: TDataItems, table: TTableSchema, api_key: dlt.secrets.value) -> None:
    ...
```

You can then set a config variable in your `.dlt/secrets.toml` like so:

```toml
[destination.my_destination]
api_key="<my-api-key>"
```

Custom destinations follow the same configuration rules as [regular named destinations](../../general-usage/destination.md#configure-a-destination)

## Use the custom destination in `dlt` pipeline

There are multiple ways to pass the custom destination function to the `dlt` pipeline:
- Directly reference the destination function

  ```py
  @dlt.destination(batch_size=10)
  def local_destination_func(items: TDataItems, table: TTableSchema) -> None:
      ...

  # Reference function directly
  p = dlt.pipeline("my_pipe", destination=local_destination_func)
  ```

  Like for [regular destinations](../../general-usage/destination.md#pass-explicit-credentials), you are allowed to pass configuration and credentials
  explicitly to the destination function.
  ```py
  @dlt.destination(batch_size=10, loader_file_format="jsonl", name="my_destination")
  def my_destination(items: TDataItems, table: TTableSchema, api_key: dlt.secrets.value) -> None:
      ...

  p = dlt.pipeline("my_pipe", destination=my_destination(api_key=os.getenv("MY_API_KEY")))
  ```

- Directly via destination reference. In this case, don't use the decorator for the destination function.
  ```py
  # File my_destination.py

  from dlt.common.destination import Destination

  # Don't use the decorator
  def local_destination_func(items: TDataItems, table: TTableSchema) -> None:
      ...

  # Via destination reference
  p = dlt.pipeline(
      "my_pipe",
      destination=Destination.from_reference(
          "destination", destination_callable=local_destination_func
      )
  )
  ```
- Via a fully qualified string to function location (this can be set in `config.toml` or through environment variables). The destination function should be located in another file.
  ```py
  # File my_pipeline.py

  from dlt.common.destination import Destination

  # Fully qualified string to function location
  p = dlt.pipeline(
      "my_pipe",
      destination=Destination.from_reference(
          "destination", destination_callable="my_destination.local_destination_func"
      )
  )
  ```

## Adjust batch size and retry policy for atomic loads
The destination keeps a local record of how many `DataItems` were processed, so if you, for example, use the custom destination to push `DataItems` to a remote API, and this
API becomes unavailable during the load resulting in a failed `dlt` pipeline run, you can repeat the run of your pipeline at a later moment and the custom destination will **restart from the whole batch that failed**. We are preventing any data from being lost, but you can still get duplicated data if you committed half of the batch, for example, to a database and then failed.
**Keeping the batch atomicity is on you**. For this reason, it makes sense to choose a batch size that you can process in one transaction (say one API request or one database transaction) so that if this request or transaction fails repeatedly, you can repeat it at the next run without pushing duplicate data to your remote location. For systems that
are not transactional and do not tolerate duplicated data, you can use a batch of size 1.

Destination functions that raise exceptions are retried 5 times before giving up (`load.raise_on_max_retries` config option). If you run the pipeline again, it will resume loading before extracting new data.

If your exception derives from `DestinationTerminalException`, the whole load job will be marked as failed and not retried again.

:::caution
If you wipe out the pipeline folder (where job files and destination state are saved), you will not be able to restart from the last failed batch.
However, it is fairly easy to back up and restore the pipeline directory, [see details below](#manage-pipeline-state-for-incremental-loading).
:::

## Increase or decrease loading parallelism
Calls to the destination function by default will be executed on multiple threads, so you need to make sure you are not using any non-thread-safe nonlocal or global variables from outside your destination function. If you need to have all calls executed from the same thread, you can set the `workers` [config variable of the load step](../../reference/performance.md#load) to 1.

:::tip
For performance reasons, we recommend keeping the multithreaded approach and making sure that you, for example, are using thread-safe connection pools to a remote database or queue.
:::

## Write disposition

`@dlt.destination` will forward all normalized `DataItems` encountered during a pipeline run to the custom destination function, so there is no notion of "write dispositions."

## Staging support

`@dlt.destination` does not support staging files in remote locations before being called at this time. If you need this feature, please let us know.

## Manage pipeline state for incremental loading
Custom destinations do not have a general mechanism to restore pipeline state. This will impact data sources that rely on the state being kept, i.e., all incremental resources.
If you wipe the pipeline directory (i.e., by deleting a folder or running on AWS Lambda or GitHub Actions where you get a clean runner), the progress of the incremental loading is lost. On the next run, you will re-acquire the data from the beginning.

While we are working on a pluggable state storage, you can fix the problem above by:
1. Not wiping the pipeline directory. For example, if you run your pipeline on an EC instance periodically, the state will be preserved.
2. By doing a restore/backup of the pipeline directory before/after it runs. This is way easier than it sounds, and [here's a script you can reuse](https://gist.github.com/rudolfix/ee6e16d8671f26ac4b9ffc915ad24b6e).

## What's next

* Check out our [Custom BigQuery Destination](../../examples/custom_destination_bigquery/) example.
* Need help with building a custom destination? Ask your questions in our [Slack Community](https://dlthub.com/community) technical help channel.

