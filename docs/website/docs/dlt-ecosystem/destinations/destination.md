---
title: Custom destination
description: Custom `dlt` destination function for reverse ETL
keywords: [reverse etl, sink, function, decorator, destination, custom destination]
---

# Custom destination: Reverse ETL

The `dlt` destination decorator allows you to receive all data passing through your pipeline in a simple function. This can be extremely useful for
reverse ETL, where you are pushing data back to an API.

You can also use this for sending data to a queue or a simple database destination that is not
yet supported by `dlt`, be aware that you will have to manually handle your own migrations in this case.

It will also allow you to simply get a path
to the files of your normalized data, so if you need direct access to parquet or jsonl files to copy them somewhere or push them to a database,
you can do this here too.

## Install `dlt` for reverse ETL

To install `dlt` without additional dependencies:
```sh
pip install dlt
```

## Set up a destination function for your pipeline

The custom destination decorator differs from other destinations in that you do not need to provide connection credentials, but rather you provide a function which gets called for all items loaded during a pipeline run or load operation.
With the `@dlt.destination` you can convert any function that takes two arguments into a `dlt` destination.

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
1. You can also remove the typing information (`TDataItems` and `TTableSchema`) from this example, typing generally are useful to know the shape of the incoming objects though.
2. There are a few other ways for declaring custom destination functions for your pipeline described below.
:::

## `@dlt.destination`, custom destination function and signature

The full signature of the destination decorator plus its function is the following:

```py
@dlt.destination(
    batch_size=10,
    loader_file_format="jsonl",
    name="my_custom_destination",
    naming_convention="direct",
    max_nesting_level=0,
    skip_dlt_columns_and_tables=True
)
def my_destination(items: TDataItems, table: TTableSchema) -> None:
    ...
```

### Decorator
* The `batch_size` parameter on the destination decorator defines how many items per function call are batched together and sent as an array. If you set a batch-size of `0`,
instead of passing in actual dataitems, you will receive one call per load job with the path of the file as the items argument. You can then open and process that file
in any way you like.
* The `loader_file_format` parameter on the destination decorator defines in which format files are stored in the load package before being sent to the destination function,
this can be `jsonl` or `parquet`.
* The `name` parameter on the destination decorator defines the name of the destination that get's created by the destination decorator.
* The `naming_convention` parameter on the destination decorator defines the name of the destination that gets created by the destination decorator. This controls how table and column names are normalized. The default is `direct` which will keep all names the same.
* The `max_nesting_level` parameter on the destination decorator defines how deep the normalizer will go to normalize complex fields on your data to create subtables. This overwrites any settings on your `source` and is set to zero to not create any nested tables by default.
* The `skip_dlt_columns_and_tables` parameter on the destination decorator defines wether internal tables and columns will be fed into the custom destination function. This is set to `True` by default.

:::note
* The custom destination sets the `max_nesting_level` to 0 by default, which means no subtables will be generated during the normalization phase.
* The custom destination also skips all internal tables and columns by default, if you need these, set `skip_dlt_columns_and_tables` to False.
:::

### Custom destination function
* The `items` parameter on the custom destination function contains the items being sent into the destination function.
* The `table` parameter contains the schema table the current call belongs to including all table hints and columns. For example, the table name can be accessed with `table["name"]`.
* You can also add config values and secrets to the function arguments, see below!


## Adding config variables and secrets
The destination decorator supports settings and secrets variables. If you, for example, plan to connect to a service that requires an API secret or a login, you can do the following:

```py
@dlt.destination(batch_size=10, loader_file_format="jsonl", name="my_destination")
def my_destination(items: TDataItems, table: TTableSchema, api_key: dlt.secrets.value) -> None:
    ...
```

You can then set a config variable in your `.dlt/secrets.toml`: like so:

```toml
[destination.my_destination]
api_key="<my-api-key>"
```

## Destination state

The destination keeps a local record of how many `DataItems` were processed, so if you, for example, use the custom destination to push `DataItems` to a remote API, and this
API becomes unavailable during the load resulting in a failed `dlt` pipeline run, you can repeat the run of your pipeline at a later stage and the destination will continue
where it left of. For this reason, it makes sense to choose a batch size that you can process in one transaction (say one API request or one database transaction) so that if this
request or transaction fails repeatedly, you can repeat it at the next run without pushing duplicate data to your remote location.

## Concurrency

Calls to the destination function by default will be executed on multiple threads, so you need to make sure you are not using any non-thread-safe nonlocal or global variables from outside
your destination function. If you need to have all calls be executed from the same thread, you can set the `workers` config variable of the load step to 1.

:::tip
For performance reasons, we recommend keeping the multithreaded approach and making sure that you, for example, are using threadsafe connection pools to a remote database or queue.
:::

## Referencing the destination function

There are multiple ways to reference the custom destination function you want to use:
- Directly reference the destination function

  ```py
  @dlt.destination(batch_size=10)
  def local_destination_func(items: TDataItems, table: TTableSchema) -> None:
      ...

  # reference function directly
  p = dlt.pipeline("my_pipe", destination=local_destination_func)
  ```
- Directly via destination reference. In this case, don't use decorator for the destination function.
  ```py
  # file my_destination.py

  from dlt.common.destination import Destination

  # don't use decorator
  def local_destination_func(items: TDataItems, table: TTableSchema) -> None:
      ...

  # via destination reference
  p = dlt.pipeline(
      "my_pipe",
      destination=Destination.from_reference(
          "destination", destination_callable=local_destination_func
      )
  )
  ```
- Via fully qualified string to function location (can be used from `config.toml` or ENV vars). Destination function should be located in another file.
  ```py
  # file my_pipeline.py

  from dlt.common.destination import Destination

  # fully qualified string to function location
  p = dlt.pipeline(
      "my_pipe",
      destination=Destination.from_reference(
          "destination", destination_callable="my_destination.local_destination_func"
      )
  )
  ```


## Write disposition

`@dlt.destination` will forward all normalized `DataItems` encountered during a pipeline run to the custom destination function, so there is no notion of "write dispositions".

## Staging support

`@dlt.destination` does not support staging files in remote locations before being called at this time. If you need this feature, please let us know.

## What's next

* Check out our [Custom BigQuery Destination](../../examples/custom_destination_bigquery/) example.
* Need a help with building a custom destination? Ask your questions in our [Slack Community](https://dlthub.com/community) technical help channel.
