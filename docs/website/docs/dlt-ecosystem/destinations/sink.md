---
title: Destination Decorator / Reverse ETL
description: Sink function `dlt` destination for reverse ETL
keywords: [reverse etl, sink, function, decorator, destination]
---

# Sink function / Reverse ETL

## Install dlt for Sink / reverse ETL
** To install the DLT without additional dependencies **
```
pip install dlt
```

## Setup Guide
### 1. Initialize the dlt project

Let's start by initializing a new dlt project as follows:

```bash
dlt init chess sink
```
> 💡 This command will initialize your pipeline with chess as the source and sink as the destination.

The above command generates several files and directories, including `.dlt/secrets.toml`.

### 2. Set up a destination function for your pipeline
The sink destination differs from other destinations in that you do not need to provide connection credentials, but rather you provide a function which 
gets called for all items loaded during a pipeline run or load operation. For the chess example, you can add the following lines at the top of the file.
With the @dlt.destination decorator you can convert any function that takes two arguments into a dlt destination. 

```python
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema

@dlt.destination(batch_size=10)
def sink(items: TDataItems, table: TTableSchema) -> None:
    print(table["name"])
    print(items)
```

To enable this destination decorator in your chess example, replace the line `destination='sink'` with `destination=sink` (without the quotes) to directly reference
the sink from your pipeline constructor. Now you can run your pipeline and see the output of all the items coming from the chess pipeline to your console.

:::tip
1. You can also remove the typing information (TDataItems and TTableSchema) from this example, typing generally are useful to know the shape of the incoming objects though.
2. There are a few other ways for declaring sink functions for your pipeline described below.
:::

## Sink decorator function and signature

The full signature of the sink decorator and a function is

```python
@dlt.destination(batch_size=10, loader_file_format="jsonl", name="my_sink")
def sink(items: TDataItems, table: TTableSchema) -> None:
    ...
```

#### Decorator
* The `batch_size` parameter on the sink decorator defines how many items per function call are batched together and sent as an array.
* The `loader_file_format` parameter on the sink decorator defines in which format files are stored in the load package before being sent to the sink function, 
this can be `jsonl` or `parquet`.
* The `name` parameter on the sink decorator defines the name of the destination that get's created by the sink decorator. 

#### Sink function
* The `items` parameter on the sink function contains the items being sent into the sink function. 
* The `table` parameter contains the schema table the current call belongs to including all table hints and columns. For example the table name can be access with table["name"]. Keep in mind that dlt also created special tables prefixed with `_dlt` which you may want to ignore when processing data.

## Sink destination state
The sink destination keeps a local record of how many DataItems were processed, so if you, for example, use the sink destination to push DataItems to a remote api, and this
api becomes unavailable during the load resulting in a failed dlt pipeline run, you can repeat the run of your pipeline at a later stage and the sink destination will continue 
where it left of. For this reason it makes sense to choose a batch size that you can process in one transaction (say one api request or one database transaction) so that if this
request or transaction fail repeatedly you can repeat it at the next run without pushing duplicate data to your remote location.

## Concurrency
Calls to the sink function by default will be executed on multiple threads, so you need to make sure you are not using any non-thread-safe nonlocal or global variables from outside
your sink function. If, for whichever reason, you need to have all calls be executed from the same thread, you can set the `workers` config variable of the load step to 1. For performance
reasons we recommend to keep the multithreaded approach and make sure that you, for example, are using threadsafe connection pools to a remote database or queue.

## Referencing the sink function
There are multiple ways to reference the sink function you want to use. These are:

```python
# file my_pipeline.py

@dlt.destination(batch_size=10)
def local_sink_func(items: TDataItems, table: TTableSchema) -> None:
    ...

# reference function directly
p = dlt.pipeline(name="my_pipe", destination=local_sink_func)

# fully qualified string to function location (can be used from config.toml or env vars)
p = dlt.pipeline(name="my_pipe", destination="sink", credentials="my_pipeline.local_sink_func")

# via destination reference
p = dlt.pipeline(name="my_pipe", destination=Destination.from_reference("sink", credentials=local_sink_func, environment="staging"))
```

## Write disposition

The sink destination will forward all normalized DataItems encountered during a pipeline run to the sink function, so there is no notion of write dispositions for the sink.

## Staging support

The sink destination does not support staging files in remote locations before being called at this time. If you need this feature, please let us know.

