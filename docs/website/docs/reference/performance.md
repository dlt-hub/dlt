---
title: Performance
description: Scale-up, parallelize and finetune dlt pipelines
keywords: [scaling, parallelism, finetuning]
---

# Performance

## Yield pages instead of rows

If you can, yield pages when producing data. This makes some processes more effective by lowering
the necessary function calls (each chunk of data that you yield goes through the extract pipeline once so if you yield a chunk of 10.000 items you will gain significant savings)
For example:
<!--@@@DLT_SNIPPET_START ./performance_snippets/performance-snippets.py::performance_chunking-->
```py
import dlt

def get_rows(limit):
    yield from map(lambda n: {"row": n}, range(limit))

@dlt.resource
def database_cursor():
    # here we yield each row returned from database separately
    yield from get_rows(10000)
```
<!--@@@DLT_SNIPPET_END ./performance_snippets/performance-snippets.py::performance_chunking-->
can be replaced with:

<!--@@@DLT_SNIPPET_START ./performance_snippets/performance-snippets.py::performance_chunking_chunk-->
```py
from itertools import islice

@dlt.resource
def database_cursor_chunked():
    # here we yield chunks of size 1000
    rows = get_rows(10000)
    while item_slice := list(islice(rows, 1000)):
        print(f"got chunk of length {len(item_slice)}")
        yield item_slice
```
<!--@@@DLT_SNIPPET_END ./performance_snippets/performance-snippets.py::performance_chunking_chunk-->

## Memory/disk management
`dlt` buffers data in memory to speed up processing and uses file system to pass data between the **extract** and **normalize** stages. You can control the size of the buffers and size and number of the files to fine-tune memory and cpu usage. Those settings impact parallelism as well, which is explained in the next chapter.

### Controlling in-memory buffers
`dlt` maintains in-memory buffers when writing intermediary files in the **extract** and **normalize** stages. The size of the buffers are controlled by specifying the number of data items held in them. Data is appended to open files when the item buffer is full, after which the buffer is cleared. You can the specify buffer size via environment variables or in `config.toml` to be more or less granular:
* set all buffers (both extract and normalize)
* set extract buffers separately from normalize buffers
* set extract buffers for particular source or resource

<!--@@@DLT_SNIPPET_START ./performance_snippets/toml-snippets.toml::buffer_toml-->
```toml
# set buffer size for extract and normalize stages
[data_writer]
buffer_max_items=100

# set buffers only in extract stage - for all sources
[sources.data_writer]
buffer_max_items=100

# set buffers only for a source with name zendesk_support
[sources.zendesk_support.data_writer]
buffer_max_items=100

# set buffers in normalize stage
[normalize.data_writer]
buffer_max_items=100
```
<!--@@@DLT_SNIPPET_END ./performance_snippets/toml-snippets.toml::buffer_toml-->

The default buffer is actually set to a moderately low value (**5000 items**), so unless you are trying to run `dlt`
on IOT sensors or other tiny infrastructures, you might actually want to increase it to speed up
processing.

### Controlling intermediary files size and rotation
`dlt` writes data to intermediary files. You can control the file size and the number of created files by setting the maximum number of data items stored in a single file or the maximum single file size. Keep in mind that the file size is computed after compression was performed.
* `dlt` uses a custom version of [`jsonl` file format](../dlt-ecosystem/file-formats/jsonl.md) between the **extract** and **normalize** stages.
* files created between the **normalize** and **load** stages are the same files that will be loaded to destination.

:::tip
The default setting is to not rotate the files so if you have a resource with millions of records, `dlt` will still create a single intermediary file to normalize and a single file to load. **If you want such data to be normalized and loaded in parallel you must enable file rotation as described below**
:::
:::note
Some file formats (ie. parquet) do not support schema changes when writing a single file and in that case they are automatically rotated when new columns are discovered.
:::

Below we set files to rotated after 100.000 items written or when the filesize exceeds 1MiB.

<!--@@@DLT_SNIPPET_START ./performance_snippets/toml-snippets.toml::file_size_toml-->
```toml
# extract and normalize stages
[data_writer]
file_max_items=100000
max_file_size=1000000

# only for the extract stage - for all sources
[sources.data_writer]
file_max_items=100000
max_file_size=1000000

# only for the extract stage of a source with name zendesk_support
[sources.zendesk_support.data_writer]
file_max_items=100000
max_file_size=1000000

# only for the normalize stage
[normalize.data_writer]
file_max_items=100000
max_file_size=1000000
```
<!--@@@DLT_SNIPPET_END ./performance_snippets/toml-snippets.toml::file_size_toml-->


### Disabling and enabling file compression
Several [text file formats](../dlt-ecosystem/file-formats/) have `gzip` compression enabled by default. If you wish that your load packages have uncompressed files (ie. to debug the content easily), change `data_writer.disable_compression` in config.toml. The entry below will disable the compression of the files processed in `normalize` stage.
<!--@@@DLT_SNIPPET_START ./performance_snippets/toml-snippets.toml::compression_toml-->
```toml
[normalize.data_writer]
disable_compression=true
```
<!--@@@DLT_SNIPPET_END ./performance_snippets/toml-snippets.toml::compression_toml-->

### Freeing disk space after loading

Keep in mind load packages are buffered to disk and are left for any troubleshooting, so you can [clear disk space by setting the `delete_completed_jobs` option](../running-in-production/running.md#data-left-behind).

### Observing cpu and memory usage
Please make sure that you have the `psutils` package installed (note that Airflow installs it by default). Then you can dump the stats periodically by setting the [progress](../general-usage/pipeline.md#display-the-loading-progress) to `log` in `config.toml`:
```toml
progress="log"
```
or when running the pipeline:
```sh
PROGRESS=log python pipeline_script.py
```

## Parallelism

### Extract
You can extract data concurrently if you write your pipelines to yield callables or awaitables that can be then evaluated in a thread or futures pool respectively.

Example below simulates a typical situation where a dlt resource is used to fetch a page of items and then details of individual items are fetched separately in the transformer. The `@dlt.defer` decorator wraps the `get_details` function in another callable that will be executed in the thread pool.
<!--@@@DLT_SNIPPET_START ./performance_snippets/performance-snippets.py::parallel_extract_callables-->
```py
import dlt
from time import sleep
from threading import currentThread

@dlt.resource
def list_items(start, limit):
    yield from range(start, start + limit)

@dlt.transformer
@dlt.defer
def get_details(item_id):
    # simulate a slow REST API where you wait 0.3 sec for each item
    sleep(0.3)
    print(f"item_id {item_id} in thread {currentThread().name}")
    # just return the results, if you yield, generator will be evaluated in main thread
    return {"row": item_id}


# evaluate the pipeline and print all the items
# resources are iterators and they are evaluated in the same way in the pipeline.run
print(list(list_items(0, 10) | get_details))
```
<!--@@@DLT_SNIPPET_END ./performance_snippets/performance-snippets.py::parallel_extract_callables-->

You can control the number of workers in the thread pool with **workers** setting. The default number of workers is **5**. Below you see a few ways to do that with different granularity
<!--@@@DLT_SNIPPET_START ./performance_snippets/toml-snippets.toml::extract_workers_toml-->
```toml
# for all sources and resources being extracted
[extract]
worker=1

# for all resources in the zendesk_support source
[sources.zendesk_support.extract]
workers=2

# for the tickets resource in the zendesk_support source
[sources.zendesk_support.tickets.extract]
workers=4
```
<!--@@@DLT_SNIPPET_END ./performance_snippets/toml-snippets.toml::extract_workers_toml-->


The example below does the same but using an async/await and futures pool:
<!--@@@DLT_SNIPPET_START ./performance_snippets/performance-snippets.py::parallel_extract_awaitables-->
```py
import asyncio

@dlt.transformer
async def a_get_details(item_id):
    # simulate a slow REST API where you wait 0.3 sec for each item
    await asyncio.sleep(0.3)
    print(f"item_id {item_id} in thread {currentThread().name}")
    # just return the results, if you yield, generator will be evaluated in main thread
    return {"row": item_id}


print(list(list_items(0, 10) | a_get_details))
```
<!--@@@DLT_SNIPPET_END ./performance_snippets/performance-snippets.py::parallel_extract_awaitables-->

You can control the number of async functions/awaitables being evaluate in parallel by setting **max_parallel_items**. The default number is *20**. Below you see a few ways to do that with different granularity
<!--@@@DLT_SNIPPET_START ./performance_snippets/toml-snippets.toml::extract_parallel_items_toml-->
```toml
# for all sources and resources being extracted
[extract]
max_parallel_items=10

# for all resources in the zendesk_support source
[sources.zendesk_support.extract]
max_parallel_items=10

# for the tickets resource in the zendesk_support source
[sources.zendesk_support.tickets.extract]
max_parallel_items=10
```
<!--@@@DLT_SNIPPET_END ./performance_snippets/toml-snippets.toml::extract_parallel_items_toml-->

:::note
**max_parallel_items** applies to thread pools as well. It sets how many items may be queued to be executed and currently executing in a thread pool by the workers. Imagine a situation where you have millions
of callables to be evaluated in a thread pool with a size of 5. This limit will instantiate only the desired amount of workers.
:::

:::caution
Generators and iterators are always evaluated in the main thread. If you have a loop that yields items, instead yield functions or async functions that will create the items when evaluated in the pool.
:::

### Normalize
The **normalize** stage uses a process pool to create load package concurrently. Each file created by the **extract** stage is sent to a process pool. **If you have just a single resource with a lot of data, you should enable [extract file rotation](#controlling-intermediary-files-size-and-rotation)**. The number of processes in the pool is controlled with `workers` config value:
<!--@@@DLT_SNIPPET_START ./performance_snippets/toml-snippets.toml::normalize_workers_toml-->
```toml
[extract.data_writer]
# force extract file rotation if size exceeds 1MiB
max_file_size=1000000

[normalize]
# use 3 worker processes to process 3 files in parallel
workers=3
```
<!--@@@DLT_SNIPPET_END ./performance_snippets/toml-snippets.toml::normalize_workers_toml-->

:::note
The default is to not parallelize normalization and to perform it in the main process.
:::

:::note
Normalization is CPU bound and can easily saturate all your cores. Never allow `dlt` to use all cores on your local machine.
:::

### Load
The **load** stage uses a thread pool for parallelization. Loading is input/output bound. `dlt` avoids any processing of the content of the load package produced by the normalizer. By default loading happens in 20 threads, each loading a single file.

As before, **if you have just a single table with millions of records you should enable [file rotation in the normalizer](#controlling-intermediary-files-size-and-rotation).**. Then  the number of parallel load jobs is controlled by the `workers` config setting.

<!--@@@DLT_SNIPPET_START ./performance_snippets/toml-snippets.toml::normalize_workers_2_toml-->
```toml
[normalize.data_writer]
# force normalize file rotation if it exceeds 1MiB
max_file_size=1000000

[load]
# have 50 concurrent load jobs
workers=50
```
<!--@@@DLT_SNIPPET_END ./performance_snippets/toml-snippets.toml::normalize_workers_2_toml-->

### Parallel pipeline config example
The example below simulates loading of a large database table with 1 000 000 records. The **config.toml** below sets the parallelization as follows:
* during extraction, files are rotated each 100 000 items, so there are 10 files with data for the same table
* the normalizer will process the data in 3 processes
* we use JSONL to load data to duckdb. We rotate JSONL files each 100 000 items so 10 files will be created.
* we use 11 threads to load the data (10 JSON files + state file)

<!--@@@DLT_SNIPPET_START ./performance_snippets/.dlt/config.toml::parallel_config_toml-->
```toml
# the pipeline name is default source name when loading resources

[sources.parallel_load.data_writer]
file_max_items=100000

[normalize]
workers=3

[data_writer]
file_max_items=100000

[load]
workers=11
```
<!--@@@DLT_SNIPPET_END ./performance_snippets/.dlt/config.toml::parallel_config_toml-->


<!--@@@DLT_SNIPPET_START ./performance_snippets/performance-snippets.py::parallel_config-->
```py
import os
import dlt
from itertools import islice
from dlt.common import pendulum

@dlt.resource(name="table")
def read_table(limit):
    rows = iter(range(limit))
    while item_slice := list(islice(rows, 1000)):
        now = pendulum.now().isoformat()
        yield [{"row": _id, "description": "this is row with id {_id}", "timestamp": now} for _id in item_slice]

# this prevents process pool to run the initialization code again
if __name__ == "__main__" or "PYTEST_CURRENT_TEST" in os.environ:
    pipeline = dlt.pipeline("parallel_load", destination="duckdb", full_refresh=True)
    pipeline.extract(read_table(1000000))

    # we should have 11 files (10 pieces for `table` and 1 for state)
    extracted_files = pipeline.list_extracted_resources()
    print(extracted_files)
    # normalize and print counts
    print(pipeline.normalize(loader_file_format="jsonl"))
    # print jobs in load package (10 + 1 as above)
    load_id = pipeline.list_normalized_load_packages()[0]
    print(pipeline.get_load_package_info(load_id))
    print(pipeline.load())
```
<!--@@@DLT_SNIPPET_END ./performance_snippets/performance-snippets.py::parallel_config-->

### Source decomposition for serial and parallel resource execution

You can decompose a pipeline into strongly connected components with
`source().decompose(strategy="scc")`. The method returns a list of dlt sources each containing a
single component. Method makes sure that no resource is executed twice.

**Serial decomposition:**

You can load such sources as tasks serially in order present of the list. Such DAG is safe for
pipelines that use the state internally.
[It is used internally by our Airflow mapper to construct DAGs.](https://github.com/dlt-hub/dlt/blob/devel/dlt/helpers/airflow_helper.py)

**Parallel decomposition**

If you are using only the resource state (which most of the pipelines really should!) you can run
your tasks in parallel.

- Perform the `scc` decomposition.
- Run each component in a pipeline with different but deterministic `pipeline_name` (same component
  \- same pipeline, you can use names of selected resources in source to construct unique id).

Each pipeline will have its private state in the destination and there won't be any clashes. As all
the components write to the same schema you may observe a that loader stage is attempting to migrate
the schema, that should be a problem though as long as your data does not create variant columns.

**Custom decomposition**

- When decomposing pipelines into tasks, be mindful of shared state.
- Dependent resources pass data to each other via generators - so they need to run on the same
  worker. Group them in a task that runs them together - otherwise some resources will be extracted twice.
- State is per-pipeline. The pipeline identifier is the pipeline name. A single pipeline state
  should be accessed serially to avoid losing details on parallel runs.


## Resources extraction, `fifo` vs. `round robin`

When extracting from resources, you have two options to determine what the order of queries to your
resources are: `fifo` and `round_robin`.

`fifo` is the default option and will result in every resource being fully extracted before the next
resource is extracted in the order that you added them to your source.

`round_robin` will result in extraction of one item from the first resource, then one item from the
second resource etc, doing as many rounds as necessary until all resources are fully extracted.

You can change this setting in your `config.toml` as follows:

<!--@@@DLT_SNIPPET_START ./performance_snippets/toml-snippets.toml::item_mode_toml-->
```toml
[extract] # global setting
next_item_mode="round_robin"

[sources.my_pipeline.extract] # setting for the "my_pipeline" pipeline
next_item_mode="fifo"
```
<!--@@@DLT_SNIPPET_END ./performance_snippets/toml-snippets.toml::item_mode_toml-->

## Use built in json parser
`dlt` uses **orjson** if available. If not it falls back to  **simplejson**. The built in parsers serialize several Python types:
- Decimal
- DateTime, Date
- Dataclasses

Import the module as follows
```python
from dlt.common import json
```

:::tip
**orjson** is fast and available on most platforms. It uses binary streams, not strings to load data natively.
- open files as binary, not string to use `load` and `dump`
- use `loadb` and `dumpb` methods to work with bytes without decoding strings
:::

## Using the built in requests client

`dlt` provides a customized [requests](https://requests.readthedocs.io/en/latest/) client with automatic retries and configurable timeouts.

We recommend using this to make API calls in your sources as it makes your pipeline more resilient to intermittent network errors and other random glitches which otherwise can cause the whole pipeline to fail.

The dlt requests client will additionally set the default user-agent header to `dlt/{DLT_VERSION_NAME}`.

For most use cases this is a drop in replacement for `requests`, so:

:heavy_multiplication_x: **Don't**

```python
import requests
```
:heavy_check_mark: **Do**

```python
from dlt.sources.helpers import requests
```

And use it just like you would use `requests`:

```python
response = requests.get('https://example.com/api/contacts', headers={'Authorization': MY_API_KEY})
data = response.json()
...
```

### Retry rules

By default failing requests are retried up to 5 times with an exponentially increasing delay. That means the first retry will wait 1 second and the fifth retry will wait 16 seconds.

If all retry attempts fail the corresponding requests exception is raised. E.g. `requests.HTTPError` or `requests.ConnectionTimeout`

All standard HTTP server errors trigger a retry. This includes:

* Error status codes:

    All status codes in the `500` range and `429` (too many requests).
    Commonly servers include a `Retry-After` header with `429` and `503` responses.
    When detected this value supersedes the standard retry delay.

* Connection and timeout errors

    When the remote server is unreachable, the connection is unexpectedly dropped or when the request takes longer than the configured `timeout`.

### Customizing retry settings

Many requests settings can be added to the runtime section in your `config.toml`. For example:

<!--@@@DLT_SNIPPET_START ./performance_snippets/toml-snippets.toml::retry_toml-->
```toml
[runtime]
request_max_attempts = 10  # Stop after 10 retry attempts instead of 5
request_backoff_factor = 1.5  # Multiplier applied to the exponential delays. Default is 1
request_timeout = 120  # Timeout in seconds
request_max_retry_delay = 30  # Cap exponential delay to 30 seconds
```
<!--@@@DLT_SNIPPET_END ./performance_snippets/toml-snippets.toml::retry_toml-->


For more control you can create your own instance of `dlt.sources.requests.Client` and use that instead of the global client.

This lets you customize which status codes and exceptions to retry on:

```python
from dlt.sources.helpers import requests

http_client = requests.Client(
    status_codes=(403, 500, 502, 503),
    exceptions=(requests.ConnectionError, requests.ChunkedEncodingError)
)
```

and you may even supply a custom retry condition in the form of a predicate.
This is sometimes needed when loading from non-standard APIs which don't use HTTP error codes.

For example:

```python
from dlt.sources.helpers import requests

def retry_if_error_key(response: Optional[requests.Response], exception: Optional[BaseException]) -> bool:
    """Decide whether to retry the request based on whether
    the json response contains an `error` key
    """
    if response is None:
        # Fall back on the default exception predicate.
        return False
    data = response.json()
    return 'error' in data

http_client = Client(
    retry_condition=retry_if_error_key
)
```

