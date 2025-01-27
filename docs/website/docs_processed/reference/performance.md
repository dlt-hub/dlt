---
title: Optimizing dlt
description: Scale-up, parallelize and finetune dlt pipelines
keywords: [scaling, parallelism, finetuning]
---

# Optimizing dlt

## Yield pages instead of rows

If possible, yield pages when producing data. This approach makes some processes more effective by reducing
the number of necessary function calls (each chunk of data that you yield goes through the extract pipeline once, so if you yield a chunk of 10,000 items, you will gain significant savings).
For example:
```py
import dlt

def get_rows(limit):
    yield from map(lambda n: {"row": n}, range(limit))

@dlt.resource
def database_cursor():
    # here we yield each row returned from database separately
    yield from get_rows(10000)
```

can be replaced with:

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


## Memory/disk management
`dlt` buffers data in memory to speed up processing and uses the file system to pass data between the **extract** and **normalize** stages. You can control the size of the buffers and the size and number of the files to fine-tune memory and CPU usage. These settings also impact parallelism, which is explained in the next chapter.

### Controlling in-memory buffers
`dlt` maintains in-memory buffers when writing intermediary files in the **extract** and **normalize** stages. The size of the buffers is controlled by specifying the number of data items held in them. Data is appended to open files when the item buffer is full, after which the buffer is cleared. You can specify the buffer size via environment variables or in `config.toml` to be more or less granular:
* set all buffers (both extract and normalize)
* set extract buffers separately from normalize buffers
* set extract buffers for a particular source or resource

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


The default buffer is actually set to a moderately low value (**5000 items**), so unless you are trying to run `dlt`
on IoT sensors or other tiny infrastructures, you might actually want to increase it to speed up
processing.

### Controlling intermediary file size and rotation
`dlt` writes data to intermediary files. You can control the file size and the number of created files by setting the maximum number of data items stored in a single file or the maximum single file size. Keep in mind that the file size is computed after compression has been performed.
* `dlt` uses a custom version of the [JSON file format](../dlt-ecosystem/file-formats/jsonl.md) between the **extract** and **normalize** stages.
* Files created between the **normalize** and **load** stages are the same files that will be loaded to the destination.

:::tip
The default setting is to not rotate the files, so if you have a resource with millions of records, `dlt` will still create a single intermediary file to normalize and a single file to load. **If you want such data to be normalized and loaded in parallel, you must enable file rotation as described below.**
:::
:::note
Some file formats (e.g., Parquet) do not support schema changes when writing a single file, and in that case, they are automatically rotated when new columns are discovered.
:::

Below, we set files to rotate after 100,000 items written or when the filesize exceeds 1MiB.

```toml
# extract and normalize stages
[data_writer]
file_max_items=100000
file_max_bytes=1000000

# only for the extract stage - for all sources
[sources.data_writer]
file_max_items=100000
file_max_bytes=1000000

# only for the extract stage of a source with name zendesk_support
[sources.zendesk_support.data_writer]
file_max_items=100000
file_max_bytes=1000000

# only for the normalize stage
[normalize.data_writer]
file_max_items=100000
file_max_bytes=1000000
```

### Disabling and enabling file compression
Several [text file formats](../dlt-ecosystem/file-formats/) have `gzip` compression enabled by default. If you wish that your load packages have uncompressed files (e.g., to debug the content easily), change `data_writer.disable_compression` in config.toml. The entry below will disable the compression of the files processed in the `normalize` stage.
```toml
[normalize.data_writer]
disable_compression=true
```


### Freeing disk space after loading

Keep in mind that load packages are buffered to disk and are left for any troubleshooting, so you can [clear disk space by setting the `delete_completed_jobs` option](../running-in-production/running.md#data-left-behind).

### Observing CPU and memory usage
Please make sure that you have the `psutil` package installed (note that Airflow installs it by default). Then, you can dump the stats periodically by setting the [progress](../general-usage/pipeline.md#display-the-loading-progress) to `log` in `config.toml`:
```toml
progress="log"
```
or when running the pipeline:
```sh
PROGRESS=log python pipeline_script.py
```

## Parallelism
You can create pipelines that extract, normalize, and load data in parallel.

### Extract
You can extract data concurrently if you write your pipelines to yield callables or awaitables, or use async generators for your resources that can then be evaluated in a thread or futures pool respectively.

This is easily accomplished by using the `parallelized` argument in the resource decorator.
Resources based on sync generators will execute each step (yield) of the generator in a thread pool, so each individual resource is still extracted one item at a time, but multiple such resources can run in parallel with each other.

Consider an example source that consists of 2 resources fetching pages of items from different API endpoints, and each of those resources is piped to transformers to fetch complete data items respectively.

The `parallelized=True` argument wraps the resources in a generator that yields callables to evaluate each generator step. These callables are executed in the thread pool. Transformers that are not generators (as shown in the example) are internally wrapped in a generator that yields once.

```py
import dlt
import time
from threading import currentThread

@dlt.resource(parallelized=True)
def list_users(n_users):
    for i in range(1, 1 + n_users):
        # Simulate network delay of a rest API call fetching a page of items
        if i % 10 == 0:
            time.sleep(0.1)
        yield i

@dlt.transformer(parallelized=True)
def get_user_details(user_id):
    # Transformer that fetches details for users in a page
    time.sleep(0.1)  # Simulate latency of a rest API call
    print(f"user_id {user_id} in thread {currentThread().name}")
    return {"entity": "user", "id": user_id}

@dlt.resource(parallelized=True)
def list_products(n_products):
    for i in range(1, 1 + n_products):
        if i % 10 == 0:
            time.sleep(0.1)
        yield i

@dlt.transformer(parallelized=True)
def get_product_details(product_id):
    time.sleep(0.1)
    print(f"product_id {product_id} in thread {currentThread().name}")
    return {"entity": "product", "id": product_id}

@dlt.source
def api_data():
    return [
        list_users(24) | get_user_details,
        list_products(32) | get_product_details,
    ]

# evaluate the pipeline and print all the items
# sources are iterators and they are evaluated in the same way in the pipeline.run
print(list(api_data()))
```


The `parallelized` flag in the `resource` and `transformer` decorators is supported for:

* Generator functions (as shown in the example)
* Generators without functions (e.g., `dlt.resource(name='some_data', parallelized=True)(iter(range(100)))`)
* `dlt.transformer` decorated functions. These can be either generator functions or regular functions that return one value

You can control the number of workers in the thread pool with the **workers** setting. The default number of workers is **5**. Below, you see a few ways to do that with different granularity.
```toml
# for all sources and resources being extracted
[extract]
workers=1

# for all resources in the zendesk_support source
[sources.zendesk_support.extract]
workers=2

# for the tickets resource in the zendesk_support source
[sources.zendesk_support.tickets.extract]
workers=4
```



The example below does the same but using an async generator as the main resource and async/await and futures pool for the transformer.
The `parallelized` flag is not supported or needed for async generators; these are wrapped and evaluated concurrently by default:
```py
import asyncio

@dlt.resource
async def a_list_items(start, limit):
    # simulate a slow REST API where you wait 0.3 sec for each item
    index = start
    while index < start + limit:
        await asyncio.sleep(0.3)
        yield index
        index += 1

@dlt.transformer
async def a_get_details(item_id):
    # simulate a slow REST API where you wait 0.3 sec for each item
    await asyncio.sleep(0.3)
    print(f"item_id {item_id} in thread {currentThread().name}")
    # just return the results, if you yield, generator will be evaluated in main thread
    return {"row": item_id}

print(list(a_list_items(0, 10) | a_get_details))
```


You can control the number of async functions/awaitables being evaluated in parallel by setting **max_parallel_items**. The default number is **20**. Below, you see a few ways to do that with different granularity.
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


:::note
**max_parallel_items** applies to thread pools as well. It sets how many items may be queued to be executed and currently executing in a thread pool by the workers. Imagine a situation where you have millions
of callables to be evaluated in a thread pool with a size of 5. This limit will instantiate only the desired amount of workers.
:::

:::caution
Generators and iterators are always evaluated in a single thread: item by item. If you have a loop that yields items that you want to evaluate
in parallel, instead yield functions or async functions that will be evaluated in separate threads or in an async pool.
:::

### Normalize
The **normalize** stage uses a process pool to create load packages concurrently. Each file created by the **extract** stage is sent to a process pool. **If you have just a single resource with a lot of data, you should enable [extract file rotation](#controlling-intermediary-files-size-and-rotation)**. The number of processes in the pool is controlled by the `workers` config value:
```toml
[extract.data_writer]
# force extract file rotation if size exceeds 1MiB
file_max_bytes=1000000

[normalize]
# use 3 worker processes to process 3 files in parallel
workers=3
```


:::note
The default is to not parallelize normalization and to perform it in the main process.
:::

:::note
Normalization is CPU-bound and can easily saturate all your cores. Never allow `dlt` to use all cores on your local machine.
:::

:::caution
The default method of spawning a process pool on Linux is **fork**. If you are using threads in your code (or libraries that use threads),
you should switch to **spawn**. Process forking does not respawn the threads and may destroy the critical sections in your code. Even logging
with Python loggers from multiple threads may lock the `normalize` step. Here's how you switch to **spawn**:
```toml
[normalize]
workers=3
start_method="spawn"
```
:::

### Load
The **load** stage uses a thread pool for parallelization. Loading is input/output-bound. `dlt` avoids any processing of the content of the load package produced by the normalizer. By default, loading happens in 20 threads, each loading a single file.

As before, **if you have just a single table with millions of records, you should enable [file rotation in the normalizer](#controlling-intermediary-files-size-and-rotation)**. Then the number of parallel load jobs is controlled by the `workers` config setting.

```toml
[normalize.data_writer]
# force normalize file rotation if it exceeds 1MiB
file_max_bytes=1000000

[load]
# have 50 concurrent load jobs
workers=50
```

The **normalize** stage in `dlt` uses a process pool to create load packages concurrently, and the settings for `file_max_items` and `file_max_bytes` play a crucial role in determining the size of data chunks. Lower values for these settings reduce the size of each chunk sent to the destination database, which is particularly helpful for managing memory constraints on the database server. By default, `dlt` writes all data rows into one large intermediary file, attempting to load all data at once. Configuring these settings enables file rotation, splitting the data into smaller, more manageable chunks. This not only improves performance but also minimizes memory-related issues when working with large tables containing millions of records.

#### Controlling destination items size
The intermediary files generated during the **normalize** stage are also used in the **load** stage. Therefore, adjusting `file_max_items` and `file_max_bytes` in the **normalize** stage directly impacts the size and number of data chunks sent to the destination, influencing loading behavior and performance.

### Parallel pipeline config example
The example below simulates the loading of a large database table with 1,000,000 records. The **config.toml** below sets the parallelization as follows:
* During extraction, files are rotated each 100,000 items, so there are 10 files with data for the same table.
* The normalizer will process the data in 3 processes.
* We use JSONL to load data to duckdb. We rotate JSONL files each 100,000 items so 10 files will be created.
* We use 11 threads to load the data (10 JSON files + state file).

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
        yield [
            {"row": _id, "description": "this is row with id {_id}", "timestamp": now}
            for _id in item_slice
        ]

# this prevents process pool to run the initialization code again
if __name__ == "__main__" or "PYTEST_CURRENT_TEST" in os.environ:
    pipeline = dlt.pipeline("parallel_load", destination="duckdb", dev_mode=True)
    pipeline.extract(read_table(1000000))

    load_id = pipeline.list_extracted_load_packages()[0]
    extracted_package = pipeline.get_load_package_info(load_id)
    # we should have 11 files (10 pieces for `table` and 1 for state)
    extracted_jobs = extracted_package.jobs["new_jobs"]
    print([str(job.job_file_info) for job in extracted_jobs])
    # normalize and print counts
    print(pipeline.normalize(loader_file_format="jsonl"))
    # print jobs in load package (10 + 1 as above)
    load_id = pipeline.list_normalized_load_packages()[0]
    print(pipeline.get_load_package_info(load_id))
    print(pipeline.load())
```




### Source decomposition for serial and parallel resource execution

You can decompose a pipeline into strongly connected components with
`source().decompose(strategy="scc")`. The method returns a list of dlt sources, each containing a
single component. The method ensures that no resource is executed twice.

**Serial decomposition:**

You can load such sources as tasks serially in the order presented in the list. Such a DAG is safe for
pipelines that use the state internally.
[It is used internally by our Airflow mapper to construct DAGs.](https://github.com/dlt-hub/dlt/blob/devel/dlt/helpers/airflow_helper.py)

**Parallel decomposition**

If you are using only the resource state (which most of the pipelines really should!), you can run
your tasks in parallel.

- Perform the `scc` decomposition.
- Run each component in a pipeline with a different but deterministic `pipeline_name` (same component
  \- same pipeline; you can use names of selected resources in the source to construct a unique id).

Each pipeline will have its private state in the destination, and there won't be any clashes. As all
the components write to the same schema, you may observe that the loader stage is attempting to migrate
the schema. That should not be a problem, though, as long as your data does not create variant columns.

**Custom decomposition**

- When decomposing pipelines into tasks, be mindful of shared state.
- Dependent resources pass data to each other via generators - so they need to run on the same
  worker. Group them in a task that runs them together - otherwise, some resources will be extracted twice.
- State is per-pipeline. The pipeline identifier is the pipeline name. A single pipeline state
  should be accessed serially to avoid losing details on parallel runs.


## Running several pipelines in parallel in a single process
You can run several pipeline instances in parallel from a single process by placing them in
separate threads. The most straightforward way is to use `ThreadPoolExecutor` and `asyncio` to execute pipeline methods.

```py
import asyncio
import dlt
from time import sleep
from concurrent.futures import ThreadPoolExecutor

# create both asyncio and thread parallel resources
@dlt.resource
async def async_table():
    for idx_ in range(10):
        await asyncio.sleep(0.1)
        yield {"async_gen": idx_}

@dlt.resource(parallelized=True)
def defer_table():
    for idx_ in range(5):
        sleep(0.1)
        yield idx_

def _run_pipeline(pipeline, gen_):
    # run the pipeline in a thread, also instantiate generators here!
    # Python does not let you use generators across threads
    return pipeline.run(gen_())

# declare pipelines in main thread then run them "async"
pipeline_1 = dlt.pipeline("pipeline_1", destination="duckdb", dev_mode=True)
pipeline_2 = dlt.pipeline("pipeline_2", destination="duckdb", dev_mode=True)

async def _run_async():
    loop = asyncio.get_running_loop()
    # from Python 3.9 you do not need explicit pool. loop.to_thread will suffice
    with ThreadPoolExecutor() as executor:
        results = await asyncio.gather(
            loop.run_in_executor(executor, _run_pipeline, pipeline_1, async_table),
            loop.run_in_executor(executor, _run_pipeline, pipeline_2, defer_table),
        )
    # results contains two LoadInfo instances
    print("pipeline_1", results[0])
    print("pipeline_2", results[1])

# load data
asyncio.run(_run_async())
# activate pipelines before they are used
pipeline_1.activate()
assert pipeline_1.last_trace.last_normalize_info.row_counts["async_table"] == 10
pipeline_2.activate()
assert pipeline_2.last_trace.last_normalize_info.row_counts["defer_table"] == 5
```


:::tip
Please note the following:
1. Do not run pipelines with the same name and working dir in parallel. State synchronization will not
work in that case.
2. When running in multiple threads and using [parallel normalize step](#normalize), use the **spawn**
process start method.
3. If you created the `Pipeline` object in the worker thread and you use it from another (i.e., the main thread),
call `pipeline.activate()` to inject the right context into the current thread.
:::

## Resources extraction, `fifo` vs. `round robin`

When extracting from resources, you have two options to determine the order of queries to your
resources: `round_robin` and `fifo`.

`round_robin` is the default option and will result in the extraction of one item from the first resource, then one item from the second resource, etc., doing as many rounds as necessary until all resources are fully extracted. If you want to extract resources in parallel, you will need to keep `round_robin`.

`fifo` is an option for sequential extraction. It will result in every resource being fully extracted until the resource generator is expired, or a configured limit is reached, then the next resource will be evaluated. Resources are extracted in the order that you added them to your source.

:::tip
Switch to `fifo` when debugging sources with many resources and connected transformers, for example [rest_api](../dlt-ecosystem/verified-sources/rest_api/index.md).
Your data will be requested in a deterministic and straightforward order - a given data item (i.e., a user record you got from an API) will be processed by all resources
and transformers until completion before starting with a new one.
:::

You can change this setting in your `config.toml` as follows:

```toml
[extract] # global setting
next_item_mode="round_robin"

[sources.my_pipeline.extract] # setting for the "my_pipeline" pipeline
next_item_mode="fifo"
```



## Use built-in JSON parser
`dlt` uses **orjson** if available. If not, it falls back to **simplejson**. The built-in parsers serialize several Python types:
- Decimal
- DateTime, Date
- Dataclasses

Import the module as follows:
```py
from dlt.common import json
```

:::tip
**orjson** is fast and available on most platforms. It uses binary streams, not strings, to load data natively.
- Open files as binary, not string, to use `load` and `dump`.
- Use `loadb` and `dumpb` methods to work with bytes without decoding strings.

You can switch to **simplejson** at any moment by (1) removing the **orjson** dependency or (2) setting the following env variable:
```sh
DLT_USE_JSON=simplejson
```
:::

## Using the built-in requests wrapper or RESTClient for API calls

Instead of using Python Requests directly, you can use the built-in [requests wrapper](../general-usage/http/requests) or [`RESTClient`](../general-usage/http/rest-client) for API calls. This will make your pipeline more resilient to intermittent network errors and other random glitches.


## Keep pipeline working folder in a bucket on constrained environments.
`dlt` stores extracted data in load packages in order to load them atomically. In case you extract a lot of data at once (ie. backfill) or
your runtime env has constrained local storage (ie. cloud functions) you can keep your data on a bucket by using [FUSE](https://github.com/libfuse/libfuse) or
any other option which your cloud provider supplies.

`dlt` users rename when saving files and  "committing" packages (folder rename). Those may be not supported on bucket filesystems. Often
`rename` is translated into `copy` automatically. In other cases `dlt` will fallback to copy itself.

In case of cloud function and gs bucket mounts, increasing the rename limit for folders is possible:
```hcl
volume_mounts {
    mount_path = "/usr/src/ingestion/pipeline_storage"
    name       = "pipeline_bucket"
  }
volumes {
  name = "pipeline_bucket"
  gcs {
    bucket    = google_storage_bucket.dlt_pipeline_data_bucket.name
    read_only = false
    mount_options = [
      "rename-dir-limit=100000"
    ]
  }
}
```
