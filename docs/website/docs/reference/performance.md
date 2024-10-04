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
<!--@@@DLT_SNIPPET ./performance_snippets/performance-snippets.py::performance_chunking-->

can be replaced with:

<!--@@@DLT_SNIPPET ./performance_snippets/performance-snippets.py::performance_chunking_chunk-->


## Memory/disk management
`dlt` buffers data in memory to speed up processing and uses the file system to pass data between the **extract** and **normalize** stages. You can control the size of the buffers and the size and number of the files to fine-tune memory and CPU usage. These settings also impact parallelism, which is explained in the next chapter.

### Controlling in-memory buffers
`dlt` maintains in-memory buffers when writing intermediary files in the **extract** and **normalize** stages. The size of the buffers is controlled by specifying the number of data items held in them. Data is appended to open files when the item buffer is full, after which the buffer is cleared. You can specify the buffer size via environment variables or in `config.toml` to be more or less granular:
* set all buffers (both extract and normalize)
* set extract buffers separately from normalize buffers
* set extract buffers for a particular source or resource

<!--@@@DLT_SNIPPET ./performance_snippets/toml-snippets.toml::buffer_toml-->


The default buffer is actually set to a moderately low value (**5000 items**), so unless you are trying to run `dlt`
on IoT sensors or other tiny infrastructures, you might actually want to increase it to speed up
processing.

### Controlling intermediary file size and rotation
`dlt` writes data to intermediary files. You can control the file size and the number of created files by setting the maximum number of data items stored in a single file or the maximum single file size. Keep in mind that the file size is computed after compression has been performed.
* `dlt` uses a custom version of the [`jsonl` file format](../dlt-ecosystem/file-formats/jsonl.md) between the **extract** and **normalize** stages.
* Files created between the **normalize** and **load** stages are the same files that will be loaded to the destination.

:::tip
The default setting is to not rotate the files, so if you have a resource with millions of records, `dlt` will still create a single intermediary file to normalize and a single file to load. **If you want such data to be normalized and loaded in parallel, you must enable file rotation as described below.**
:::
:::note
Some file formats (e.g., Parquet) do not support schema changes when writing a single file, and in that case, they are automatically rotated when new columns are discovered.
:::

Below, we set files to rotate after 100,000 items written or when the filesize exceeds 1MiB.

<!--@@@DLT_SNIPPET ./performance_snippets/toml-snippets.toml::file_size_toml-->



### Disabling and enabling file compression
Several [text file formats](../dlt-ecosystem/file-formats/) have `gzip` compression enabled by default. If you wish that your load packages have uncompressed files (e.g., to debug the content easily), change `data_writer.disable_compression` in config.toml. The entry below will disable the compression of the files processed in the `normalize` stage.
<!--@@@DLT_SNIPPET ./performance_snippets/toml-snippets.toml::compression_toml-->


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

<!--@@@DLT_SNIPPET ./performance_snippets/performance-snippets.py::parallel_extract_callables-->


The `parallelized` flag in the `resource` and `transformer` decorators is supported for:

* Generator functions (as shown in the example)
* Generators without functions (e.g., `dlt.resource(name='some_data', parallelized=True)(iter(range(100)))`)
* `dlt.transformer` decorated functions. These can be either generator functions or regular functions that return one value

You can control the number of workers in the thread pool with the **workers** setting. The default number of workers is **5**. Below, you see a few ways to do that with different granularity.
<!--@@@DLT_SNIPPET ./performance_snippets/toml-snippets.toml::extract_workers_toml-->



The example below does the same but using an async generator as the main resource and async/await and futures pool for the transformer.
The `parallelized` flag is not supported or needed for async generators; these are wrapped and evaluated concurrently by default:
<!--@@@DLT_SNIPPET ./performance_snippets/performance-snippets.py::parallel_extract_awaitables-->


You can control the number of async functions/awaitables being evaluated in parallel by setting **max_parallel_items**. The default number is **20**. Below, you see a few ways to do that with different granularity.
<!--@@@DLT_SNIPPET ./performance_snippets/toml-snippets.toml::extract_parallel_items_toml-->


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
<!--@@@DLT_SNIPPET ./performance_snippets/toml-snippets.toml::normalize_workers_toml-->


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

<!--@@@DLT_SNIPPET ./performance_snippets/toml-snippets.toml::normalize_workers_2_toml-->

Since the normalize stage uses a process pool to create load packages concurrently, adjusting the `file_max_items` and `file_max_bytes` settings can significantly impact load behavior. By setting a lower value for `file_max_items`, you reduce the size of each data chunk sent to the destination database, which can be particularly useful for managing memory constraints on the database server. Without explicit configuration of `file_max_items`, `dlt` writes all data rows into one large intermediary file, attempting to insert all data from this single file. Configuring `file_max_items` ensures data is inserted in manageable chunks, enhancing performance and preventing potential memory issues.

### Parallel pipeline config example
The example below simulates the loading of a large database table with 1,000,000 records. The **config.toml** below sets the parallelization as follows:
* During extraction, files are rotated each 100,000 items, so there are 10 files with data for the same table.
* The normalizer will process the data in 3 processes.
* We use JSONL to load data to duckdb. We rotate JSONL files each 100,000 items so 10 files will be created.
* We use 11 threads to load the data (10 JSON files + state file).

<!--@@@DLT_SNIPPET ./performance_snippets/.dlt/config.toml::parallel_config_toml-->



<!--@@@DLT_SNIPPET ./performance_snippets/performance-snippets.py::parallel_config-->




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

<!--@@@DLT_SNIPPET ./performance_snippets/performance-snippets.py::parallel_pipelines-->


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

<!--@@@DLT_SNIPPET ./performance_snippets/toml-snippets.toml::item_mode_toml-->



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

