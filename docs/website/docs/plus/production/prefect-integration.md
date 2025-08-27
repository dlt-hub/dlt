---
title: "Prefect Integration"
description: Run pipelines with Prefect
keywords: ["prefect", "pipeline", "retry", "trace", "logs", "artifacts", "progress", "callbacks"]
---
# Prefect Integration

dlt+ offers a few tools and helpers to make running dlt pipelines in prefect a seamless experience.

## Key features

- **Prefect Collector:** a dedicated way to do real-time [progress monitoring] and summary reports after each pipeline stage
- **Retry integration:** retries without loosing intermediate results, execute custom code between retries
- **Source decomposition flows:** Create tasks and flows that will execute your pipelines in parallel with the right settings

## Prefect Collector

The `PrefectCollector` creates rich artifacts in the Prefect UI to monitor dlt pipeline progress and results. Simply pass it to your pipeline's `progress` parameter to enable detailed monitoring.

```python
from dlt_plus._runner.prefect_collector import PrefectCollector

@task
def my_dlt_task():
    pipeline = dlt.pipeline(
        pipeline_name="my_pipeline",
        destination="duckdb",
        dataset_name="my_dataset",
        progress=PrefectCollector()
    )
    
    load_info = pipeline.run(source)
```

### Progress and Summary Reports

If the task is executed the `PrefectCollector` will create progress artifacts that will update
during a stage as well as summary report after a pipeline step has completed.
For example during extract you will see how many resources have been extracted and once the extract 
is completed you will see how many rows have been processed for each resource: 

![Prefect Extract Artifacts](https://storage.googleapis.com/dlt-blog-images/docs-prefect-extract-artifact.png)

Also, each summary report contains basic information about the pipeline, the destination the execution 
environment and system resources (CPU, memory) usage.

### Schema Change Reports

The `PrefectCollector` will also create artifacts when schema changes are detected.
This is useful to see which tables and columns have changed between runs.

![Prefect Schema Change Detection](https://storage.googleapis.com/dlt-blog-images/docs-prefect-schema-change-artifact.png)

### Logging

Prefect has built-in functionality to [include logs from other libraries](https://docs.prefect.io/v3/advanced/logging-customization#include-logs-from-other-libraries) and display them as part of their UI.

You can tell prefect to include `dlt`'s logs by setting the corresponding prefect environment variable, for example by adding this to your `.env` file:
```sh
PREFECT_LOGGING_EXTRA_LOGGERS=dlt
```

You should now see dlt's logs in the prefect UI and be able to [query them with the prefect CLI](https://docs.prefect.io/v3/how-to-guides/workflows/add-logging#access-logs-from-the-command-line).


## Runner integration

The `PrefectCollector` integrates seamlessly with the [dlt+ runner](../production/pipeline-runner.md). 

If you are running your pipelines with the dlt+ runner, information about the pipelines run configuration and the trace pipeline 
will also be included in the artifacts in the `Run Configuration` section.

### Pipeline Retries

Prefect retry-mechanism is not a perfect fit for dlt pipelines.
As data gets processed, `dlt` creates intermediate files and that are stored in the pipelines working directory. 
When a pipeline run fails and is retried with prefect that data might not be available anymore, e.g. if the flow is retried on a different worker node or
the code was run inside an ephemeral docker container.

To do so, you can use the dlt+ runners [retry configuration](../production/pipeline-runner#retry-policies) inside your prefect tasks or flows.
That way the pipeline state will be preserved and intermediate results across retry attempts.

```python
from dlt_plus import runner
from tenacity import Retrying, stop_after_attempt
from dlt.sources.sql_database import sql_database

MY_RETRY_POLICY = Retrying(stop=stop_after_attempt(3), reraise=True)

@task(log_prints=True) # use log_prints=True to dlt.progress outputs in the logs of the prefect ui
def resilient_pipeline_task():
    pipeline = dlt.pipeline("my_pipeline", destination="duckdb", progress=PrefectCollector())
    source = sql_database(table_names=["items"])
    
    load_info = runner(
        pipeline, 
        retry_policy=MY_RETRY_POLICY,
        retry_pipeline_steps=["extract", "normalize", "load"]
    ).run(source)
    
    return load_info
```

Detailed error artifacts with full tracebacks are automatically created for each retry attempt as well as a final report.

![Prefect Retry Artifacts](https://storage.googleapis.com/dlt-blog-images/docs-prefect-retry-artifacts.png)

## Custom Callbacks

The `PrefectCollector` is a subclass of the `PlusLogCollector` class, which allows you to implement [custom callbacks](../production/observability#custom-callbacks).

```python
from dlt_plus._runner.prefect_collector import PrefectCollector
from prefect.logging import get_run_logger
from tenacity import Retrying, stop_after_attempt
from dlt.sources.sql_database import sql_database

MY_RETRY_POLICY = Retrying(stop=stop_after_attempt(3), reraise=True)

class MyCustomCollector(PrefectCollector):
    def on_retry(
        self,
        pipeline: SupportsPipeline,
        trace: PipelineTrace,
        retry_attempt: int,
        last_attempt: bool,
        exception: Exception,
    ) -> None:
        # your code here will be executed before a retry is attempted
        ...

@task
def my_task():
    # instantiate your logger with a task-aware logger from prefect
    prefect_logger = get_run_logger()
    my_collector = MyCustomCollector(1, logger=prefect_logger)

    # pass your collector to the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="my_pipeline",
        destination="dummy", # use dummy destination to trigger failed load
        progress=my_collector
    )
    # run your pipeline with the source in the dlt_plus.runner()
    source = sql_database(table_names=["items"])
    load_info = dlt_plus.runner(pipeline, retry_policy=MY_RETRY_POLICY).run(source)
    return load_info
```

## Source decomposition

dlt+ also provides helpers that you can use to create tasks and flows that you can drop 
into your prefect code with the options to parameterize the runner and run the resources in your source
in parallel tasks.

```py
import dlt
import dlt_plus
from dlt_plus._runner.prefect_helpers import create_run_pipeline_flow
from prefect.task_runners import ThreadPoolTaskRunner
from prefect import flow, task

@flow(task_runner=ThreadPoolTaskRunner(max_workers=5))
def my_main_flow():
    # define your pipeline and source inside the flow
    pipeline = dlt.pipeline(
        pipeline_name="my_pipeline",
        destination=dlt.destinations.duckdb(destination_name="my_destination"),
        dataset_name="my_dataset",
    )
    source = sql_database(schema="my_schema")

    # create a subflow that will run each resource in a dedicated pipeline
    run_pipeline_with_parallel_tables = create_run_pipeline_flow(
        pipeline=pipeline,
        source=source,
        decompose=True,
        flow_name="my_parallelized_flow",
    )

    # run it, passing args that you would usually pass to the pipeline.run() method
    run_pipeline_with_parallel_tables(write_disposition="append")

if __name__ == "__main__":
    my_main_flow()

```

This will derive a task-pipeline with a deterministic name for each resource in the source and run them in parallel.
Because they have different names, pipelines can run in parallel without interfering with each other.

You can read more about source decomposition and what to watch out for when using it in the [performance section](../../reference/performance#source-decomposition-for-serial-and-parallel-resource-execution).

![Prefect Source Decomposition Flow](https://storage.googleapis.com/dlt-blog-images/docs-prefect-source-decomposition.png)

:::note
For stability reasons, this actually runs one resource alone and then all others in parallel.
This is because otherwise, on the first run, all resources would try to create the same dlt-tables.
:::
:::warning
The flow created by this helper must be executed with the thread pool task runner.
Other runners, which try to distribute tasks across multiple machines won't work with these helpers, because they
pass pipeline and source objects as arguments, which can not be pickled or serialized in order to be sent to other machines.
If want to build tasks that run on multiple machines, you will have to initialize both pipeline and sources inside those tasks.
:::

## On parallelization

Both `dlt` and prefect can be configured to use multithreading and multiprocessing, but at different stages of the pipeline. 
This is a short summary of parallelization in `dlt` that you should be aware of 
if you are thinking about how to parallelize your pipelines in prefect.
Read [this page](../../reference/performance#parallelism-within-a-pipeline) for a detailed explanation.

During the extraction phase, `dlt` can use multithreading to parallelize the extraction of resources and, if supported, to evaluate each `yield` of the resource generator in a separate thread.
Multiprocessing is used during the normalization phase, which is CPU intensive. The extracted data gets split into files and each process can normalize one file at a time.
During the load phase, `dlt` uses multithreading to parallelize the loading of resources, where each file can be loaded in a separate thread. The default behavior is to 
use a threadpool with 30 threads.
Whether or not these phases are parallelized is controlled by setting the respective environment variables.

Prefect supports either multithreading or multiprocessing, in that tasks inside a flow can be submitted to either a threadpool (using `task_runner=ThreadPoolTaskRunner`) or a processpool, for example when using the `DaskRunner` from `prefect-dask`-library.

So if you're machine running prefect has multiple cores, you can use both multithreading and multiprocessing.
Be aware though that the amount of workers multipy between the prefect and dlt.

For example, this code will run three threads each creating a threadpool with 10 workers, so 30 threads in total:

```py
from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner

@task
def my_task(endpoint: str)
    os.environ["LOAD__WORKERS"] = "10" # << this will be applied to each task
    # your code that instantiates a dlt pipeline and source based on some piece of data


@flow(task_runner=ThreadPoolTaskRunner(max_workers=3))
def my_flow():
    table_names = ["table1", "table2", "table3"]
    futures = []
    for table_name in table_names:
        futures.append(my_task.submit(table_name))

    wait(futures)

```