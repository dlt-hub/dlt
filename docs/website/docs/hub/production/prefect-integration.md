---
title: "Prefect Integration"
description: Run pipelines with Prefect
keywords: ["prefect", "retry", "logs", "artifacts", "progress", "callbacks", "source decomposition", "parallelization"]
---
# Prefect Integration

dlt+ offers a few tools and helpers to make running dlt pipelines in prefect a seamless experience.

## Key features

- **Prefect Collector:** a dedicated way to do real-time progress monitoring and summary reports after each pipeline stage
- **Retry integration:** retries without losing intermediate results, execute custom code between retries
- **Source decomposition flows:** Create tasks and flows that will execute your pipelines in parallel with the right settings

## Prefect Collector

The `PrefectCollector` creates rich [artifacts](https://docs.prefect.io/v3/concepts/artifacts#artifacts) in the Prefect UI to monitor dlt pipeline progress and results.
Artifacts are one of Prefect's ways to visualize outputs or side effects that your runs produce, capture updates over time or attach metadata to your runs.
`dlt` artifacts include information about the processed data, resource consumption, runtime environment and the pipeline configuration.

Simply pass the `PrefectCollector` to your pipeline's `progress` parameter.

```py
from dlthub._runner.prefect_collector import PrefectCollector

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

### Progress and Summary Reports by pipeline stage

If the task is executed the `PrefectCollector` will create progress artifacts that will update
during a stage as well as summary report after a pipeline step has completed.
For example during extract you will see how many resources have been extracted and once the extract 
is completed you will see how many rows have been processed for each resource: 

![Prefect Extract Artifacts](https://storage.googleapis.com/dlt-blog-images/docs-prefect-extract-artifact.png)

Each summary report includes basic information about the pipeline, the destination, the execution, and system resource usage (CPU and memory).

### Schema Change Reports

The `PrefectCollector` will also create special artifacts when schema changes are detected.
For example, if a new table is created or an existing table is updated, the `PrefectCollector` will create a markdown-report that includes the complete definition of the new table or the updated columns.

This is useful to see which tables and columns have changed between runs.

![Prefect Schema Change Detection](https://storage.googleapis.com/dlt-blog-images/docs-prefect-schema-change-artifact.png)

### Logging

Prefect has built-in functionality to [include logs from other libraries](https://docs.prefect.io/v3/advanced/logging-customization#include-logs-from-other-libraries) and display them as part of their UI.

You can tell prefect to include `dlt`'s logs by setting the corresponding prefect configuration value:
<Tabs
  groupId="config-type"
  defaultValue="prefect-cli"
  values={[
    {"label": ".env", "value": "env"},
    {"label": "prefect-cli", "value": "prefect-cli"}
  ]}>

<TabItem value="env">
```sh
PREFECT_LOGGING_EXTRA_LOGGERS=dlt
```
</TabItem>
<TabItem value="prefect-cli">
```sh
prefect config set PREFECT_LOGGING_EXTRA_LOGGERS=dlt
```
</TabItem>
</Tabs>

You should now see dlt's logs in the prefect UI and be able to [query them with the prefect CLI](https://docs.prefect.io/v3/how-to-guides/workflows/add-logging#access-logs-from-the-command-line).


## Runner integration

The `PrefectCollector` integrates seamlessly with the [dlt+ runner](../production/pipeline-runner.md). 

If you are running your pipelines with the dlt+ runner, information about the pipeline's run configuration and the trace pipeline 
will also be included in the artifacts in the `Run Configuration` section.

### Pipeline Retries

Both dlt+ and Prefect provide retry mechanisms, but they are not compatible out-of-the-box.
Most importantly, the dlt+ Runner implements retries on the same filesystem as the original run.
This is relevant because `dlt` generates intermediate files in the [pipeline working directory](../../general-usage/pipeline#pipeline-working-directory), which represent the intermediate results of the pipeline steps.
With Prefect, whether or not a retry runs on the same filesystem depends on your infrastructure and your deployment.
For example, if you are running code in ephemeral docker containers and your pipeline fails during the load-step, this working directory will be gone and the next retry will have to do the extraction and normalization steps again.

If you use the dlt+ Runner the retry will pick up from where the previous run left off, saving you much time and compute.

To do so, [define a retry policy](../production/pipeline-runner#retry-policies) and run your pipelines with the dlt+ runner inside your prefect tasks or flows.

```py
from dlthub import runner
from tenacity import Retrying, stop_after_attempt
from dlt.sources.sql_database import sql_database

MY_RETRY_POLICY = Retrying(stop=stop_after_attempt(3), reraise=True)

@task(log_prints=True) # use log_prints=True to see dlt.progress outputs in the logs of the prefect ui
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

The `PrefectCollector` is a subclass of the `PlusLogCollector` class, which allows you to implement [custom callbacks](../production/observability.md).

```py
from dlthub._runner.prefect_collector import PrefectCollector
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
    # instantiate your collector with a task-aware logger from prefect
    prefect_logger = get_run_logger()
    my_collector = MyCustomCollector(1, logger=prefect_logger)

    # pass your collector to the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="my_pipeline",
        destination="dummy", # use dummy destination to trigger failed load
        progress=my_collector
    )
    # run your pipeline with the source in the dlthub.runner()
    source = sql_database(table_names=["items"])
    load_info = dlthub.runner(pipeline, retry_policy=MY_RETRY_POLICY).run(source)
    return load_info
```

## Source decomposition

dlt+ also provides helpers that you can use to create tasks and flows that you can drop 
into your prefect code with the options to parameterize the runner and run the resources in your source
in parallel tasks.

```py
import dlt
import dlthub
from dlthub._runner.prefect_helpers import create_run_pipeline_flow
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

You can read more about source decomposition and what to watch out for when using it on the [performance page](../../reference/performance#source-decomposition-for-serial-and-parallel-resource-execution).

![Prefect Source Decomposition Flow](https://storage.googleapis.com/dlt-blog-images/docs-prefect-source-decomposition.png)

:::note
For stability reasons, this actually runs one resource alone and then all others in parallel.
This prevents multiple resources from attempting to create the same dlt tables simultaneously during the initial run.
:::
## On parallelization

Both `dlt` and Prefect are implementing parallelization in multiple ways and can complement each other.
While `dlt` options can be used to optimize the execution of a single pipeline, Prefect's features can be used to run multiple pipelines at the same time.

`dlt` will use different parallelism strategies for each stage, depending on the stage and the source, which you can read in detail about on the [performance page](../../reference/performance#parallelism-within-a-pipeline).
These are the key facts that you need to be aware of if you want to parallelize your pipelines in prefect:
During extraction, `dlt` can use multithreading to parallelize the extraction of resources and, if supported by the source, to evaluate each `yield` of the resource generator in a separate thread (`parallelized=True` in the resource decorator).
During normalize, which is usually CPU intensive, `dlt` will use multiprocessing to normalize the data in separate processes. During load, `dlt` will use multithreading to parallelize the loading of resources, where each file can be loaded in a separate thread. The default behavior is to use a threadpool with 30 threads.
Since version 1.13.0, `dlt` will detect automatically if it's executed inside prefect and spawn subprocesses with the correct method, so all parallelization features are available just like in your local environment.
You can use the usual configuration options (env-vars or `config.toml`) or use the corresponding parameters of the prefect helpers.

<Tabs
  groupId="config-type"
  defaultValue="env"
  values={[
    {"label": ".env", "value": "env"},
    {"label": "config.toml", "value": "config.toml"},
    {"label": "python", "value": "python"}
  ]}>

<TabItem value="env">

```sh
# for all pipelines
EXTRACT__WORKERS=30

# pipeline specific
MY_PIPELINE_NAME__NORMALIZE__WORKERS=3  # 3 processes for normalization
MY_PIPELINE_NAME__NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS=10000  # max 10k items per file
MY_PIPELINE_NAME__NORMALIZE__DATA_WRITER__FILE_MAX_BYTES=1000000  # max 1MB per file
```

</TabItem>
<TabItem value="config.toml">
```toml
[extract]
workers=30

[my_pipeline_name.normalize]
workers=3
file_max_items=10000  # max 10k items per file
file_max_bytes=1000000  # max 1MB per file
```
</TabItem>
<TabItem value="python">
```py
from dlthub._runner.prefect_helpers import create_run_pipeline_flow
from prefect.task_runners import ThreadPoolTaskRunner
from prefect import flow, task

@flow(task_runner=ThreadPoolTaskRunner(max_workers=5))
def my_main_flow():
    # define your pipeline and source inside the flow
    pipeline = dlt.pipeline("my_pipeline", destination="duckdb", dataset_name="my_dataset")
    source = sql_database(schema="my_schema").parallelize()

    # create a subflow that will decompose the source and run each resource of it with an isolated
    # task pipeline using the given parallelism settings
    decompose_flow = create_run_pipeline_flow(
        pipeline=pipeline,
        source=source,
        decompose=True,
        extract_workers=10,      # 10 threads for extraction
        normalize_workers=3,     # 3 processes for normalization
        file_max_items=10000,    # max 10k items per file
        file_max_bytes=1000000,  # max 1MB per file
        flow_name="my_parallelized_flow",
    )
    # run the flow
    load_info = decompose_flow()
```

</TabItem>
</Tabs>

The infrastructure you run your Prefect jobs on must have multiple cores in order to actually benefit from `dlt`'s multiprocessing.

Prefect implements parallelization by using either a threadpool or a processpool to execute tasks in parallel, as controlled by the `task_runner` parameter of the flow.
For `dlt`, we suggest that a task should be running one pipeline with one datasource (read [this](https://docs.prefect.io/v3/concepts/flows#why-both-flows-and-tasks) for more details on how to use tasks and flows).
In the above example, we create a subflow with tasks for each resource in the source and run up to five task pipelines in parallel, each of which uses multiple threads and processes for extraction and normalization.

:::note
The flow and tasks created by these helpers must be executed with the `ThreadPoolTaskRunner`.
Other runners, such as the `DaskRunner` from `prefect-dask` library, which distribute tasks across multiple machines won't work with these helpers, because they
pass pipeline and source objects as arguments, which cannot be pickled or serialized in order to be sent to other machines.
If you want to build tasks that run on multiple machines, you have to write your tasks in a way that both pipeline and sources can be initialized inside those tasks.
:::
