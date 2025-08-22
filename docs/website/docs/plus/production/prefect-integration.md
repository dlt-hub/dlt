---
title: "Prefect Integration"
description: Run pipelines with Prefect
keywords: ["prefect", "pipeline", "retry", "trace", "logs", "artifacts", "progress", "callbacks"]
---
# Prefect Integration

dlt+ offers a few tools and helpers to make running dlt pipelines in prefect a seamless experience.

## Key features

- **Prefect Collector:** a dedicated way to do [progress monitoring], creating summary reports for each pipeline stage
- **Retry integration:** run custom code at relevant points during the pipeline execution
- **Parameterized tasks and flows:** Create tasks and flows that will execute your pipelines with the right settings
- **dlt_task decorator:** parameteriziable prefect-task that reports dlt's progress to the prefect ui


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

![Prefect Extract Artifacts](images/prefect-extract-artifact.png)

Also, each summary report contains basic information about the pipeline, the destination the execution 
environment and system resources (CPU, memory) usage.

### Schema Change Reports

The `PrefectCollector` will also create artifacts when schema changes are detected.
This is useful to see which tables and columns have changed between runs.

![Prefect Schema Change Detection](images/prefect-schema-change-artifact.png)

## Runner integration

The `PrefectCollector` integrates seamlessly with the [dlt+ runner](../production/pipeline-runner.md). 

If you are running your pipelines with the dlt+ runner, information about the pipelines run configuration and the trace pipeline 
will also be included in the artifacts.

### Pipeline Retries

When running Prefect on distributed infrastructure, prefect's retries may execute on different machines, causing intermediate files to be lost and pipeline have to start over.

Use the dlt+ runner instead to preserve pipeline state and intermediate results across retry attempts.

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

![Prefect Retry Artifacts](images/prefect-retry-artifacts.png)

## Helpers to create tasks and flows

todo: use file prefect_helpers.py and use them in your code, todo copy examples from prefcet_helpers_demo.py

## `@dlt_task` decorator
dlt+ provides a customizable `@dlt_task` for decorating methods that executes dlt-pipelines
Prefect doesn't use an explicit DAG, but instead uses control flow of the users code. Units of work are defined by decorating functions with `@task` or `@flow`.

### Parallelization



