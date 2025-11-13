---
title: "Pipeline runner"
description: Run pipelines with the dltHub runner
keywords: ["runner", "pipeline", "retry", "trace"]
---
# Runner

dltHub provides a production-ready runner for your pipelines. It offers robust error handling, retry mechanisms, and near atomic trace storage to destinations of your choice.

## Usage

The runner will be used automatically if you do `dlt pipeline run` inside a project, or you can use
`dlt.hub.runner()` directly in your code where you define your pipeline and data.

<Tabs
  groupId="config-type"
  defaultValue="cli"
  values={[
    {"label": "CLI", "value": "cli"},
    {"label": "Python", "value": "python"}
  ]}>

<TabItem value="cli">

```sh
dlt pipeline my_pipeline run
```

</TabItem>

<TabItem value="python">

```py
import dlt

pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")

@dlt.resource(table_name="my_table")
def my_resource():
    return [1, 2, 3]

load_info = dlt.hub.runner(pipeline).run(my_resource())
print(load_info)
```

</TabItem>

</Tabs>

## Configuration

The runner is configured through the `run_config` section of a pipeline in your `dlt.yml` file
or by passing arguments to the `dlt.hub.runner()` function in your code.
Configuration via environment variables or `config.toml` is still under development.

### Complete configuration example

<Tabs
  groupId="config-type"
  defaultValue="yml"
  values={[
    {"label": "dlt.yml", "value": "yml"},
    {"label": "Python", "value": "python"}
  ]}>

<TabItem value="yml">

```yaml
pipelines:
  my_pipeline:
    source: my_source
    destination: duckdb
    run_config:
      store_trace_info: true
      run_from_clean_folder: true
      retry_policy:
        type: backoff
        max_attempts: 5
        multiplier: 2
        min: 1
        max: 10
      retry_pipeline_steps: ["load"]
```

</TabItem>

<TabItem value="python">

```py
import dlt
from tenacity import Retrying, stop_after_attempt, wait_exponential

pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")

@dlt.resource(table_name="my_table")
def my_resource():
    return [1, 2, 3]

load_info = dlt.hub.runner(
    pipeline,
    store_trace_info=True,
    run_from_clean_folder=True,
    retry_policy=Retrying(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=1, max=10),
        reraise=True
    ),
    retry_pipeline_steps=["load"]
).run(my_resource())
```

</TabItem>

</Tabs>



## Trace storage

The `store_trace_info` parameter enables automatic storage of the pipeline's runtime [trace](https://github.com/dlt-hub/dlt/blob/273420b2574a518a7488443253ab1e0971b136e8/dlt/pipeline/trace.py#L126), which contains detailed information about a run, e.g., timings of each step, schema changes, and load packages (see [here](../../running-in-production/running#inspect-and-save-the-load-info-and-trace)).

The runner will convert the trace into a `dict` and try loading it to the destination using a separate pipeline, which runs directly after each successful or failed attempt of the main pipeline. If any pending data is finalized before running the main pipeline, the trace of that finalization is also stored.

Setting `store_trace_info=True` will derive the configuration of the trace pipeline from the main pipeline.
That trace pipeline will be named `_trace_<pipeline_name>` and will write to the same destination as the main pipeline.

```yaml
pipelines:
  my_pipeline:
    run_config:
      store_trace_info: true
```

Alternatively, you can explicitly define a trace pipeline for example, if you want 
to use store your traces in a different filesystem than your production data:

<Tabs
  groupId="config-type"
  defaultValue="yml"
  values={[
    {"label": "dlt.yml", "value": "yml"},
    {"label": "Python", "value": "python"}
  ]}>

<TabItem value="yml">

```yaml
destination:
  log_filesystem:
    type: filesystem
    bucket_url: "s3://my-bucket/logs/dlt_traces"

pipelines:
  my_pipeline:
    source: my_source
    destination: duckdb
    run_config:
      store_trace_info: trace_pipeline
    
  trace_pipeline:
    source: my_source # << this will not actually be used but cannot be empty
    destination: log_filesystem
```

</TabItem>

<TabItem value="python">
```py
import dlt
from tenacity import Retrying, stop_after_attempt

@dlt.resource(table_name="numbers")
def my_resource():
    return [1, 2, 3]

# your main pipeline
pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")

# your trace pipeline
# os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "s3://my-bucket/logs/dlt_traces"
trace_pipeline = dlt.pipeline(
    pipeline_name="my_trace_pipeline",
    destination="filesystem",
    dataset_name="my_pipeline_trace_dataset",
)

load_info = dlt.hub.runner(pipeline, store_trace_info=trace_pipeline).run(my_resource())
print(load_info)
```
</TabItem>

</Tabs>

### Trace table naming

Traces are loaded into a table named `<pipeline_name>_trace`. This means you can use the same trace pipeline for 
multiple pipelines without conflicts.

## Run from clean folder

When the `run_from_clean_folder` option is enabled, the [pipeline working directory](../../general-usage/pipeline#pipeline-working-directory) is removed before the pipeline runs. The state, schema, and all files from previous runs are deleted, and state and schema are synchronized from the destination (see [dlt pipeline sync](../../reference/command-line-interface.md#dlt-pipeline-sync)).

```yaml
pipelines:
  my_pipeline:
    run_config:
      run_from_clean_folder: true
```

:::note
The dltHub runner behaves slightly differently from `pipeline.run()` when there exists pending data in the pipeline's working directory: `pipeline.run()` will load only the pending data and needs to be invoked again, whereas the dltHub runner will run with the pending data, and then automatically run again with the given data.
:::

## Retry policies

The Runner supports configurable retry policies to handle errors during pipeline execution. Retry policies apply to both finalizing pending data from previous loads and running the pipeline with the given data.

For the python interface you can define any retry policy you want using the [tenacity](https://tenacity.readthedocs.io/en/latest/) library. For the yaml interface you can configure three different retry policies:

| Policy type | Description | Configuration |
|-------------|-------------|---------------|
| None | No retry (single attempt) | `type: none` |
| Fixed | Fixed number of attempts with no backoff | `type: fixed`, `max_attempts: N` |
| Backoff | Exponential backoff with configurable parameters | `type: backoff`, `max_attempts: N`, `multiplier: N`, `min: N`, `max: N` |

:::note
When storing the trace, the runner will alternate between trying to load the pipeline and loading the trace. So after the
first attempt fails, it will try to load the trace of the first attempt. If the trace pipeline also fails, the
trace file will be kept as `traces/<transaction_id>_attempt_1_trace.json` in the pipeline's
working directory and the pipeline will be retried. Failures to load the trace will be logged as warnings, but
do not affect the main pipeline's execution.
:::


### Retry pipeline steps

The runner will automatically apply [a helper method](https://github.com/dlt-hub/dlt/blob/273420b2574a518a7488443253ab1e0971b136e8/dlt/pipeline/helpers.py#L30) to all given policies to define whether or not a retry should be attempted. 

It takes into account the type of the exception and the pipeline step at which it occurred.
For example, any exceptions inheriting from `TerminalException` such as those related to missing credentials will end the run immediately,
whereas a `PipelineStepFailed`-exception such as it might occur due to a connection that timed-out could be be retried.

For the latter, the `retry_pipeline_steps` parameter can be used to further control during which pipeline steps a retry will be attempted.

| Configuration | Behavior |
|---------------|----------|
| `retry_pipeline_steps: ["load"]` | Only retry the load step (default) |
| `retry_pipeline_steps: ["normalize", "load"]` | Retry both normalize and load steps |
| `retry_pipeline_steps: ["extract", "normalize", "load"]` | Retry all main steps |

**Example configuration:**
This is how you can configure the runner to retry 5 times unless the error is terminal or occurs during the extract step:

<Tabs
  groupId="config-type"
  defaultValue="yml"
  values={[
    {"label": "dlt.yml", "value": "yml"},
    {"label": "Python", "value": "python"}
  ]}>

<TabItem value="yml">

```yaml
pipelines:
  my_pipeline:
    source: github_source
    run_config:
      retry_policy:
        type: fixed
        max_attempts: 5
      retry_pipeline_steps: ["normalize", "load"]
```

</TabItem>

<TabItem value="python">

```py
import dlt
from tenacity import Retrying, stop_after_attempt, wait_fixed

pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")

@dlt.resource(table_name="my_table")
def my_resource():
    return [1, 2, 3]

load_info = dlt.hub.runner(
  pipeline,
  retry_policy=Retrying(stop=stop_after_attempt(5), wait=wait_fixed(2), reraise=True),
  retry_pipeline_steps=["normalize", "load"]
).run(my_resource())
print(load_info)
```

</TabItem>

</Tabs>

## Callbacks

You can implement [custom callbacks](observability.md) by inheriting from dltHub's `PlusLogCollector` class.
If such a collector is attached to a pipeline, the runner will detect it and will call its `on_before`, `on_after` and `on_retry` methods.
