---
title: "Pipeline Runner"
description: Run pipelines with the dlt+ Runner
keywords: ["runner", "pipeline", "retry", "trace"]
---
# Runner

dlt+ provides a production-ready runner for your pipelines. It offers robust error handling, retry mechanisms, and near atomic trace storage to destinations of your choice.

## Key features

- **Trace storage** to a configurable destination for debugging and monitoring pipeline executions
- **Automatic retry policies** with configurable backoff strategies
- **Clean state management** to ensure consistent pipeline runs

## Usage

The runner will be used automatically if you do `dlt pipeline run` inside a project, or you can use
`dlt_plus.runner()` directly in your code where you define your pipeline and data.

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
import dlt_plus
pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")

@dlt.resource(table_name="my_table")
def my_resource():
    return [1, 2, 3]

load_info = dlt_plus.runner(pipeline).run(my_resource())
print(load_info)
```

</TabItem>

</Tabs>

## Configuration

The runner is configured through the `run_config` section of a pipeline in your `dlt.yml` file.
For direct access, you can also import and use the runner directly via the [Python interface](./pipeline-runner.md#python-api) in your code.
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
import dlt_plus
from tenacity import Retrying, stop_after_attempt, wait_exponential

pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")

@dlt.resource(table_name="my_table")
def my_resource():
    return [1, 2, 3]

load_info = dlt_plus.runner(
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

The `store_trace_info` parameter enables automatic storage of the pipeline's runtime [trace](https://github.com/dlt-hub/dlt/blob/273420b2574a518a7488443253ab1e0971b136e8/dlt/pipeline/trace.py#L126), which contains detailed information about a run, e.g., timings of each step, schema changes, and exceptions (see [here](../../running-in-production/running#inspect-and-save-the-load-info-and-trace)).

The runner will convert the trace into a `dict` and try loading it to the destination using a separate pipeline, which runs directly after each successful or failed attempt of the main pipeline. If any pending data is finalized before running the main pipeline, the trace of that finalization is also stored.

Setting `store_trace_info=True` will derive the trace pipeline writing configuration from the main pipeline.
That trace pipeline will be named `_trace_<pipeline_name>` and will write to the same destination as the main pipeline.

```yaml
pipelines:
  my_pipeline:
    run_config:
      store_trace_info: true
```

Alternatively, you can explicitly define a trace pipeline in your `dlt.yml`, for example, if you want 
to use a different destination to separate production data from traces:

```yaml
destination:
  log_filesystem:
    type: filesystem
    bucket_url: "file:///logs/dlt_traces"

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
The dlt+ runner behaves slightly differently from `pipeline.run()` when there exists pending data in the pipeline's working directory: In that case, `pipeline.run()` will load only the pending data and needs to be invoked, whereas the dlt+ runner will run with the pending data, and then automatically run again with the given data.
:::

## Retry policies

The Runner supports configurable retry policies to handle errors during pipeline execution. Retry policies apply to:

- Finalizing pending data from previous loads
- Running the pipeline with the given data

:::note
The runner will alternate between trying to load the pipeline and loading the trace. So after the
first attempt fails, it will try to load the trace of the first attempt. If that also fails, the
trace file will be kept as `traces/<transaction_id>_attempt_1_trace.json` in the pipeline's
working directory and the pipeline will be retried. Failures to load the trace will be logged, but
do not affect the main pipeline's execution.
:::

### Available retry policies

| Policy type | Description | Configuration |
|-------------|-------------|---------------|
| None | No retry (single attempt) | `type: none` |
| Fixed | Fixed number of attempts with no backoff | `type: fixed`, `max_attempts: N` |
| Backoff | Exponential backoff with configurable parameters | `type: backoff`, `max_attempts: N`, `multiplier: N`, `min: N`, `max: N` |


### Retry pipeline steps

As a general rule, the runner will not retry on terminal exceptions, such as errors related to 
missing credentials or configurations. For other exceptions, it will retry if the error occurred during the specified pipeline phase, which is controlled by the `retry_pipeline_steps` parameter.

**Configuration examples:**

| Configuration | Behavior |
|---------------|----------|
| `retry_pipeline_steps: ["load"]` | Only retry the load step (default) |
| `retry_pipeline_steps: ["normalize", "load"]` | Retry both normalize and load steps |
| `retry_pipeline_steps: ["extract", "normalize", "load"]` | Retry all main steps |

**Complete example:**
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

## Python API

You can also use the runner directly in your Python code:

```py
import dlt
import dlt_plus
from tenacity import Retrying, stop_after_attempt

@dlt.resource(table_name="numbers")
def my_resource():
    return [1, 2, 3]

pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination="duckdb",
    dataset_name="my_dataset",
)
# Define a trace pipeline writing to a remote filesystem
# os.environ["BUCKET_URL"] = "s3://...."
trace_pipeline = dlt.pipeline(
    pipeline_name="my_trace_pipeline",
    destination="filesystem",
    dataset_name="my_pipeline_trace_dataset",
)

load_info = dlt_plus.runner(
  pipeline,
  store_trace_info=trace_pipeline,
  run_from_clean_folder=False,
  retry_policy=Retrying(stop=stop_after_attempt(5), reraise=True),
  retry_pipeline_steps=["extract", "normalize", "load"]
).run(my_resource(), write_disposition="append")
print(load_info)

# Or just to finalize pending data reliably
load_info = dlt_plus.runner(pipeline, store_trace_info=True).finalize()
print(load_info)
```

