---
title: "Dlt+ Runner"
description: Run pipelines with the Dlt+ Runner
keywords: ["runner", "pipeline", "retry", "trace"]
---
# Runner


The dlt+ Runner provides a production-ready run command for your pipelines. It offers robust error handling, retry mechanisms, and 
near atomic trace storage to destinations of your choice.
The runner can be configured for each pipeline via your `dlt.yml`, or you can use it directly
in your python code.

## Key features

- **Trace storage** to a configurable destination for debugging and monitoring pipeline executions
- **Automatic retry policies** with configurable backoff strategies
- **Clean state management** to ensure consistent pipeline runs

## Configuration

The Runner is configured through the `run_config` section in your `dlt.yml` file.

### Complete configuration example

```yaml
pipelines:
  my_pipeline:
    source: my_source
    destination: duckdb
    run_config:
      run_from_clean_folder: true
      store_trace_info: true
      retry_policy:
        type: backoff
        max_attempts: 5
        multiplier: 2
        min: 1
        max: 10
      retry_pipeline_steps: ["load"]
```

## Run from clean folder

The `run_from_clean_folder` parameter controls whether the pipeline starts from a clean state before running.
When enabled, the pipeline's working directory which holds the state, schema and any pending data are deleted 
and state and schema are synchronized from the destination.

```yaml
pipelines:
  my_pipeline:
    run_config:
      run_from_clean_folder: true
```

:::note
The dlt+ runner behaves differently from `pipeline.run()` when there exists pending data in the pipeline's working directory:
`pipeline.run()` will load only the pending data instead and needs to be invoked again for the given data.
The dlt+ runner will also try finalizing the pending data, applying the retry policy and the trace settings, and then also
try running with the given data.
:::


## Trace storage

The `store_trace_info` parameter enables automatic storage of the pipelines trace, which can be used for for debugging, auditing, and monitoring pipeline runs.

### Trace pipeline configuration

Traces are loaded using a separate pipeline, which runs directly after each successful or failed attempt of the main pipeline.
If any pending data is finalized before running the main pipeline, the trace of that finalization is also stored.

```yaml
pipelines:
  my_pipeline:
    run_config:
      store_trace_info: true
```
Setting `store_trace_info: true` will derive the trace pipeline writing configuration from the main pipeline.
That trace pipeline will be named `_trace_<pipeline_name>` and write to the same destination as the main pipeline.

Alternatively, you can explicitly define a trace pipeline in your `dlt.yml`, e.g. if you want 
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
    source: my_source # << this will not actually be used but can not be empty
    destination: log_filesystem
```

### Trace table naming

Traces are loaded into a table named `<pipeline_name>_trace`. This means you can use the same trace pipeline for 
multiple pipelines without conflicts.

## Retry policies

The Runner supports configurable retry policies to handle errors during pipeline execution. Retry policies apply to:

- Finalizing pending data from previous loads
- Running the pipeline with the given data

:::note
The runner will alternate between trying to load the pipeline and loading the trace. So after the
first attempt failed, it will try to load the trace of the first attempt. If that also fails, the
trace file will be kept as `traces/<transaction_id>_attempt_1_trace.json` in the pipelines
working directory and the pipeline will be retried. Failures to load the trace will logged, but
do not affect the main pipelines execution.
:::

### Available retry policies

| Policy type | Description | Configuration |
|-------------|-------------|---------------|
| None | No retry (single attempt) | `type: none` |
| Fixed | Fixed number of attempts with no backoff | `type: fixed`, `max_attempts: N` |
| Backoff | Exponential backoff with configurable parameters | `type: backoff`, `max_attempts: N`, `multiplier: N`, `min: N`, `max: N` |


### Retry pipeline steps

As a general rule, the runner will not retry on terminal exceptions, such as errors related to 
missing credentials or configurations. For other exceptions, it will retry if the error occured, if it 
occured during the specified pipeline phase, which is controlled by the `retry_pipeline_steps` parameter.

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

You can also use the runner directly in your python code, e.g. to finalize pending data or to run pipelines.

```py
import dlt
from dlt_plus
from tenacity import Retrying, stop_after_attempt

@dlt.resource(table_name="numbers")
def my_resource():
    return [1, 2, 3]

pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination="duckdb",
    dataset_name="my_dataset",
)

load_info = dlt_plus.runner(pipeline, run_from_clean_folder=True).run(my_resource(), write_disposition="append")
print(load_info)

# or just to finalize pending data reliably
pipeline.extract(["a", "b", "c"], table_name="letters")
load_info = dlt_plus.runner(
    pipeline, 
    retry_policy=Retrying(stop=stop_after_attempt(2), reraise=True),
    store_trace_info=True,
).finalize()
print(load_info)
```

